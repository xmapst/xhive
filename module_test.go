package xhive

import (
    "context"
    "errors"
    "sync/atomic"
    "syscall"
    "testing"
    "time"
    
    "github.com/xmapst/xhive/chanrpc"
    "github.com/xmapst/xhive/timer"
)

type testModule struct {
    name        string
    server      *chanrpc.Server
    initErr     error
    runDelay    time.Duration
    onInitFn    func() error
    onRunFn     func(context.Context)
    onDestroyFn func()
    
    initCount    atomic.Int32
    runCount     atomic.Int32
    destroyCount atomic.Int32
    runCh        chan struct{}
    destroyCh    chan struct{}
}

func newTestModule(name string) *testModule {
    return &testModule{
        name:      name,
        server:    chanrpc.NewServer(8),
        runCh:     make(chan struct{}, 1),
        destroyCh: make(chan struct{}, 1),
    }
}

func (m *testModule) Name() string { return m.name }

func (m *testModule) OnInit() error {
    m.initCount.Add(1)
    if m.onInitFn != nil {
        return m.onInitFn()
    }
    return m.initErr
}

func (m *testModule) OnRun(ctx context.Context) {
    m.runCount.Add(1)
    select {
    case m.runCh <- struct{}{}:
    default:
    }
    if m.onRunFn != nil {
        m.onRunFn(ctx)
        return
    }
    if m.runDelay > 0 {
        timer := time.NewTimer(m.runDelay)
        defer timer.Stop()
        select {
        case <-ctx.Done():
        case <-timer.C:
        }
        return
    }
    <-ctx.Done()
}

func (m *testModule) OnDestroy() {
    m.destroyCount.Add(1)
    if m.onDestroyFn != nil {
        m.onDestroyFn()
    }
    select {
    case m.destroyCh <- struct{}{}:
    default:
    }
}

func (m *testModule) ChanRPC() *chanrpc.Server { return m.server }

func waitSignal(t *testing.T, ch <-chan struct{}, name string) {
    t.Helper()
    select {
    case <-ch:
    case <-time.After(2 * time.Second):
        t.Fatalf("wait %s timeout", name)
    }
}

func TestAppRegisterAndStartStopLifecycle(t *testing.T) {
    a := newApp()
    m1 := newTestModule("m1")
    m2 := newTestModule("m2")
    
    if err := a.Register(m1, m2); err != nil {
        t.Fatalf("Register() error = %v", err)
    }
    if got := len(a.modules); got != 2 {
        t.Fatalf("len(modules) = %d, want 2", got)
    }
    
    if !a.start() {
        t.Fatal("start() = false, want true")
    }
    if got := a.State(); got != AppStateRun {
        t.Fatalf("State() = %d, want %d", got, AppStateRun)
    }
    waitSignal(t, m1.runCh, "m1 run")
    waitSignal(t, m2.runCh, "m2 run")
    
    a.stop()
    if got := a.State(); got != AppStateNone {
        t.Fatalf("State() after stop = %d, want %d", got, AppStateNone)
    }
    waitSignal(t, m1.destroyCh, "m1 destroy")
    waitSignal(t, m2.destroyCh, "m2 destroy")
    if m1.destroyCount.Load() != 1 || m2.destroyCount.Load() != 1 {
        t.Fatalf("destroy count = (%d,%d), want (1,1)", m1.destroyCount.Load(), m2.destroyCount.Load())
    }
}

func TestAppRegisterRejectsRunningState(t *testing.T) {
    a := newApp()
    a.setState(AppStateRun)
    
    err := a.Register(newTestModule("m1"))
    if err == nil {
        t.Fatal("Register() error = nil, want non-nil")
    }
}

func TestAppStartFailsWithNoModules(t *testing.T) {
    a := newApp()
    if a.start() {
        t.Fatal("start() = true, want false")
    }
    if got := a.State(); got != AppStateNone {
        t.Fatalf("State() = %d, want %d", got, AppStateNone)
    }
}

func TestAppStartFailsWhenInitError(t *testing.T) {
    a := newApp()
    boom := errors.New("boom")
    m1 := newTestModule("m1")
    m2 := newTestModule("m2")
    m2.initErr = boom
    
    if err := a.Register(m1, m2); err != nil {
        t.Fatalf("Register() error = %v", err)
    }
    if a.start() {
        t.Fatal("start() = true, want false")
    }
    if got := a.State(); got != AppStateInit {
        t.Fatalf("State() = %d, want %d", got, AppStateInit)
    }
    if m1.runCount.Load() != 0 || m2.runCount.Load() != 0 {
        t.Fatalf("run count = (%d,%d), want (0,0)", m1.runCount.Load(), m2.runCount.Load())
    }
}

func TestAppStatsAndChanRPC(t *testing.T) {
    a := newApp()
    staticMod := newTestModule("static")
    if err := a.Register(staticMod); err != nil {
        t.Fatalf("Register() error = %v", err)
    }
    ci := &chanrpc.CallInfo{Request: "req"}
    staticMod.server.ChanCall <- ci
    
    dynamicMod := newTestModule("dynamic")
    a.dynamicModules.Store(dynamicMod.Name(), &moduleWrapper{IModule: dynamicMod})
    dynamicMod.server.ChanCall <- &chanrpc.CallInfo{Request: "req2"}
    
    stats := a.Stats()
    if stats == "" {
        t.Fatal("Stats() = empty, want non-empty")
    }
    if a.ChanRPC("static") != staticMod.server {
        t.Fatal("ChanRPC(static) mismatch")
    }
    if a.ChanRPC("dynamic") != dynamicMod.server {
        t.Fatal("ChanRPC(dynamic) mismatch")
    }
    if a.ChanRPC("missing") != nil {
        t.Fatal("ChanRPC(missing) != nil")
    }
}

func TestAppDynamicModuleLifecycle(t *testing.T) {
    a := newApp()
    m := newTestModule("dyn")
    
    if err := a.AddDynamicModules(m); err != nil {
        t.Fatalf("AddDynamicModules() error = %v", err)
    }
    waitSignal(t, m.runCh, "dynamic run")
    if got := len(a.DynamicModules()); got != 1 {
        t.Fatalf("DynamicModules len = %d, want 1", got)
    }
    
    if !a.RemoveDynamicModule("dyn") {
        t.Fatal("RemoveDynamicModule() = false, want true")
    }
    waitSignal(t, m.destroyCh, "dynamic destroy")
    if a.RemoveDynamicModule("dyn") {
        t.Fatal("RemoveDynamicModule() second call = true, want false")
    }
}

func TestAppAddDynamicModulesInitError(t *testing.T) {
    a := newApp()
    m := newTestModule("dyn")
    m.initErr = errors.New("init failed")
    
    err := a.AddDynamicModules(m)
    if err == nil {
        t.Fatal("AddDynamicModules() error = nil, want non-nil")
    }
    if got := len(a.DynamicModules()); got != 0 {
        t.Fatalf("DynamicModules len = %d, want 0", got)
    }
}

func TestAppDestroyModuleRecover(t *testing.T) {
    a := newApp()
    m := newTestModule("panic-destroy")
    m.onDestroyFn = func() { panic("destroy panic") }
    
    a.destroyModule(&moduleWrapper{IModule: m})
    if got := m.destroyCount.Load(); got != 1 {
        t.Fatalf("destroyCount = %d, want 1", got)
    }
}

func TestSignalManagerRegisterAndReservedSignals(t *testing.T) {
    sm := NewSignalManager()
    trap := func() {}
    if err := sm.Register(trap, syscall.SIGHUP); err != nil {
        t.Fatalf("Register(SIGHUP) error = %v", err)
    }
    if got := len(sm.signals[syscall.SIGHUP]); got != 1 {
        t.Fatalf("len(signals[SIGHUP]) = %d, want 1", got)
    }
    if err := sm.Register(trap, syscall.SIGTERM); err == nil {
        t.Fatal("Register(SIGTERM) error = nil, want non-nil")
    }
}

func TestSignalManagerStartStopAndTrapExecution(t *testing.T) {
    sm := NewSignalManager()
    var stopCount atomic.Int32
    var trapCount atomic.Int32
    
    if err := sm.Register(func() {
        trapCount.Add(1)
    }, syscall.SIGHUP); err != nil {
        t.Fatalf("Register() error = %v", err)
    }
    
    sm.Start(func() {
        stopCount.Add(1)
    })
    if sm.sigCh == nil {
        t.Fatal("sigCh = nil, want initialized channel")
    }
    if len(sm.signals[syscall.SIGTERM]) != 1 {
        t.Fatalf("len(signals[SIGTERM]) = %d, want 1", len(sm.signals[syscall.SIGTERM]))
    }
    if len(sm.signals[syscall.SIGHUP]) == 0 {
        t.Fatal("signals[SIGHUP] should not be empty after Start")
    }
    
    for _, trap := range sm.signals[syscall.SIGHUP] {
        trap()
    }
    for _, trap := range sm.signals[syscall.SIGTERM] {
        trap()
    }
    
    if stopCount.Load() != 1 {
        t.Fatalf("stopCount = %d, want 1", stopCount.Load())
    }
    if trapCount.Load() != 1 {
        t.Fatalf("trapCount = %d, want 1", trapCount.Load())
    }
    
    sm.Stop()
    if sm.sigCh != nil {
        t.Fatal("sigCh != nil after Stop")
    }
    sm.Stop()
}
func TestSkeletonBasicHelpers(t *testing.T) {
    s := NewSkeleton("sk")
    if s.Name() != "sk" {
        t.Fatalf("Name() = %q, want %q", s.Name(), "sk")
    }
    if s.ChanRPC() == nil {
        t.Fatal("ChanRPC() = nil")
    }
    if ts := s.dayStartTs(time.Date(2026, 6, 10, 12, 34, 56, 0, time.Local).UnixMilli()); ts > time.Now().Add(24*time.Hour).UnixMilli() {
        t.Fatalf("dayStartTs() unexpected value %d", ts)
    }
    
    s.recordStat("rpc", 10)
    if dump := s.DumpStat(10); dump == "" {
        t.Fatal("DumpStat() = empty")
    }
    s.dumpStat(true)
    if dump := s.DumpStat(10); dump == "" {
        t.Fatal("DumpStat() after reset should still contain global payload")
    }
}

func TestSkeletonTimerOperations(t *testing.T) {
	s := NewSkeleton("timer")
	triggered := make(chan map[string]string, 1)
	s.RegisterTimer("once", func(_ int64, metadata map[string]string) {
		triggered <- metadata
	})
	s.timer.Run()
	defer s.timer.Stop()

	id := s.NewTimer("once", 20, chanrpcOptionAdapter()...)
	if id == 0 {
		t.Fatal("NewTimer() returned 0")
	}
	if s.timer.Find(id) == nil {
		t.Fatal("Find() = nil")
	}

	select {
	case ev := <-s.timer.Event():
		ev.Cb()
	case <-time.After(2 * time.Second):
		t.Fatal("timer event timeout")
	}
	select {
	case <-triggered:
	case <-time.After(2 * time.Second):
		t.Fatal("timer handler timeout")
	}
}
func chanrpcOptionAdapter() []timer.Option {
    return []timer.Option{timer.WithMetadata(map[string]string{"k": "v"})}
}
