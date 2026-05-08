package xhive

import (
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/xmapst/xhive/chanrpc"
	"github.com/xmapst/xhive/timer"
)

// testModule 是用于 app/skeleton/signal 集成测试的最小 IModule 实现。
// 通过原子计数和 channel 显式观测 OnInit/OnRun/OnDestroy 的调用顺序，避免测试依赖 sleep 推断生命周期。
type testModule struct {
	name        string
	server      *chanrpc.Server
	initErr     error
	initCount   atomic.Int32
	runCount    atomic.Int32
	destroyCnt  atomic.Int32
	runStarted  chan struct{}
	runStopped  chan struct{}
	destroyHook func()
}

func newTestModule(name string) *testModule {
	return &testModule{
		name:       name,
		server:     chanrpc.NewServer(4),
		runStarted: make(chan struct{}),
		runStopped: make(chan struct{}),
	}
}

func (m *testModule) Name() string { return m.name }

func (m *testModule) OnInit() error {
	m.initCount.Add(1)
	return m.initErr
}

func (m *testModule) Serve(ctx context.Context) {
	m.runCount.Add(1)
	close(m.runStarted)
	<-ctx.Done()
	close(m.runStopped)
}

func (m *testModule) OnDestroy() {
	m.destroyCnt.Add(1)
	if m.destroyHook != nil {
		m.destroyHook()
	}
	if m.server != nil && !m.server.IsClosed() {
		m.server.Close()
	}
}

func (m *testModule) ChanRPC() *chanrpc.Server { return m.server }

func waitClosed(t *testing.T, ch <-chan struct{}, timeout time.Duration) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatal("timeout waiting channel close")
	}
}

// TestAppRegisterStartStopStatsAndChanRPC 覆盖静态模块完整生命周期：注册、启动、状态切换、ChanRPC 查询、
// Stats 输出以及停止时 OnDestroy/OnRun 退出是否都被执行。
func TestAppRegisterStartStopStatsAndChanRPC(t *testing.T) {
	a := newApp()
	m1 := newTestModule("m1")
	m2 := newTestModule("m2")
	if err := a.Register(m1); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if !a.start(m2) {
		t.Fatal("start should succeed")
	}
	waitClosed(t, m1.runStarted, time.Second)
	waitClosed(t, m2.runStarted, time.Second)
	if a.State() != AppStateRun {
		t.Fatalf("state = %d, want AppStateRun", a.State())
	}
	if got := a.ChanRPC("m1"); got != m1.server {
		t.Fatal("ChanRPC did not return static module server")
	}
	stats := a.Stats()
	if !strings.Contains(stats, "static: m1") || !strings.Contains(stats, "static: m2") {
		t.Fatalf("unexpected stats: %s", stats)
	}
	if err := a.Register(newTestModule("late")); err == nil {
		t.Fatal("Register while running should fail")
	}

	a.stop()
	waitClosed(t, m1.runStopped, time.Second)
	waitClosed(t, m2.runStopped, time.Second)
	if a.State() != AppStateNone {
		t.Fatalf("state after stop = %d, want AppStateNone", a.State())
	}
	if m1.destroyCnt.Load() != 1 || m2.destroyCnt.Load() != 1 {
		t.Fatalf("destroy counts: m1=%d m2=%d", m1.destroyCnt.Load(), m2.destroyCnt.Load())
	}
}

func TestAppStartValidationAndInitFailure(t *testing.T) {
	a := newApp()
	if a.start() {
		t.Fatal("start without modules should fail")
	}
	if a.State() != AppStateNone {
		t.Fatalf("state after empty start = %d, want none", a.State())
	}

	bad := newTestModule("bad")
	bad.initErr = errors.New("init failed")
	if a.start(bad) {
		t.Fatal("start with init error should fail")
	}
	if bad.runCount.Load() != 0 {
		t.Fatal("failed init module should not run")
	}
}

// TestAppDynamicModulesLifecycle 覆盖动态模块热加载/卸载路径：模块应在 AddDynamicModules 后立即运行，
// 在 RemoveDynamicModule 返回前完成 OnDestroy、context cancel 和 goroutine 退出。
func TestAppDynamicModulesLifecycle(t *testing.T) {
	a := newApp()
	dyn := newTestModule("dyn")
	if _, err := a.AddDynamicModules(nil, dyn); err != nil {
		t.Fatalf("AddDynamicModules failed: %v", err)
	}
	waitClosed(t, dyn.runStarted, time.Second)
	if got := a.ChanRPC("dyn"); got != dyn.server {
		t.Fatal("ChanRPC did not return dynamic server")
	}
	names := a.DynamicModules()
	if !slices.Contains(names, "dyn") {
		t.Fatalf("DynamicModules = %v, want dyn", names)
	}
	if !strings.Contains(a.Stats(), "dynamic: dyn") {
		t.Fatalf("Stats missing dynamic module: %s", a.Stats())
	}
	if !a.RemoveDynamicModule("dyn") {
		t.Fatal("RemoveDynamicModule should return true")
	}
	waitClosed(t, dyn.runStopped, time.Second)
	if a.RemoveDynamicModule("dyn") {
		t.Fatal("RemoveDynamicModule on missing should return false")
	}
}

func TestAppDynamicInitFailureDoesNotStoreModule(t *testing.T) {
	a := newApp()
	bad := newTestModule("bad-dyn")
	bad.initErr = errors.New("boom")
	if _, err := a.AddDynamicModules(bad); err == nil {
		t.Fatal("AddDynamicModules should fail")
	}
	if a.ChanRPC("bad-dyn") != nil {
		t.Fatal("failed dynamic module should not be stored")
	}
}

func TestAppDestroyModuleRecoversPanic(t *testing.T) {
	a := newApp()
	m := newTestModule("panic-destroy")
	m.destroyHook = func() { panic("destroy panic") }
	a.destroyModule(&moduleWrapper{IModule: m})
	if m.destroyCnt.Load() != 1 {
		t.Fatalf("destroy count = %d, want 1", m.destroyCnt.Load())
	}
}

func TestSignalManagerRegisterStartStopAndPanicRecovery(t *testing.T) {
	sm := NewSignalManager()
	if err := sm.Register(func() {}, syscall.SIGTERM); err == nil {
		t.Fatal("register reserved signal should fail")
	}

	called := make(chan struct{}, 1)
	if err := sm.Register(func() { called <- struct{}{} }, syscall.SIGHUP); err != nil {
		t.Fatalf("Register SIGHUP failed: %v", err)
	}
	stopCalled := make(chan struct{}, 1)
	sm.Start(func() { stopCalled <- struct{}{} })
	defer sm.Stop()
	if sm.sigCh == nil {
		t.Fatal("sigCh should be initialized")
	}
	if err := sm.Register(func() { panic("trap panic") }, os.Interrupt); err == nil {
		t.Fatal("register os.Interrupt after Start should still reject reserved signal")
	}

	sm.sigCh <- syscall.SIGHUP
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting sighup trap")
	}

	sm.sigCh <- syscall.SIGTERM
	select {
	case <-stopCalled:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting stop trap")
	}
}

func TestSignalManagerRegisterAfterStartAddsSignal(t *testing.T) {
	sm := NewSignalManager()
	sm.Start(func() {})
	defer sm.Stop()
	called := make(chan struct{}, 1)
	if err := sm.Register(func() { called <- struct{}{} }, syscall.SIGHUP); err != nil {
		t.Fatalf("Register after Start failed: %v", err)
	}
	sm.sigCh <- syscall.SIGHUP
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting SIGHUP trap")
	}
}

func TestSkeletonOptionsWrappersAndEventLoop(t *testing.T) {
	s := NewSkeleton("sk", WithTimerChanLen(2), WithServerChanLen(3), WithClientChanLen(4), WithStatCap(16))
	if s.Name() != "sk" {
		t.Fatalf("Name = %s", s.Name())
	}
	if s.ChanRPC() == nil {
		t.Fatal("ChanRPC should not be nil")
	}
	if s.startOfDay(time.Date(2026, 7, 7, 12, 1, 2, 0, time.UTC)).Hour() != 0 {
		t.Fatal("startOfDay should return midnight")
	}

	timerCalled := make(chan map[string]string, 1)
	s.RegisterTimer("short", func(_ int64, md map[string]string) { timerCalled <- md })
	id := s.NewTimer("short", 10*time.Millisecond, timer.WithMetadata(map[string]string{"x": "y"}))
	if id == 0 {
		t.Fatal("NewTimer returned zero")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Serve(ctx)
		close(done)
	}()

	select {
	case md := <-timerCalled:
		if md["x"] != "y" {
			t.Fatalf("timer metadata = %#v", md)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting skeleton timer")
	}

	cancel()
	waitClosed(t, done, time.Second)
}
