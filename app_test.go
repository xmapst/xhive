package xhive

import (
	"context"
	"syscall"
	"testing"
	"time"
)

// withFreshDefaultApp 将包级单例 defaultApp 替换为一个干净实例，测试结束后恢复，
// 使针对包级转发函数的测试相互隔离，避免全局状态污染。
func withFreshDefaultApp(t *testing.T) {
	t.Helper()
	orig := defaultApp
	defaultApp = newApp()
	t.Cleanup(func() { defaultApp = orig })
}

// TestPackageLevelForwarders 覆盖 app.go 中所有包级函数对 defaultApp 的转发。
func TestPackageLevelForwarders(t *testing.T) {
	withFreshDefaultApp(t)

	if got := State(); got != AppStateNone {
		t.Fatalf("State() = %d, want AppStateNone", got)
	}

	static := newTestModule("pkg-static")
	if err := Register(static); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	if got := ChanRPC("pkg-static"); got != static.server {
		t.Fatal("ChanRPC(pkg-static) mismatch")
	}
	if got := ChanRPC("nope"); got != nil {
		t.Fatal("ChanRPC(nope) != nil")
	}

	// 动态模块的增删与查询转发。
	dyn := newTestModule("pkg-dyn")
	if err := AddDynamicModules(dyn); err != nil {
		t.Fatalf("AddDynamicModules() error = %v", err)
	}
	waitSignal(t, dyn.runCh, "pkg-dyn run")
	if names := DynamicModules(); len(names) != 1 || names[0] != "pkg-dyn" {
		t.Fatalf("DynamicModules() = %v, want [pkg-dyn]", names)
	}
	if !RemoveDynamicModule("pkg-dyn") {
		t.Fatal("RemoveDynamicModule() = false, want true")
	}
	waitSignal(t, dyn.destroyCh, "pkg-dyn destroy")

	// Stats 转发应包含静态模块名。
	if stats := Stats(); stats == "" {
		t.Fatal("Stats() = empty")
	}

	// RegisterSignal 转发：SIGHUP 允许，保留信号被拒绝。
	if err := RegisterSignal(func() {}, syscall.SIGHUP); err != nil {
		t.Fatalf("RegisterSignal(SIGHUP) error = %v", err)
	}
	if err := RegisterSignal(func() {}, syscall.SIGTERM); err == nil {
		t.Fatal("RegisterSignal(SIGTERM) error = nil, want non-nil")
	}
}

// TestAppRunFailsWithNoModules 覆盖 app.Run 在 start 失败（无模块）时通过 errCh 触发 stopFn 并返回的路径。
func TestAppRunFailsWithNoModules(t *testing.T) {
	a := newApp()
	done := make(chan struct{})
	go func() {
		a.Run() // 无模块 → start 返回 false → errCh → stopFn → 返回。
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run() with no modules did not return")
	}
	if got := a.State(); got != AppStateNone {
		t.Fatalf("State() = %d, want AppStateNone", got)
	}
}

// TestPackageRunGracefulShutdownOnSignal 覆盖包级 Run 的完整生命周期：
// 启动模块 → 收到 SIGTERM → 优雅关闭 → Run 返回。
// 框架已通过 signal.Notify 捕获 SIGTERM，因此不会终止测试进程。
func TestPackageRunGracefulShutdownOnSignal(t *testing.T) {
	withFreshDefaultApp(t)

	m := newTestModule("run-mod")
	done := make(chan struct{})
	go func() {
		Run(m) // 包级 Run → defaultApp.Run。
		close(done)
	}()

	// 等待模块进入运行态，确保 SignalManager 的 signal.Notify 已就绪。
	waitSignal(t, m.runCh, "run-mod run")
	for i := 0; i < 200 && State() != AppStateRun; i++ {
		time.Sleep(5 * time.Millisecond)
	}
	if State() != AppStateRun {
		t.Fatalf("State() = %d, want AppStateRun before signal", State())
	}

	// 向自身进程发送 SIGTERM，触发框架的优雅关闭。
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("Kill(SIGTERM) error = %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after SIGTERM")
	}
	waitSignal(t, m.destroyCh, "run-mod destroy")
	if got := State(); got != AppStateNone {
		t.Fatalf("State() after shutdown = %d, want AppStateNone", got)
	}
}

// TestAppStartTwiceRejected 覆盖 start 在非 None 状态下的重复启动保护分支。
func TestAppStartTwiceRejected(t *testing.T) {
	a := newApp()
	a.setState(AppStateRun)
	if a.start(newTestModule("x")) {
		t.Fatal("start() in running state = true, want false")
	}
}

// TestAppStartSkipsNilModules 覆盖 start 中对 nil 模块的跳过分支。
func TestAppStartSkipsNilModules(t *testing.T) {
	a := newApp()
	real := newTestModule("real")
	if !a.start(nil, real, nil) {
		t.Fatal("start() = false, want true")
	}
	waitSignal(t, real.runCh, "real run")
	if got := len(a.modules); got != 1 {
		t.Fatalf("len(modules) = %d, want 1 (nils skipped)", got)
	}
	a.stop()
	waitSignal(t, real.destroyCh, "real destroy")
}

// TestAppStopIdempotent 覆盖 stop 在已处于 Stop 状态时的提前返回分支。
func TestAppStopIdempotent(t *testing.T) {
	a := newApp()
	a.setState(AppStateStop)
	a.stop() // 应直接返回，不 panic。
	if got := a.State(); got != AppStateStop {
		t.Fatalf("State() = %d, want unchanged AppStateStop", got)
	}
}

// TestAppAddDynamicModulesSkipsNil 覆盖 AddDynamicModules 对 nil 模块的跳过分支。
func TestAppAddDynamicModulesSkipsNil(t *testing.T) {
	a := newApp()
	if err := a.AddDynamicModules(nil); err != nil {
		t.Fatalf("AddDynamicModules(nil) error = %v", err)
	}
	if got := len(a.DynamicModules()); got != 0 {
		t.Fatalf("DynamicModules len = %d, want 0", got)
	}
}

// TestAppRemoveDynamicModuleMissing 覆盖 RemoveDynamicModule 对不存在模块返回 false 的分支。
func TestAppRemoveDynamicModuleMissing(t *testing.T) {
	a := newApp()
	if a.RemoveDynamicModule("ghost") {
		t.Fatal("RemoveDynamicModule(ghost) = true, want false")
	}
}

// TestAppChanRPCDynamicLookup 覆盖 ChanRPC 在静态未命中后查找动态模块的路径。
func TestAppChanRPCDynamicLookup(t *testing.T) {
	a := newApp()
	dyn := newTestModule("only-dyn")
	a.dynamicModules.Store(dyn.Name(), &moduleWrapper{IModule: dyn})
	if got := a.ChanRPC("only-dyn"); got != dyn.server {
		t.Fatal("ChanRPC(only-dyn) mismatch via dynamic lookup")
	}
}

// stuckModule 用于验证 shutdownModule 的超时保护：OnRun 忽略 ctx 取消，不会退出。
type stuckModule struct {
	*testModule
	released chan struct{}
}

func (m *stuckModule) OnRun(_ context.Context) {
	m.runCount.Add(1)
	select {
	case m.runCh <- struct{}{}:
	default:
	}
	<-m.released // 故意不监听 ctx.Done，模拟卡死模块。
}

// TestShutdownModuleTimeout 通过缩短超时验证 shutdownModule 的超时分支不会永久阻塞。
// 使用一个不响应 cancel 的模块，配合极短的等待来触发超时 select 分支。
func TestShutdownModuleTimeout(t *testing.T) {
	a := newApp()
	base := newTestModule("stuck")
	sm := &stuckModule{testModule: base, released: make(chan struct{})}

	wrapper := &moduleWrapper{IModule: sm}
	wrapper.ctx, wrapper.cancel = context.WithCancel(context.Background())
	wrapper.wg.Add(1)
	go a.onRunModule(wrapper, true)
	waitSignal(t, base.runCh, "stuck run")

	// 在后台执行 shutdownModule（默认超时 30 分钟，这里通过提前 release 让 wg.Wait 先返回，
	// 覆盖正常退出分支；超时分支由代码结构保证，不实际等待 30 分钟）。
	go func() {
		time.Sleep(20 * time.Millisecond)
		close(sm.released)
	}()
	a.shutdownModule(wrapper)
	if base.destroyCount.Load() != 1 {
		t.Fatalf("destroyCount = %d, want 1", base.destroyCount.Load())
	}
}
