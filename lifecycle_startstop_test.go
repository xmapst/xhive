package xhive

import (
	"context"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/xmapst/xhive/chanrpc"
)

// ssModule 是本文件专用的最小 IModule 实现，用于观测 start / stop 交接。
//
// 不复用 app_test.go 的 testModule：那里的 OnDestroy 会顺手关闭 server、
// 且被多个用例共享，本组用例需要的是"每个钩子被调用了几次"这种纯记账语义，
// 单独定义一份可以避免任何一方的行为调整影响到另一方。
//
// 所有观测点都是原子量或 channel，不依赖 sleep 推断时序。
type ssModule struct {
	name     string
	priority uint
	server   *chanrpc.Server

	initCount  atomic.Int32
	serveCount atomic.Int32
	destroyCnt atomic.Int32
	closeCnt   atomic.Int32

	// ready 在 Serve 进入等待前关闭，等价于 Skeleton.ready 的语义；
	// stopped 在 Serve 返回前关闭，用于确认事件循环真的退出了。
	ready   chan struct{}
	stopped chan struct{}

	// initHook 让用例把 OnInit 卡在任意时刻，从而精确构造
	// "start 仍在初始化、stop 同时到达"这个窗口。
	initHook func()
}

func newSSModule(name string) *ssModule {
	return &ssModule{
		name:    name,
		server:  chanrpc.NewServer(chanrpc.WithChanLen(4)),
		ready:   make(chan struct{}),
		stopped: make(chan struct{}),
	}
}

func (m *ssModule) Name() string             { return m.name }
func (m *ssModule) Priority() uint           { return m.priority }
func (m *ssModule) ChanRPC() *chanrpc.Server { return m.server }

func (m *ssModule) OnInit() error {
	m.initCount.Add(1)
	if m.initHook != nil {
		m.initHook()
	}
	return nil
}

func (m *ssModule) Serve(ctx context.Context) {
	m.serveCount.Add(1)
	close(m.ready)
	<-ctx.Done()
	close(m.stopped)
}

func (m *ssModule) Ready() <-chan struct{} { return m.ready }

func (m *ssModule) OnDestroy() { m.destroyCnt.Add(1) }

func (m *ssModule) Close() error {
	m.closeCnt.Add(1)
	return nil
}

// waitAppState 等待应用进入指定状态。
//
// start 把状态置为 AppStateRun 是它返回前的最后一步，调用方在包外没有可以
// select 的事件，只能观察 State()。这里等待的是"条件成立"而不是"固定时长"，
// 超时只是失败上限，因此机器快慢不会影响结论。
func waitAppState(t *testing.T, a *app, want int32, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if a.State() == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting app state %d, got %d", want, a.State())
		}
		time.Sleep(time.Millisecond)
	}
}

// sendFrameworkStopSignal 向应用自己的信号分发器投递一次 SIGTERM。
//
// Run 只有两条返回路径：start 失败，或者 stopFn 被触发后 close(stopped)。
// 正常停机走的是后者，而 stopFn 唯一的触发入口就是 SignalManager，
// 因此这是让一个健康运行中的 Run 正常返回的唯一途径。
// sigCh 为 nil 说明分发器已经被 Stop 过——对本组用例而言这本身就是错误，
// 意味着有人在应用还活着的时候把它的停机通道拆掉了。
func sendFrameworkStopSignal(t *testing.T, a *app) {
	t.Helper()
	a.sm.RLock()
	ch := a.sm.sigCh
	a.sm.RUnlock()
	if ch == nil {
		t.Fatal("signal dispatcher already stopped, the running Run can no longer receive a shutdown signal")
	}
	ch <- syscall.SIGTERM
}

// TestRunRejectsReentrantCall 锁住 Run 的并发/重入闸门（runInFlight）。
//
// 为什么重要：start 返回 false 有多种原因，其中"应用已经启动"是一种
// **不该触发关闭**的失败——它什么都没改，只是拒绝了这次启动。但 Run 的
// select 看到 errCh 就会无条件调 stopFn，于是第二次 Run 会把第一个 Run
// 正在管理的、健康运行中的应用连同全部模块一起销毁掉；更糟的是关闭由
// 第二个 Run 的 stopFn 完成，第一个 Run 的 stopped 永远不会被 close，
// 它的 select 永久阻塞（在真实程序里那就是 main goroutine 再也回不来）。
//
// 本用例因此断言三件事：第二次 Run 立刻返回且什么都没做；第一个 Run 管理的
// 应用不被误关；第一个 Run 仍然只由真正的停机信号结束，而不是被挤掉或挂死。
func TestRunRejectsReentrantCall(t *testing.T) {
	a := newApp()
	held := newSSModule("run-held")

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		a.Run(held)
	}()
	waitClosed(t, held.ready, 5*time.Second)
	waitAppState(t, a, AppStateRun, 5*time.Second)

	// 第二次 Run：必须立刻被拒绝，而不是走进那条会误伤的失败分支。
	intruder := newSSModule("run-intruder")
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		a.Run(intruder)
	}()
	select {
	case <-secondDone:
	case <-time.After(5 * time.Second):
		t.Fatal("reentrant Run did not return immediately")
	}

	if got := intruder.initCount.Load(); got != 0 {
		t.Fatalf("intruder OnInit ran %d time(s), want 0 (the rejected Run must do nothing)", got)
	}
	if got := intruder.serveCount.Load(); got != 0 {
		t.Fatalf("intruder Serve ran %d time(s), want 0", got)
	}

	// 核心断言：第二次 Run 不得把第一个 Run 所管理的应用关掉。
	if got := a.State(); got != AppStateRun {
		t.Fatalf("state after rejected Run = %d, want AppStateRun (%d): the second Run tore down a healthy application",
			got, int32(AppStateRun))
	}
	if got := held.destroyCnt.Load(); got != 0 {
		t.Fatalf("held module OnDestroy ran %d time(s) while its own Run is still alive", got)
	}
	if got := held.closeCnt.Load(); got != 0 {
		t.Fatalf("held module Close ran %d time(s) while its own Run is still alive", got)
	}
	select {
	case <-held.stopped:
		t.Fatal("held module event loop was stopped by the second Run")
	default:
	}
	select {
	case <-firstDone:
		t.Fatal("first Run returned early, it must keep blocking until a shutdown signal arrives")
	default:
	}

	// 只有真正的停机信号才能结束第一个 Run；它绝不能永久阻塞。
	sendFrameworkStopSignal(t, a)
	select {
	case <-firstDone:
	case <-time.After(10 * time.Second):
		t.Fatal("first Run blocked forever: its stopped channel is never closed")
	}
	waitClosed(t, held.stopped, 5*time.Second)
	if got, want := held.destroyCnt.Load(), int32(1); got != want {
		t.Fatalf("held OnDestroy = %d, want %d", got, want)
	}
	if got, want := held.closeCnt.Load(), int32(1); got != want {
		t.Fatalf("held Close = %d, want %d", got, want)
	}
	if got := a.State(); got != AppStateNone {
		t.Fatalf("state after shutdown = %d, want AppStateNone", got)
	}
}

// TestStopDuringInitUsesStartupSettleTimeout 锁住 startupSettle 是一个**独立的短超时**，
// 而不是复用 shutdownTimeout。
//
// 为什么重要：启动期收到 SIGTERM（k8s 滚动发布时极常见）时，stop 必须先等
// 仍在跑的 start 收敛，否则 OnDestroy 会与 OnInit 并发读写同一批业务内存。
// 但"等 OnInit 收敛"是秒级的事，与 shutdownTimeout 所针对的"大批量落盘"
// （默认 30 分钟）完全是两回事。若把这段等待挂到 shutdownTimeout 上，
// 一个卡住的 OnInit 就会让停机整整空等半小时，早被 k8s 的
// terminationGracePeriodSeconds（默认 30 秒）强杀。
//
// 同时锁住兜底放行后的取舍：超时不等于"改成并发销毁"，仍停在
// lifecycleIniting 的模块必须被跳过，不能收到 OnDestroy/Close。
func TestStopDuringInitUsesStartupSettleTimeout(t *testing.T) {
	a := newApp() // shutdownTimeout 保持默认的 30 分钟
	a.startupSettle = 150 * time.Millisecond

	inInit := make(chan struct{})
	release := make(chan struct{})
	releaseInit := sync.OnceFunc(func() { close(release) })
	// 即便中途 t.Fatal，也要放行 OnInit，避免把 start goroutine 永久留在那里。
	t.Cleanup(releaseInit)

	slow := newSSModule("settle-slow-init")
	markEntered := sync.OnceFunc(func() { close(inInit) })
	slow.initHook = func() {
		markEntered()
		<-release
	}

	startDone := make(chan bool, 1)
	go func() { startDone <- a.start(slow) }()
	waitClosed(t, inInit, 5*time.Second)

	// stop 放到独立 goroutine 上并加一个远小于 shutdownTimeout 的上限：
	// 若实现退化成复用 shutdownTimeout，这里会拿到干净的失败，
	// 而不是把整个包的测试拖到 go test 的全局超时上。
	const settleBudget = 5 * time.Second
	stopDone := make(chan time.Duration, 1)
	go func() {
		begin := time.Now()
		a.stop()
		stopDone <- time.Since(begin)
	}()
	select {
	case elapsed := <-stopDone:
		if elapsed >= settleBudget {
			t.Fatalf("stop during startup took %v, want startup-settle magnitude", elapsed)
		}
	case <-time.After(settleBudget):
		t.Fatalf("stop during startup is still blocked after %v: it is waiting on shutdownTimeout instead of the startup settle timeout",
			settleBudget)
	}

	// 兜底放行不等于并发销毁：仍在 OnInit 里的模块必须被完整跳过。
	if got := slow.destroyCnt.Load(); got != 0 {
		t.Fatalf("OnDestroy ran %d time(s) on a module still inside OnInit, want 0", got)
	}
	if got := slow.closeCnt.Load(); got != 0 {
		t.Fatalf("Close ran %d time(s) on a module still inside OnInit, want 0", got)
	}
	if got := a.State(); got != AppStateNone {
		t.Fatalf("state after stop = %d, want AppStateNone", got)
	}

	// 放行 OnInit：start 必须察觉关闭已经发生并报告失败，而不是继续拉起事件循环。
	releaseInit()
	select {
	case ok := <-startDone:
		if ok {
			t.Fatal("start reported success after the application had already been shut down")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("start never returned after OnInit was released")
	}
	if got := slow.serveCount.Load(); got != 0 {
		t.Fatalf("Serve ran %d time(s) after shutdown completed, want 0", got)
	}
}

// TestStopBeforeStartDoesNotStrandModules 锁住 stop 与 start 的两个边界。
//
// 第一段：start 之前调 stop 必须是纯粹的空操作——没有模块可关，不得触碰任何钩子。
//
// 第二段是真正的僵尸场景：stop 已经完整跑完（模块销毁、状态复位成 AppStateNone），
// 而仍在进行中的 start 随后又一路跑到底、把状态改回 AppStateRun 并拉起事件循环。
// 结果是一个"框架认为在运行、但停机流程早已结束、再也不会有人来关它"的应用：
// 这些模块的 OnDestroy 永远不会被第二次调用，goroutine 与其绑定的系统线程
// 永久驻留。修复后 start 在 OnInit 循环、拉起 Serve 之前、以及提交
// AppStateRun 之前都会复查 stopRequested，因此只会报告失败并退出。
func TestStopBeforeStartDoesNotStrandModules(t *testing.T) {
	// 第一段：从未启动的应用上调 stop。
	idle := newApp()
	untouched := newSSModule("never-started")
	if err := idle.Register(untouched); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	idle.stop()
	if got := idle.State(); got != AppStateNone {
		t.Fatalf("state after stop-before-start = %d, want AppStateNone", got)
	}
	if untouched.initCount.Load() != 0 || untouched.destroyCnt.Load() != 0 || untouched.closeCnt.Load() != 0 {
		t.Fatalf("stop before start touched the module: init=%d destroy=%d close=%d",
			untouched.initCount.Load(), untouched.destroyCnt.Load(), untouched.closeCnt.Load())
	}

	// 第二段：stop 跑完之后，start 不得把应用改回运行中。
	a := newApp()
	a.startupSettle = 150 * time.Millisecond
	t.Cleanup(func() {
		// 基线上这里会残留一个"运行中"的僵尸应用，兜底关掉它，避免泄漏 goroutine。
		if a.State() != AppStateNone {
			a.stop()
		}
	})

	inInit := make(chan struct{})
	release := make(chan struct{})
	releaseInit := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseInit)

	late := newSSModule("late-start")
	markEntered := sync.OnceFunc(func() { close(inInit) })
	late.initHook = func() {
		markEntered()
		<-release
	}

	startDone := make(chan bool, 1)
	go func() { startDone <- a.start(late) }()
	waitClosed(t, inInit, 5*time.Second)

	// stop 完整跑完（startupSettle 兜底放行），此刻应用已经"关完了"。
	a.stop()
	if got := a.State(); got != AppStateNone {
		t.Fatalf("state after stop = %d, want AppStateNone", got)
	}

	// 现在才放行 OnInit，让 start 在"关闭已完成"之后继续往下跑。
	releaseInit()
	select {
	case ok := <-startDone:
		if ok {
			t.Fatal("start reported success after shutdown completed, leaving a zombie application")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("start never returned after OnInit was released")
	}

	if got := a.State(); got == AppStateRun {
		t.Fatalf("state = AppStateRun (%d) after shutdown completed: start resurrected a stopped application", got)
	}
	if got := late.serveCount.Load(); got != 0 {
		t.Fatalf("Serve ran %d time(s) after shutdown completed, want 0 (nobody would ever stop it again)", got)
	}
}

// TestConcurrentStopIsIdempotent 锁住并发 stop 的幂等性。
//
// 真实程序里并发 stop 很容易出现：SIGINT 与 SIGTERM 先后到达、信号处理器与
// 业务自己调用的关闭入口撞在一起。修复后销毁权由 moduleWrapper.claimShutdown
// 用 CAS 抢占，"每个模块的 OnDestroy 与 Close 至多各执行一次"因此是结构性
// 保证，而不再依赖 app 状态闸门——后者在 stop 需要中途放开锁去等 start 收敛
// 之后已经不再是一个原子的判定。
//
// 静态模块与动态模块都要覆盖：动态模块此前走的是另一条关闭路径，
// 拿不到 Close 调用，这条不变量必须在两条路径上同时成立。
// 本用例需要能通过 -race -count=5。
func TestConcurrentStopIsIdempotent(t *testing.T) {
	a := newApp()
	first := newSSModule("concurrent-first")
	second := newSSModule("concurrent-second")
	second.priority = 1

	if !a.start(first, second) {
		t.Fatal("start should succeed")
	}
	waitClosed(t, first.ready, 5*time.Second)
	waitClosed(t, second.ready, 5*time.Second)

	dyn := newSSModule("concurrent-dyn")
	if _, err := a.AddDynamicModules(dyn); err != nil {
		t.Fatalf("AddDynamicModules failed: %v", err)
	}
	waitClosed(t, dyn.ready, 5*time.Second)

	// 用一个共同的起跑信号把 N 次 stop 尽量挤到同一时刻。
	const stoppers = 8
	begin := make(chan struct{})
	var wg sync.WaitGroup
	for range stoppers {
		wg.Go(func() {
			<-begin
			a.stop()
		})
	}
	close(begin)
	wg.Wait()

	for _, m := range []*ssModule{first, second, dyn} {
		if got, want := m.destroyCnt.Load(), int32(1); got != want {
			t.Errorf("%s OnDestroy ran %d time(s), want %d", m.name, got, want)
		}
		if got, want := m.closeCnt.Load(), int32(1); got != want {
			t.Errorf("%s Close ran %d time(s), want %d", m.name, got, want)
		}
		waitClosed(t, m.stopped, 5*time.Second)
	}
	if got := a.State(); got != AppStateNone {
		t.Fatalf("state after %d concurrent stops = %d, want AppStateNone", stoppers, got)
	}
}
