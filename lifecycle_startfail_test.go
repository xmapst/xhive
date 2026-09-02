package xhive

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xmapst/xhive/chanrpc"
)

// startFailModule 是本文件（启动失败路径）专用的 IModule 实现。
//
// 不直接复用 app_test.go 里的 testModule，有两个原因：
//  1. 本组用例必须先在**未修复的基线**上跑出 FAIL 才有意义，而基线那份
//     testModule 还没有 closeCnt / initHook / keepServerOpen 等字段，
//     用它写出来的用例根本无法在基线上编译，也就无从对照；
//  2. 启动失败路径要断言的恰恰是"Serve 一次都不该被拉起"，而 bug 的表现
//     形式就是 Serve 被错误地（甚至重复地）拉起。若 runStarted/runStopped
//     用裸 close，测试自己会先 panic 掉，把真正的断言结果掩盖成一次崩溃。
//     因此这里用 sync.Once 保护，让"Serve 跑了几次"完整地体现在 runCount 上。
//
// OnDestroy 刻意不去关闭自己的 server：本组用例要观察的正是"框架有没有替
// 从未 Serve 过的模块关 server / 排空积压"，模块自己顺手关掉就再也看不出来了。
type startFailModule struct {
	name     string
	priority uint
	server   *chanrpc.Server
	initErr  error

	// initHook 在计数之后、返回 initErr 之前执行，用于制造"OnInit 阻塞中"的窗口。
	initHook func()

	initCount  atomic.Int32
	runCount   atomic.Int32
	destroyCnt atomic.Int32
	closeCnt   atomic.Int32

	runStarted  chan struct{}
	runStopped  chan struct{}
	startedOnce sync.Once
	stoppedOnce sync.Once
}

func newStartFailModule(name string) *startFailModule {
	return &startFailModule{
		name:       name,
		server:     chanrpc.NewServer(chanrpc.WithChanLen(4)),
		runStarted: make(chan struct{}),
		runStopped: make(chan struct{}),
	}
}

func (m *startFailModule) Name() string   { return m.name }
func (m *startFailModule) Priority() uint { return m.priority }

func (m *startFailModule) OnInit() error {
	m.initCount.Add(1)
	if m.initHook != nil {
		m.initHook()
	}
	return m.initErr
}

func (m *startFailModule) Serve(ctx context.Context) {
	m.runCount.Add(1)
	m.startedOnce.Do(func() { close(m.runStarted) })
	<-ctx.Done()
	m.stoppedOnce.Do(func() { close(m.runStopped) })
}

// Ready 复用 runStarted，语义等价于 Skeleton.ready 在进入 select 前关闭。
func (m *startFailModule) Ready() <-chan struct{} { return m.runStarted }

func (m *startFailModule) OnDestroy() {
	m.destroyCnt.Add(1)
}

func (m *startFailModule) ChanRPC() *chanrpc.Server { return m.server }

func (m *startFailModule) Close() error {
	m.closeCnt.Add(1)
	return nil
}

// assertStartFailUntouched 断言一个模块从未被框架托管过任何资源：既没跑过 Serve，
// 也不该收到 OnDestroy / Close。启动失败路径上的核心契约就是这一条。
func assertStartFailUntouched(t *testing.T, m *startFailModule) {
	t.Helper()
	if m.runCount.Load() != 0 {
		t.Errorf("%s: Serve 跑了 %d 次，want 0", m.name, m.runCount.Load())
	}
	if m.destroyCnt.Load() != 0 || m.closeCnt.Load() != 0 {
		t.Errorf("%s: destroy=%d close=%d, want 0/0（OnInit 未成功的模块不得被销毁）",
			m.name, m.destroyCnt.Load(), m.closeCnt.Load())
	}
}

// TestRunStopsAfterInitFailure 覆盖**真实生产入口 a.Run** 的启动失败路径：
// start 返回 false → errCh → stopFn → stop。此前所有启动失败用例都是直接调
// a.start + a.stop 拼出来的，Run 里"start 失败自动触发关闭"这一段代码
// （包括它是否会自己返回、返回时应用是否已复位）完全没有测试覆盖。
//
// 这条路径是生产上最常见的一种崩溃现场：进程刚起来，某个依赖没连上，
// OnInit 返回 error，接着框架自动执行一遍完整的停机。用例锁住三件事：
//  1. Run 不需要任何信号就能自行返回（否则 main goroutine 永久挂死）；
//  2. 只有 OnInit 成功的模块拿到 OnDestroy/Close——OnInit 失败的模块按契约
//     已自行回滚，排在它后面的模块连 OnInit 都没跑过，字段全是零值，
//     对它们调 OnDestroy 就是让业务在未构造完成的对象上执行"释放所有资源"；
//  3. Run 返回时应用已完整复位为 AppStateNone。
func TestRunStopsAfterInitFailure(t *testing.T) {
	a := newApp()

	// Priority 决定 OnInit 顺序：ok → bad → never。
	ok := newStartFailModule("run-ok")
	bad := newStartFailModule("run-bad")
	bad.priority = 1
	bad.initErr = errors.New("dependency unavailable")
	never := newStartFailModule("run-never")
	never.priority = 2

	// Run 会注册 SIGINT/SIGTERM 处理器并阻塞，这里放到独立 goroutine 上跑，
	// 既能验证"它自己会返回"，又不会在回归时把整个测试进程拖死。
	// Run 返回前会调用 sm.Stop() 注销信号监听，测试结束后不留全局副作用。
	done := make(chan struct{})
	go func() {
		a.Run(ok, bad, never)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run 在 start 失败后没有自行返回（生产上就是 main goroutine 永久挂死）")
	}

	if ok.initCount.Load() != 1 {
		t.Fatalf("run-ok OnInit 调用了 %d 次，want 1", ok.initCount.Load())
	}
	if ok.destroyCnt.Load() != 1 || ok.closeCnt.Load() != 1 {
		t.Fatalf("run-ok: destroy=%d close=%d, want 1/1（OnInit 成功的模块必须被完整销毁）",
			ok.destroyCnt.Load(), ok.closeCnt.Load())
	}
	if ok.runCount.Load() != 0 {
		t.Fatalf("run-ok Serve 跑了 %d 次，want 0（启动在拉起 Serve 之前就中止了）", ok.runCount.Load())
	}

	// bad 自己 OnInit 失败，按 IModule.OnInit 的契约由它自行回滚，框架不得再销毁一次。
	if bad.initCount.Load() != 1 {
		t.Fatalf("run-bad OnInit 调用了 %d 次，want 1", bad.initCount.Load())
	}
	assertStartFailUntouched(t, bad)

	// never 排在失败模块之后，OnInit 一次都没被调用过。
	if never.initCount.Load() != 0 {
		t.Fatalf("run-never OnInit 调用了 %d 次，want 0", never.initCount.Load())
	}
	assertStartFailUntouched(t, never)

	if a.State() != AppStateNone {
		t.Fatalf("Run 返回后 state = %d, want AppStateNone", a.State())
	}
}

// TestStartFailureAbandonsBacklogWithoutRunningHandlers 锁住启动失败路径上
// 积压请求的处置方式：**明确丢弃并回错误，而不是在停机 goroutine 上执行它们的 handler**。
//
// 场景是 LIFO 停机顺序的核心用途：source 在自己的 OnDestroy 里把停机前的最后
// 一批状态投给更早注册、按 LIFO 还没轮到销毁的 sink。正常停机路径已有
// TestOnDestroyCanCastToModuleShuttingDownLater 覆盖（那里 sink 的事件循环还活着，
// 数据会被真正处理）；本用例走的是**启动中途失败**那条路：sink 的 Serve 一次都没跑过。
//
// 这里有一个不得不做的取舍。让积压请求"落地"意味着由停机 goroutine 去执行 sink
// 的 handler，而那会踩两个坑：handler 对业务状态的访问不再局限于单一 goroutine，
// actor 模型赖以免锁的前提被打破；更要命的是 handler 若发起同步 Call、对端又是
// 另一个事件循环从未运行过的模块，回包永远不会产生，整个 stop 就永久卡死
// （chanrpc.Server.Close 的 closeDrainTimeout 只保护"等下一个请求"，
// 保护不了 handler 自身的执行）。本用例的 sink handler 就是这样写的。
//
// 因此框架选择 Server.Abandon：队列排空、每个调用方收到 ErrServerAbandoned、
// handler 一次都不执行。丢是丢了，但丢得明明白白——比基线的"投递成功后随进程
// 静默蒸发"要好，也比"可能永久卡死停机"要好。这个模块从未宣告过就绪
// （Ready 未关闭），本就不该处理任何请求。
func TestStartFailureAbandonsBacklogWithoutRunningHandlers(t *testing.T) {
	// Skeleton 的 Cast/Call 都是通过包级单例 defaultApp 寻址的，
	// 必须把单例换成本用例私有的实例，用完还原。
	oldDefault := defaultApp
	defaultApp = newApp()
	defer func() { defaultApp = oldDefault }()

	var handlerRan atomic.Bool
	sink := newSkeletonTestModule("startfail-sink")
	sink.onInit = func(m *skeletonTestModule) error {
		return m.RegisterChanRPC(skeletonRPCReq{}, func(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
			handlerRan.Store(true)
			// 这一行就是那条 high 级回归的引信：它同步等待另一个事件循环
			// 从未运行过的模块回包，永远等不到。只要框架敢在停机 goroutine 上
			// 执行本 handler，stop 就再也回不来。
			sink.Call("startfail-peer", skeletonRPCReq{Value: "never answered"})
			return nil
		})
	}

	// peer 同样只 OnInit 成功、Serve 从未运行，用来做上面那次同步 Call 的黑洞对端。
	// 它必须注册在 sink **之前**：LIFO 下 sink 先销毁，轮到它排空积压时 peer 还没关，
	// 那次同步 Call 才会真的等下去。若 peer 排在后面，它的 server 已被收掉，
	// Call 会立刻失败返回，卡死就复现不出来了。
	peer := newSkeletonTestModule("startfail-peer")

	// source 注册在 sink 之后，按 LIFO 先于 sink 销毁，
	// 它的 OnDestroy 执行时 sink 还没轮到关闭，投递会成功入队。
	source := newSkeletonTestModule("startfail-source")
	source.onDestroy = func() {
		source.Cast("startfail-sink", skeletonRPCReq{Value: "final flush on start failure"})
	}

	// bad 排在最后，OnInit 失败导致整个启动中止——此时 sink/peer/source 的
	// Serve 都还没被拉起，三者都停在"OnInit 已成功但事件循环从未运行"。
	bad := newSkeletonTestModule("startfail-bad")
	bad.onInit = func(*skeletonTestModule) error { return errors.New("boom") }

	if defaultApp.start(peer, sink, source, bad) {
		t.Fatal("start 应当因为 bad 的 OnInit 失败而返回 false")
	}

	// stop 必须能返回。放 goroutine 里跑并设超时，是因为这条用例要防的正是
	// "停机永久卡死"——直接调用的话回归一旦复现，测试会挂到整个包超时，
	// 报出来的是一堆无关的 goroutine dump 而不是这里的失败信息。
	stopped := make(chan struct{})
	go func() {
		defaultApp.stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("stop 永久卡死：积压请求的 handler 被在停机 goroutine 上执行，" +
			"其中的同步 Call 等不到一个从未存在的事件循环回包")
	}

	if handlerRan.Load() {
		t.Error("sink 的 handler 被执行了：事件循环从未运行过的模块不应处理任何请求，" +
			"更不应把 handler 跑在框架的停机 goroutine 上")
	}

	// server 必须已关闭且队列已排空：否则"投递成功但没人消费"的窗口依然存在。
	if !sink.ChanRPC().IsClosed() {
		t.Error("sink 的 server 应当被框架收掉（否则后续投递仍会静默入队蒸发）")
	}
	if n := sink.ChanRPC().Len(); n != 0 {
		t.Errorf("sink 队列积压 %d 条，want 0（Abandon 必须把积压排空）", n)
	}
}

// TestStartFailureAbortsBeforeServing 锁住 start 在**拉起 Serve goroutine 之前**
// 的那一道 stopRequested 复查。
//
// 这道复查很容易被误认为多余：OnInit 循环每轮开头已经检查过一次了。但全部
// OnInit 都成功时循环不会再进入循环体，那次检查根本不会发生。而 stop 若走了
// startupSettle 的兜底放行分支（启动期收到 SIGTERM、某个 OnInit 迟迟不返回，
// k8s 滚动发布时很常见），此刻它可能已经把这批模块销毁完毕、状态也复位成
// AppStateNone 了。start 若继续往下走，就会给已经 OnDestroy 过的模块重新拉起
// Serve，并用 lifecycleServing 覆盖掉它们的 lifecycleDestroyed——一个已经释放
// 完资源的模块重新开始对外提供服务，且再也没有人会去关闭它。
//
// 用例用显式事件同步复现这个窗口，不依赖 sleep 推断时序：
// blocker 的 OnInit 卡住 → stop 在 startupSettle 之后兜底放行并完整跑完
// （blocker 停在 initing 被跳过销毁，early 被完整销毁）→ 确认 stop 已返回，
// 再放行 OnInit，让 start 走到"拉起 Serve"之前的那一步。
func TestStartFailureAbortsBeforeServing(t *testing.T) {
	a := newApp()
	// 缩短兜底放行时间：默认 30 秒是给真实 OnInit（连依赖、加载配置）留的余量，
	// 用例只需要它到时即可。
	a.startupSettle = 100 * time.Millisecond

	early := newStartFailModule("abort-early") // priority 0：先 OnInit，成功
	blocker := newStartFailModule("abort-blocker")
	blocker.priority = 1 // 排在最后，让 OnInit 循环结束后不再有第二次 stopRequested 检查

	initEntered := make(chan struct{})
	release := make(chan struct{})
	blocker.initHook = func() {
		close(initEntered)
		<-release
	}

	startResult := make(chan bool, 1)
	go func() { startResult <- a.start(early, blocker) }()

	// 等到 blocker 真正进入 OnInit：此刻 early 已 inited、blocker 停在 initing，
	// 应用状态为 AppStateInit，正是 stop 需要等待 start 收敛的那个窗口。
	<-initEntered

	stopReturned := make(chan struct{})
	go func() {
		a.stop()
		close(stopReturned)
	}()

	// stop 不会被 blocker 卡住：initing 的模块只 cancel、跳过销毁。
	// 等它整个返回，才谈得上"关闭已经完成，start 绝不能再把模块拉起来"。
	select {
	case <-stopReturned:
	case <-time.After(10 * time.Second):
		t.Fatal("stop 没有在 startupSettle 超时后兜底放行")
	}
	if got := a.State(); got != AppStateNone {
		t.Fatalf("stop 返回后 state = %d, want AppStateNone", got)
	}
	if early.destroyCnt.Load() != 1 || early.closeCnt.Load() != 1 {
		t.Fatalf("abort-early: destroy=%d close=%d, want 1/1（stop 应已完整销毁它）",
			early.destroyCnt.Load(), early.closeCnt.Load())
	}

	// 放行 OnInit，让 start 继续往下——它必须在拉起 Serve 之前中止。
	close(release)
	select {
	case ok := <-startResult:
		if ok {
			t.Fatal("start 应当在关闭完成后返回 false，而不是报告启动成功")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("start 没有返回")
	}

	if early.runCount.Load() != 0 {
		t.Fatalf("abort-early 的 Serve 被拉起了 %d 次，want 0（它已经 OnDestroy 过了）",
			early.runCount.Load())
	}
	if blocker.runCount.Load() != 0 {
		t.Fatalf("abort-blocker 的 Serve 被拉起了 %d 次，want 0", blocker.runCount.Load())
	}
	// 生命周期状态不能被 lifecycleServing 覆盖：它是关闭流程判断"该不该销毁、
	// 要不要等 goroutine"的唯一依据，被覆盖之后整套记账就失真了。
	if got := startFailStaticWrapper(t, a, "abort-early").lifecycleState(); got != lifecycleDestroyed {
		t.Fatalf("abort-early lifecycle = %s, want destroyed（不得被重新拉起覆盖成 serving）", got)
	}
	// initing 的模块被跳过销毁，这是"宁可漏一次销毁，也不要与 OnInit 并发读写"的取舍。
	if blocker.destroyCnt.Load() != 0 {
		t.Fatalf("abort-blocker destroy = %d, want 0（OnInit 仍在执行时不得并发销毁）",
			blocker.destroyCnt.Load())
	}
	if got := a.State(); got != AppStateNone {
		t.Fatalf("start 中止后 state = %d, want AppStateNone（绝不能改回 AppStateRun）", got)
	}
}

// startFailStaticWrapper 按名称取出静态模块的 wrapper，用于断言模块级生命周期状态。
// 取读锁是因为 modules 切片会被 start 排序，虽然调用点都在 start 返回之后，
// 仍按框架自己的并发约定访问。
func startFailStaticWrapper(t *testing.T, a *app, name string) *moduleWrapper {
	t.Helper()
	a.RLock()
	defer a.RUnlock()
	for _, wrapper := range a.modules {
		if wrapper.Name() == name {
			return wrapper
		}
	}
	t.Fatalf("静态模块 %q 不存在", name)
	return nil
}

// TestAppStartInitFailureLeavesNoDestroyOnUninitialized 是对 app_test.go 里
// TestAppStartValidationAndInitFailure 的补齐。
//
// 那个用例在 start 返回 false 之后就结束了，从不调用 stop——而破坏正是发生在
// stop 里：真实入口 Run 在 start 失败后一定会跑一遍完整停机（见
// TestRunStopsAfterInitFailure），基线的 stop 会无差别地对所有静态模块执行
// OnDestroy/Close，包括 OnInit 返回 error 的模块和 OnInit 一次都没跑过的模块。
// 只断言到 start 为止，就恰好停在了 bug 发生的前一步。
//
// 顺带保留原用例的两条入参校验（空启动失败、失败后状态保持 AppStateNone），
// 使这条路径在"校验失败 → 初始化失败 → 停机"三段上都有断言。
func TestAppStartInitFailureLeavesNoDestroyOnUninitialized(t *testing.T) {
	a := newApp()

	// 空启动：拒绝启动且不得改变状态（也不能留下 AppStateInit 这种中间态）。
	if a.start() {
		t.Fatal("没有任何模块时 start 应当失败")
	}
	if a.State() != AppStateNone {
		t.Fatalf("空启动后 state = %d, want AppStateNone", a.State())
	}
	// 从未启动过的应用上 stop 应当是幂等的空操作。
	a.stop()
	if a.State() != AppStateNone {
		t.Fatalf("未启动时 stop 后 state = %d, want AppStateNone", a.State())
	}

	bad := newStartFailModule("solo-bad")
	bad.initErr = errors.New("init failed")
	later := newStartFailModule("solo-later")
	later.priority = 1 // 排在 bad 之后，OnInit 不该被调用

	if a.start(bad, later) {
		t.Fatal("OnInit 失败时 start 应当失败")
	}
	if bad.initCount.Load() != 1 {
		t.Fatalf("solo-bad OnInit 调用了 %d 次，want 1", bad.initCount.Load())
	}
	if bad.runCount.Load() != 0 {
		t.Fatalf("solo-bad Serve 跑了 %d 次，want 0", bad.runCount.Load())
	}

	// 这一步才是补齐的关键：Run 在 start 失败后必然会跑的那次停机。
	a.stop()

	// OnInit 返回 error 的模块按契约已自行回滚，框架不得再销毁一次。
	assertStartFailUntouched(t, bad)
	// 排在它之后的模块 OnInit 一次都没被调用过，字段全是零值。
	if later.initCount.Load() != 0 {
		t.Fatalf("solo-later OnInit 调用了 %d 次，want 0", later.initCount.Load())
	}
	assertStartFailUntouched(t, later)

	if a.State() != AppStateNone {
		t.Fatalf("stop 后 state = %d, want AppStateNone", a.State())
	}
}
