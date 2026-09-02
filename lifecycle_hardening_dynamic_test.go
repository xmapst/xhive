package xhive

import (
	"context"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xmapst/xhive/chanrpc"
)

// 本文件锁定「动态模块并发加固」这一组行为，全部围绕 AddDynamicModules 的
// 占位（LoadOrStore）、可见性（moduleWrapper.visible）、初始化期回滚
// （OnInit 之后复查 stopRequested）以及 RemoveDynamicModule 的返回值语义。
//
// 这些用例共同守住的核心不变量只有一条：
// **动态模块表里出现的名字，要么对应一个已经就绪、且将来一定会被销毁的模块，
// 要么根本就不该被外界看见。** 加固前这条不变量在三个地方漏了：
// 重名检查与登记之间隔着整个 OnInit（TOCTOU）、初始化期开始的关闭会造就孤儿模块、
// 卸载超时却谎报清理完成。

// hdModule 是本文件专用的模块桩。
//
// 之所以不复用 app_test.go 的 testModule：这一组用例需要在 OnInit 里做同步点、
// 需要一个刻意不响应 ctx.Done 的 Serve，并且需要严格区分「Serve 从未被拉起」
// 与「Serve 跑过又退出」——用独立类型可以把这些观测点写清楚，也避免与其他
// 测试组共用脚手架时互相牵制。
type hdModule struct {
	name     string
	priority uint
	server   *chanrpc.Server

	initCnt    atomic.Int32
	serveCnt   atomic.Int32
	destroyCnt atomic.Int32
	closeCnt   atomic.Int32

	ready  chan struct{} // Serve 一进入就关闭，语义等价 Skeleton 的 ready
	served chan struct{} // Serve 返回时关闭，用于确认 goroutine 真的退出了

	initHook func() // 在 OnInit 内部执行，用例借它把同步点插进初始化过程中

	// serveBlock 非 nil 时 Serve 只等它关闭，**故意忽略 ctx.Done**，
	// 用来模拟"不响应停止信号"的模块，触发 shutdownTimeout 路径。
	serveBlock chan struct{}
}

func newHDModule(name string) *hdModule {
	return &hdModule{
		name:   name,
		server: chanrpc.NewServer(chanrpc.WithChanLen(4)),
		ready:  make(chan struct{}),
		served: make(chan struct{}),
	}
}

func (m *hdModule) Name() string   { return m.name }
func (m *hdModule) Priority() uint { return m.priority }

func (m *hdModule) OnInit() error {
	m.initCnt.Add(1)
	if m.initHook != nil {
		m.initHook()
	}
	return nil
}

func (m *hdModule) Serve(ctx context.Context) {
	m.serveCnt.Add(1)
	defer close(m.served)
	close(m.ready)
	if m.serveBlock != nil {
		<-m.serveBlock
		return
	}
	<-ctx.Done()
}

func (m *hdModule) Ready() <-chan struct{} { return m.ready }

// OnDestroy 刻意不去关闭自己的 server：框架是否替"事件循环从未运行过"的模块
// 收掉 server 是另一组用例的观测点，这里不要抢在框架前面把它关了。
func (m *hdModule) OnDestroy() { m.destroyCnt.Add(1) }

func (m *hdModule) ChanRPC() *chanrpc.Server { return m.server }

func (m *hdModule) Close() error {
	m.closeCnt.Add(1)
	return nil
}

// hdWaitClosed 等待一个 channel 关闭。超时只作为"卡死"的兜底判定，
// 正常路径上永远不会等到超时——用例的时序全部由 channel 同步点决定，
// 不靠时间窗口撞运气。
func hdWaitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting %s", what)
	}
}

// TestAddDynamicModulesConcurrentSameNameAddsOnce 锁定 LoadOrStore 占位修掉的 TOCTOU。
//
// 锁住什么：N 个 goroutine 并发用同名模块调 AddDynamicModules 时，
// 恰好一个成功、其余全部拿到 "already exists"；被拒绝的那些模块
// **连 OnInit 都不该被调用**（占位发生在 OnInit 之前），Serve 更不该被拉起；
// 最终动态模块表里只有一个条目，卸载后没有残留 goroutine。
//
// 为什么重要：加固前的顺序是「Load 检查重名 → OnInit → startServing →
// waitReady → Store」，检查与登记之间隔着整个初始化过程。两个并发调用会
// 双双通过检查、各自跑完 OnInit 并启动 goroutine，最后 Store 的那个静默覆盖
// 前一个——被覆盖的 wrapper 从此再无任何引用能触及：cancel 永不调用、
// goroutine 连同 LockOSThread 绑定的系统线程永久驻留、OnDestroy/Close 永不执行，
// 而两个同名模块还在同时对外服务（同一个监听端口、同一把分布式锁）。
//
// 加固前的表现：workers 个调用**全部**返回成功，OnInit 与 Serve 各跑 workers 遍。
//
// 时序如何做到确定：所有参与者共用一个 barrier（WaitGroup）。
// 赢家在 OnInit 里 Done 之后 Wait 住，落败者在 AddDynamicModules 返回之后补一次
// Done——于是"落败者完成占位争抢"这件事必然发生在"赢家仍停在 OnInit 里"的窗口内，
// 正是加固前那个 TOCTOU 窗口。加固前所有人都会进 OnInit，同样凑齐 N 个 Done，
// 因此两个版本都不会死锁，差别只体现在断言上。
func TestAddDynamicModulesConcurrentSameNameAddsOnce(t *testing.T) {
	const workers = 6
	const name = "hd-racer"

	a := newApp()

	var barrier sync.WaitGroup
	barrier.Add(workers)

	mods := make([]*hdModule, workers)
	for i := range mods {
		m := newHDModule(name)
		m.initHook = func() {
			// 抢到占位的那个模块停在这里，直到其余参与者全部完成争抢。
			barrier.Done()
			barrier.Wait()
		}
		mods[i] = m
	}

	type addOutcome struct {
		results []AddDynamicModuleResult
		err     error
	}
	outcomes := make(chan addOutcome, workers)

	var wg sync.WaitGroup
	for _, m := range mods {
		wg.Go(func() {
			results, err := a.AddDynamicModules(m)
			// 被占位挡在 OnInit 之前的参与者在这里补一次到达，
			// 否则赢家会永远等不齐 N 个 Done。加固前没有人走这条路径
			// （所有模块都进过 OnInit），Done 总数同样是 N。
			if m.initCnt.Load() == 0 {
				barrier.Done()
			}
			outcomes <- addOutcome{results: results, err: err}
		})
	}
	wg.Wait()
	close(outcomes)

	var succeeded, rejected int
	for outcome := range outcomes {
		if len(outcome.results) != 1 {
			t.Fatalf("AddDynamicModules results = %+v, want exactly one entry", outcome.results)
		}
		res := outcome.results[0]
		if res.Name != name {
			t.Fatalf("result name = %q, want %q", res.Name, name)
		}
		if res.Err == nil {
			if outcome.err != nil {
				t.Fatalf("result reports success but err = %v", outcome.err)
			}
			succeeded++
			continue
		}
		rejected++
		if outcome.err == nil {
			t.Fatal("failed result must also be summarized in the returned err")
		}
		if !strings.Contains(res.Err.Error(), "already exists") {
			t.Fatalf("rejection reason = %v, want an \"already exists\" error", res.Err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("successful AddDynamicModules calls = %d, want exactly 1 (name occupancy is a TOCTOU-free atomic claim)", succeeded)
	}
	if rejected != workers-1 {
		t.Fatalf("rejected calls = %d, want %d", rejected, workers-1)
	}

	// 逐个模块复核：占位挡在 OnInit **之前**，落败者必须一次业务代码都没跑过。
	var winner *hdModule
	for i, m := range mods {
		if m.initCnt.Load() == 0 {
			if m.serveCnt.Load() != 0 {
				t.Fatalf("module[%d] was rejected but its Serve ran %d times", i, m.serveCnt.Load())
			}
			if m.destroyCnt.Load() != 0 || m.closeCnt.Load() != 0 {
				t.Fatalf("module[%d] was rejected but got destroy=%d close=%d",
					i, m.destroyCnt.Load(), m.closeCnt.Load())
			}
			continue
		}
		if m.initCnt.Load() != 1 {
			t.Fatalf("module[%d] OnInit ran %d times, want 1", i, m.initCnt.Load())
		}
		if winner != nil {
			t.Fatalf("OnInit ran on more than one module (module[%d] and %q): "+
				"the duplicate name check happened before OnInit, not atomically with it", i, winner.name)
		}
		winner = m
	}
	if winner == nil {
		t.Fatal("no module was initialized at all")
	}

	// 动态模块表里只有一个条目，且指向真正跑起来的那个模块。
	if names := a.DynamicModules(); len(names) != 1 || names[0] != name {
		t.Fatalf("DynamicModules = %v, want exactly [%s]", names, name)
	}
	if got := a.ChanRPC(name); got != winner.server {
		t.Fatalf("ChanRPC(%q) does not point at the module that actually runs", name)
	}
	hdWaitClosed(t, winner.ready, "winner Serve to start")

	// 卸载之后不留残余：赢家的 goroutine 确实退出，落败者从头到尾没有 goroutine。
	if !a.RemoveDynamicModule(name) {
		t.Fatal("RemoveDynamicModule should report a complete shutdown")
	}
	hdWaitClosed(t, winner.served, "winner Serve to exit")
	if winner.destroyCnt.Load() != 1 || winner.closeCnt.Load() != 1 {
		t.Fatalf("winner destroy=%d close=%d, want 1/1", winner.destroyCnt.Load(), winner.closeCnt.Load())
	}
	if names := a.DynamicModules(); len(names) != 0 {
		t.Fatalf("DynamicModules after removal = %v, want empty", names)
	}
	for i, m := range mods {
		if m == winner {
			continue
		}
		select {
		case <-m.served:
			t.Fatalf("module[%d] was rejected but a Serve goroutine still ran and exited", i)
		default:
		}
	}
}

// TestAddDynamicModulesRolledBackWhenShutdownStartsDuringInit 锁定 OnInit 之后
// 对 stopRequested 的复查。
//
// 锁住什么：一批动态模块的初始化跨越了整个 a.stop()（关闭在 OnInit 期间从头
// 跑到尾，连 "shutdown complete" 都已宣告）时，这批模块一个都不许被登记进去：
//   - 被关闭流程抢占（停在 lifecycleIniting）的那个模块，由 AddDynamicModules
//     发现 CAS 推进失败后如实报错，不会被拉回"活着"的状态；
//   - 排在它后面、OnInit 完整跑完的那个模块，必须**就地回滚**：
//     拿到 OnDestroy + Close，且 Serve 一次都不许被拉起。
//
// 最终动态模块表为空。
//
// 为什么重要：入口那道 stopRequested 闸门只挡得住"调用时已经在关闭"，挡不住
// "调用通过之后才开始关闭"。OnInit 动辄几百毫秒到数秒（建连接池、拉配置），
// 这段时间足够 stop 跑完两轮 removeAllDynamicModules。此刻若照常 startServing，
// 得到的是一个永不退出、永不 OnDestroy 的孤儿：进程已经宣告优雅关闭完成，
// 它却还持有连接、还在写盘。
//
// 加固前的表现：两个模块**双双返回成功**并启动 goroutine，OnDestroy/Close
// 永不执行——应用一边报告 shutdown complete，一边多出两条常驻 goroutine。
//
// 时序如何做到确定：blocker 的 OnInit 阻塞在 releaseInit 上，主测试 goroutine
// 在收到 initEntered 之后同步跑完整个 a.stop()，返回后才放行 OnInit。
// 于是"关闭已完整结束"与"初始化尚未返回"这两件事的先后关系是硬定的，
// 不依赖任何时间窗口。
func TestAddDynamicModulesRolledBackWhenShutdownStartsDuringInit(t *testing.T) {
	a := newApp()

	// 需要一个静态模块把应用推进到 AppStateRun，stop 才会真正走关闭流程。
	anchor := newHDModule("hd-anchor")
	if !a.start(anchor) {
		t.Fatal("start should succeed")
	}
	hdWaitClosed(t, anchor.ready, "anchor Serve to start")

	initEntered := make(chan struct{})
	releaseInit := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseInit) }) }
	t.Cleanup(release) // 任何一条 t.Fatal 路径都不会把 OnInit 永久挂住

	// blocker 先被初始化（优先级更小），它的 OnInit 就是本用例的同步点。
	blocker := newHDModule("hd-blocker")
	blocker.priority = 0
	blocker.initHook = func() {
		close(initEntered)
		<-releaseInit
	}
	// victim 排在 blocker 之后。等它开始初始化时 stop 早已结束，
	// 它的 OnInit 会完整成功——正是"OnInit 之后复查 stopRequested"要接住的那个。
	victim := newHDModule("hd-victim")
	victim.priority = 1

	type addOutcome struct {
		results []AddDynamicModuleResult
		err     error
	}
	done := make(chan addOutcome, 1)
	go func() {
		results, err := a.AddDynamicModules(blocker, victim)
		done <- addOutcome{results: results, err: err}
	}()

	hdWaitClosed(t, initEntered, "blocker OnInit to start")

	// 关闭在初始化期间完整跑完，包括宣告 shutdown complete。
	a.stop()
	if state := a.State(); state != AppStateNone {
		t.Fatalf("state after stop = %d, want AppStateNone", state)
	}
	if anchor.destroyCnt.Load() != 1 {
		t.Fatalf("anchor destroy = %d, want 1", anchor.destroyCnt.Load())
	}

	release()

	var outcome addOutcome
	select {
	case outcome = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("AddDynamicModules never returned after OnInit was released")
	}

	if outcome.err == nil {
		t.Fatalf("AddDynamicModules must fail once shutdown has run during OnInit, results = %+v", outcome.results)
	}
	if len(outcome.results) != 2 {
		t.Fatalf("results = %+v, want one entry per module", outcome.results)
	}
	for _, res := range outcome.results {
		if res.Err == nil {
			t.Fatalf("module %q was reported as successfully added after shutdown completed", res.Name)
		}
	}

	// victim：OnInit 成功过，因此框架必须替它把资源收干净（就地回滚），
	// 但绝不能把它启动起来。
	if victim.initCnt.Load() != 1 {
		t.Fatalf("victim OnInit ran %d times, want 1", victim.initCnt.Load())
	}
	if victim.serveCnt.Load() != 0 {
		t.Fatalf("victim Serve ran %d times, want 0 (nothing may start after shutdown completed)", victim.serveCnt.Load())
	}
	if victim.destroyCnt.Load() != 1 || victim.closeCnt.Load() != 1 {
		t.Fatalf("victim destroy=%d close=%d, want 1/1 (an initialized module must be rolled back in place, not orphaned)",
			victim.destroyCnt.Load(), victim.closeCnt.Load())
	}

	// blocker：它的 OnInit 横跨了整个关闭流程，与 victim 的处置必须完全一致——
	// 不许启动，但资源必须被收干净。
	//
	// 这里体现的是一条职责划分：停机流程只负责已完成登记（visible）的模块，
	// 占位期模块归发起添加的那个调用方。反过来做——让 stop 去抢占一个仍在
	// OnInit 的模块——它只能跳过 OnDestroy（不能与 OnInit 并发读写业务内存），
	// 于是 OnInit 已经建好的连接池、占好的端口就再没有任何引用能触及了。
	// 现在由 AddDynamicModules 在 OnInit 返回之后就地回滚，两边都不会漏。
	if blocker.serveCnt.Load() != 0 {
		t.Fatalf("blocker Serve ran %d times, want 0", blocker.serveCnt.Load())
	}
	if blocker.destroyCnt.Load() != 1 || blocker.closeCnt.Load() != 1 {
		t.Fatalf("blocker destroy=%d close=%d, want 1/1 (an initialized module must be rolled back in place, not skipped and leaked)",
			blocker.destroyCnt.Load(), blocker.closeCnt.Load())
	}

	if names := a.DynamicModules(); len(names) != 0 {
		t.Fatalf("DynamicModules = %v, want empty after shutdown", names)
	}
	if a.ChanRPC("hd-victim") != nil || a.ChanRPC("hd-blocker") != nil {
		t.Fatal("no module may stay reachable via ChanRPC after shutdown completed")
	}
}

// TestDynamicModuleInvisibleUntilReady 锁定 moduleWrapper.visible 标志。
//
// 锁住什么：占位期间（名字已被 LoadOrStore 占住，但 OnInit 还没返回）
//   - ChanRPC(name) 必须返回 nil、DynamicModules() 不含它——对外语义仍然是
//     "等模块就绪之后才可寻址"；
//   - 但名字确实已经被占住：同名模块此刻再来一次 AddDynamicModules 会被拒绝，
//     且它的 OnInit 一次都不会执行。
//
// 这两条合起来才是 visible 标志的完整契约：**"名字已占用"与"模块可服务"
// 是两件独立的事**。加固把重名检查提前到 OnInit 之前（否则并发重名无法可靠
// 拦截），代价是模块表里会短暂出现一个尚未就绪的条目；visible 标志负责让这个
// 实现细节不外泄，保证寻址语义与加固前完全一致。
//
// 加固前的表现：占位并不存在，同名模块会照常跑完 OnInit、启动 Serve 并登记，
// 后写入的静默覆盖前一个——本用例的"同名必须被拒绝"一节因此 FAIL。
// （不可寻址那一节在加固前同样成立：那时靠的是"根本还没存进表里"，
// 加固后靠的是 visible 标志，两条路径通向同一个对外语义。）
func TestDynamicModuleInvisibleUntilReady(t *testing.T) {
	const name = "hd-pending"

	a := newApp()

	initEntered := make(chan struct{})
	releaseInit := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseInit) }) }
	t.Cleanup(release)

	pending := newHDModule(name)
	pending.initHook = func() {
		close(initEntered)
		<-releaseInit
	}

	done := make(chan error, 1)
	go func() {
		_, err := a.AddDynamicModules(pending)
		done <- err
	}()
	hdWaitClosed(t, initEntered, "pending OnInit to start")

	// —— 占位窗口内 ——
	if got := a.ChanRPC(name); got != nil {
		t.Fatal("a module that has not become ready must not be addressable via ChanRPC")
	}
	if names := a.DynamicModules(); slices.Contains(names, name) {
		t.Fatalf("DynamicModules = %v, must not list a module that is still initializing", names)
	}

	// 名字却已经被占住：同名模块此刻必须被挡下，且不许执行 OnInit。
	shadow := newHDModule(name)
	shadowResults, shadowErr := a.AddDynamicModules(shadow)
	if shadowErr == nil {
		t.Fatalf("a duplicate name must be rejected during the placeholder window, results = %+v", shadowResults)
	}
	if len(shadowResults) != 1 || shadowResults[0].Err == nil ||
		!strings.Contains(shadowResults[0].Err.Error(), "already exists") {
		t.Fatalf("duplicate rejection = %+v, want a single \"already exists\" failure", shadowResults)
	}
	if shadow.initCnt.Load() != 0 {
		t.Fatalf("rejected duplicate OnInit ran %d times, want 0", shadow.initCnt.Load())
	}
	if shadow.serveCnt.Load() != 0 {
		t.Fatalf("rejected duplicate Serve ran %d times, want 0", shadow.serveCnt.Load())
	}

	// —— 就绪之后 ——
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("AddDynamicModules failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("AddDynamicModules never returned after OnInit was released")
	}

	hdWaitClosed(t, pending.ready, "pending Serve to start")
	if got := a.ChanRPC(name); got != pending.server {
		t.Fatal("a ready dynamic module must be addressable via ChanRPC")
	}
	if names := a.DynamicModules(); !slices.Contains(names, name) {
		t.Fatalf("DynamicModules = %v, want it to contain %q once ready", names, name)
	}

	if !a.RemoveDynamicModule(name) {
		t.Fatal("RemoveDynamicModule should report a complete shutdown")
	}
	hdWaitClosed(t, pending.served, "pending Serve to exit")
	if names := a.DynamicModules(); len(names) != 0 {
		t.Fatalf("DynamicModules after removal = %v, want empty", names)
	}
}

// TestRemoveDynamicModuleReportsIncompleteShutdown 锁定 RemoveDynamicModule 的
// 返回值语义。
//
// 锁住什么：模块的 Serve 不响应 ctx.Done、等待 goroutine 退出超时、框架据此
// 跳过 OnDestroy/Close 时，RemoveDynamicModule 必须返回 **false**。
//
// 为什么重要：返回值是调用方（热加载接口、运维脚本）判断"能不能用同名模块
// 重新加载"的唯一依据。此刻模块已经被摘出模块表，重名检查再也拦不住新实例，
// 而旧实例的 goroutine 还在跑、OnDestroy 永远不会执行——新旧两个实例会同时
// 持有同一份外部资源（监听端口、连接池、分布式锁）。加固前这条路径返回 true，
// 调用方收到的是"清理完成"的假象，恰恰会在最危险的时候放行重新加载。
//
// 加固前的表现：RemoveDynamicModule 无条件 `return true`，
// 即便 OnDestroy/Close 一次都没执行。
//
// 时序如何做到确定：超时由 WithShutdownTimeout 显式设定，而 Serve 在用例
// 主动放行之前**永远**不会退出，因此超时分支是必然发生的，不是概率事件。
func TestRemoveDynamicModuleReportsIncompleteShutdown(t *testing.T) {
	const name = "hd-stuck"

	a := newApp(WithShutdownTimeout(100 * time.Millisecond))

	stuck := newHDModule(name)
	stuck.serveBlock = make(chan struct{})
	// 收尾：这个模块的 goroutine 会在卸载失败后继续残留，用例结束前必须让它
	// 退出，否则会带着一个 LockOSThread 的线程污染后续用例（以及 -race 的报告）。
	t.Cleanup(func() {
		close(stuck.serveBlock)
		select {
		case <-stuck.served:
		case <-time.After(5 * time.Second):
			t.Errorf("stuck module goroutine never exited")
		}
	})

	if _, err := a.AddDynamicModules(stuck); err != nil {
		t.Fatalf("AddDynamicModules failed: %v", err)
	}
	hdWaitClosed(t, stuck.ready, "stuck Serve to start")

	removed := make(chan bool, 1)
	go func() { removed <- a.RemoveDynamicModule(name) }()

	select {
	case ok := <-removed:
		if ok {
			t.Fatal("RemoveDynamicModule reported a complete shutdown although the module never stopped " +
				"and neither OnDestroy nor Close ran")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RemoveDynamicModule ignored the shutdown timeout and blocked")
	}

	// 超时路径必须跳过销毁（宁可漏一次落地，也不要与仍在运行的事件循环并发）。
	if stuck.destroyCnt.Load() != 0 || stuck.closeCnt.Load() != 0 {
		t.Fatalf("destroy=%d close=%d, want 0/0 on the timeout path",
			stuck.destroyCnt.Load(), stuck.closeCnt.Load())
	}
	// 模块确实已经被摘出模块表——这正是返回值必须说实话的原因。
	if names := a.DynamicModules(); len(names) != 0 {
		t.Fatalf("DynamicModules = %v, want empty (the module was already detached)", names)
	}
	if a.ChanRPC(name) != nil {
		t.Fatal("a detached module must not stay reachable via ChanRPC")
	}
	// 再卸载一次同样是 false：模块已经不在表里。
	if a.RemoveDynamicModule(name) {
		t.Fatal("RemoveDynamicModule on a missing module must return false")
	}
}
