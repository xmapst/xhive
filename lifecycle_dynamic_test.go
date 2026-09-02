package xhive

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xmapst/xhive/chanrpc"
)

// dynPingReq 只用来探测 chanrpc.Client 的入口校验结果（check 的返回值），
// 不关心对端是否真的处理它，因此不需要注册任何 handler。
type dynPingReq struct{}

// dynSkeletonModule 内嵌**真实的** *Skeleton，并且刻意不重写 Close。
//
// app_test.go 的 testModule 自带一个只记数的 Close，用它做断言只能证明
// “框架调过某个 Close”，证明不了真实模块的资源到底有没有被释放——真实模块的
// 出站 client 由内嵌的 Skeleton 持有，唯一会关闭它的地方就是 Skeleton.Close。
// 这里让框架调用的就是 Skeleton.Close 本身，于是可以直接观测 client 的关闭状态。
type dynSkeletonModule struct {
	*Skeleton
	// clientClosedInDestroy 记录 OnDestroy 执行的那一刻 client 是否已经关闭。
	// 用来锁住“先 OnDestroy 再 Close”这个顺序：OnDestroy 通常还要用 client
	// 把停机前的最后一批状态投递给尚未关闭的模块，此刻 client 必须仍然可用。
	clientClosedInDestroy atomic.Bool
	destroyed             chan struct{}
}

func newDynSkeletonModule(name string) *dynSkeletonModule {
	return &dynSkeletonModule{
		Skeleton:  NewSkeleton(name, WithTimerChanLen(8), WithServerChanLen(8), WithClientChanLen(8), WithStatCap(32)),
		destroyed: make(chan struct{}),
	}
}

func (m *dynSkeletonModule) OnInit() error { return nil }

func (m *dynSkeletonModule) OnDestroy() {
	m.clientClosedInDestroy.Store(m.client.IsClosed())
	close(m.destroyed)
}

// dynCountModule 是本文件自带的最小 IModule 实现：计数 OnInit/Serve/OnDestroy/Close，
// 并可通过 onEvent 把关闭事件按真实发生顺序写进一条共享序列。
//
// 不复用 app_test.go 的 testModule，是因为这里的用例要同时在“未修复的基线”上
// 跑一遍确认 FAIL，而基线版本的 testModule 还没有 closeCnt/closeHook 这些字段；
// 把测试需要的观测点收在自己的类型里，两边才是同一份测试代码。
type dynCountModule struct {
	name       string
	priority   uint
	server     *chanrpc.Server
	initErr    error
	initCount  atomic.Int32
	runCount   atomic.Int32
	destroyCnt atomic.Int32
	closeCnt   atomic.Int32
	runStarted chan struct{}
	runStopped chan struct{}
	onEvent    func(string)
}

func newDynCountModule(name string) *dynCountModule {
	return &dynCountModule{
		name:       name,
		server:     chanrpc.NewServer(chanrpc.WithChanLen(4)),
		runStarted: make(chan struct{}),
		runStopped: make(chan struct{}),
	}
}

func (m *dynCountModule) Name() string   { return m.name }
func (m *dynCountModule) Priority() uint { return m.priority }

func (m *dynCountModule) OnInit() error {
	m.initCount.Add(1)
	return m.initErr
}

func (m *dynCountModule) Serve(ctx context.Context) {
	m.runCount.Add(1)
	close(m.runStarted)
	<-ctx.Done()
	close(m.runStopped)
}

func (m *dynCountModule) Ready() <-chan struct{} { return m.runStarted }

func (m *dynCountModule) OnDestroy() {
	m.destroyCnt.Add(1)
	m.record("destroy:" + m.name)
	if m.server != nil && !m.server.IsClosed() {
		m.server.Close()
	}
}

func (m *dynCountModule) ChanRPC() *chanrpc.Server { return m.server }

func (m *dynCountModule) Close() error {
	m.closeCnt.Add(1)
	m.record("close:" + m.name)
	return nil
}

func (m *dynCountModule) record(event string) {
	if m.onEvent != nil {
		m.onEvent(event)
	}
}

// TestRemoveDynamicModuleCallsClose 锁住动态模块卸载路径上最容易被漏掉的一步：
// RemoveDynamicModule 必须调用模块的 Close，而不是只做 OnDestroy + 等 goroutine 退出。
//
// 用真实的 Skeleton 模块而不是自带 Close 计数的假模块：Close 对真实模块的意义
// 是“释放出站 chanrpc client”，只有 Skeleton.Close 会做这件事。漏调 Close 的后果
// 不是少了一次回调，而是 client 永远停在 open 状态——它的 pending 异步回调不会被
// 排空（注册的 Callback 再也不会执行，业务状态可能停在半路），而且由于
// Client.check 只看 closed 标志，卸载之后任何还持有该模块引用的代码继续
// Cast/AsyncCall 都会被判定为“合法投递”，而不是立刻拿到 ErrClientClosed。
//
// 顺带锁住顺序：client 必须在 OnDestroy **之后**才关闭，否则 OnDestroy 里
// 向其它模块投递最后一批状态的逻辑会静默失败。
func TestRemoveDynamicModuleCallsClose(t *testing.T) {
	a := newApp()
	m := newDynSkeletonModule("skeleton-dyn")

	if _, err := a.AddDynamicModules(m); err != nil {
		t.Fatalf("AddDynamicModules failed: %v", err)
	}
	if m.client.IsClosed() {
		t.Fatal("client should still be open while the module is serving")
	}

	// probe 是一个独立的、始终打开的 Server。chanrpc.Client.check 的判定顺序是
	// server == nil → server.IsClosed → client.IsClosed，只有对端完全正常时，
	// 拿到 ErrClientClosed 才能证明“失败原因确实是本模块的 client 被关掉了”。
	probe := chanrpc.NewServer(chanrpc.WithChanLen(1))

	if !a.RemoveDynamicModule("skeleton-dyn") {
		t.Fatal("RemoveDynamicModule should return true")
	}
	waitClosed(t, m.destroyed, time.Second)

	if !m.client.IsClosed() {
		t.Fatal("RemoveDynamicModule must call Close: skeleton client is still open after unload")
	}
	err := m.client.AsyncCall(probe, dynPingReq{}, func(*chanrpc.RetInfo) {})
	if !errors.Is(err, chanrpc.ErrClientClosed) {
		t.Fatalf("AsyncCall after unload err = %v, want %v", err, chanrpc.ErrClientClosed)
	}
	if m.clientClosedInDestroy.Load() {
		t.Fatal("client must stay open during OnDestroy, it is closed only afterwards by Close")
	}
	// 事件循环退出时由 Skeleton.Serve 的 ctx.Done 分支关掉 server，
	// 与 client 分两步释放，这里一并确认 server 侧确实已经收口。
	if !m.ChanRPC().IsClosed() {
		t.Fatal("server should be closed once the event loop exits")
	}
}

// TestDynamicModuleOnInitFailureGetsNoDestroy 锁住动态路径与静态路径在
// “OnInit 失败的模块该不该销毁”上的一致性：OnInit 返回 error 的模块按
// IModule.OnInit 的契约已经自行回滚过，框架不得再调用 OnDestroy/Close
// （否则业务会在一个半构造的对象上执行清理：落一份空快照覆盖好数据、
// 从注册中心摘掉一个自己从未注册过的实例……这些都不会 panic，因此
// destroyModule 的 recover 也发现不了），同时它既不该被 Serve，也不该
// 出现在 ChanRPC / DynamicModules 的可见集合里。
//
// 用例最后走一次完整的动态模块停机：正常模块此时才各拿到一次 OnDestroy 和
// 一次 Close（成对出现），而失败模块的两个计数必须始终为 0——把“不销毁”从
// “Add 返回那一刻还没销毁”加强为“直到应用关完也没有销毁”。
//
// 关于“失败模块的 context 被 cancel”：这一步无法从外部观测。newModuleWrapper
// 用的是 context.WithCancel(context.Background())，而 Background().Done() 为 nil，
// propagateCancel 因此既不会把子节点挂到父节点上，也不会起监视 goroutine；
// 失败的 wrapper 又不会被存进 dynamicModules，测试拿不到它的 ctx。也就是说
// 漏掉 cancel 在运行期没有任何可观测差异（goroutine 数、context 树都一样），
// 只能靠代码审查保证。这里断言的是它可观测的另一半：失败模块被彻底丢弃，
// 不进入任何后续生命周期回调。
func TestDynamicModuleOnInitFailureGetsNoDestroy(t *testing.T) {
	a := newApp()
	ok1 := newDynCountModule("dyn-init-ok1")
	bad := newDynCountModule("dyn-init-bad")
	bad.initErr = errors.New("boom")
	ok2 := newDynCountModule("dyn-init-ok2")

	// 三者同优先级，AddDynamicModules 使用稳定排序，处理顺序即传入顺序。
	results, err := a.AddDynamicModules(ok1, bad, ok2)
	if err == nil {
		t.Fatal("AddDynamicModules should report the failing module")
	}
	if len(results) != 3 {
		t.Fatalf("results length = %d, want 3", len(results))
	}
	if results[1].Name != "dyn-init-bad" || results[1].Err == nil {
		t.Fatalf("results[1] = %+v, want dyn-init-bad failure", results[1])
	}
	waitClosed(t, ok1.runStarted, time.Second)
	waitClosed(t, ok2.runStarted, time.Second)

	if bad.initCount.Load() != 1 {
		t.Fatalf("bad OnInit count = %d, want 1", bad.initCount.Load())
	}
	if bad.runCount.Load() != 0 {
		t.Fatalf("bad Serve count = %d, want 0 (init failed)", bad.runCount.Load())
	}
	if bad.destroyCnt.Load() != 0 || bad.closeCnt.Load() != 0 {
		t.Fatalf("bad: destroy=%d close=%d, want 0/0 right after the failed add",
			bad.destroyCnt.Load(), bad.closeCnt.Load())
	}
	if a.ChanRPC("dyn-init-bad") != nil {
		t.Fatal("failed dynamic module must not be reachable via ChanRPC")
	}
	if names := a.DynamicModules(); slices.Contains(names, "dyn-init-bad") {
		t.Fatalf("DynamicModules = %v, must not contain the failed module", names)
	}

	// 完整关掉这一批动态模块：成功的模块此刻走完整条销毁链，失败的模块不受影响。
	a.removeAllDynamicModules()
	waitClosed(t, ok1.runStopped, time.Second)
	waitClosed(t, ok2.runStopped, time.Second)

	if bad.destroyCnt.Load() != 0 || bad.closeCnt.Load() != 0 {
		t.Fatalf("bad: destroy=%d close=%d, want 0/0 after shutdown",
			bad.destroyCnt.Load(), bad.closeCnt.Load())
	}
	for _, m := range []*dynCountModule{ok1, ok2} {
		if m.destroyCnt.Load() != 1 || m.closeCnt.Load() != 1 {
			t.Fatalf("%s: destroy=%d close=%d, want 1/1 (OnInit succeeded)",
				m.name, m.destroyCnt.Load(), m.closeCnt.Load())
		}
	}
}

// TestRemoveDynamicModuleUnknownName 锁住 RemoveDynamicModule 的两条边界语义：
//
//  1. 卸载一个不存在的名字返回 false，且不产生任何副作用——尤其不能误伤
//     其它正在运行的动态模块（把名字打错就停掉别的模块，是热加载接口最不该有的行为）。
//  2. dynamicModules 里若混进了非 *moduleWrapper 的值（框架自己不会写入这种值，
//     属于防御性分支），仍然返回 false，但该键会被 LoadAndDelete 顺手摘掉。
//     这是把 Load+Delete 合并成 LoadAndDelete 之后的行为变化，重要之处在于：
//     这种污染值只要还留在 map 里，就会一直出现在 DynamicModules() 的列表里，
//     而 removeAllDynamicModules 的 Range 又会跳过它（类型断言失败），
//     于是它变成一个永远清不掉的幽灵条目。
func TestRemoveDynamicModuleUnknownName(t *testing.T) {
	a := newApp()
	live := newDynCountModule("dyn-live")
	if _, err := a.AddDynamicModules(live); err != nil {
		t.Fatalf("AddDynamicModules failed: %v", err)
	}
	waitClosed(t, live.runStarted, time.Second)

	if a.RemoveDynamicModule("dyn-missing") {
		t.Fatal("RemoveDynamicModule on an unknown name should return false")
	}
	assertDynLiveUntouched(t, a, live, "after removing an unknown name")

	// 非 *moduleWrapper 的值：断言失败仍返回 false，但键必须已经被摘除。
	a.dynamicModules.Store("dyn-broken", "not-a-wrapper")
	if a.RemoveDynamicModule("dyn-broken") {
		t.Fatal("RemoveDynamicModule should return false for a non-wrapper value")
	}
	if _, ok := a.dynamicModules.Load("dyn-broken"); ok {
		t.Fatal("a non-wrapper value must be removed from dynamicModules, otherwise it stays visible forever")
	}
	assertDynLiveUntouched(t, a, live, "after removing a non-wrapper value")

	// 正常卸载仍然可用，证明前面两次失败的卸载没有把状态搞坏。
	if !a.RemoveDynamicModule("dyn-live") {
		t.Fatal("RemoveDynamicModule should return true for a live module")
	}
	waitClosed(t, live.runStopped, time.Second)
	if live.destroyCnt.Load() != 1 || live.closeCnt.Load() != 1 {
		t.Fatalf("dyn-live: destroy=%d close=%d, want 1/1", live.destroyCnt.Load(), live.closeCnt.Load())
	}
}

// assertDynLiveUntouched 检查 dyn-live 仍然完好：没被销毁、没被摘除、Serve 还在跑。
func assertDynLiveUntouched(t *testing.T, a *app, live *dynCountModule, when string) {
	t.Helper()
	if live.destroyCnt.Load() != 0 || live.closeCnt.Load() != 0 {
		t.Fatalf("%s: dyn-live destroy=%d close=%d, want 0/0",
			when, live.destroyCnt.Load(), live.closeCnt.Load())
	}
	if names := a.DynamicModules(); !slices.Equal(names, []string{"dyn-live"}) {
		t.Fatalf("%s: DynamicModules = %v, want [dyn-live]", when, names)
	}
	if a.ChanRPC("dyn-live") != live.server {
		t.Fatalf("%s: dyn-live is no longer reachable via ChanRPC", when)
	}
	select {
	case <-live.runStopped:
		t.Fatalf("%s: dyn-live Serve exited, it should still be running", when)
	default:
	}
}

// TestDynamicAndStaticShutdownOrder 用一条显式事件序列锁住停机顺序的两个层次：
//
//   - 动态模块整体先于静态模块关闭：动态模块通常依赖静态模块提供的服务
//     （配置、存储、连接池），静态模块先走 OnDestroy 会让还在跑的动态模块
//     对着已释放的资源发消息。
//   - 动态模块内部按 (Priority, seq) 倒序，即与初始化顺序严格相反，
//     与静态模块的 LIFO 语义一致；seq 是必须的，因为 sync.Map.Range
//     不保证遍历顺序，既不能拿 Range 的顺序当添加顺序，也不能退化成按名称排序。
//
// 序列里同时记录 OnDestroy 和 Close，因为“关闭顺序正确”本身就包含
// “每个模块都走完了完整的两段式关闭”——只有 OnDestroy 没有 Close 的模块，
// 它的出站 client 从未释放，等于关了一半。
//
// 名称刻意让字典序与期望顺序错开（aaa-dyn 后添加却应先关、zzz-dyn 先添加却应后关），
// 用来区分“按添加序号 tie-break”和“按名称 tie-break”两种实现。
func TestDynamicAndStaticShutdownOrder(t *testing.T) {
	a := newApp()

	var mu sync.Mutex
	var events []string
	record := func(event string) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, event)
	}

	// 静态模块：static-low(0) 先于 static-high(1) 初始化，关闭时倒序。
	staticLow := newDynCountModule("static-low")
	staticHigh := newDynCountModule("static-high")
	staticHigh.priority = 1

	// 动态模块：priority=9 的 high-dyn 单独一批先加（seq=1），
	// 随后同优先级的 zzz-dyn、aaa-dyn 一批加入（seq=2、3）。
	// 初始化顺序按 (Priority, seq) 升序：zzz-dyn → aaa-dyn → high-dyn，
	// 因此关闭顺序应为 high-dyn → aaa-dyn → zzz-dyn。
	highDyn := newDynCountModule("high-dyn")
	highDyn.priority = 9
	zzzDyn := newDynCountModule("zzz-dyn")
	aaaDyn := newDynCountModule("aaa-dyn")

	all := []*dynCountModule{staticLow, staticHigh, highDyn, zzzDyn, aaaDyn}
	for _, m := range all {
		m.onEvent = record
	}

	if !a.start(staticLow, staticHigh) {
		t.Fatal("start should succeed")
	}
	if _, err := a.AddDynamicModules(highDyn); err != nil {
		t.Fatalf("AddDynamicModules(highDyn) failed: %v", err)
	}
	if _, err := a.AddDynamicModules(zzzDyn, aaaDyn); err != nil {
		t.Fatalf("AddDynamicModules(zzzDyn, aaaDyn) failed: %v", err)
	}
	for _, m := range all {
		waitClosed(t, m.runStarted, time.Second)
	}

	a.stop()
	for _, m := range all {
		waitClosed(t, m.runStopped, time.Second)
	}

	want := []string{
		"destroy:high-dyn", "close:high-dyn",
		"destroy:aaa-dyn", "close:aaa-dyn",
		"destroy:zzz-dyn", "close:zzz-dyn",
		"destroy:static-high", "close:static-high",
		"destroy:static-low", "close:static-low",
	}
	mu.Lock()
	got := slices.Clone(events)
	mu.Unlock()
	if !slices.Equal(got, want) {
		t.Fatalf("shutdown event sequence =\n  %v\nwant\n  %v", got, want)
	}
}
