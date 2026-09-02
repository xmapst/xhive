package xhive

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xmapst/xhive/chanrpc"
)

// ---------------------------------------------------------------------------
// 用例 1：占位期间被 RemoveDynamicModule 抢走销毁权之后，
// startServing 的 setLifecycle 直写把已经 Destroyed 的模块拉回 Serving，
// 并在 OnDestroy/Close 之后重新拉起 Serve。
// ---------------------------------------------------------------------------

func TestPlaceholderSeqHasNoDataRace(t *testing.T) {
	a := newApp()
	base := newTestModule("base")
	if !a.start(base) {
		t.Fatal("start should succeed")
	}
	defer a.stop()

	// 先放一个已就绪的动态模块，保证 removeAllDynamicModules 的排序
	// 真的会调用比较器（单元素切片不会比较）。
	peer := newTestModule("peer")
	if _, err := a.AddDynamicModules(peer); err != nil {
		t.Fatalf("AddDynamicModules(peer) failed: %v", err)
	}

	gate := make(chan struct{})
	victim := newTestModule("victim")
	victim.serveHook = func(ctx context.Context) bool {
		<-gate // Serve 到这里才关闭 ready，于是 Add 卡在 waitReady 上
		close(victim.runStarted)
		<-ctx.Done()
		close(victim.runStopped)
		return false
	}

	addDone := make(chan struct{})
	go func() {
		defer close(addDone)
		_, _ = a.AddDynamicModules(victim)
	}()

	// 等 victim 的占位进入 lifecycleServing 且 Add 卡在 waitReady。
	deadline := time.Now().Add(2 * time.Second)
	for {
		value, ok := a.dynamicModules.Load("victim")
		if ok {
			if w, ok := value.(*moduleWrapper); ok && w.lifecycleState() == lifecycleServing {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("victim placeholder never reached serving")
		}
		time.Sleep(time.Millisecond)
	}

	sweepDone := make(chan struct{})
	go func() {
		defer close(sweepDone)
		<-gate
		a.removeAllDynamicModules() // Range → SortStableFunc → 读 victim.seq
	}()

	// 两条分支同为 gate 的兄弟：主 goroutine → 各自有 HB，
	// 但它们彼此之间没有任何同步，seq 的读/写就此并发。
	close(gate)

	<-addDone
	<-sweepDone
}

// ---------------------------------------------------------------------------
// 用例 3：stop 的 removeAllDynamicModules 扫到仍在 OnInit 的占位，
// 把本可以干净回滚（完整 OnDestroy+Close）的场景降级成"OnInit 成功但
// OnDestroy 永不执行"的确定性泄漏。
// ---------------------------------------------------------------------------

func TestPlaceholderRolledBackCleanlyDuringShutdown(t *testing.T) {
	a := newApp()
	base := newTestModule("base")
	if !a.start(base) {
		t.Fatal("start should succeed")
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	victim := newTestModule("victim")
	victim.initHook = func() {
		close(entered)
		<-release // OnInit 建连接池/拉配置，耗时数百毫秒
	}

	addDone := make(chan struct{})
	var addErr error
	go func() {
		defer close(addDone)
		_, addErr = a.AddDynamicModules(victim)
	}()

	<-entered // 占位已落进 dynamicModules，状态 lifecycleIniting

	stopDone := make(chan struct{})
	go func() {
		defer close(stopDone)
		a.stop()
	}()

	// 给 stop 足够时间跑完两轮 removeAllDynamicModules（会 claim 住占位）。
	time.Sleep(200 * time.Millisecond)
	close(release)

	<-addDone
	<-stopDone

	t.Logf("addErr=%v initCount=%d destroyCnt=%d closeCnt=%d runCount=%d",
		addErr, victim.initCount.Load(), victim.destroyCnt.Load(),
		victim.closeCnt.Load(), victim.runCount.Load())

	if victim.initCount.Load() != 1 {
		t.Fatalf("OnInit count = %d, want 1", victim.initCount.Load())
	}
	if victim.destroyCnt.Load() != 1 {
		t.Errorf("OnInit 成功返回却没有收到 OnDestroy（资源泄漏）: destroyCnt=%d", victim.destroyCnt.Load())
	}
}

// ---------------------------------------------------------------------------
// 用例 4：Stats() 没有跟随 getChanRPCDynamic / DynamicModules 一起
// 检查 visible，会在占位期间对仍在 OnInit 的模块调用 ChanRPC()。
// ---------------------------------------------------------------------------

// lateServerModule 在 OnInit 里才创建自己的 ChanRPC 服务端——
// 占位方案之前这是安全的（模块在 OnInit 之后才进入 dynamicModules）。
type lateServerModule struct {
	name       string
	server     *chanrpc.Server // 普通字段，OnInit 中写入
	gate       chan struct{}
	entered    chan struct{}
	runStarted chan struct{}
	runStopped chan struct{}
}

func (m *lateServerModule) Name() string   { return m.name }
func (m *lateServerModule) Priority() uint { return 0 }
func (m *lateServerModule) OnInit() error {
	close(m.entered)
	<-m.gate
	m.server = chanrpc.NewServer(chanrpc.WithChanLen(4))
	return nil
}
func (m *lateServerModule) Serve(ctx context.Context) {
	close(m.runStarted)
	<-ctx.Done()
	close(m.runStopped)
}
func (m *lateServerModule) Ready() <-chan struct{} { return m.runStarted }
func (m *lateServerModule) OnDestroy()             {}
func (m *lateServerModule) ChanRPC() *chanrpc.Server {
	return m.server
}
func (m *lateServerModule) Close() error { return nil }

func TestStatsSkipsPlaceholderDuringOnInit(t *testing.T) {
	a := newApp()
	base := newTestModule("base")
	if !a.start(base) {
		t.Fatal("start should succeed")
	}
	defer a.stop()

	m := &lateServerModule{
		name:       "late",
		gate:       make(chan struct{}),
		entered:    make(chan struct{}),
		runStarted: make(chan struct{}),
		runStopped: make(chan struct{}),
	}

	addDone := make(chan struct{})
	go func() {
		defer close(addDone)
		_, _ = a.AddDynamicModules(m)
	}()

	<-m.entered // 占位已在 map 里，OnInit 尚未写 m.server

	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		<-m.gate
		_ = a.Stats() // Range → wrapper.ChanRPC() → 读 m.server
	}()

	close(m.gate) // 两条分支同为 gate 的兄弟，彼此无 HB

	<-statsDone
	<-addDone
}

// ---------------------------------------------------------------------------

func TestPlaceholderNotResurrectedAfterDestroy(t *testing.T) {
	a := newApp()
	base := newTestModule("base")
	if !a.start(base) {
		t.Fatal("start should succeed")
	}
	defer a.stop()

	var mu sync.Mutex
	var events []string
	rec := func(s string) {
		mu.Lock()
		events = append(events, s)
		mu.Unlock()
	}

	m := newTestModule("victim")
	m.keepServerOpen = true

	initReturning := make(chan struct{})
	spinnerHot := make(chan struct{})
	m.initHook = func() {
		rec("OnInit")
		close(initReturning)
		<-spinnerHot // 等卸载方先进入自旋，OnInit 再返回
	}

	serveExited := make(chan struct{})
	m.serveHook = func(ctx context.Context) bool {
		rec("Serve")
		close(m.runStarted) // = Ready()，放行 waitReady
		<-ctx.Done()
		time.Sleep(100 * time.Millisecond) // 事件循环仍然活着
		close(serveExited)
		close(m.runStopped)
		return false
	}
	m.destroyHook = func() {
		select {
		case <-serveExited:
			rec("OnDestroy(serve已退出)")
		default:
			rec("OnDestroy(SERVE仍在运行)")
		}
	}
	m.closeHook = func() { rec("Close") }

	sniperDone := make(chan struct{})
	var removed, sawInited bool
	go func() {
		defer close(sniperDone)
		<-initReturning
		close(spinnerHot)
		// 自旋等占位推进到 lifecycleInited，然后用公开 API 卸载它。
		// 这正是"热加载接口并发 add/remove 同名模块"的真实形态。
		for {
			value, ok := a.dynamicModules.Load("victim")
			if !ok {
				return
			}
			w, ok := value.(*moduleWrapper)
			if !ok {
				return
			}
			switch w.lifecycleState() {
			case lifecycleRegistered, lifecycleIniting:
				continue
			case lifecycleInited:
				sawInited = true
				removed = a.RemoveDynamicModule("victim")
				return
			default:
				return // 没抢到窗口
			}
		}
	}()

	results, err := a.AddDynamicModules(m)
	<-sniperDone

	if !sawInited {
		t.Skip("未命中 Inited → startServing 窗口，加大 -count 重跑")
	}

	// 让被复活的 Serve 走完，events 才完整
	select {
	case <-serveExited:
	case <-time.After(2 * time.Second):
	}

	mu.Lock()
	seq := strings.Join(events, ",")
	mu.Unlock()

	value, stillInMap := a.dynamicModules.Load("victim")
	finalState := lifecycleRegistered
	if w, ok := value.(*moduleWrapper); ok {
		finalState = w.lifecycleState()
	}
	t.Logf("events=[%s] addErr=%v results=%+v removed=%v destroyCnt=%d closeCnt=%d runCount=%d dyn=%v stillInMap=%v final=%s",
		seq, err, results, removed, m.destroyCnt.Load(), m.closeCnt.Load(),
		m.runCount.Load(), a.DynamicModules(), stillInMap, finalState)

	if strings.Contains(seq, "OnDestroy(SERVE仍在运行)") {
		t.Errorf("OnDestroy 与仍在运行的事件循环并发（真实模块即 fatal）: [%s]", seq)
	}
	if err == nil && m.destroyCnt.Load() == 1 {
		t.Errorf("AddDynamicModules 报告成功，但模块已被 OnDestroy/Close 销毁: results=%+v", results)
	}
	if m.runCount.Load() > 0 && m.destroyCnt.Load() > 0 {
		t.Errorf("同一个模块既被销毁又被拉起了事件循环: runCount=%d destroyCnt=%d",
			m.runCount.Load(), m.destroyCnt.Load())
	}
}

// 用例 1b：claimShutdown 抢到 lifecycleInited 并不代表"没有 goroutine 需要等待"。
// shutdownModule 的 inited 分支不做任何等待，而 startServing 的 setLifecycle 直写
// 会在抢占之后照样把事件循环拉起来 —— OnDestroy 于是与 Serve 并发。
// 自旋只是把生产环境里那个纳秒级窗口放大，抢占本身走的仍是 RemoveDynamicModule
// 的两步（先摘表、再 shutdownModule）。
func TestInitedBranchDoesNotWaitForServe(t *testing.T) {
	a := newApp()
	base := newTestModule("base")
	if !a.start(base) {
		t.Fatal("start should succeed")
	}
	defer a.stop()

	m := newTestModule("victim")
	m.keepServerOpen = true

	initReturning := make(chan struct{})
	spinnerHot := make(chan struct{})
	m.initHook = func() {
		close(initReturning)
		<-spinnerHot
	}

	var serveRunning atomic.Bool
	var destroyedWhileServing atomic.Bool
	m.serveHook = func(ctx context.Context) bool {
		serveRunning.Store(true)
		close(m.runStarted)
		<-ctx.Done()
		time.Sleep(80 * time.Millisecond) // 事件循环仍在跑
		serveRunning.Store(false)
		close(m.runStopped)
		return false
	}
	m.destroyHook = func() {
		if serveRunning.Load() {
			destroyedWhileServing.Store(true)
		}
	}

	var claimed moduleLifecycle = -1
	sniperDone := make(chan struct{})
	go func() {
		defer close(sniperDone)
		<-initReturning
		value, ok := a.dynamicModules.Load("victim")
		if !ok {
			return
		}
		w := value.(*moduleWrapper)
		// RemoveDynamicModule 第一步：把它摘出模块表
		a.dynamicModules.CompareAndDelete("victim", w)
		close(spinnerHot)
		for {
			switch w.lifecycleState() {
			case lifecycleRegistered, lifecycleIniting:
				continue
			case lifecycleInited:
				claimed = lifecycleInited
				a.shutdownModule(w) // RemoveDynamicModule 第二步
				return
			default:
				claimed = w.lifecycleState()
				return
			}
		}
	}()

	_, _ = a.AddDynamicModules(m)
	<-sniperDone
	select {
	case <-m.runStopped:
	case <-time.After(time.Second):
	}

	t.Logf("claimed=%s runCount=%d destroyCnt=%d destroyedWhileServing=%v",
		claimed, m.runCount.Load(), m.destroyCnt.Load(), destroyedWhileServing.Load())

	if claimed != lifecycleInited {
		t.Skip("没抢到 Inited 窗口，加大 -count 重跑")
	}
	if destroyedWhileServing.Load() {
		t.Errorf("OnDestroy 与仍在运行的事件循环并发：真实模块在这里就是不可 recover 的 fatal")
	}
}

// ---------------------------------------------------------------------------

func TestNoOrphanWhenStopRunsDuringWaitReady(t *testing.T) {
	a := newApp()
	base := newTestModule("base")
	if !a.start(base) {
		t.Fatal("start should succeed")
	}

	victim := newTestModule("victim")
	victim.serveHook = func(ctx context.Context) bool {
		// 事件循环起步慢（真实模块里就是建表、预热缓存、注册 handler）
		time.Sleep(400 * time.Millisecond)
		close(victim.runStarted) // = Ready()
		<-ctx.Done()
		close(victim.runStopped)
		return false
	}

	addDone := make(chan struct{})
	var addErr error
	go func() {
		defer close(addDone)
		_, addErr = a.AddDynamicModules(victim)
	}()

	// 等 Add 走到 startServing 之后、卡在 waitReady 上
	deadline := time.Now().Add(3 * time.Second)
	for {
		if value, ok := a.dynamicModules.Load("victim"); ok {
			if w, ok := value.(*moduleWrapper); ok && w.lifecycleState() == lifecycleServing {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("victim never reached serving")
		}
		time.Sleep(time.Millisecond)
	}

	a.stop() // 两轮 removeAllDynamicModules 都跳过 invisible 的 victim
	<-addDone

	select {
	case <-victim.runStopped:
	case <-time.After(500 * time.Millisecond):
	}

	t.Logf("addErr=%v state=%d destroyCnt=%d closeCnt=%d runCount=%d dyn=%v",
		addErr, a.State(), victim.destroyCnt.Load(), victim.closeCnt.Load(),
		victim.runCount.Load(), a.DynamicModules())

	if victim.runCount.Load() == 0 {
		t.Skip("Serve 未被拉起，没进入待测窗口")
	}
	if victim.destroyCnt.Load() != 1 {
		t.Errorf("stop 已宣告 shutdown complete，但 victim 从未 OnDestroy（孤儿模块）: destroyCnt=%d",
			victim.destroyCnt.Load())
	}
	select {
	case <-victim.runStopped:
	default:
		t.Errorf("victim 的 Serve goroutine 在 stop 之后仍未退出（goroutine + LockOSThread 线程泄漏）")
	}
}
