package xhive

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xmapst/xhive/chanrpc"
)

// ---------------------------------------------------------------------------
// 用例 1：占位期间被 RemoveDynamicModule 抢走销毁权之后，
// startServing 的 setLifecycle 直写把已经 Destroyed 的模块拉回 Serving，
// 并在 OnDestroy/Close 之后重新拉起 Serve。
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
	m.initHook = func() {
		rec("OnInit")
		close(initReturning)
	}
	m.destroyHook = func() { rec("OnDestroy") }
	m.closeHook = func() { rec("Close") }
	m.serveHook = func(ctx context.Context) bool {
		rec("Serve")
		return true
	}

	sniperDone := make(chan struct{})
	var removed bool
	var sawInited bool
	go func() {
		defer close(sniperDone)
		<-initReturning
		// 自旋等待占位推进到 lifecycleInited，然后用**公开 API**卸载它。
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
		t.Skip("未命中 Inited → startServing 窗口，重跑 -count 更多次")
	}

	mu.Lock()
	seq := strings.Join(events, ",")
	mu.Unlock()

	t.Logf("events=%s addErr=%v results=%+v removed=%v destroyCnt=%d closeCnt=%d runCount=%d dyn=%v",
		seq, err, results, removed, m.destroyCnt.Load(), m.closeCnt.Load(), m.runCount.Load(), a.DynamicModules())

	if value, ok := a.dynamicModules.Load("victim"); ok {
		t.Logf("still in map: %v", value)
	}

	if strings.Contains(seq, "OnDestroy,Close,Serve") {
		t.Errorf("Serve 在 OnDestroy/Close 之后才被拉起（use-after-destroy）: %s", seq)
	}
	if err == nil && m.destroyCnt.Load() == 1 {
		t.Errorf("AddDynamicModules 报告成功，但模块已被 OnDestroy 销毁: results=%+v", results)
	}
}

// ---------------------------------------------------------------------------
// 用例 2：removeAllDynamicModules 的排序比较器读取占位条目的
// wrapper.seq（普通 uint64 字段），与 AddDynamicModules 写入 seq 无任何同步。
// 两条路径同为 gate 的兄弟分支，彼此之间没有 happens-before。
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
