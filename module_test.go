package xhive

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xmapst/xhive/chanrpc"
)

func TestAppRegisterNilStatsNoRPCAndStopIdempotent(t *testing.T) {
	a := newApp()
	noRPC := newTestModule("no-rpc")
	noRPC.server = nil
	if err := a.Register(nil, noRPC); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if len(a.modules) != 1 {
		t.Fatalf("registered module count = %d, want 1", len(a.modules))
	}
	if got := a.ChanRPC("no-rpc"); got != nil {
		t.Fatalf("ChanRPC for no-rpc = %#v, want nil", got)
	}
	stats := a.Stats()
	if !strings.Contains(stats, "static: no-rpc, rpc_queue_length: N/A") {
		t.Fatalf("Stats missing N/A rpc length: %s", stats)
	}

	a.stop()
	if a.State() != AppStateNone {
		t.Fatalf("stop before start state = %d, want none", a.State())
	}
	if !a.start() {
		// 空启动应失败且仍保持 None，随后再次 stop 应保持幂等且不 panic。
		if a.State() != AppStateNone {
			t.Fatalf("empty start state = %d, want none", a.State())
		}
		a.stop()
	}
}

func TestAppStartCannotStartTwiceAndStopOrder(t *testing.T) {
	a := newApp()
	order := make(chan string, 4)
	m1 := newTestModule("first")
	m2 := newTestModule("second")
	m1.destroyHook = func() { order <- "first" }
	m2.destroyHook = func() { order <- "second" }

	if !a.start(m1, m2) {
		t.Fatal("start should succeed")
	}
	waitClosed(t, m1.runStarted, time.Second)
	waitClosed(t, m2.runStarted, time.Second)
	if a.start(newTestModule("again")) {
		t.Fatal("start twice should fail")
	}

	a.stop()
	want := []string{"second", "first"}
	got := []string{<-order, <-order}
	if !slices.Equal(got, want) {
		t.Fatalf("destroy order = %v, want %v", got, want)
	}
	a.setState(AppStateStop)
	a.stop()
	if a.State() != AppStateStop {
		t.Fatalf("stop while stopping state = %d, want stop", a.State())
	}
}

// TestOnDestroyCanCastToModuleShuttingDownLater 是"client 要在 OnDestroy 之后
// 才关闭"这条约束的直接回归测试：LIFO 停机顺序下，先关闭的模块在自己的
// OnDestroy 里必须仍然能把最后一批数据 Cast 给还没轮到关闭的模块——典型
// 场景是某个模块停机时要把汇总状态转投给专门负责收尾处理的模块（这类
// 收尾模块通常注册在最前面，因此按 LIFO 顺序最后关闭）。若 Skeleton.Close
// 提前关掉了 client，这次 Cast 会静默失败，sink 永远收不到消息。
func TestOnDestroyCanCastToModuleShuttingDownLater(t *testing.T) {
	oldDefault := defaultApp
	defaultApp = newApp()
	defer func() { defaultApp = oldDefault }()

	received := make(chan string, 1)
	sink := newSkeletonTestModule("sink")
	sink.onInit = func(m *skeletonTestModule) error {
		return m.RegisterChanRPC(skeletonRPCReq{}, func(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
			received <- ci.Request.(skeletonRPCReq).Value
			return nil
		})
	}

	// sink 注册在前，按 LIFO 停机顺序最后关闭；source 的 OnDestroy 执行时
	// sink 应仍在正常运行。
	source := newSkeletonTestModule("source")
	source.onDestroy = func() {
		source.Cast("sink", skeletonRPCReq{Value: "final flush"})
	}

	if !defaultApp.start(sink, source) {
		t.Fatal("start should succeed")
	}
	defaultApp.stop()

	select {
	case got := <-received:
		if got != "final flush" {
			t.Fatalf("received = %q, want %q", got, "final flush")
		}
	case <-time.After(time.Second):
		t.Fatal("sink did not receive the Cast sent from source's OnDestroy")
	}
}

func TestAppDynamicPartialFailureKeepsStartedModulesAndRemoveAll(t *testing.T) {
	a := newApp()
	good := newTestModule("good-dyn")
	bad := newTestModule("bad-dyn")
	bad.initErr = errors.New("bad init")

	// good/bad 优先级相同（默认值 0），AddDynamicModules 对同优先级模块采用稳定排序，
	// 保留传入顺序，因此处理顺序与结果顺序都与 AddDynamicModules(good, bad) 一致。
	results, err := a.AddDynamicModules(good, bad)
	if err == nil {
		t.Fatal("AddDynamicModules should fail when later module init fails")
	}
	if len(results) != 2 {
		t.Fatalf("results length = %d, want 2", len(results))
	}
	if results[0].Name != "good-dyn" || results[0].Err != nil {
		t.Fatalf("results[0] = %+v, want good-dyn success", results[0])
	}
	if results[1].Name != "bad-dyn" || results[1].Err == nil {
		t.Fatalf("results[1] = %+v, want bad-dyn failure", results[1])
	}
	waitClosed(t, good.runStarted, time.Second)
	if a.ChanRPC("good-dyn") != good.server {
		t.Fatal("successfully started dynamic module should remain stored")
	}
	if a.ChanRPC("bad-dyn") != nil {
		t.Fatal("failed dynamic module should not be stored")
	}

	a.removeAllDynamicModules()
	waitClosed(t, good.runStopped, time.Second)
	if len(a.DynamicModules()) != 0 {
		t.Fatalf("dynamic modules after removeAll = %v, want empty", a.DynamicModules())
	}

	a.dynamicModules.Store("broken", "not-wrapper")
	if a.RemoveDynamicModule("broken") {
		t.Fatal("RemoveDynamicModule should return false for non-wrapper value")
	}
	a.dynamicModules.Delete("broken")
}

// TestRemoveAllDynamicModulesReversesPriorityOrder 验证 removeAllDynamicModules
// 按优先级倒序关闭：初始化按优先级升序（同优先级按 AddDynamicModules 的传入顺序，
// 即 moduleWrapper.seq 升序），关闭顺序应与之完全相反，后添加的模块先关闭，
// 与静态模块的 LIFO 停机语义保持一致。
//
// mid1/mid2 故意使用与传入顺序相反的名称字典序（mid1="zzz-first" 先传入但字典序更大，
// mid2="aaa-second" 后传入但字典序更小），用来区分"按添加顺序 tie-break"与
// "按名称字典序 tie-break"两种实现——如果 removeAllDynamicModules 误用名称排序，
// 这里的期望顺序会先失败。
func TestRemoveAllDynamicModulesReversesPriorityOrder(t *testing.T) {
	a := newApp()

	low := newTestModule("low")
	low.priority = 1
	mid1 := newTestModule("zzz-first")
	mid1.priority = 5
	mid2 := newTestModule("aaa-second")
	mid2.priority = 5
	high := newTestModule("high")
	high.priority = 10

	var order []string
	for _, m := range []*testModule{low, mid1, mid2, high} {
		name := m.name
		m.destroyHook = func() { order = append(order, name) }
	}

	// 传入顺序：high, low, mid1(zzz-first), mid2(aaa-second)。
	if _, err := a.AddDynamicModules(high, low, mid1, mid2); err != nil {
		t.Fatalf("AddDynamicModules failed: %v", err)
	}

	a.removeAllDynamicModules()

	// 初始化顺序（优先级升序，同优先级按传入顺序）为 low, zzz-first, aaa-second, high；
	// 倒序关闭顺序应为 high, aaa-second, zzz-first, low。
	want := []string{"high", "aaa-second", "zzz-first", "low"}
	if !slices.Equal(order, want) {
		t.Fatalf("destroy order = %v, want %v", order, want)
	}
}
