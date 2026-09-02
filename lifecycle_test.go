package xhive

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/xmapst/xhive/chanrpc"
)

// TestStartFailureDestroysOnlyInitializedModules 锁定本次修复的核心契约：
// OnInit 失败导致启动中止时，框架只对 OnInit 已成功的模块调用 OnDestroy/Close；
// OnInit 返回 error 的模块（半初始化，自己负责回滚）和排在它之后、
// OnInit 一次都没被调用过的模块，都不会收到任何销毁回调。
func TestStartFailureDestroysOnlyInitializedModules(t *testing.T) {
	a := newApp()
	ok1 := newTestModule("ok1")
	ok2 := newTestModule("ok2")
	bad := newTestModule("bad")
	bad.initErr = errors.New("boom")
	never1 := newTestModule("never1")
	never2 := newTestModule("never2")

	// Priority 决定 OnInit 顺序：ok1 ok2 bad never1 never2
	for i, m := range []*testModule{ok1, ok2, bad, never1, never2} {
		m.priority = uint(i)
	}

	if a.start(ok1, ok2, bad, never1, never2) {
		t.Fatal("start should fail")
	}
	// Run 在 start 返回 false 后调用 stop，这里直接复现该路径。
	a.stop()

	for _, m := range []*testModule{ok1, ok2} {
		if m.destroyCnt.Load() != 1 || m.closeCnt.Load() != 1 {
			t.Fatalf("%s: destroy=%d close=%d, want 1/1 (OnInit succeeded)",
				m.name, m.destroyCnt.Load(), m.closeCnt.Load())
		}
	}
	if bad.destroyCnt.Load() != 0 || bad.closeCnt.Load() != 0 {
		t.Fatalf("bad: destroy=%d close=%d, want 0/0 (OnInit returned error)",
			bad.destroyCnt.Load(), bad.closeCnt.Load())
	}
	for _, m := range []*testModule{never1, never2} {
		if m.initCount.Load() != 0 {
			t.Fatalf("%s OnInit should never have been called", m.name)
		}
		if m.destroyCnt.Load() != 0 || m.closeCnt.Load() != 0 {
			t.Fatalf("%s: destroy=%d close=%d, want 0/0 (OnInit never called)",
				m.name, m.destroyCnt.Load(), m.closeCnt.Load())
		}
	}
	if a.State() != AppStateNone {
		t.Fatalf("state after stop = %d, want AppStateNone", a.State())
	}
}

// TestStartFailureClosesServersOfNeverServedModules 锁定"OnDestroy 执行时本模块
// Server 已关闭"这条不变量在启动失败路径上同样成立：事件循环从未运行过的模块，
// 其 Server 由框架代关，别的模块投递过来会立刻拿到 ErrServerClosed，
// 而不是静默入队（Cast/AsyncCall）或永久阻塞（同步 Call）。
func TestStartFailureClosesServersOfNeverServedModules(t *testing.T) {
	a := newApp()
	sink := newTestModule("sink")   // OnInit 成功，但 Serve 从未启动
	bad := newTestModule("bad-two") // OnInit 失败
	bad.priority = 1
	bad.initErr = errors.New("boom")
	never := newTestModule("never-inited")
	never.priority = 2
	// 不让模块自己关 server：这里要验证的正是框架有没有替它关。
	for _, m := range []*testModule{sink, bad, never} {
		m.keepServerOpen = true
	}

	if a.start(sink, bad, never) {
		t.Fatal("start should fail")
	}
	a.stop()

	for _, m := range []*testModule{sink, bad, never} {
		if !m.server.IsClosed() {
			t.Fatalf("%s server should be closed after shutdown", m.name)
		}
	}

	// 同步 Call 必须立即拿到错误，而不是永久阻塞。
	client := chanrpc.NewClient()
	done := make(chan *chanrpc.RetInfo, 1)
	go func() { done <- client.Call(a.ChanRPC("never-inited"), skeletonRPCReq{}) }()
	select {
	case ri := <-done:
		if !errors.Is(ri.Err, chanrpc.ErrServerClosed) {
			t.Fatalf("Call err = %v, want ErrServerClosed", ri.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Call to a never-served module blocked forever")
	}
}

// TestShutdownContinuesAfterCloseP anic 锁定 Close 的 panic 不会打断 LIFO 关闭链：
// 收尾/持久化模块通常注册在最前面，按 LIFO 最后关闭，一次 Close panic
// 此前会让它们整段拿不到 OnDestroy。
func TestShutdownContinuesAfterClosePanic(t *testing.T) {
	a := newApp()
	first := newTestModule("first")   // LIFO 里最后关闭
	second := newTestModule("second") // LIFO 里最先关闭，它的 Close 会 panic
	second.priority = 1
	second.closeHook = func() { panic("close panic") }

	if !a.start(first, second) {
		t.Fatal("start should succeed")
	}
	waitClosed(t, first.runStarted, time.Second)
	waitClosed(t, second.runStarted, time.Second)

	a.stop()

	if first.destroyCnt.Load() != 1 || first.closeCnt.Load() != 1 {
		t.Fatalf("first: destroy=%d close=%d, want 1/1 (must survive second's Close panic)",
			first.destroyCnt.Load(), first.closeCnt.Load())
	}
	if a.State() != AppStateNone {
		t.Fatalf("state = %d, want AppStateNone", a.State())
	}
}

// TestRemoveDynamicModuleWaitsForServeExitBeforeDestroy 锁定动态模块的关闭时序
// 与静态模块一致：OnDestroy 必须排在 Serve goroutine 退出之后，否则 OnDestroy
// 会与事件循环并发读写同一批业务内存（Go 运行时 fatal，recover 拦不住）。
func TestRemoveDynamicModuleWaitsForServeExitBeforeDestroy(t *testing.T) {
	a := newApp()
	if !a.start(newTestModule("anchor")) {
		t.Fatal("start should succeed")
	}
	defer a.stop()

	var mu sync.Mutex
	var events []string
	dyn := newTestModule("dyn-order")
	dyn.serveHook = func(ctx context.Context) bool {
		close(dyn.runStarted)
		<-ctx.Done()
		mu.Lock()
		events = append(events, "serve-exit")
		mu.Unlock()
		close(dyn.runStopped)
		return false
	}
	dyn.destroyHook = func() {
		mu.Lock()
		events = append(events, "destroy")
		mu.Unlock()
	}
	dyn.closeHook = func() {
		mu.Lock()
		events = append(events, "close")
		mu.Unlock()
	}

	if _, err := a.AddDynamicModules(dyn); err != nil {
		t.Fatalf("AddDynamicModules failed: %v", err)
	}
	if !a.RemoveDynamicModule("dyn-order") {
		t.Fatal("RemoveDynamicModule should return true")
	}

	mu.Lock()
	got := slices.Clone(events)
	mu.Unlock()
	want := []string{"serve-exit", "destroy", "close"}
	if !slices.Equal(got, want) {
		t.Fatalf("dynamic shutdown order = %v, want %v", got, want)
	}
}

// TestRemoveDynamicModuleIsInvisibleBeforeDestroy 锁定 use-after-destroy 的修复：
// RemoveDynamicModule 一进入就把模块从 dynamicModules 摘掉，
// 销毁期间 ChanRPC(name) 再也查不到它，不会有新的投递被 handler 执行。
func TestRemoveDynamicModuleIsInvisibleBeforeDestroy(t *testing.T) {
	a := newApp()
	dyn := newTestModule("dyn-visible")
	visible := make(chan bool, 1)
	dyn.destroyHook = func() { visible <- a.ChanRPC("dyn-visible") != nil }

	if _, err := a.AddDynamicModules(dyn); err != nil {
		t.Fatalf("AddDynamicModules failed: %v", err)
	}
	if a.ChanRPC("dyn-visible") == nil {
		t.Fatal("running dynamic module should be reachable")
	}
	if !a.RemoveDynamicModule("dyn-visible") {
		t.Fatal("RemoveDynamicModule should return true")
	}
	if <-visible {
		t.Fatal("module must not be reachable via ChanRPC while OnDestroy runs")
	}
	if dyn.closeCnt.Load() != 1 {
		t.Fatalf("dynamic Close count = %d, want 1", dyn.closeCnt.Load())
	}
}

// TestRemoveDynamicModuleConcurrentDestroysOnce 锁定并发卸载同名模块时
// OnDestroy/Close 至多各执行一次（LoadAndDelete + 生命周期 CAS 双重保证）。
func TestRemoveDynamicModuleConcurrentDestroysOnce(t *testing.T) {
	a := newApp()
	dyn := newTestModule("dup")
	if _, err := a.AddDynamicModules(dyn); err != nil {
		t.Fatalf("AddDynamicModules failed: %v", err)
	}

	var wg sync.WaitGroup
	removed := make(chan bool, 8)
	for range 8 {
		wg.Go(func() { removed <- a.RemoveDynamicModule("dup") })
	}
	wg.Wait()
	close(removed)

	trueCount := 0
	for r := range removed {
		if r {
			trueCount++
		}
	}
	if trueCount != 1 {
		t.Fatalf("RemoveDynamicModule returned true %d times, want 1", trueCount)
	}
	if dyn.destroyCnt.Load() != 1 || dyn.closeCnt.Load() != 1 {
		t.Fatalf("destroy=%d close=%d, want 1/1", dyn.destroyCnt.Load(), dyn.closeCnt.Load())
	}
}

// TestAddDynamicModulesRejectsDuplicateName 锁定重名不再静默覆盖：
// 被覆盖的旧 wrapper 此前会永久孤儿化（goroutine 常驻、OnDestroy 永不执行）。
func TestAddDynamicModulesRejectsDuplicateName(t *testing.T) {
	a := newApp()
	m1 := newTestModule("same")
	m2 := newTestModule("same")
	if _, err := a.AddDynamicModules(m1); err != nil {
		t.Fatalf("first add failed: %v", err)
	}
	results, err := a.AddDynamicModules(m2)
	if err == nil {
		t.Fatal("duplicate name should be rejected")
	}
	if len(results) != 1 || results[0].Err == nil {
		t.Fatalf("results = %+v, want one failure", results)
	}
	if m2.initCount.Load() != 0 {
		t.Fatal("duplicate module OnInit should not run")
	}
	a.removeAllDynamicModules()
	if m1.destroyCnt.Load() != 1 {
		t.Fatalf("original module destroy = %d, want 1", m1.destroyCnt.Load())
	}
}

// TestAddDynamicModulesRejectedAfterStop 锁定停机后不再接受动态模块：
// 此刻 removeAllDynamicModules 已经跑过，登记进来的模块再也不会被关闭。
func TestAddDynamicModulesRejectedAfterStop(t *testing.T) {
	a := newApp()
	if !a.start(newTestModule("anchor2")) {
		t.Fatal("start should succeed")
	}
	a.stop()

	leaked := newTestModule("leaked")
	if _, err := a.AddDynamicModules(leaked); err == nil {
		t.Fatal("AddDynamicModules after stop should fail")
	}
	if leaked.initCount.Load() != 0 {
		t.Fatal("module must not be initialized after shutdown")
	}
	if len(a.DynamicModules()) != 0 {
		t.Fatalf("dynamic modules = %v, want empty", a.DynamicModules())
	}
}

// TestAddDynamicModulesDoesNotHangWhenServeExitsBeforeReady 锁定就绪等待不再
// 永久阻塞：动态模块的 panic 不退出进程，Serve 若在 close(ready) 之前就结束，
// 此前 AddDynamicModules 会永远卡在 <-wrapper.Ready() 上。
func TestAddDynamicModulesDoesNotHangWhenServeExitsBeforeReady(t *testing.T) {
	a := newApp()
	dyn := newTestModule("early-exit")
	dyn.serveHook = func(context.Context) bool { panic("serve panic before ready") }

	done := make(chan error, 1)
	go func() {
		_, err := a.AddDynamicModules(dyn)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("AddDynamicModules should report the failed module")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("AddDynamicModules blocked on Ready() forever")
	}
	if len(a.DynamicModules()) != 0 {
		t.Fatalf("dynamic modules = %v, want empty", a.DynamicModules())
	}
}

// TestRestartAfterStopIsRefused 锁定"已停止的应用不可重启"：
// 此前第二次 start 会复用 ctx 已取消、ready 已关闭的旧 wrapper，
// 报告"启动成功"后再由 close(已关闭的 ready) panic 触发 os.Exit(255)。
func TestRestartAfterStopIsRefused(t *testing.T) {
	a := newApp()
	m := newTestModule("restart")
	if !a.start(m) {
		t.Fatal("first start should succeed")
	}
	a.stop()
	if a.State() != AppStateNone {
		t.Fatalf("state = %d, want AppStateNone", a.State())
	}
	if a.start() {
		t.Fatal("restart must be refused")
	}
	if a.State() != AppStateNone {
		t.Fatalf("state after refused restart = %d, want AppStateNone", a.State())
	}
	if m.runCount.Load() != 1 {
		t.Fatalf("Serve ran %d times, want 1", m.runCount.Load())
	}
}

// TestStopDuringInitDoesNotRaceWithOnInit 锁定 start / stop 的交接：
// 启动期间收到关闭信号（k8s 滚动发布时极常见）时，stop 必须等 start 跑完，
// 不能与仍在执行的 OnInit 并发调用 OnDestroy；且 start 不得在关闭完成后
// 把状态改回 AppStateRun。用 -race 运行本用例可直接暴露旧实现的数据竞争。
func TestStopDuringInitDoesNotRaceWithOnInit(t *testing.T) {
	a := newApp(WithShutdownTimeout(5 * time.Second))
	inInit := make(chan struct{})
	slow := newTestModule("slow-init")
	shared := map[string]int{}
	var once sync.Once
	slow.initHook = func() {
		once.Do(func() { close(inInit) })
		for i := range 200 {
			shared["k"] = i // 与 OnDestroy 争抢的业务内存
		}
		time.Sleep(150 * time.Millisecond)
	}
	slow.destroyHook = func() { shared["k"] = -1 }

	go a.start(slow)
	<-inInit
	a.stop()

	if got := a.State(); got != AppStateNone {
		t.Fatalf("state after stop = %d, want AppStateNone (never AppStateRun)", got)
	}
}

// TestRemoveDynamicModuleHonoursShutdownTimeout 锁定 WithShutdownTimeout 对动态
// 模块生效：此前 RemoveDynamicModule 用裸 wg.Wait，一个不响应 ctx 的动态模块
// 会把整个 stop 永久卡死，静态模块再也拿不到 OnDestroy。
func TestRemoveDynamicModuleHonoursShutdownTimeout(t *testing.T) {
	a := newApp(WithShutdownTimeout(200 * time.Millisecond))
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	stubborn := newTestModule("stubborn")
	stubborn.serveHook = func(context.Context) bool {
		close(stubborn.runStarted)
		<-release // 故意不响应 ctx.Done
		return false
	}
	if _, err := a.AddDynamicModules(stubborn); err != nil {
		t.Fatalf("AddDynamicModules failed: %v", err)
	}

	done := make(chan struct{})
	go func() { a.RemoveDynamicModule("stubborn"); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("RemoveDynamicModule ignored shutdownTimeout and blocked forever")
	}
	// 超时路径跳过销毁，与静态模块的取舍一致（宁可漏一次落地，也不要 fatal）。
	if stubborn.destroyCnt.Load() != 0 {
		t.Fatalf("destroy count = %d, want 0 (timeout must skip OnDestroy)", stubborn.destroyCnt.Load())
	}
}
