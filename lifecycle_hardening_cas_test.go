package xhive

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xmapst/xhive/chanrpc"
)

// 本文件锁住第二轮加固中「状态机 CAS 推进」与「停机收口」两组不变量：
//
//  1. 启动路径只能用 advanceLifecycle 做 CAS 推进——停机流程一旦抢占了模块，
//     启动路径不得把它重新写回"活着"的状态；
//  2. stop 入口的闸门检查与 AppStateStop 置位必须原子，第二条并发的 stop
//     不能穿过闸门再跑一遍完整的关闭流程；
//  3. startServing 里 wg.Add 必须排在 lifecycleServing 置位之前，
//     使"观察到 Serving"蕴含"wg 计数已 ≥ 1"，OnDestroy 永远排在 Serve 退出之后；
//  4. shutdownModule 的 closeIdleServer 收口必须覆盖 lifecycleServing 分支，
//     使 Serve 提前 panic 的模块不会留下一个永远开着、永远没人消费的 ChanRPC 服务端。
//
// 所有用例只使用本文件自己定义的模块类型（cas 前缀），不依赖也不修改 app_test.go。

// -----------------------------------------------------------------------------
// 公共脚手架
// -----------------------------------------------------------------------------

// casGateModule 是一个 OnInit 可以被测试精确卡住的最小模块。
//
// initEntered 在 OnInit 一进入就关闭，releaseInit 关闭之前 OnInit 不返回：
// 这对 channel 把"应用停在 AppStateInit、某个模块正处于 lifecycleIniting"
// 这一瞬间变成一个确定性的同步点，用例因此不需要靠 sleep 去猜时序。
//
// OnDestroy 刻意不去关闭自己的 ChanRPC 服务端，也不做任何清理动作，
// 只累加计数：这样"框架有没有替我销毁/关闭"完全由框架的行为决定，
// 不会被模块自身的收尾动作掩盖。
type casGateModule struct {
	name        string
	priority    uint
	server      *chanrpc.Server
	initEntered chan struct{}
	releaseInit chan struct{}
	ready       chan struct{}
	serveCnt    atomic.Int32
	destroyCnt  atomic.Int32
	closeCnt    atomic.Int32
}

func newCasGateModule(name string) *casGateModule {
	return &casGateModule{
		name:        name,
		server:      chanrpc.NewServer(chanrpc.WithChanLen(4)),
		initEntered: make(chan struct{}),
		releaseInit: make(chan struct{}),
		ready:       make(chan struct{}),
	}
}

func (m *casGateModule) Name() string           { return m.name }
func (m *casGateModule) Priority() uint         { return m.priority }
func (m *casGateModule) Ready() <-chan struct{} { return m.ready }

func (m *casGateModule) OnInit() error {
	close(m.initEntered)
	<-m.releaseInit
	return nil
}

func (m *casGateModule) Serve(ctx context.Context) {
	m.serveCnt.Add(1)
	close(m.ready)
	<-ctx.Done()
}

func (m *casGateModule) OnDestroy()               { m.destroyCnt.Add(1) }
func (m *casGateModule) Close() error             { m.closeCnt.Add(1); return nil }
func (m *casGateModule) ChanRPC() *chanrpc.Server { return m.server }

// casWaitTrue 自旋等待一个原子布尔变为 true。
//
// 这里等的是一个**状态**（"stop 已经越过入口临界区"），不是一段时间：
// timeout 仅仅是防止用例在回归时挂死的兜底，正常路径上循环只转几圈就结束。
func casWaitTrue(t *testing.T, b *atomic.Bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !b.Load() {
		if time.Now().After(deadline) {
			t.Fatalf("%s (等待超过 %s)", msg, timeout)
		}
		runtime.Gosched()
	}
}

// casWaitDone 等待一个 channel 关闭，超时即判定用例失败。
func casWaitDone(t *testing.T, ch <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("%s (等待超过 %s)", msg, timeout)
	}
}

// casCloseOnce 幂等关闭，供 t.Cleanup 兜底解卡用。
func casCloseOnce(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

// -----------------------------------------------------------------------------
// 用例 1：停机抢占过的模块不得被 start 复活
// -----------------------------------------------------------------------------

// TestStopClaimedModuleIsNotResurrectedByStart 锁住 moduleWrapper.advanceLifecycle。
//
// 场景：启动期收到停止信号，stop 走 startupSettle 兜底放行，抢占了一个仍然卡在
// OnInit 里的模块（lifecycleIniting → lifecycleDestroying，并按既定取舍跳过销毁，
// 以免 OnDestroy 与仍在执行的 OnInit 并发读写同一批业务内存）。
// 随后 OnInit 返回 nil，start 走到"推进到 Inited"这一步。
//
// 锁住的行为：这一步必须是 CAS 推进而不是直写。模块的销毁权已经归 stop，
// start 只能就此收手。
//
// 为什么重要：stop 的 LIFO 循环早已走过这个模块，绝不会再回来第二次。
// 直写把 Destroying 覆盖成 Inited 之后，模块就永久停在一个
// "资源已分配、按契约必须销毁"的状态上，而框架里再也没有人会来销毁它——
// OnDestroy / Close 永远不执行，连"这里泄漏了"这条 error 日志都不会有。
// 更糟的路径是覆盖成 Serving：一个已经被放弃的模块被重新拉起事件循环，
// 而应用状态早已复位成 AppStateNone。
//
// 加固前（start 里用 setLifecycle 直写）的表现：模块最终停在 lifecycleInited。
func TestStopClaimedModuleIsNotResurrectedByStart(t *testing.T) {
	a := newApp()
	// start 被卡在 OnInit 里，startDone 永远不会关闭，stop 必然走兜底放行分支。
	// 缩短等待只是为了让用例跑得快，结果本身是确定的（不依赖时间窗口撞运气）。
	a.startupSettle = 50 * time.Millisecond

	m := newCasGateModule("claimed-during-init")
	t.Cleanup(func() { casCloseOnce(m.releaseInit) })

	startResult := make(chan bool, 1)
	go func() { startResult <- a.start(m) }()

	// 同步点一：OnInit 已经进入且尚未返回 ⇒ 应用状态为 AppStateInit，
	// 模块状态为 lifecycleIniting，startDone 非 nil 且仍然打开。
	casWaitDone(t, m.initEntered, 5*time.Second, "OnInit 没有被调用")

	stopDone := make(chan struct{})
	go func() { defer close(stopDone); a.stop() }()

	// 同步点二：stop 已经完整跑完（含 startupSettle 兜底放行 + 抢占该模块）。
	casWaitDone(t, stopDone, 10*time.Second, "stop 没有在 startupSettle 兜底后收敛")

	if got := a.State(); got != AppStateNone {
		t.Fatalf("stop 返回后应用状态 = %d，期望 AppStateNone(%d)", got, AppStateNone)
	}

	// 现在才放 OnInit 返回：start 接着要把模块推进到 Inited，而销毁权已经不归它。
	close(m.releaseInit)

	select {
	case ok := <-startResult:
		if ok {
			t.Fatal("start 在模块已被停机抢占后仍然返回 true")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("start 没有返回")
	}

	a.RLock()
	wrapper := a.modules[0]
	a.RUnlock()

	if got := wrapper.lifecycleState(); got != lifecycleDestroying {
		t.Fatalf("模块生命周期 = %s，期望 destroying；"+
			"start 用直写覆盖了 stop 抢到的销毁权，该模块此后再也不会有人销毁", got)
	}
	if got := m.serveCnt.Load(); got != 0 {
		t.Fatalf("Serve 被拉起了 %d 次，期望 0：已被停机抢占的模块不得再启动事件循环", got)
	}
	if got := m.destroyCnt.Load(); got != 0 {
		t.Fatalf("OnDestroy 被调用了 %d 次，期望 0："+
			"模块仍在 OnInit 中，框架必须跳过销毁而不是与 OnInit 并发", got)
	}
	if got := a.State(); got == AppStateRun {
		t.Fatal("应用状态被 start 改回了 AppStateRun")
	}
}

// -----------------------------------------------------------------------------
// 用例 2：并发 stop 只允许一条流程真正执行关闭
// -----------------------------------------------------------------------------

// TestConcurrentStopIsSerialized 锁住 stop 入口"闸门检查 + AppStateStop 置位"的原子性。
//
// 场景：应用停在 AppStateInit（某个模块的 OnInit 被卡住），第一条 stop 已经越过
// 入口临界区、正在等待 start 收敛；此时第二条 stop 进来。
//
// 锁住的行为：第二条 stop 必须在入口就被 "already stopping" 挡回，立即返回，
// 既不参与等待，也不执行任何关闭动作。
//
// 为什么重要：入口的状态置位若拖到"等待 start 收敛"之后，那段等待期间锁是放开的、
// 状态还停在 AppStateInit，第二条 stop 会一路穿过闸门跟着往下走。它会陪等一整个
// startupSettle（用例里就是这条：加固前第二次 stop 直接被挂住），醒来后若第一条
// 已经跑完并把状态复位成 AppStateNone，复查同样拦不住它，于是整套关闭流程被跑第二遍：
// 再扫一遍动态模块、再刷一遍 shutdown initiated/complete 日志，外部观察到的 State()
// 出现第二轮 None → Stop → None 抖动。模块级别的重复销毁虽有 claimShutdown 兜底，
// 但"应用只关闭一次"这条语义已经不成立了。
//
// 加固前的表现：第二次 stop 阻塞在 startupSettle 上（本用例把它设成 30 秒），
// 用例在 1.5 秒的上界处失败。
func TestConcurrentStopIsSerialized(t *testing.T) {
	a := newApp()
	// 刻意设得很长：只要第二条 stop 越过了入口闸门，它就必然被这段等待挂住
	// （startDone 永远不会关闭，因为 OnInit 被卡着）。因此"第二次 stop 是否
	// 立即返回"是一个确定性的行为判定，1.5 秒的上界只是失败时的兜底，
	// 不参与正确性推理。
	a.startupSettle = 30 * time.Second

	m := newCasGateModule("blocked-in-init")
	t.Cleanup(func() { casCloseOnce(m.releaseInit) })

	startResult := make(chan bool, 1)
	go func() { startResult <- a.start(m) }()
	casWaitDone(t, m.initEntered, 5*time.Second, "OnInit 没有被调用")

	// 第一条 stop：越过入口闸门后会停在"等待 start 收敛"上。
	stopFirstDone := make(chan struct{})
	go func() { defer close(stopFirstDone); a.stop() }()

	// 同步点：stopRequested 只可能由第一条 stop 置位（第二条还没启动），
	// 它变 true 即表示第一条已经离开入口临界区。加固前后这一步的行为一致，
	// 差别在于加固后 AppStateStop 已经在同一个临界区里置好了。
	casWaitTrue(t, &a.stopRequested, 5*time.Second, "第一条 stop 没有越过入口闸门")

	// 第二条 stop：必须立即返回。
	stopSecondDone := make(chan struct{})
	go func() { defer close(stopSecondDone); a.stop() }()
	select {
	case <-stopSecondDone:
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("第二次 stop 没有在入口被挡回，而是跟着第一条流程一起等待 start 收敛：" +
			"闸门检查与 AppStateStop 置位不原子")
	}

	// 第二条既然是被挡回的，它不该动过任何东西：应用仍处于关闭中，
	// 第一条还卡在等待 start 收敛，没有任何模块被销毁。
	if got := a.State(); got != AppStateStop {
		t.Fatalf("第二次 stop 返回后应用状态 = %d，期望 AppStateStop(%d)", got, AppStateStop)
	}
	if got := m.destroyCnt.Load(); got != 0 {
		t.Fatalf("第二次 stop 返回后 OnDestroy 已被调用 %d 次，期望 0："+
			"第一条 stop 还停在等待 start 收敛，销毁不该由第二条流程发起", got)
	}

	// 放 OnInit 返回 → start 收敛 → 第一条 stop 继续，完成唯一的一次销毁。
	close(m.releaseInit)
	casWaitDone(t, stopFirstDone, 10*time.Second, "第一条 stop 没有收敛")
	select {
	case ok := <-startResult:
		if ok {
			t.Fatal("start 在关闭已发起后仍然返回 true")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("start 没有返回")
	}

	if got := m.destroyCnt.Load(); got != 1 {
		t.Fatalf("OnDestroy 调用次数 = %d，期望恰好 1", got)
	}
	if got := m.closeCnt.Load(); got != 1 {
		t.Fatalf("Close 调用次数 = %d，期望恰好 1", got)
	}
	if got := a.State(); got != AppStateNone {
		t.Fatalf("全部收敛后应用状态 = %d，期望 AppStateNone(%d)", got, AppStateNone)
	}

	a.RLock()
	wrapper := a.modules[0]
	a.RUnlock()
	if got := wrapper.lifecycleState(); got != lifecycleDestroyed {
		t.Fatalf("模块生命周期 = %s，期望 destroyed", got)
	}

	// 关闭完成之后再来一次 stop：应用已回到 AppStateNone，必须原地返回，
	// 不得再触发一轮 None → Stop → None 抖动，也不得重复销毁。
	stopThirdDone := make(chan struct{})
	go func() { defer close(stopThirdDone); a.stop() }()
	casWaitDone(t, stopThirdDone, 5*time.Second, "已停止的应用上再调 stop 没有立即返回")
	if got := m.destroyCnt.Load(); got != 1 {
		t.Fatalf("第三次 stop 之后 OnDestroy 调用次数 = %d，期望仍为 1", got)
	}
	if got := a.State(); got != AppStateNone {
		t.Fatalf("第三次 stop 之后应用状态 = %d，期望 AppStateNone(%d)", got, AppStateNone)
	}
}

// -----------------------------------------------------------------------------
// 用例 3：观察到 Serving 就必须等得到 Serve 退出
// -----------------------------------------------------------------------------

// casServeOrderModule 持有一份**非原子**的业务内存（state map）：
// Serve 往里写、OnDestroy 从里面读。这是刻意的——只用原子量的模块即使
// OnDestroy 与 Serve 真的并发了，-race 也不会报任何东西，用例会假绿。
//
// serveExited / destroySawServeExit 同样是普通字段（非原子），
// 它们既是"serve-exit 必须排在 destroy 之前"这条事件序的显式断言载体，
// 也是 -race 的第二个探针。
type casServeOrderModule struct {
	name     string
	priority uint
	ready    chan struct{}
	// gate 非 nil 时，Serve 会先等它（或等 ctx 取消）再宣告就绪，
	// 用来把 start 确定性地停在 waitReady 上。
	gate chan struct{}
	// serveEntered 非 nil 时，Serve 一进入就关闭它。
	serveEntered chan struct{}

	state               map[string]int
	serveExited         bool
	destroySawServeExit bool
	destroyRan          bool
	closeRan            bool
}

func newCasServeOrderModule(name string) *casServeOrderModule {
	return &casServeOrderModule{
		name:  name,
		ready: make(chan struct{}),
		state: make(map[string]int),
	}
}

func (m *casServeOrderModule) Name() string             { return m.name }
func (m *casServeOrderModule) Priority() uint           { return m.priority }
func (m *casServeOrderModule) Ready() <-chan struct{}   { return m.ready }
func (m *casServeOrderModule) OnInit() error            { return nil }
func (m *casServeOrderModule) ChanRPC() *chanrpc.Server { return nil }

func (m *casServeOrderModule) Serve(ctx context.Context) {
	m.state["serve_enter"]++
	if m.serveEntered != nil {
		close(m.serveEntered)
	}
	if m.gate != nil {
		select {
		case <-m.gate:
		case <-ctx.Done():
			m.state["serve_exit"]++
			m.serveExited = true
			return
		}
	}
	close(m.ready)
	// 一小段纯业务内存写：加固前若 OnDestroy 抢跑，-race 会在这几行和
	// OnDestroy 的读之间报出数据竞争。
	for i := range 32 {
		m.state["tick"] = i
	}
	<-ctx.Done()
	m.state["serve_exit"]++
	m.serveExited = true
}

func (m *casServeOrderModule) OnDestroy() {
	// 事件序的显式断言：OnDestroy 执行时，Serve 必须已经退出。
	m.destroySawServeExit = m.serveExited
	m.state["destroy"]++
	m.destroyRan = true
}

func (m *casServeOrderModule) Close() error { m.closeRan = true; return nil }

// TestServeStartRaceDoesNotSkipWait 锁住 startServing 里 wg.Add 的位置。
//
// 加固后的写法是：
//
//	wrapper.wg.Add(1)
//	wrapper.setLifecycle(lifecycleServing)
//	go func() { defer wrapper.wg.Done(); a.serveModule(...) }()
//
// 由此得到一条可以被外部观察者依赖的不变量：**任何观察到 lifecycleServing
// 的 goroutine，都必然也观察到 wg 计数已经 ≥ 1**（Go 的原子操作是顺序一致的）。
// 关闭流程只对 lifecycleServing 的模块执行 wg.Wait，这条不变量正是它成立的前提。
//
// 加固前是 setLifecycle 在前、wg.Go 内部的 Add 在后：抢在这两步中间的 stop
// 会读到 Serving，然后对一个计数仍为 0 的 wg 立刻 Wait 成功，紧接着开始 OnDestroy——
// 而 Serve 此刻才刚被拉起。事件循环与 OnDestroy 并发读写同一批业务内存，
// 是 Go 运行时直接 fatal、且 recover 拦不住的那一类错误。
//
// 用例分两段：
//
//	阶段一（定向探针）：一个自旋 goroutine 死等 lifecycleServing，一看到就立刻
//	  去抢销毁权。它只在观察到 Serving 之后才动手，因此加固后**恒定安全**
//	  （观察到 Serving ⇒ Add 已发生 ⇒ waitModuleExit 必然等到 Serve 退出），
//	  而加固前正好落在那个窗口里。这里直接驱动 startServing / shutdownModule，
//	  而不是 start / stop：后两者被 startDone 串行化，真正重叠只发生在
//	  startupSettle 兜底那条路径上，而那条路径上还存在一个加固前后都有的窗口
//	  （stop 恰好在模块处于 lifecycleInited 时抢占，紧接着 start 才调 startServing），
//	  用它做压测只会让用例变成随机翻车，而不是回归护栏。
//
//	阶段二（确定性并发）：把 start 停在 waitReady 上，让 stop 走 startupSettle
//	  兜底放行，与 start 真正并发跑完整条关闭路径，断言每个模块的 OnDestroy
//	  都观察到 Serve 已经退出。
//
// **本用例需要在 -race 下运行**（与本仓库既有的 go test -race ./... 一致）。
// 阶段一里 waitModuleExit 的 wg.Wait 与 startServing 的 wg.Add 之间没有任何
// happens-before 关系，竞态检测器凭 WaitGroup 自带的 race 标注（Add 从 0 起算时
// 对 wg.sema 的 read、首个阻塞 Wait 对 wg.sema 的 write）当场报出来，
// 加固前每次运行必现。不开 -race 时就只剩"Wait 实时地抢在 Add 之前返回"这一种
// 命中方式，那个窗口只有十几纳秒，偶尔才由 destroySawServeExit 断言捕获。
func TestServeStartRaceDoesNotSkipWait(t *testing.T) {
	// ---- 阶段一：定向探针 ----
	const probeRounds = 1500
	for i := range probeRounds {
		a := newApp()
		// 收紧超时：一旦不变量被破坏导致 wg.Wait 挂住，用例应尽快失败而不是挂死。
		a.shutdownTimeout = 10 * time.Second

		m := newCasServeOrderModule(fmt.Sprintf("probe-%d", i))
		w := newModuleWrapper(m)
		// 模拟 OnInit 已成功返回：这正是 start / AddDynamicModules 调用
		// startServing 之前模块所处的状态。
		w.setLifecycle(lifecycleInited)

		spinning := make(chan struct{})
		shutdownDone := make(chan struct{})
		go func() {
			defer close(shutdownDone)
			close(spinning)
			// 紧凑自旋，尽可能贴着状态置位的那一瞬间抢销毁权。
			// 只在观察到 lifecycleServing 之后才动手，这正是加固后那条不变量
			// 唯一的适用前提——所以这个探针加固后恒定安全，加固前才会翻车。
			for w.lifecycleState() != lifecycleServing {
				runtime.Gosched()
			}
			// 刻意不取返回值：shutdownModule 的 bool 返回值是第二轮加固才加上的，
			// 用了它这个用例就无法在加固前的版本上编译，也就无从验证它确实能抓到缺陷。
			a.shutdownModule(w)
		}()
		<-spinning

		a.startServing(w, true)

		select {
		case <-shutdownDone:
		case <-time.After(30 * time.Second):
			t.Fatalf("第 %d 轮：shutdownModule 没有返回", i)
		}

		// 这两个字段由销毁 goroutine 写、经 channel 传递到这里，读取是有序的。
		if !m.destroyRan {
			t.Fatalf("第 %d 轮：OnDestroy 没有被调用", i)
		}
		if !m.destroySawServeExit {
			t.Fatalf("第 %d 轮：OnDestroy 开始执行时 Serve 还没退出——"+
				"关闭流程读到了 lifecycleServing，却对一个计数仍为 0 的 wg Wait 成功了", i)
		}
		if !m.closeRan {
			t.Fatalf("第 %d 轮：Close 没有被调用", i)
		}
	}

	// ---- 阶段二：确定性的 start / stop 并发 ----
	a := newApp()
	// start 会停在 waitReady 上（blocked 模块迟迟不宣告就绪），startDone 因此
	// 不会关闭，stop 必然走 startupSettle 兜底放行，与 start 真正并发。
	a.startupSettle = 50 * time.Millisecond
	a.shutdownTimeout = 10 * time.Second

	blocked := newCasServeOrderModule("serve-blocked")
	blocked.priority = 0
	blocked.gate = make(chan struct{}) // 永不释放：只能由 ctx 取消退出
	last := newCasServeOrderModule("serve-last")
	last.priority = 1
	last.serveEntered = make(chan struct{})

	startResult := make(chan bool, 1)
	go func() { startResult <- a.start(blocked, last) }()

	// 同步点：last 是 startServing 循环里的最后一个模块，它的 Serve 进入
	// 说明整个 startServing 循环已经走完，start 此刻必定停在 waitReady 上。
	casWaitDone(t, last.serveEntered, 10*time.Second, "最后一个模块的 Serve 没有被拉起")

	stopDone := make(chan struct{})
	go func() { defer close(stopDone); a.stop() }()
	casWaitDone(t, stopDone, 30*time.Second, "stop 没有收敛")

	select {
	case <-startResult:
	case <-time.After(10 * time.Second):
		t.Fatal("start 没有返回")
	}

	for _, m := range []*casServeOrderModule{blocked, last} {
		if !m.destroyRan {
			t.Fatalf("模块 %s 没有收到 OnDestroy", m.name)
		}
		if !m.destroySawServeExit {
			t.Fatalf("模块 %s 的 OnDestroy 开始执行时 Serve 还没退出："+
				"关闭流程没有等到事件循环结束就动了业务内存", m.name)
		}
		if !m.closeRan {
			t.Fatalf("模块 %s 没有收到 Close", m.name)
		}
	}
}

// -----------------------------------------------------------------------------
// 用例 4：Serve 提前 panic 的模块，Server 也必须被收掉
// -----------------------------------------------------------------------------

// casPanicServeModule 的 Serve 在关闭 ready 之前就 panic：
// 模块已经处于 lifecycleServing，但它的 ChanRPC 服务端从来没有被任何人关过
// （正常路径上这件事由 Skeleton.Serve 的 ctx.Done 分支完成）。
//
// OnDestroy / Close 刻意不碰 server：框架漏关时用例必须能看出来，
// 模块自己顺手关掉会把这个缺陷完全掩盖。
type casPanicServeModule struct {
	name       string
	server     *chanrpc.Server
	ready      chan struct{} // 永不关闭
	destroyCnt atomic.Int32
	closeCnt   atomic.Int32
}

// casPingReq 是一条注册到该模块上的请求消息。
// 注册过 handler，才能让"同步 Call 卡死"与"消息未注册"这两种失败区分开来。
type casPingReq struct{}

func newCasPanicServeModule(name string) *casPanicServeModule {
	m := &casPanicServeModule{
		name:   name,
		server: chanrpc.NewServer(chanrpc.WithChanLen(4)),
		ready:  make(chan struct{}),
	}
	_ = m.server.Register(casPingReq{}, func(_ *chanrpc.CallInfo) *chanrpc.RetInfo { return &chanrpc.RetInfo{} })
	return m
}

func (m *casPanicServeModule) Name() string             { return m.name }
func (m *casPanicServeModule) Priority() uint           { return 0 }
func (m *casPanicServeModule) Ready() <-chan struct{}   { return m.ready }
func (m *casPanicServeModule) OnInit() error            { return nil }
func (m *casPanicServeModule) ChanRPC() *chanrpc.Server { return m.server }

func (m *casPanicServeModule) Serve(_ context.Context) {
	// 在 close(ready) 之前 panic：动态模块的 panic 只记日志、不退进程，
	// waitReady 因此走的是"goroutine 已退出而 ready 从未关闭"这条失败路径。
	panic("serve panics before signalling ready")
}

func (m *casPanicServeModule) OnDestroy()   { m.destroyCnt.Add(1) }
func (m *casPanicServeModule) Close() error { m.closeCnt.Add(1); return nil }

// TestStartFailureClosesServerOfModuleWhoseServePanicked 锁住 shutdownModule 末尾
// 那一次统一的 closeIdleServer 收口。
//
// 场景：一个动态模块的 Serve 在宣告就绪之前 panic。此刻模块状态是
// lifecycleServing（startServing 已经置位），AddDynamicModules 的 waitReady
// 失败路径把它交给 shutdownModule 收场。
//
// 锁住的行为：销毁完成后，该模块的 ChanRPC 服务端必须已经关闭。
//
// 为什么重要：「OnDestroy 执行时本模块 Server 已关闭」是这套关闭逻辑的核心不变量，
// 它必须在**所有**分支上成立，而不只是"Serve 正常走到 ctx.Done 分支"那一条。
// Server 留着不关，别的模块向它投递时 IsClosed 仍是 false：
// Cast/AsyncCall 会"成功"入队后随进程静默蒸发，同步 Call 更糟——对端事件循环
// 根本不存在，回包永远不会产生，调用方永久阻塞（只每 5 秒打一条 warn），
// 而 shutdownTimeout 只覆盖 wg.Wait，覆盖不到 OnDestroy 里的这次调用。
//
// 加固前：shutdownModule 只在 lifecycleInited 分支里关 Server，
// lifecycleServing 分支默认"Serve 自己关过了"，于是这个模块的 Server 一直开着。
func TestStartFailureClosesServerOfModuleWhoseServePanicked(t *testing.T) {
	a := newApp()
	a.shutdownTimeout = 10 * time.Second

	m := newCasPanicServeModule("serve-panics-before-ready")

	results, err := a.AddDynamicModules(m)
	if err == nil {
		t.Fatal("AddDynamicModules 应当失败：Serve 在宣告就绪之前就 panic 了")
	}
	if len(results) != 1 || results[0].Err == nil {
		t.Fatalf("results = %#v，期望恰好一条且带错误", results)
	}

	// 走的是 lifecycleServing 分支：goroutine 已退出，OnDestroy / Close 应完整执行。
	if got := m.destroyCnt.Load(); got != 1 {
		t.Fatalf("OnDestroy 调用次数 = %d，期望 1", got)
	}
	if got := m.closeCnt.Load(); got != 1 {
		t.Fatalf("Close 调用次数 = %d，期望 1", got)
	}

	// 核心断言：框架必须替它把 Server 收掉。
	if !m.server.IsClosed() {
		t.Fatal("模块销毁完成后 ChanRPC 服务端仍然开着：" +
			"Serve 在 ctx.Done 分支之前就 panic 了，没有任何人关过它，" +
			"shutdownModule 的 lifecycleServing 分支必须由统一收口补上这一步")
	}

	// 上面那条不变量的实际后果：Server 关掉之后，别的模块投过来的同步调用
	// 会立刻拿到一个明确的错误；留着不关则会一直等一个永远不会产生的回包。
	client := chanrpc.NewClient()
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	ri := client.CallWithContext(ctx, m.server, casPingReq{})
	if !errors.Is(ri.Err, chanrpc.ErrServerClosed) {
		t.Fatalf("向已销毁模块发起同步调用得到 err = %v，期望 %v："+
			"事件循环从未运行过，调用方必须立刻拿到错误而不是永久等待",
			ri.Err, chanrpc.ErrServerClosed)
	}

	// 模块不该留在动态模块表里。
	if got := a.ChanRPC(m.name); got != nil {
		t.Fatal("启动失败的动态模块仍然可以通过 ChanRPC 寻址")
	}
	if got := a.DynamicModules(); len(got) != 0 {
		t.Fatalf("DynamicModules = %v，期望为空", got)
	}
}
