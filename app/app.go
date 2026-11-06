package app

import (
	"cmp"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xmapst/xhive/chanrpc"
	"github.com/xmapst/xhive/signal"
)

// App 是应用框架的核心结构，统一管理静态模块列表和动态模块集合。
//
// 并发安全说明：
//   - modules 切片与 state 共用同一把 RWMutex 保护：Register/start 需要在
//     持锁状态下原子地完成"检查状态 + 追加模块"，避免两个操作分离时出现
//     TOCTOU 竞态（例如并发调用 Register 与 Run 时都读到 StateNone）
//   - dynamicModules 使用 sync.Map，原生支持并发增删改查
//     新增动态模块时还须持有同一把写锁检查静态模块，保证整个 App 内名称唯一
//   - 信号注册与分发由 signal.Manager 内部的 RWMutex 保护
type App struct {
	sm              *signal.Manager
	dynamicModules  sync.Map         // 动态模块集合，key 为模块名，支持运行时热加载
	modules         []*moduleWrapper // 静态模块列表，按优先级排序，启动后不允许修改
	dynSeq          atomic.Uint64    // 动态模块添加序号生成器，见 moduleWrapper.seq 的说明
	startDone       chan struct{}    // start 进行中时非 nil，start 返回前关闭；受 RWMutex 保护，见 stop 的启动交接说明
	shutdownTimeout time.Duration    // 单个模块优雅关闭的最大等待时间，可通过 WithShutdownTimeout 自定义
	startupSettle   time.Duration    // stop 等待仍在进行的 start 收敛的最大时间，见 defaultStartupSettleTimeout
	state           atomic.Int32     // 应用全局状态，读写均在持锁状态下进行，Stats/State 对外仍以原子读保证快照一致
	stopRequested   atomic.Bool      // stop 已被请求；start 在各阶段之间检查它并中止启动，避免关闭完成后又把状态改回 StateRun
	runInFlight     atomic.Bool      // Run 正在执行；同一个 App 上不允许并发 Run，见 Run 的说明
	sync.RWMutex                     // 保护 modules 切片，并使状态变更与 modules 写入保持原子性
}

// New 创建新的应用框架实例，初始状态为 StateNone。
//
// opts 可配置该实例的关闭行为，例如 WithShutdownTimeout；nil 选项会被忽略。
// 返回实例与包级函数使用的 defaultApp 独立，模块注册、RPC 寻址及信号注册
// 应通过同一个实例的方法完成。
func New(opts ...Option) *App {
	a := &App{
		sm:              signal.NewManager(),
		modules:         make([]*moduleWrapper, 0),
		shutdownTimeout: defaultShutdownTimeout,
		startupSettle:   defaultStartupSettleTimeout,
	}
	a.setState(StateNone)
	for _, opt := range opts {
		if opt != nil {
			opt(a)
		}
	}
	return a
}

// setState 更新应用状态。调用方必须已持有写锁（Lock），
// 状态变更与 modules 切片的读写共享同一把锁，避免出现"检查状态"和
// "追加模块"两步操作之间被其他 goroutine 插入导致的 TOCTOU 竞态。
func (a *App) setState(state int32) {
	a.state.Store(state)
}

// State 通过原子读取获取应用当前状态，可在任意 goroutine 中安全调用。
//
// 注意：State 不加锁，是一个"当前时刻的近似快照"，
// 用于外部监控和只读判断场景；涉及状态变更的操作（Register/start/stop）
// 内部会在持锁状态下重新校验，不依赖此处的读取结果做决策。
func (a *App) State() int32 {
	return a.state.Load()
}

// Stats 返回所有模块（静态 + 动态）的 RPC 队列积压状态统计字符串。
//
// 输出格式："{static|dynamic}: {模块名}, rpc_queue_length: {队列长度}, rpc_queue_cap: {队列容量}"
// rpc_queue_length 反映模块消息积压程度，与 rpc_queue_cap 相除即得水位，
// 是性能瓶颈和消息处理速率的重要观测指标，也是可直接用于告警的量。
// 两者同时为 N/A 表示该模块未配置 ChanRPC 服务端（如纯定时器模块）。
func (a *App) Stats() string {
	var builder strings.Builder

	// 遍历静态模块
	a.RLock()
	staticModules := slices.Clone(a.modules)
	a.RUnlock()
	for _, wrapper := range staticModules {
		a.appendModuleStats(&builder, "static", wrapper)
	}

	// 遍历动态模块（sync.Map.Range 保证并发安全）。
	// 跳过尚未完成登记的占位条目：它们的 OnInit 正在另一条 goroutine 上运行，
	// 此刻读它们的 Name/ChanRPC 就是与 OnInit 并发读写同一批字段。
	a.dynamicModules.Range(func(key, value any) bool {
		if wrapper, ok := value.(*moduleWrapper); ok && wrapper.visible.Load() {
			a.appendModuleStats(&builder, "dynamic", wrapper)
		}
		return true
	})

	return builder.String()
}

// appendModuleStats 将单个模块的状态信息追加到 builder，内部实现复用。
//
// 直接使用 fmt.Fprintf 写入 builder，避免 fmt.Sprintf 产生的中间字符串
// 分配后再 WriteString 拷贝一次，减少一次内存分配与拷贝。
func (a *App) appendModuleStats(builder *strings.Builder, moduleType string, wrapper *moduleWrapper) {
	rpcServer := wrapper.ChanRPC()

	if rpcServer != nil {
		// 同时输出容量：队列有界，只看积压量看不出离打满还有多远，
		// 水位（length/cap）才是能拿来告警的东西。
		_, _ = fmt.Fprintf(builder, "%s: %s, rpc_queue_length: %d, rpc_queue_cap: %d\n",
			moduleType, wrapper.Name(), rpcServer.Len(), rpcServer.Cap())
	} else {
		_, _ = fmt.Fprintf(builder, "%s: %s, rpc_queue_length: N/A, rpc_queue_cap: N/A\n",
			moduleType, wrapper.Name())
	}
}

// ChanRPC 通过模块名获取对应模块的 ChanRPC 服务端，用于跨模块消息投递。
//
// 查找策略：优先从静态模块列表中查找（加读锁），未命中时再查找动态模块（无锁，sync.Map 保证安全）。
// 两步查找分开处理的原因：静态模块列表需要锁，而 sync.Map 无需锁，
// 分开可以在找到静态模块时尽早释放读锁，减少锁持有时间。
func (a *App) ChanRPC(name string) *chanrpc.Server {
	a.RLock()
	for _, wrapper := range a.modules {
		if wrapper.Name() == name {
			rpcServer := wrapper.ChanRPC()
			a.RUnlock()
			return rpcServer
		}
	}
	a.RUnlock()

	return a.getChanRPCDynamic(name)
}

// getChanRPCDynamic 从动态模块集合中查找 ChanRPC 服务端。
func (a *App) getChanRPCDynamic(name string) *chanrpc.Server {
	if value, ok := a.dynamicModules.Load(name); ok {
		if wrapper, ok := value.(*moduleWrapper); ok {
			// 只有完成登记的模块对外可见。AddDynamicModules 在 OnInit 之前就用
			// LoadOrStore 把名字占住了（那是防并发重名的唯一可靠手段），
			// 占位期间模块还没就绪，不能让别人寻址到它——这条判断把"名字已占用"
			// 与"模块可服务"这两件事分开，对外语义仍然是"等模块 Ready 之后
			// ChanRPC(name) 才查得到它"。
			if !wrapper.visible.Load() {
				return nil
			}
			return wrapper.ChanRPC()
		}
	}
	return nil
}

// Register 在应用启动前注册静态模块。
//
// 静态模块在应用整个生命周期中持续运行，不支持热卸载。
// 若应用已处于运行或停止状态则返回错误，防止运行时并发修改 modules 切片引发数据竞争。
// 模块名称必须在本 App 的静态、动态模块之间唯一；整批校验通过后才追加，
// 任一名称冲突都会返回错误，不会留下部分注册的模块。
//
// 状态检查与追加操作在同一把写锁内完成（而非分离为"先读状态、再加锁追加"），
// 避免 Register 与 Run 并发调用时，两个 goroutine 都读到 StateNone
// 后同时进入各自的追加/启动流程，产生 TOCTOU 竞态。
func (a *App) Register(mods ...IModule) error {
	a.Lock()
	defer a.Unlock()

	if a.state.Load() != StateNone {
		return fmt.Errorf("application is already running")
	}

	return a.registerModulesLocked(mods...)
}

// registerModulesLocked 校验整批静态模块的名称，再一次性登记。
// 调用方必须持有 a 的写锁；动态模块的名称占位也使用该锁，避免两类注册并发重名。
func (a *App) registerModulesLocked(mods ...IModule) error {
	seen := make(map[string]struct{}, len(a.modules)+len(mods))
	for _, wrapper := range a.modules {
		seen[wrapper.Name()] = struct{}{}
	}

	names := make([]string, len(mods))
	for i, mod := range mods {
		if mod == nil {
			continue
		}
		name := mod.Name()
		if _, exists := seen[name]; exists {
			return fmt.Errorf("module %q already exists", name)
		}
		// 尚在初始化、还不能被 ChanRPC 寻址的动态模块也占有名称。
		if _, exists := a.dynamicModules.Load(name); exists {
			return fmt.Errorf("module %q already exists", name)
		}
		seen[name] = struct{}{}
		names[i] = name
	}

	for i, mod := range mods {
		if mod != nil {
			a.modules = append(a.modules, newModuleWrapper(mod, names[i]))
		}
	}
	return nil
}

// Run 注册并启动所有模块，阻塞至所有信号处理完毕（通常是收到 SIGINT/SIGTERM 后优雅关闭）。
//
// 框架默认注册 SIGINT/SIGTERM → 优雅关闭，SIGHUP → 仅记录日志继续运行。
// SIGKILL 仅作为框架保留信号禁止业务注册，操作系统不会把它投递给进程处理。
// 业务层可在 Run 调用前通过 RegisterSignal 覆盖默认处理器，或注册额外信号（如 SIGUSR1）。
//
// 同一个 App 上不允许并发或重入调用 Run。start 有多种返回 error 的原因，其中
// "应用已经启动"是一种**不该触发关闭**的失败：它什么都没改，只是拒绝了这次启动。
// 但下面的 select 看到 errCh 就会无条件调 stopFn，把那个健康运行中的应用连同
// 全部模块一起销毁掉；更糟的是关闭由第二个 Run 的 stopFn 完成，第一个 Run 的
// stopped 永远不会被 close，它的 select 永久阻塞（通常就是 main goroutine）。
// 因此这里直接拒绝第二次 Run，而不是让它走进那条会误伤的失败分支。
//
// 返回值：启动失败时返回 start 给出的失败原因（模块 OnInit 的 error 经 %w 包装，
// 可用 errors.Is/As 解包），此时已启动的模块已被完整关闭；重入调用被拒绝时返回
// 对应错误且不做任何改动；正常收到停机信号并完成优雅关闭时返回 nil。
func (a *App) Run(mods ...IModule) error {
	if !a.runInFlight.CompareAndSwap(false, true) {
		slog.Error("application is already running, Run must not be called concurrently or reentrantly")
		return fmt.Errorf("application is already running, Run must not be called concurrently or reentrantly")
	}
	defer a.runInFlight.Store(false)

	stopped := make(chan struct{})
	stopFn := sync.OnceFunc(func() {
		a.stop()
		close(stopped)
	})

	a.sm.Start(stopFn)

	var errCh = make(chan error, 1)
	go func() {
		if err := a.start(mods...); err != nil {
			errCh <- err
		}
	}()

	var err error
	select {
	case err = <-errCh:
		slog.Error("app start failed", slog.Any("error", err))
		stopFn()
	case <-stopped:
	}

	a.sm.Stop()
	return err
}

// start 按顺序初始化并启动所有已注册的模块。
//
// 执行流程：
//  1. 状态检查 + 将 Run 参数中的模块追加到 modules 列表，两步在同一把写锁内完成，
//     防止并发调用 Register/start 时出现 TOCTOU 竞态（支持 Register + Run 两种注册方式）
//  2. 依次调用 OnInit，任一失败则中止启动并返回描述失败原因的 error
//  3. 为每个模块启动独立 goroutine 并运行 Serve
//
// 顶层 panic recover：捕获启动过程中的意外 panic，记录完整堆栈后以退出码 255 终止进程，
// 防止进程在不确定状态下继续运行造成数据损坏。
func (a *App) start(mods ...IModule) error {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("application panic recovered", slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
			os.Exit(255)
		}
	}()

	a.Lock()
	currentState := a.state.Load()
	if currentState != StateNone {
		a.Unlock()
		slog.Error("application cannot start twice", slog.Int64("current_state", int64(currentState)))
		return fmt.Errorf("application cannot start twice, current state: %d", currentState)
	}

	if err := a.registerModulesLocked(mods...); err != nil {
		a.Unlock()
		return err
	}
	moduleCount := len(a.modules)
	if moduleCount == 0 {
		a.Unlock()
		slog.Warn("no modules provided to start")
		return fmt.Errorf("no modules provided to start")
	}

	// 拒绝重启：stop 把应用状态复位为 AppStateNone，但它并不重建 modules 里的
	// wrapper——ctx 已被 cancel、exited 已关闭、模块自身的一次性资源（例如
	// Skeleton.ready 这个只 make 一次的 channel）也已消耗。沿用旧 wrapper 再启动
	// 一次，Serve 会立刻从已取消的 ctx 返回，或在 close(已关闭的 ready) 上 panic，
	// 而 start 反而会因为 Ready() 立即就绪而报告"启动成功"。与其产出这样一个
	// 僵尸应用，不如在这里明确拒绝，并由 AppStateNone 的注释如实说明不支持重启。
	for _, wrapper := range a.modules {
		if state := wrapper.lifecycleState(); state != lifecycleRegistered {
			a.Unlock()
			slog.Error("application cannot restart, module already consumed its lifecycle",
				slog.String("module", wrapper.Name()), slog.String("module_state", state.String()))
			return fmt.Errorf("application cannot restart, module %s already consumed its lifecycle (state: %s)", wrapper.Name(), state)
		}
	}

	// 按 Priority 稳定排序：优先级不同时按优先级升序；优先级相同时稳定排序
	// 保留原始 append（即 Register/start 调用）顺序，使同优先级模块之间的
	// 依赖关系仍可通过注册顺序表达——不能改用名称等其他字段作为 tie-break，
	// 否则会与"被依赖模块先注册"这个既有约定脱节，详见 stop 中对称的 LIFO 关闭逻辑。
	//
	// 注意：这只保证同优先级组内部不被打乱，不代表整体维持 append 的线性顺序——
	// 不同优先级之间该怎么排还是按 Priority 来，本就是 Priority 存在的意义。
	// 例如按下面顺序依次 append（Priority 未标注的表示默认值 0）：
	//
	//	A(0) B(0) C(2) D(2) E(5) F(0) G(0) H(6) I(7) J(0)
	//
	// 排序后按优先级分组、组内保留 append 相对顺序：
	//
	//	priority=0: A B F G J
	//	priority=2: C D
	//	priority=5: E
	//	priority=6: H
	//	priority=7: I
	//
	// 最终 staticModules 顺序为 A B F G J C D E H I——F/G/J 虽然是在 C/D/E 之后
	// 才 append 的，但优先级更低（0 < 2/5），排序后反而排到了 C/D/E 前面；
	// 关闭时按此顺序整体倒序（LIFO），即 I H E D C J G F B A。
	slices.SortStableFunc(a.modules, func(i, j *moduleWrapper) int {
		return cmp.Compare(i.Priority(), j.Priority())
	})
	staticModules := slices.Clone(a.modules)

	a.setState(StateInit)
	// 向 stop 公示"启动进行中"：stop 观察到 AppStateInit 时会先等这个 channel
	// 关闭，把关闭动作串到启动之后，避免 OnDestroy 与仍在执行的 OnInit 并发。
	a.stopRequested.Store(false)
	startDone := make(chan struct{})
	a.startDone = startDone
	a.Unlock()
	defer close(startDone)

	slog.Info("application starting", slog.Int("module_count", moduleCount))
	for _, wrapper := range staticModules {
		slog.Info("module startup order", slog.String("module", wrapper.Name()))
	}

	// 按上面排好序的 staticModules 顺序依次初始化，保证模块间的启动依赖关系
	// （被依赖模块先初始化）；未显式设置 Priority 时即等价于注册顺序
	// 逐个推进模块生命周期状态：Registered → Initing → Inited / InitFailed。
	// 这份记账正是 shutdownModule 区分"该不该销毁"的唯一依据；启动中途失败时，
	// 排在失败模块之后的模块停在 lifecycleRegistered，不会收到 OnDestroy/Close。
	// 全程用 advanceLifecycle 的 CAS 推进而非直写：并发的 stop 随时可能
	// 通过 claimShutdown 接管某个模块，推进失败即表示"它已经不归我管了"。
	for _, wrapper := range staticModules {
		if a.stopRequested.Load() {
			slog.Warn("application start aborted by shutdown request", slog.String("module", wrapper.Name()))
			return fmt.Errorf("application start aborted by shutdown request before initializing module %s", wrapper.Name())
		}
		if !wrapper.advanceLifecycle(lifecycleRegistered, lifecycleIniting) {
			slog.Warn("module already claimed by shutdown, start aborted",
				slog.String("module", wrapper.Name()), slog.String("module_state", wrapper.lifecycleState().String()))
			return fmt.Errorf("application start aborted, module %s already claimed by shutdown", wrapper.Name())
		}
		if err := wrapper.OnInit(); err != nil {
			// 推进失败说明 stop 已在 OnInit 期间接管了这个模块，它自己会收尾，
			// 这里不再改状态；无论哪种情况启动都到此为止。
			wrapper.advanceLifecycle(lifecycleIniting, lifecycleInitFailed)
			slog.Error("module initialization failed", slog.String("module", wrapper.Name()), slog.Any("error", err))
			return fmt.Errorf("module %s initialization failed: %w", wrapper.Name(), err)
		}
		if !wrapper.advanceLifecycle(lifecycleIniting, lifecycleInited) {
			// stop 在 OnInit 执行期间抢走了销毁权（它会跳过销毁以避开与 OnInit
			// 的并发）。模块资源已经分配却不会有人释放，这一点必须留痕——
			// 它是"启动期收到停止信号"这条罕见路径上唯一的资源泄漏出口。
			slog.Error("module claimed by shutdown while initializing, resources may leak",
				slog.String("module", wrapper.Name()))
			return fmt.Errorf("module %s claimed by shutdown while initializing", wrapper.Name())
		}
	}

	// 拉起 goroutine 之前再复查一道：上面的 OnInit 循环只在每次迭代开头检查
	// stopRequested，全部 OnInit 成功时不会再进入循环体，检查也就不会发生。
	// 而 stop 若走了 startupSettle 的兜底分支，此刻可能已经把这批模块销毁完毕
	// 并把状态复位成 AppStateNone。继续往下会给已经 OnDestroy 过的模块重新
	// 拉起 Serve，并用 lifecycleServing 覆盖掉它们的 lifecycleDestroyed。
	if a.stopRequested.Load() {
		slog.Warn("application start aborted by shutdown request before serving")
		return fmt.Errorf("application start aborted by shutdown request before serving")
	}

	// 所有模块初始化完成后，并发启动各自的 goroutine
	for _, wrapper := range staticModules {
		if !a.startServing(wrapper, false) {
			slog.Warn("application start aborted, module already claimed by shutdown",
				slog.String("module", wrapper.Name()))
			return fmt.Errorf("application start aborted, module %s already claimed by shutdown", wrapper.Name())
		}
	}

	// 等待所有模块就绪后才将应用标记为 AppStateRun。
	// 这不会阻止已就绪的静态模块提前向其他模块投递消息，
	// Priority 也只决定 OnInit 顺序，不保证 Serve 的就绪顺序。
	// waitReady 同时监视 goroutine 是否已经退出：Serve 若在 close(ready) 之前
	// panic 或提前 return，只等 ready 会让 start 永久挂死。
	for _, wrapper := range staticModules {
		if !wrapper.waitReady() {
			slog.Error("module serve exited before becoming ready", slog.String("module", wrapper.Name()))
			return fmt.Errorf("module %s serve exited before becoming ready", wrapper.Name())
		}
	}

	a.Lock()
	// 启动尾声再确认一次：期间若已收到关闭请求，绝不能把一个正在/已经关闭的
	// 应用重新标记为 AppStateRun。
	if a.stopRequested.Load() || a.state.Load() != StateInit {
		a.Unlock()
		slog.Warn("application start aborted by shutdown request")
		return fmt.Errorf("application start aborted by shutdown request")
	}
	a.setState(StateRun)
	a.Unlock()
	slog.Info("application started successfully")
	return nil
}

// startServing 把模块推进到 lifecycleServing 并拉起它的事件循环 goroutine。
//
// 状态先于 goroutine 置位：关闭流程只对 lifecycleServing 的模块执行 wg.Wait，
// 若先起 goroutine 再置位，并发的 stop 可能读到 lifecycleInited 而跳过等待。
//
// **不要把 wg.Add + go 合并回 wg.Go。** 见下面对顺序的说明：wg.Go 会把 Add
// 挪到状态置位之后，重新打开那个"stop 对 0 计数 wg 立刻 Wait 成功"的窗口。
// 返回 false 表示模块已被并发的关闭流程接管，goroutine 未启动，调用方应就此收手。
func (a *App) startServing(wrapper *moduleWrapper, dynamic bool) bool {
	// wg.Add 必须排在状态置位**之前**，不能用 wg.Go 把两者合成一步。
	// 关闭流程只对 lifecycleServing 的模块执行 wg.Wait：若先置位再 Add，
	// 恰好卡在这两行之间的并发 stop 会抢到 lifecycleServing，然后对一个计数
	// 仍为 0 的 wg 立刻 Wait 成功，紧接着开始 OnDestroy——而 Serve 此刻才被
	// 拉起，事件循环与 OnDestroy 并发读写同一批业务内存，正是不可 recover 的 fatal。
	// 先 Add 就没有这个窗口：stop 要么还没看到 Serving（跳过等待，但那时
	// goroutine 也确实还没起），要么看到 Serving 且 wg 计数已经是 1。
	wrapper.wg.Add(1)

	// 用 CAS 而不是直写，理由与启动路径其余各处相同，但这里的后果最严重：
	// 一个并发的 RemoveDynamicModule 可能刚刚把这个模块完整销毁过
	// （OnDestroy → Close → Destroyed），直写会把它覆盖成 Serving 并拉起
	// 事件循环——一个已经释放完资源的模块就这样复活了，之后再也不会有人
	// 销毁它第二次。CAS 失败即表示模块已被接管，goroutine 不能起。
	if !wrapper.advanceLifecycle(lifecycleInited, lifecycleServing) {
		wrapper.wg.Done()
		slog.Warn("module claimed before serving, goroutine not started",
			slog.String("module", wrapper.Name()), slog.String("module_state", wrapper.lifecycleState().String()))
		return false
	}

	go func() {
		defer wrapper.wg.Done()
		a.serveModule(wrapper, dynamic)
	}()
	return true
}

// serveModule 在独立 goroutine 中运行模块的 Serve 主循环。
//
// runtime.LockOSThread 将 goroutine 绑定到专用系统线程：
//   - 保证某些依赖线程本地状态的库（如 OpenGL、部分 CGO 库）能正常工作
//   - 代价是增加系统线程数，对纯 Go 模块而言可考虑移除此调用以减少线程开销
//
// panic 处理策略差异：
//   - 静态模块（dynamic=false）panic 后调用 os.Exit(255)，确保进程不在不确定状态下运行
//   - 动态模块（dynamic=true）panic 仅记录日志，不影响其他模块和进程的正常运行
func (a *App) serveModule(wrapper *moduleWrapper, dynamic bool) {
	runtime.LockOSThread()
	defer func() {
		runtime.UnlockOSThread()
		// 无论正常返回还是 panic，都要公示"goroutine 已结束"，
		// 让 waitReady 里等待就绪的一方不至于永久挂在一个永远不会关闭的 ready 上。
		close(wrapper.exited)
		if r := recover(); r != nil {
			slog.Error("module panic recovered", slog.String("module", wrapper.Name()), slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
			if !dynamic {
				os.Exit(255)
			}
		}
	}()

	slog.Info("started module", slog.String("module", wrapper.Name()))
	wrapper.Serve(wrapper.ctx)
	slog.Info("module stopped", slog.String("module", wrapper.Name()))
}

// stop 按逆序优雅关闭所有模块，保证依赖关系正确解除。
//
// 关闭顺序设计：
//  1. 先关闭所有动态模块（依赖于静态模块，故先于静态模块关闭）
//  2. 再按静态模块的逆启动顺序（LIFO）关闭，后启动的先关闭
//
// 逆序关闭保证了"被依赖模块（先启动）在依赖它的模块（后启动）完全停止后才销毁"的时序，
// 避免在销毁时访问已销毁模块的资源。
func (a *App) stop() {
	a.Lock()
	currentState := a.state.Load()
	if currentState == StateStop {
		a.Unlock()
		slog.Warn("application already stopping")
		return
	}
	if currentState == StateNone {
		a.Unlock()
		slog.Warn("application is not running")
		return
	}
	// 在同一个临界区里就把状态置成 AppStateStop，而不是等启动收敛之后再置：
	// 下面等待 startDone 时锁是放开的，若那时状态还停在 AppStateInit，
	// 并发的第二次 stop 会一路穿过上面的闸门跟着往下走。销毁本身有
	// claimShutdown 的 CAS 兜底不会重复执行，但两条流程会各自扫一遍动态模块、
	// 刷两遍 "shutdown initiated/complete" 日志，还会让外部观察到的 State()
	// 出现 None → Stop → None 的抖动。闸门与状态置位原子化之后，
	// 第二次 stop 在入口就被 "already stopping" 挡住。
	a.setState(StateStop)
	// 向 start 公示关闭意图，再决定是否需要等它跑完。
	a.stopRequested.Store(true)
	startDone := a.startDone
	a.Unlock()

	// 启动仍在进行时不能直接开始销毁：start 跑在独立 goroutine 上（见 Run），
	// 此刻某个模块的 OnInit 可能正在写自己的业务内存，并发调用 OnDestroy 就是
	// 对同一批 map 的并发读写——Go 运行时对此直接 fatal 且不可 recover。
	// 因此把关闭串到启动之后：等 startDone 关闭，或等到 startupSettle 兜底。
	// 即便超时兜底放行，仍在执行 OnInit 的那个模块也会停在 lifecycleIniting，
	// shutdownModule 会跳过它的销毁，不会退化成并发访问。
	//
	// 这里刻意用 startupSettle 而不是 shutdownTimeout：等的是 OnInit 收敛（秒级），
	// 不是模块落盘（可能很久）。复用 30 分钟的 shutdownTimeout 会让"启动期收到
	// SIGTERM"退化成半小时空等，见 defaultStartupSettleTimeout 的说明。
	if currentState == StateInit && startDone != nil {
		timer := time.NewTimer(a.startupSettle)
		select {
		case <-startDone:
		case <-timer.C:
			slog.Error("application start did not finish before startup settle timeout, proceeding",
				slog.Duration("startup_settle_timeout", a.startupSettle))
		}
		timer.Stop()
	}

	// 状态已在入口置好，这里只需要取一份模块快照。
	a.RLock()
	staticModules := slices.Clone(a.modules)
	a.RUnlock()

	slog.Info("application shutdown initiated")

	// 先关闭动态模块，它们通常依赖静态模块提供的服务
	a.removeAllDynamicModules()

	// 按逆序关闭静态模块，保证依赖关系正确解除（后启动的先关闭）
	for _, wrapper := range slices.Backward(staticModules) {
		a.shutdownModule(wrapper)
	}

	// 兜底再扫一遍：AddDynamicModules 的状态判定与 Store 之间存在一个窗口，
	// 恰好在这个窗口里通过检查的调用仍可能在首轮 removeAllDynamicModules
	// 之后把模块登记进来。再扫一次把它们收掉，把"永久泄漏一个带 goroutine
	// 的模块"降级为"多关一轮"。正常情况下这是一次空 Range，没有额外开销。
	a.removeAllDynamicModules()

	a.Lock()
	a.setState(StateNone)
	a.Unlock()
	slog.Info("application shutdown complete")
}

// shutdownModule 依模块自身的生命周期状态关闭单个模块，静态与动态模块共用同一条路径。
//
// 销毁权通过 claimShutdown 原子抢占：每个模块的 OnDestroy 与 Close 至多各执行
// 一次，这是结构性保证，不再依赖 Run 的 sync.Once 与 App 的状态闸门。
//
// 按抢占到的状态分派，**这正是"什么该销毁、什么不该销毁"的完整契约**：
//
//	registered / initFailed → 只 cancel + 关掉自己的 Server，不调 OnDestroy / Close
//	initing                 → 只 cancel，放弃销毁（OnInit 仍在别的 goroutine 上写业务内存）
//	inited                  → cancel + 由框架代关 Server → OnDestroy → Close（无 goroutine 可等）
//	serving                 → cancel → 等待 goroutine 退出（含超时保护）→ OnDestroy → Close
//
// registered / initFailed 不调 OnDestroy 的理由：OnDestroy 的契约是"释放所有资源"，
// 而这两类模块要么一次都没被 OnInit 过（字段全是零值），要么 OnInit 已按契约
// 自行回滚过。对它们调 OnDestroy 会让业务在一个未构造完成的对象上执行清理——
// nil 解引用只会被 destroyModule 的 recover 转成一条日志，真正危险的是不 panic
// 的那一半：把空快照落盘覆盖掉好数据、从注册中心摘掉一个自己从未注册过的实例、
// 释放本就不归自己所有的共享句柄。该行为与 AddDynamicModules 对 OnInit 失败模块
// 的处理（跳过、不销毁）现在完全一致。
//
// **OnDestroy 必须排在 goroutine 退出之后。** 模块是单 goroutine actor，
// 业务内存全靠「只在 Serve 这条 goroutine 上访问」来免锁。
// 若在 Serve 仍在消费消息时就调 OnDestroy（它跑在本关闭 goroutine 上），
// OnDestroy 里任何遍历业务容器的动作都会与事件循环并发读写同一批 map——
// Go 运行时对此直接 fatal 且不可 recover，destroyModule 的 recover 拦不住，
// 进程当场 abort，排在后面的模块（通常是持久化模块）连关闭的机会都没有。
// lifecycleIniting 分支放弃销毁、以及等待超时后跳过 OnDestroy，都是同一条理由。
//
// server 的释放（排空并真正执行积压请求）正常情况下收在 Serve 自己的 ctx.Done
// 分支里，与"停事件循环"是同一步（见 xhive.Skeleton.Serve / chanrpc.Server.Close）。
// 事件循环从未运行过的模块没有人替它做这件事，由 closeIdleServer 代劳，
// 使「OnDestroy 执行时本模块 Server 已关闭」这条不变量在所有路径上都成立。
//
// **client 的释放不能收在那里**：OnDestroy 通常要用 client 把停机前的
// 最后一批状态投递给其它模块（例如汇总数据后转交给专门负责收尾处理的
// 模块），提前关掉 client 会让这些投递全部失败。因此 client 由这里在 OnDestroy
// 返回之后单独关闭（见 xhive.Skeleton.Close）。
// OnDestroy 若想给别的模块投递最后一批数据，对方模块必须还没轮到自己
// 关闭——这正是本函数外层 LIFO 停机顺序要保证的前提。
//
// **但不能给自己投递。** OnDestroy 执行到这一步时，本模块自己的 Server
// 一定已经关闭（Serving 模块由 Serve 退出时关闭，Inited 模块由 closeIdleServer
// 关闭），Cast/Call/AsyncCall 到自己（包括 m.Call(m.ChanRPC(), ...) 这种自调用）
// 都会因为投递到已关闭队列而失败。OnDestroy 需要的收尾工作应该直接写成同步
// 函数调用/循环，不要指望能把处理逻辑重新投递回本模块的事件循环——此刻事件
// 循环已经不存在，没有人能消费这条自投递（详见 IModule.OnDestroy 的说明）。
// 返回值表示本次调用是否走完了完整的销毁（OnDestroy → Close），
// false 出现在三种情形：销毁权被别人拿走、模块仍在 OnInit 中被跳过、
// 等待 goroutine 退出超时。调用方（RemoveDynamicModule）据此如实告知使用者。
func (a *App) shutdownModule(wrapper *moduleWrapper) bool {
	state := wrapper.claimShutdown()
	if state == lifecycleDestroying || state == lifecycleDestroyed {
		// 销毁权已被别的调用方拿走（重复 stop、stop 与 RemoveDynamicModule 并发、
		// 并发卸载同名动态模块），直接返回，保证销毁严格只发生一次。
		return false
	}

	slog.Info("stopping module", slog.String("module", wrapper.Name()), slog.String("module_state", state.String()))

	// 停止信号总要发出：即便 Serve 从未启动，cancel 也要调用，
	// 以释放 context 树上这个节点，并让任何持有该 ctx 的业务逻辑及时脱身。
	wrapper.cancel()

	// case 按状态机的自然顺序排列（Registered → Initing → InitFailed → Inited → Serving），
	// 与 moduleLifecycle 常量的声明顺序一致，便于对照检查有没有漏掉哪个状态。
	switch state {
	case lifecycleInitFailed:
		// OnInit 从未成功——要么一次都没被调用（Registered，启动在它之前就中止了），
		// 要么返回了 error 并已按契约自行回滚（InitFailed）。两种情况下模块都没有
		// 由框架托管的资源，不调 OnDestroy / Close。
		// 但仍要收掉它的 Server：否则别的模块向它投递时 IsClosed 为 false，
		// Cast/AsyncCall 会"成功"入队后静默蒸发，同步 Call 更是永久阻塞。
		a.closeIdleServer(wrapper)
		slog.Info("module destroy skipped, OnInit never succeeded",
			slog.String("module", wrapper.Name()), slog.String("module_state", state.String()))
		wrapper.setLifecycle(lifecycleDestroyed)
		// 这类模块没有任何由框架托管的资源，"不调 OnDestroy"本身就是正确且
		// 完整的收尾，因此算成功。
		return true
	case lifecycleIniting:
		// OnInit 仍在另一条 goroutine 上执行，此刻碰它的业务内存必然是并发读写。
		// 宁可漏掉一次销毁，也不要 fatal——与关闭超时分支同一条取舍。
		//
		// **连它的 Server 都不能碰。** closeIdleServer 要先调 wrapper.ChanRPC()，
		// 那是模块自己的方法，读的是 OnInit 此刻可能正在赋值的字段；
		// 在这里代关 server 等于把"不与 OnInit 并发"这条规矩自己破了一次。
		// 代价是这个模块的 server 保持打开，别的模块向它投递不会立刻失败——
		// 但这条路径本身就已经是"启动期卡住 + 收到停止信号"的异常现场，
		// 记一条 error 让运维能定位，比为了收个 server 去冒 fatal 的风险划算。
		slog.Error("module still initializing, destroy skipped", slog.String("module", wrapper.Name()))
		return false
	case lifecycleInited:
		// 资源已分配但事件循环从未启动：没有 goroutine 需要等待，
		// server 由下面的统一收口代劳。
	case lifecycleServing:
		if !a.waitModuleExit(wrapper) {
			// 超时说明 Serve 还没退出，此时调 OnDestroy 就回到了并发读写的老问题上，
			// 因此直接放弃本模块的销毁：宁可漏掉一次落地，也不要 fatal。
			// 状态停在 lifecycleDestroying，不会被标记为已完成销毁。
			return false
		}
	default:
		// 兜底而非省略：状态机是这套关闭逻辑的唯一依据，日后新增一个
		// moduleLifecycle 却忘了在上面加分支时，模块会落到这里被明确记录并跳过，
		// 而不是静默穿过 switch 去对一个未知状态的模块执行 OnDestroy。
		// （lifecycleDestroying / lifecycleDestroyed 已在函数开头拦下，到不了这里。）
		slog.Error("unexpected module lifecycle state, destroy skipped",
			slog.String("module", wrapper.Name()), slog.String("module_state", state.String()))
		return false
	}

	// 统一收口，保证「OnDestroy 执行时本模块 Server 已关闭」这条不变量在
	// **所有**路径上都成立，而不只是正常停机那条：
	//   - Serving 且正常退出：Serve 的 ctx.Done 分支已经 Close 过，这里因
	//     IsClosed 为真而跳过（幂等）；
	//   - Serving 但 Serve 在 ctx.Done 分支之前 panic 或提前 return（动态模块
	//     的 panic 只记日志不退进程，AddDynamicModules 的 waitReady 失败路径
	//     走的就是这里）：没有人关过它，由这里收掉；
	//   - Inited：事件循环从未存在，同样由这里收掉。
	a.closeIdleServer(wrapper)

	// 事件循环已停（或从未存在），业务内存此刻只有本 goroutine 访问，
	// OnDestroy 可以安全遍历；client 仍然打开，OnDestroy 里的
	// Cast/Call/AsyncCall 仍然可以正常投递给尚未关闭的模块。
	slog.Info("destroying module", slog.String("module", wrapper.Name()))
	a.destroyModule(wrapper)

	// 最后释放 client：OnDestroy 里的投递到此已全部发出
	a.closeModule(wrapper)
	wrapper.setLifecycle(lifecycleDestroyed)
	slog.Info("module shutdown complete", slog.String("module", wrapper.Name()))
	return true
}

// waitModuleExit 等待模块 goroutine 退出，返回 false 表示等待超时。
//
// 超时保护通过独立 goroutine + done channel 实现，而非直接阻塞，
// 原因是 wg.Wait 本身不支持超时，需要借助 select 和 timer 组合。
// 超时后不强制退出，仅记录错误，因为强制终止可能导致数据损坏（如正在写数据库）。
//
// 动态模块的卸载现在同样走这里，WithShutdownTimeout 因此对动态模块也生效：
// 此前 RemoveDynamicModule 用的是裸 wg.Wait，一个不响应 ctx 的动态模块
// 会把整个 stop 永久卡在 removeAllDynamicModules 上，静态模块再也拿不到 OnDestroy。
func (a *App) waitModuleExit(wrapper *moduleWrapper) bool {
	done := make(chan struct{})
	go func() {
		wrapper.wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(a.shutdownTimeout)
	defer timer.Stop()
	select {
	case <-done:
		slog.Info("module goroutine exited", slog.String("module", wrapper.Name()))
		return true
	case <-timer.C:
		slog.Error("module shutdown timeout, OnDestroy skipped", slog.String("module", wrapper.Name()))
		return false
	}
}

// closeIdleServer 收掉一个事件循环从未运行过的模块的 ChanRPC 服务端。
//
// server 的关闭正常由 xhive.Skeleton.Serve 的 ctx.Done 分支完成。启动中途失败时
// 这些模块的 Serve 一次都没跑过，没有任何 goroutine 会替它们关 server，于是：
//   - 别的模块 OnDestroy 里的 Cast/AsyncCall 因为 IsClosed 为 false 而"投递成功"，
//     然后随进程退出静默蒸发，没有任何错误或日志；
//   - 同步 Call 更糟：对端事件循环根本不存在，回包永远不会产生，
//     调用方永久阻塞在 chanrpc 的等待循环里（只每 5 秒打一条 warn），
//     而 shutdownTimeout 只覆盖 wg.Wait，覆盖不到 OnDestroy 本身。
//
// 用的是 Abandon 而不是 Close，这个区别很关键。Close 会把队列里的积压请求
// 真正**执行**一遍，那正是它要求"只能在所属模块自己的事件循环上调用"的原因；
// 而这里跑的是框架的停机 goroutine。在这里执行别人的 handler 会踩两个坑：
// 一是 handler 对业务状态的访问不再局限于单一 goroutine，actor 模型赖以免锁
// 的前提被打破；二是 handler 若发起同步 Call、而对端同样是个事件循环从未运行
// 过的模块，回包永远不会产生，整个 stop 就永久卡死——Close 的 closeDrainTimeout
// 只保护"等待队列里的下一个请求"，保护不了 handler 自身的执行。
//
// Abandon 因此选择把积压请求逐一回成 ErrServerAbandoned：调用方拿到一个明确
// 的失败，而不是静默蒸发或永久等待。丢掉这些请求是正确的——该模块从未宣告
// 就绪（Ready 未关闭），本就不该处理任何请求。
func (a *App) closeIdleServer(wrapper *moduleWrapper) {
	server := wrapper.ChanRPC()
	if server == nil || server.IsClosed() {
		return
	}
	server.Abandon()
}

// closeModule 调用模块的 Close 并捕获其中可能发生的 panic。
//
// 与 destroyModule 对称：Close 同样是 IModule 的公开钩子，业务普遍会重写它来
// 释放出站连接池、生产者、文件句柄。此前它裸调用在 recover 之外，一次 panic
// 会一路冲出 shutdownModule → stop 的 LIFO 循环 → Run，排在后面（即更早注册、
// 通常是持久化/收尾）的所有模块整段拿不到 OnDestroy。
func (a *App) closeModule(wrapper *moduleWrapper) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("module close panic recovered", slog.String("module", wrapper.Name()), slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
		}
	}()

	if err := wrapper.Close(); err != nil {
		slog.Error("module close failed", slog.String("module", wrapper.Name()), slog.Any("error", err))
	}
}

// destroyModule 调用模块的 OnDestroy 并捕获其中可能发生的 panic。
//
// 防御性 panic 捕获的必要性：在关闭流程中，部分资源可能已半释放，
// 若某模块的 OnDestroy 因访问已释放资源而 panic，必须隔离该 panic，
// 确保其他模块的关闭流程不受影响，避免资源泄漏。
func (a *App) destroyModule(wrapper *moduleWrapper) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("module destroy panic recovered", slog.String("module", wrapper.Name()), slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
		}
	}()

	wrapper.OnDestroy()
}

// DynamicModules 返回当前所有动态模块的名称列表，用于监控和管理。
func (a *App) DynamicModules() (res []string) {
	a.dynamicModules.Range(func(key, value any) bool {
		// 与 getChanRPCDynamic 一致：跳过仍在 OnInit / 等待就绪的占位条目，
		// 对外只呈现真正已经在服务的模块。
		if wrapper, ok := value.(*moduleWrapper); ok && !wrapper.visible.Load() {
			return true
		}
		res = append(res, key.(string))
		return true
	})
	return
}

// AddDynamicModuleResult 记录 AddDynamicModules 中单个模块的处理结果。
//
// 相比原先"任一模块初始化失败即刻返回 error，调用方无法知道具体
// 哪些模块成功、哪些失败"的方式，结构化结果使调用方可以精确地知道
// 每个模块的最终状态，从而决定是否需要针对失败模块重试或告警，
// 而不必去猜测已经悄悄启动成功的模块有哪些。
type AddDynamicModuleResult struct {
	Name string // 模块名称
	Err  error  // 初始化错误；nil 表示该模块已成功初始化并启动
}

// AddDynamicModules 在运行时动态添加并启动一批模块，支持热加载。
//
// 与静态模块相比，动态模块的特殊之处：
//   - Serve 的 panic 仅记录日志（serveModule 的 dynamic=true 参数控制）；
//     OnInit 在调用方 goroutine 上执行，其 panic 不被框架捕获
//   - 支持通过 RemoveDynamicModule 单独卸载，不影响其他模块
//   - 模块按 Priority 升序初始化，同优先级时保留传入 mods 的参数顺序（稳定排序），
//     任一失败不会中止后续模块的初始化尝试（已成功初始化的模块不会因后面某个模块失败而回滚）
//   - 名称必须在本 App 的静态、动态模块之间唯一；冲突模块不会执行 OnInit 或 Serve
//
// 返回值为每个非 nil 模块的处理结果列表，调用方可据此精确判断哪些模块
// 成功、哪些失败及失败原因；若全部成功，err 为 nil，否则 err 汇总了
// 所有失败模块的名称，便于快速定位问题而无需遍历 results。
func (a *App) AddDynamicModules(mods ...IModule) (results []AddDynamicModuleResult, err error) {
	// 关闭中/已关闭的应用不再接受新模块：stop 的 removeAllDynamicModules 一旦跑过，
	// 此刻登记进来的模块再也没有人会去关闭它——OnDestroy/Close 永远不会被调用，
	// goroutine 连同 LockOSThread 绑定的系统线程一起永久驻留；更糟的是它的
	// OnInit/Serve 会向正在被 LIFO 销毁的静态模块发起 ChanRPC 调用。
	//
	// 判据用 stopRequested 而不是 State()：stop 在最后会把状态复位成 AppStateNone，
	// 单看状态无法区分"从未启动的新应用"（允许预置动态模块）和"已经关完的应用"
	// （必须拒绝）。stopRequested 在 stop 入口置位、只由 start 复位，正好区分两者。
	if a.stopRequested.Load() {
		return nil, fmt.Errorf("application is shutting down or already stopped, dynamic modules rejected")
	}

	var failedNames []string
	var wrappers []*moduleWrapper
	for _, mod := range mods {
		if mod == nil {
			continue
		}
		// 复用 newModuleWrapper，而不是手工拼装：wrapper 新增字段（exited、
		// lifecycle）时不必记得在这里同步一遍，这正是该构造函数存在的意义。
		wrappers = append(wrappers, newModuleWrapper(mod, mod.Name()))
	}

	// 动态模块同样按 Priority 稳定排序：同优先级时保留传入顺序，
	// 使调用方仍可通过参数顺序表达同优先级模块间的依赖关系（与 start 的排序规则一致，
	// 不同优先级之间会重新排序，只有同优先级组内部不被打乱，示例见 start 中的注释）。
	slices.SortStableFunc(wrappers, func(i, j *moduleWrapper) int {
		return cmp.Compare(i.Priority(), j.Priority())
	})
	for _, wrapper := range wrappers {
		name := wrapper.Name()
		fail := func(initErr error) {
			slog.Error("module init error", slog.String("module", name), slog.Any("error", initErr))
			results = append(results, AddDynamicModuleResult{Name: name, Err: initErr})
			failedNames = append(failedNames, name)
		}

		// **先原子占住名字，再 OnInit。** 此前是"Load 检查重名 → OnInit →
		// startServing → waitReady → Store"，检查与登记之间隔着整个初始化过程，
		// 两个并发调用会双双通过检查、各自跑完 OnInit 并启动 goroutine，最后
		// 后写入的那个静默覆盖前一个：被覆盖的 wrapper 从此再无任何引用能触及，
		// cancel 永不调用、goroutine 连同 LockOSThread 绑定的系统线程永久驻留、
		// OnDestroy/Close 永不执行，而两个同名模块还在同时对外服务。
		// reserveDynamicModule 在同一把 App 写锁内检查静态模块并完成动态占位，
		// 与 Register/start 互斥，同时防住两类模块之间的名称冲突。
		//
		// 占位期间模块尚未就绪，靠 wrapper.visible 挡住对外可见性，
		// 语义仍是"等模块 Ready 之后 ChanRPC(name) 才查得到它"（见 getChanRPCDynamic）。
		// 顺带的好处是：占位一旦落进 dynamicModules，正在进行的 stop 就能
		// 通过 removeAllDynamicModules 看见它，不会再漏掉一个还在 OnInit 的模块。
		// seq 必须在占位**之前**写定：占位一落进表里，wrapper 就可能被别的
		// goroutine 读到，而 seq 是普通字段不是原子量，那时再写就是数据竞争。
		// 代价是被拒绝的模块也会消耗一个序号，这无关紧要——seq 只用于同优先级
		// 内部的相对排序，不要求连续。
		wrapper.seq = a.dynSeq.Add(1)

		if reserveErr := a.reserveDynamicModule(wrapper); reserveErr != nil {
			wrapper.cancel()
			fail(reserveErr)
			continue
		}

		// 必须检查这次 CAS：占位一落进表里，一个并发的 RemoveDynamicModule 就
		// 可能立刻摘走它并抢到销毁权。那时模块还是 lifecycleRegistered，
		// shutdownModule 走"OnInit 从未成功"分支、宣告收尾完整并返回 true——
		// 卸载方据此回复"已卸载"。此刻若无视失败的 CAS 继续跑 OnInit，
		// 就会在框架已经宣告该模块不存在之后，再建一套连接池、再占一次端口，
		// 而这些资源没有任何引用能触及。
		if !wrapper.advanceLifecycle(lifecycleRegistered, lifecycleIniting) {
			a.dynamicModules.CompareAndDelete(name, wrapper)
			fail(fmt.Errorf("dynamic module %q claimed before initialization", name))
			continue
		}
		if initErr := wrapper.OnInit(); initErr != nil {
			wrapper.advanceLifecycle(lifecycleIniting, lifecycleInitFailed)
			// 摘除占位并释放 context 节点：wrapper 到此被彻底丢弃。
			// OnInit 已分配的业务资源由 OnInit 自己在返回 error 前回滚，
			// 框架不调用 OnDestroy/Close——与静态路径的契约完全一致。
			a.dynamicModules.CompareAndDelete(name, wrapper)
			wrapper.cancel()
			fail(initErr)
			continue
		}
		// CAS 推进而非直写：占位已经落进 dynamicModules，一个并发的 stop
		// 可能在 OnInit 执行期间就通过 removeAllDynamicModules 接管了这个模块
		// （抢到 lifecycleIniting 后会跳过销毁，以避开与 OnInit 的并发）。
		// 此刻直写会把 Destroying 覆盖成 Inited，把一个已被放弃的模块重新
		// 拉回"活着"的状态，然后照常 startServing——那正是孤儿模块的来源。
		if !wrapper.advanceLifecycle(lifecycleIniting, lifecycleInited) {
			a.dynamicModules.CompareAndDelete(name, wrapper)
			slog.Error("module claimed by shutdown while initializing, resources may leak",
				slog.String("module", name), slog.String("module_state", wrapper.lifecycleState().String()))
			fail(fmt.Errorf("dynamic module %q claimed by shutdown while initializing", name))
			continue
		}

		// OnInit 期间收到了停止信号：入口那道闸门只挡住"调用时已经在关闭"，
		// 挡不住"调用通过之后才开始关闭"。OnInit 动辄几百毫秒到数秒（建连接池、
		// 拉配置），这段时间足够 stop 跑完两轮 removeAllDynamicModules 并宣告
		// "shutdown complete"。此刻再把模块启动起来，它就成了一个永不退出、
		// 永不 OnDestroy 的孤儿。这里就地收掉它：状态是 lifecycleInited，
		// shutdownModule 会完整执行 OnDestroy → Close。
		if a.stopRequested.Load() {
			a.dynamicModules.CompareAndDelete(name, wrapper)
			a.shutdownModule(wrapper)
			fail(fmt.Errorf("application is shutting down, dynamic module %q rolled back", name))
			continue
		}

		if !a.startServing(wrapper, true) { // dynamic=true：panic 不退出进程
			// 并发的 RemoveDynamicModule 已经接管（甚至可能已销毁完）这个模块，
			// 占位若还在则摘掉，本次添加算失败。
			a.dynamicModules.CompareAndDelete(name, wrapper)
			fail(fmt.Errorf("dynamic module %q claimed by shutdown before serving", name))
			continue
		}
		// 等待事件循环就绪后才对外可见，保证 ChanRPC 一定能接收。
		// waitReady 同时监视 goroutine 退出：Serve 若在 close(ready) 之前 panic，
		// 动态模块的 panic 不会退出进程，只等 ready 会让调用方（常是 HTTP 热加载
		// 接口的处理协程）永久挂死。
		if !wrapper.waitReady() {
			slog.Error("module serve exited before becoming ready", slog.String("module", name))
			a.dynamicModules.CompareAndDelete(name, wrapper)
			a.shutdownModule(wrapper)
			fail(fmt.Errorf("dynamic module %q serve exited before becoming ready", name))
			continue
		}

		// 置为可见：至此模块才真正对 ChanRPC/Stats/DynamicModules 以及停机
		// 流程存在。seq 早在占位之前就写好了（见上），这里只翻可见性开关。
		wrapper.visible.Store(true)

		// 翻开可见性之后再复查一次。前一次复查（OnInit 之后）挡不住这段窗口：
		// 那之后还要 startServing + waitReady，Serve 启动本身就要几毫秒，
		// 足够一次 stop 跑完两轮 removeAllDynamicModules——而那两轮因为
		// visible 还是 false 全都跳过了这个模块。等这里翻开开关时，
		// 停机流程早已结束，谁也不会再来关它：goroutine 连同 LockOSThread
		// 绑定的系统线程永久驻留，OnDestroy/Close 永不执行，而且没有任何日志。
		//
		// 顺序上先 Store 再复查是必要的：反过来的话，恰好在"复查通过"与
		// "Store" 之间开始的 stop 仍然看不见这个模块，窗口没有被消掉。
		// 先翻开可见性意味着从这一刻起 stop 一定能看到它，两边至少有一方
		// 会负责收尾——重复销毁由 claimShutdown 的 CAS 兜住。
		if a.stopRequested.Load() {
			a.dynamicModules.CompareAndDelete(name, wrapper)
			a.shutdownModule(wrapper)
			fail(fmt.Errorf("application started shutting down, dynamic module %q rolled back", name))
			continue
		}

		results = append(results, AddDynamicModuleResult{Name: name})
	}

	if len(failedNames) > 0 {
		err = fmt.Errorf("dynamic modules init failed: %s", strings.Join(failedNames, ", "))
	}
	return results, err
}

// reserveDynamicModule 将静态名称检查与动态名称占位放在同一临界区。
// 只在登记期间持锁，业务 OnInit/Serve 不在锁内执行。
func (a *App) reserveDynamicModule(wrapper *moduleWrapper) error {
	a.Lock()
	defer a.Unlock()

	name := wrapper.Name()
	for _, static := range a.modules {
		if static.Name() == name {
			return fmt.Errorf("module %q already exists", name)
		}
	}
	if _, loaded := a.dynamicModules.LoadOrStore(name, wrapper); loaded {
		return fmt.Errorf("module %q already exists", name)
	}
	return nil
}

// RemoveDynamicModule 同步移除并销毁指定名称的动态模块。
//
// 完整操作序列：
//  1. Load 并确认模块已完成登记；尚在初始化或等待就绪的占位条目不处理
//  2. CompareAndDelete 原子摘除刚才读到的 wrapper，模块立即对 ChanRPC 不可见
//  3. shutdownModule：cancel → 等待 goroutine 退出（受 WithShutdownTimeout 保护）
//     → OnDestroy → Close，与静态模块使用同一条关闭路径
//
// CompareAndDelete 保证并发卸载时只有一个调用方拿到该实例，
// 也不会误删在读取之后才添加的同名新实例。
//
// 返回 true 表示销毁已完整执行。模块不存在、尚未登记、已被其他调用方摘除，
// 或关闭流程未能完成时返回 false。若因 Serve 退出超时而返回 false，
// 模块已经从表中摘除，OnDestroy/Close 被跳过；应先处理旧实例的资源再重新加载。
func (a *App) RemoveDynamicModule(name string) bool {
	value, ok := a.dynamicModules.Load(name)
	if !ok {
		return false
	}

	wrapper, ok := value.(*moduleWrapper)
	if !ok {
		// 表里存的不是一个合法 wrapper（只可能来自包内误用）。摘掉它免得
		// 后续调用反复撞上同一个坏条目，但不声称销毁成功。
		a.dynamicModules.CompareAndDelete(name, value)
		return false
	}

	// **只处理已完成登记的模块。** 名字可能只是 AddDynamicModules 的一个占位，
	// 它的 OnInit 还在跑。摘走这样的占位有害无益：抢到 lifecycleIniting 只能
	// 跳过销毁（不能与 OnInit 并发），而 AddDynamicModules 那边的 CAS 随后失败，
	// 结果是 OnInit 已经建好的连接池、占好的端口谁都不再持有——两个调用都
	// 报失败，资源却确定性泄漏。就算抢到的是 lifecycleRegistered，
	// shutdownModule 也会宣告"无资源、收尾完整"并返回 true，而 OnInit 随后
	// 照跑不误。这两种情况都不是使用者想要的"卸载"。
	//
	// 语义上也说得通：模块还没就绪，ChanRPC(name) 查不到它、DynamicModules()
	// 也不列它，那么"卸载一个还不存在的模块"返回 false 是自洽的。
	// 它的收尾归 AddDynamicModules 负责——OnInit 之后与置为可见之后各有一次
	// stopRequested 复查，停机时会就地回滚。
	if !wrapper.visible.Load() {
		return false
	}

	// CompareAndDelete 而不是 LoadAndDelete：从上面 Load 到这里，另一个并发的
	// 卸载可能已经把它摘走并换上了别的 wrapper（先 remove 再 add 同名模块）。
	// 只有摘到自己刚才看见的那一个才继续，否则说明这次卸载已经被别人做掉了。
	if !a.dynamicModules.CompareAndDelete(name, wrapper) {
		return false
	}

	return a.shutdownModule(wrapper)
}

// removeAllDynamicModules 按优先级倒序关闭所有动态模块。
//
// 先收集快照再逐一移除，而非在 Range 回调中直接移除：
// sync.Map 的文档说明 Range 期间调用 Delete 是安全的，但先收集快照能使逻辑更清晰，
// 且避免在 Range 内部嵌套 RemoveDynamicModule（其中包含 wg.Wait）可能引发的潜在问题。
//
// 排序规则与 AddDynamicModules 的初始化顺序保持一致：优先级升序，同优先级按
// 添加序号（moduleWrapper.seq）升序——不能用 sync.Map.Range 收集到的切片顺序
// 直接当作添加顺序，Range 明确不保证遍历顺序；也不能像静态模块那样退化为按
// 名称排序，那样会与"同优先级按添加顺序表达依赖关系"的约定脱节。不同优先级
// 之间同样会被重新排序，只有同优先级组内部保留添加顺序，示例见 start 中的注释。
// 倒序遍历即得到与初始化相反的关闭顺序：后添加的模块先关闭，与静态模块的
// LIFO 停机语义保持一致，避免动态模块间的依赖关系在关闭阶段被打破。
//
// **只处理已完成登记（visible）的模块。** AddDynamicModules 为了原子地防住
// 并发重名，会在 OnInit 之前就用 LoadOrStore 把名字占进这张表；那些条目此刻
// 正被自己的 OnInit 修改，既不该被这里读（排序要读 Priority/seq）、也不该被
// 这里销毁——抢占一个仍在 OnInit 的模块只能跳过它的 OnDestroy（避开与 OnInit
// 的并发读写），结果就是它的资源永远没人释放。
//
// 占位期模块的收尾由 AddDynamicModules 自己负责：它在 OnInit 返回后会复查
// stopRequested，发现关闭已经开始就地执行完整的 shutdownModule。职责这样划分
// 之后两边都不会漏：可见的归 stop，不可见的归添加它的那个调用方。
func (a *App) removeAllDynamicModules() {
	var wrappers []*moduleWrapper

	a.dynamicModules.Range(func(key, value any) bool {
		if wrapper, ok := value.(*moduleWrapper); ok && wrapper.visible.Load() {
			wrappers = append(wrappers, wrapper)
		}
		return true
	})

	slices.SortStableFunc(wrappers, func(i, j *moduleWrapper) int {
		if n := cmp.Compare(i.Priority(), j.Priority()); n != 0 {
			return n
		}
		return cmp.Compare(i.seq, j.seq)
	})

	for _, wrapper := range slices.Backward(wrappers) {
		a.RemoveDynamicModule(wrapper.Name())
	}
}

// RegisterSignal 注册信号。
//
// 同一信号可多次注册；收到信号时会为每个处理器启动 goroutine 并等待全部完成，不保证完成顺序：
//   - SIGHUP：可叠加多个热重载逻辑，每个处理器独立执行
//
// SIGINT / SIGTERM 为可捕获的框架保留信号；SIGKILL 不可被进程捕获，也作为保留信号禁止业务注册。
//
// 示例（为当前应用实例 a 注册 SIGHUP 热重载）：
//
//	if err := a.RegisterSignal(func() {
//	    slog.Info("收到 SIGHUP，重新加载配置")
//	    reloadConfig()
//	}, syscall.SIGHUP); err != nil {
//	    return err
//	}
func (a *App) RegisterSignal(trap signal.Trap, sigs ...os.Signal) error {
	return a.sm.Register(trap, sigs...)
}
