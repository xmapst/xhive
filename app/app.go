package app

import (
	"cmp"
	"context"
	"errors"
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
	sm              *signal.Manager  // 当前 App 的进程信号管理器；仅 Run 启用，RunContext 不注册信号
	lockOSThread    bool             // 是否为模块 Serve goroutine 绑定 OS 线程；默认开启，由构造选项确定
	panicExit       bool             // 应用启动或静态 Serve panic 时是否退出进程；关闭后返回错误并有序清理
	startupTimeout  time.Duration    // 单次等待模块 Ready 的上限，不限制 OnInit 的执行时间
	failures        chan error       // 静态模块异常退出的有界通知通道；非阻塞发送，由 Run/RunContext 接收并触发关闭
	ready           chan struct{}    // 全部静态模块就绪并进入 StateRun 后，由 start 关闭的一次性信号
	shutdownErr     error            // 由关闭流程串行汇总的清理错误；Run/RunContext 在关闭完成后读取
	dynamicModules  sync.Map         // 动态模块集合，key 为模块名，支持运行时热加载
	modules         []*moduleWrapper // 静态模块列表，按优先级排序，启动后不允许修改
	dynSeq          atomic.Uint64    // 动态模块添加序号生成器，见 moduleWrapper.seq 的说明
	startDone       chan struct{}    // start 进行中时非 nil，start 返回前关闭；受 RWMutex 保护，见 stop 的启动交接说明
	shutdownTimeout time.Duration    // 单次排空与卸载等待上限；全应用关闭超时后继续等待实际退出
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
		lockOSThread:    true,
		panicExit:       true,
		startupTimeout:  30 * time.Second,
		failures:        make(chan error, 1),
		ready:           make(chan struct{}),
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
		if wrapper, ok := value.(*moduleWrapper); ok && wrapper.visible.Load() && !wrapper.retiring.Load() {
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
			if !wrapper.visible.Load() || wrapper.retiring.Load() {
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
	return a.run(context.Background(), true, mods...)
}

func (a *App) run(ctx context.Context, signals bool, mods ...IModule) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !a.runInFlight.CompareAndSwap(false, true) {
		return errors.New("application is already running")
	}
	defer a.runInFlight.Store(false)
	stopped := make(chan struct{})
	stopFn := sync.OnceFunc(func() {
		a.stop()
		close(stopped)
	})
	if signals {
		a.sm.Start(stopFn)
		defer a.sm.Stop()
	}
	errCh := make(chan error, 1)
	go func() {
		if err := a.start(mods...); err != nil {
			errCh <- err
		}
	}()
	var err error
	select {
	case err = <-errCh:
		stopFn()
	case err = <-a.failures:
		stopFn()
	case <-ctx.Done():
		stopFn()
	case <-stopped:
	}
	return errors.Join(err, a.shutdownErr)
}

// start 按顺序初始化并启动所有已注册的模块。
//
// 执行流程：
//  1. 状态检查 + 将 Run 参数中的模块追加到 modules 列表，两步在同一把写锁内完成，
//     防止并发调用 Register/start 时出现 TOCTOU 竞态（支持 Register + Run 两种注册方式）
//  2. 依次调用 OnInit，任一失败则中止启动并返回描述失败原因的 error
//  3. 为每个模块启动独立 goroutine 并运行 Serve
//
// 启动 panic 默认终止进程；WithPanicExit(false) 将其转成启动失败并有序关闭。
func (a *App) start(mods ...IModule) (err error) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("application initialization panicked", slog.String("panic_type", fmt.Sprintf("%T", r)), slog.String("stack", string(debug.Stack())))
			if a.panicExit {
				os.Exit(255)
			}
			err = errors.New("application initialization panicked")
		}
	}()

	a.Lock()
	currentState := a.state.Load()
	if currentState != StateNone || a.stopRequested.Load() {
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
			return errors.New("application shutdown interrupted initialization")
		}
		if err := a.initializeModule(wrapper); err != nil {
			return fmt.Errorf("module %s initialization failed: %w", wrapper.Name(), err)
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
	// awaitReady 同时监视 goroutine 是否已经退出：Serve 若在 close(ready) 之前
	// panic 或提前 return，只等 ready 会让 start 永久挂死。
	for _, wrapper := range staticModules {
		if !a.awaitReady(wrapper) {
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
	close(a.ready)
	a.Unlock()
	slog.Info("application started successfully")
	return nil
}

// startServing 把模块推进到 lifecycleServing 并拉起它的事件循环 goroutine。
//
// 状态先于 goroutine 置位，关闭流程看到 Serving 就等待构造时创建的 exited。
// CAS 成功后必须拉起 goroutine，由它在 Serve 及框架收尾全部返回后关闭 exited。
// 返回 false 表示模块已被并发的关闭流程接管，goroutine 未启动，调用方应就此收手。
func (a *App) startServing(wrapper *moduleWrapper, dynamic bool) bool {
	// 用 CAS 而不是直写，理由与启动路径其余各处相同，但这里的后果最严重：
	// 一个并发的 RemoveDynamicModule 可能刚刚把这个模块完整销毁过
	// （OnDestroy → Close → Destroyed），直写会把它覆盖成 Serving 并拉起
	// 事件循环——一个已经释放完资源的模块就这样复活了，之后再也不会有人
	// 销毁它第二次。CAS 失败即表示模块已被接管，goroutine 不能起。
	if !wrapper.advanceLifecycle(lifecycleInited, lifecycleServing) {
		slog.Warn("module claimed before serving, goroutine not started",
			slog.String("module", wrapper.Name()), slog.String("module_state", wrapper.lifecycleState().String()))
		return false
	}

	go func() {
		defer close(wrapper.exited)
		a.serveModule(wrapper, dynamic)
	}()
	return true
}

// serveModule 在独立 goroutine 中运行模块的 Serve 主循环。
//
// runtime.LockOSThread 将 goroutine 绑定到专用系统线程：
//   - 保证某些依赖线程本地状态的库（如 OpenGL、部分 CGO 库）能正常工作
//   - 纯 Go 模块可通过 WithOSThreadLock(false) 关闭线程绑定
//
// panic 处理策略差异：
//   - 静态模块默认 panic 后终止进程；WithPanicExit(false) 改为返回失败并有序关闭
//   - 动态模块（dynamic=true）panic 仅记录日志，不影响其他模块和进程的正常运行
func (a *App) serveModule(wrapper *moduleWrapper, dynamic bool) {
	if a.lockOSThread {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}
	defer func() {
		value := recover()
		if value != nil {
			slog.Error("module event loop panicked", slog.String("module", wrapper.Name()), slog.String("panic_type", fmt.Sprintf("%T", value)), slog.String("stack", string(debug.Stack())))
			if !dynamic && a.panicExit {
				os.Exit(255)
			}
		}
		if !dynamic && wrapper.ctx.Err() == nil {
			a.moduleFailure(fmt.Errorf("static module %s exited unexpectedly", wrapper.Name()))
		}
	}()
	wrapper.Serve(wrapper.ctx)
}

// stop 按逆序优雅关闭所有模块，保证依赖关系正确解除。
//
// 关闭顺序设计：
//  1. 按静态模块逆序调用 Quiesce，冻结控制入口，保留共享服务
//  2. 完整关闭所有动态模块，包括仍在初始化或排空的实例
//  3. 按静态模块的逆启动顺序（LIFO）关闭，后启动的先关闭
//
// 逆序关闭保证了"被依赖模块（先启动）在依赖它的模块（后启动）完全停止后才销毁"的时序，
// 避免在销毁时访问已销毁模块的资源。
func (a *App) stop() {
	a.Lock()
	currentState := a.state.Load()
	if currentState == StateStop || (currentState == StateNone && a.stopRequested.Load()) {
		a.Unlock()
		return
	}
	a.setState(StateStop)
	a.stopRequested.Store(true)
	startDone := a.startDone
	a.Unlock()
	if currentState == StateInit && startDone != nil {
		timer := time.NewTimer(a.startupSettle)
		select {
		case <-startDone:
		case <-timer.C:
			slog.Warn("application startup still settling")
		}
		timer.Stop()
	}
	a.RLock()
	staticModules := slices.Clone(a.modules)
	a.RUnlock()
	// Freeze controllers first; shared services remain live for dynamic drain.
	for _, wrapper := range slices.Backward(staticModules) {
		if wrapper.lifecycleState() != lifecycleServing {
			continue
		}
		select {
		case <-wrapper.Ready():
			if quiescer, ok := wrapper.IModule.(Quiescer); ok {
				a.retryPreparation(wrapper, quiescer.Quiesce)
			}
		default:
		}
	}
	a.removeAllDynamicModules()
	for _, wrapper := range slices.Backward(staticModules) {
		a.shutdownErr = errors.Join(a.shutdownErr, a.waitShutdown(wrapper))
	}
	a.removeAllDynamicModules()
	a.Lock()
	a.setState(StateNone)
	a.Unlock()
}

// shutdownModule begins framework-owned cleanup and bounds this wait only.
// The module name and cleanup task survive a timeout; App shutdown joins them.
func (a *App) shutdownModule(wrapper *moduleWrapper) bool {
	a.beginShutdown(wrapper)
	timer := time.NewTimer(a.shutdownTimeout)
	defer timer.Stop()
	select {
	case <-wrapper.shutdownDone:
		return wrapper.shutdownErr == nil
	case <-timer.C:
		return false
	}
}

// waitModuleExit 复用退出屏障，返回 false 表示本次等待超时。
// 调用方继续等待时不重复创建 goroutine，也不销毁仍在运行的模块或依赖。
func (a *App) waitModuleExit(wrapper *moduleWrapper) bool {
	timer := time.NewTimer(a.shutdownTimeout)
	defer timer.Stop()
	select {
	case <-wrapper.exited:
		slog.Info("module goroutine exited", slog.String("module", wrapper.Name()))
		return true
	case <-timer.C:
		slog.Warn("module shutdown still waiting", slog.String("module", wrapper.Name()))
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
func (a *App) closeModule(wrapper *moduleWrapper) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("module %s cleanup panicked", wrapper.Name())
		}
	}()
	return wrapper.Close()
}

// destroyModule runs only after Serve and its producers have fully exited.
func (a *App) destroyModule(wrapper *moduleWrapper) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("module %s destruction panicked", wrapper.Name())
		}
	}()
	wrapper.OnDestroy()
	return nil
}

// DynamicModules 返回当前所有动态模块的名称列表，用于监控和管理。
func (a *App) DynamicModules() (res []string) {
	a.dynamicModules.Range(func(key, value any) bool {
		// 与 getChanRPCDynamic 一致：跳过仍在 OnInit / 等待就绪的占位条目，
		// 对外只呈现真正已经在服务的模块。
		if wrapper, ok := value.(*moduleWrapper); ok && (!wrapper.visible.Load() || wrapper.retiring.Load()) {
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
//   - Serve 的 panic 仅记录日志；OnInit 默认向调用方传播 panic，
//     WithPanicExit(false) 将 OnInit panic 转成该模块的添加失败
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
	// （必须拒绝）。stopRequested 在 stop 入口置位且不再复位，区分这两种情况。
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

		// 初始化前通过 CAS 确认占位仍归启动方；全应用停止可能已经接管它。
		if initErr := a.initializeModule(wrapper); initErr != nil {
			a.shutdownModule(wrapper)
			fail(initErr)
			continue
		}

		// 初始化期间开始停止时，交由后台清理等待 OnInit 返回并释放资源。
		if a.stopRequested.Load() || wrapper.retiring.Load() {
			a.shutdownModule(wrapper)
			fail(fmt.Errorf("application is shutting down, dynamic module %q rolled back", name))
			continue
		}

		if !a.startServing(wrapper, true) { // dynamic=true：panic 不退出进程
			// 停机已接管该实例；清理完成前仍保留名称占位。
			a.shutdownModule(wrapper)
			fail(fmt.Errorf("dynamic module %q claimed by shutdown before serving", name))
			continue
		}
		// 等待事件循环就绪后才对外可见，保证 ChanRPC 一定能接收。
		// awaitReady 同时监视 goroutine 退出：Serve 若在 close(ready) 之前 panic，
		// 动态模块的 panic 不会退出进程，只等 ready 会让调用方（常是 HTTP 热加载
		// 接口的处理协程）永久挂死。
		if !a.awaitReady(wrapper) {
			slog.Error("module serve exited before becoming ready", slog.String("module", name))
			a.shutdownModule(wrapper)
			fail(fmt.Errorf("dynamic module %q serve exited before becoming ready", name))
			continue
		}

		// 就绪后开放寻址。停机从名称占位开始就能看见该实例。
		wrapper.visible.Store(true)

		// 与停机交错时不返回添加成功；同一实例的清理由 shutdownOnce 合并。
		if a.stopRequested.Load() || wrapper.retiring.Load() {
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

	if a.stopRequested.Load() {
		return errors.New("application is shutting down")
	}
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

// RemoveDynamicModule retires a dynamic module and waits up to shutdownTimeout.
// A timeout returns false while cleanup continues with the name still reserved.
func (a *App) RemoveDynamicModule(name string) bool {
	if _, exists := a.dynamicModules.Load(name); !exists {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), a.shutdownTimeout)
	defer cancel()
	return a.RemoveDynamicModuleContext(ctx, name) == nil
}

// removeAllDynamicModules joins every reserved dynamic owner in reverse order,
// including initialization and cleanup still in progress.
func (a *App) removeAllDynamicModules() {
	var wrappers []*moduleWrapper
	a.dynamicModules.Range(func(_, value any) bool {
		if wrapper, ok := value.(*moduleWrapper); ok {
			wrappers = append(wrappers, wrapper)
		}
		return true
	})
	slices.SortStableFunc(wrappers, func(i, j *moduleWrapper) int {
		if order := cmp.Compare(i.Priority(), j.Priority()); order != 0 {
			return order
		}
		return cmp.Compare(i.seq, j.seq)
	})
	for _, wrapper := range slices.Backward(wrappers) {
		a.shutdownErr = errors.Join(a.shutdownErr, a.waitShutdown(wrapper))
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
