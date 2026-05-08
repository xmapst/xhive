package xhive

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xmapst/xhive/chanrpc"
)

// IModule 定义应用模块的完整生命周期接口。
//
// 框架通过此接口管理模块从初始化到销毁的全过程，每个模块代表一个独立的业务单元，
// 拥有独立的 goroutine、RPC 服务端和定时器管理器。
// 模块之间通过 ChanRPC 通信，天然隔离内部状态，无需跨模块加锁。
type IModule interface {
	// Name 返回模块唯一名称，用于日志标识和跨模块 RPC 寻址。
	Name() string
	// OnInit 执行模块初始化，任一模块失败则终止整个应用启动流程。
	OnInit() error
	// Serve 执行模块主循环，应监听 ctx.Done() 并在收到取消信号时退出。
	Serve(ctx context.Context)
	// OnDestroy 执行模块销毁，在 goroutine 完全退出前调用，负责释放所有资源。
	OnDestroy()
	// ChanRPC 返回模块的 ChanRPC 服务端，nil 表示该模块不接受外部 RPC 调用。
	ChanRPC() *chanrpc.Server
}

const (
	// AppStateNone 表示应用未启动或已完全停止，可安全重新启动。
	AppStateNone = iota
	// AppStateInit 表示应用正在初始化，所有模块的 OnInit 正在按序执行。
	AppStateInit
	// AppStateRun 表示应用运行中，所有模块已成功启动并处于活跃状态。
	AppStateRun
	// AppStateStop 表示应用正在优雅关闭，模块正按逆序依次停止。
	AppStateStop
)

const (
	// defaultShutdownTimeout 单个模块优雅关闭的默认最大等待时间。
	// 设置为 30 分钟是为了兼容可能持有长时间锁或大批量数据落盘的模块，
	// 超时后记录错误日志但不强制终止，避免数据损坏，由运维介入处理。
	//
	// 该值仅作为 app.shutdownTimeout 字段的默认值；不同模块对关闭时限的
	// 容忍度差异很大，写死的全局常量缺乏灵活性，业务可通过
	// WithShutdownTimeout 选项按需覆盖。
	defaultShutdownTimeout = 30 * time.Minute
)

// AppOption 用于自定义 app 实例的可选行为。
type AppOption func(*app)

// WithShutdownTimeout 自定义单个模块优雅关闭的最大等待时间。
//
// 超时后仅记录错误日志，不会强制终止模块 goroutine（避免数据损坏），
// 因此该值应结合具体模块可能持有的最长阻塞操作（如大批量落盘、外部调用）来设置。
// d <= 0 时该选项不生效，沿用 defaultShutdownTimeout。
func WithShutdownTimeout(d time.Duration) AppOption {
	return func(a *app) {
		if d > 0 {
			a.shutdownTimeout = d
		}
	}
}

// moduleWrapper 为 IModule 附加框架运行时所需的控制元数据。
//
// ctx/cancel 构成模块停止信号通道：框架通过调用 cancel 通知模块 OnRun 应退出主循环；
// wg 用于等待模块 goroutine 完全退出，保证关闭流程可同步等待完成。
type moduleWrapper struct {
	IModule
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// newModuleWrapper 构造 moduleWrapper 并初始化其停止信号 context。
//
// 该构造逻辑原先在 Register 与 start 中各自重复实现一次，收敛到此处
// 之后两处调用方只需一行代码即可复用，避免后续新增字段时两处遗漏同步修改。
func newModuleWrapper(mod IModule) *moduleWrapper {
	wrapper := &moduleWrapper{IModule: mod}
	wrapper.ctx, wrapper.cancel = context.WithCancel(context.Background())
	return wrapper
}

// app 是应用框架的核心结构，统一管理静态模块列表和动态模块集合。
//
// 并发安全说明：
//   - modules 切片与 state 共用同一把 RWMutex 保护：Register/start 需要在
//     持锁状态下原子地完成"检查状态 + 追加模块"，避免两个操作分离时出现
//     TOCTOU 竞态（例如并发调用 Register 与 Run 时都读到 AppStateNone）
//   - dynamicModules 使用 sync.Map，原生支持并发增删改查
//   - 信号注册与分发由 SignalManager 内部的 RWMutex 保护
type app struct {
	sm              *SignalManager
	dynamicModules  sync.Map         // 动态模块集合，key 为模块名，支持运行时热加载
	modules         []*moduleWrapper // 静态模块列表，按优先级排序，启动后不允许修改
	shutdownTimeout time.Duration    // 单个模块优雅关闭的最大等待时间，可通过 WithShutdownTimeout 自定义
	sync.RWMutex                     // 保护 modules 切片与 state 字段的读写
	state           atomic.Int32     // 应用全局状态，读写均在持锁状态下进行，Stats/State 对外仍以原子读保证快照一致
}

// newApp 创建新的应用框架实例，初始状态为 AppStateNone。
func newApp(opts ...AppOption) *app {
	a := &app{
		sm:              NewSignalManager(),
		modules:         make([]*moduleWrapper, 0),
		shutdownTimeout: defaultShutdownTimeout,
	}
	a.setState(AppStateNone)
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
func (a *app) setState(state int32) {
	a.state.Store(state)
}

// State 通过原子读取获取应用当前状态，可在任意 goroutine 中安全调用。
//
// 注意：State 不加锁，是一个"当前时刻的近似快照"，
// 用于外部监控和只读判断场景；涉及状态变更的操作（Register/start/stop）
// 内部会在持锁状态下重新校验，不依赖此处的读取结果做决策。
func (a *app) State() int32 {
	return a.state.Load()
}

// Stats 返回所有模块（静态 + 动态）的 RPC 队列积压状态统计字符串。
//
// 输出格式："{static|dynamic}: {模块名}, rpc_queue_length: {队列长度}"
// rpc_queue_length 反映模块消息积压程度，是性能瓶颈和消息处理速率的重要观测指标。
// N/A 表示该模块未配置 ChanRPC 服务端（如纯定时器模块）。
func (a *app) Stats() string {
	a.RLock()
	defer a.RUnlock()

	var builder strings.Builder

	// 遍历静态模块
	for _, wrapper := range a.modules {
		a.appendModuleStats(&builder, "static", wrapper)
	}

	// 遍历动态模块（sync.Map.Range 保证并发安全）
	a.dynamicModules.Range(func(key, value any) bool {
		if wrapper, ok := value.(*moduleWrapper); ok {
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
func (a *app) appendModuleStats(builder *strings.Builder, moduleType string, wrapper *moduleWrapper) {
	rpcServer := wrapper.ChanRPC()

	if rpcServer != nil {
		_, _ = fmt.Fprintf(builder, "%s: %s, rpc_queue_length: %d\n",
			moduleType, wrapper.Name(), rpcServer.Len())
	} else {
		_, _ = fmt.Fprintf(builder, "%s: %s, rpc_queue_length: N/A\n",
			moduleType, wrapper.Name())
	}
}

// ChanRPC 通过模块名获取对应模块的 ChanRPC 服务端，用于跨模块消息投递。
//
// 查找策略：优先从静态模块列表中查找（加读锁），未命中时再查找动态模块（无锁，sync.Map 保证安全）。
// 两步查找分开处理的原因：静态模块列表需要锁，而 sync.Map 无需锁，
// 分开可以在找到静态模块时尽早释放读锁，减少锁持有时间。
func (a *app) ChanRPC(name string) *chanrpc.Server {
	a.RLock()
	for _, wrapper := range a.modules {
		if wrapper.Name() == name {
			a.RUnlock()
			return wrapper.ChanRPC()
		}
	}
	a.RUnlock()

	return a.getChanRPCDynamic(name)
}

// getChanRPCDynamic 从动态模块集合中查找 ChanRPC 服务端。
func (a *app) getChanRPCDynamic(name string) *chanrpc.Server {
	if value, ok := a.dynamicModules.Load(name); ok {
		if wrapper, ok := value.(*moduleWrapper); ok {
			return wrapper.ChanRPC()
		}
	}
	return nil
}

// Register 在应用启动前注册静态模块。
//
// 静态模块在应用整个生命周期中持续运行，不支持热卸载。
// 若应用已处于运行或停止状态则返回错误，防止运行时并发修改 modules 切片引发数据竞争。
//
// 状态检查与追加操作在同一把写锁内完成（而非分离为"先读状态、再加锁追加"），
// 避免 Register 与 Run 并发调用时，两个 goroutine 都读到 AppStateNone
// 后同时进入各自的追加/启动流程，产生 TOCTOU 竞态。
func (a *app) Register(mods ...IModule) error {
	a.Lock()
	defer a.Unlock()

	if a.state.Load() != AppStateNone {
		return fmt.Errorf("application is already running")
	}

	for _, mod := range mods {
		if mod == nil {
			continue
		}
		a.modules = append(a.modules, newModuleWrapper(mod))
	}

	return nil
}

// Run 注册并启动所有模块，阻塞至所有信号处理完毕（通常是收到 SIGINT/SIGTERM 后优雅关闭）。
//
// 框架默认注册 SIGINT/SIGTERM → 优雅关闭，SIGHUP → 仅记录日志继续运行。
// SIGKILL 仅作为框架保留信号禁止业务注册，操作系统不会把它投递给进程处理。
// 业务层可在 Run 调用前通过 RegisterSignal 覆盖默认处理器，或注册额外信号（如 SIGUSR1）。
func (a *app) Run(mods ...IModule) {
	stopped := make(chan struct{})
	stopOnce := sync.Once{}
	stopFn := func() {
		stopOnce.Do(func() {
			a.stop()
			close(stopped)
		})
	}

	a.sm.Start(stopFn)

	var errCh = make(chan bool, 1)
	go func() {
		if !a.start(mods...) {
			errCh <- true
		}
	}()

	select {
	case <-errCh:
		slog.Error("app start failed")
		stopFn()
	case <-stopped:
	}

	a.sm.Stop()
}

// start 按顺序初始化并启动所有已注册的模块。
//
// 执行流程：
//  1. 状态检查 + 将 Run 参数中的模块追加到 modules 列表，两步在同一把写锁内完成，
//     防止并发调用 Register/start 时出现 TOCTOU 竞态（支持 Register + Run 两种注册方式）
//  2. 依次调用 OnInit，任一失败则中止启动并返回 false
//  3. 为每个模块启动独立 goroutine 并运行 OnRun
//
// 顶层 panic recover：捕获启动过程中的意外 panic，记录完整堆栈后以退出码 255 终止进程，
// 防止进程在不确定状态下继续运行造成数据损坏。
func (a *app) start(mods ...IModule) bool {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("application panic recovered", "panic", r, "stack", string(debug.Stack()))
			os.Exit(255)
		}
	}()

	a.Lock()
	currentState := a.state.Load()
	if currentState != AppStateNone {
		a.Unlock()
		slog.Error("application cannot start twice", "current_state", currentState)
		return false
	}

	for _, mod := range mods {
		if mod == nil {
			continue
		}
		a.modules = append(a.modules, newModuleWrapper(mod))
	}
	moduleCount := len(a.modules)
	if moduleCount == 0 {
		a.Unlock()
		slog.Warn("no modules provided to start")
		return false
	}
	a.setState(AppStateInit)
	a.Unlock()

	slog.Info("application starting", "module_count", moduleCount)
	for _, wrapper := range a.modules {
		slog.Info("module startup order", "module", wrapper.Name())
	}

	// 按注册顺序依次初始化，保证模块间的启动依赖关系（被依赖模块先初始化）
	for _, wrapper := range a.modules {
		if err := wrapper.OnInit(); err != nil {
			slog.Error("module initialization failed", "module", wrapper.Name(), "err", err)
			return false
		}
	}

	// 所有模块初始化完成后，并发启动各自的 goroutine
	for _, wrapper := range a.modules {
		wrapper.wg.Add(1)
		go a.serveModule(wrapper, false)
	}

	a.Lock()
	a.setState(AppStateRun)
	a.Unlock()
	slog.Info("application started successfully")
	return true
}

// serveModule 在独立 goroutine 中运行模块的 OnRun 主循环。
//
// runtime.LockOSThread 将 goroutine 绑定到专用系统线程：
//   - 保证某些依赖线程本地状态的库（如 OpenGL、部分 CGO 库）能正常工作
//   - 代价是增加系统线程数，对纯 Go 模块而言可考虑移除此调用以减少线程开销
//
// panic 处理策略差异：
//   - 静态模块（dynamic=false）panic 后调用 os.Exit(255)，确保进程不在不确定状态下运行
//   - 动态模块（dynamic=true）panic 仅记录日志，不影响其他模块和进程的正常运行
func (a *app) serveModule(wrapper *moduleWrapper, dynamic bool) {
	runtime.LockOSThread()
	defer func() {
		runtime.UnlockOSThread()
		wrapper.wg.Done()
		if r := recover(); r != nil {
			slog.Error("module panic recovered", "module", wrapper.Name(), "panic", r, "stack", string(debug.Stack()))
			if !dynamic {
				os.Exit(255)
			}
		}
	}()

	slog.Info("started module", "module", wrapper.Name())
	wrapper.Serve(wrapper.ctx)
	slog.Info("module stopped", "module", wrapper.Name())
}

// stop 按逆序优雅关闭所有模块，保证依赖关系正确解除。
//
// 关闭顺序设计：
//  1. 先关闭所有动态模块（依赖于静态模块，故先于静态模块关闭）
//  2. 再按静态模块的逆启动顺序（LIFO）关闭，后启动的先关闭
//
// 逆序关闭保证了"被依赖模块（先启动）在依赖它的模块（后启动）完全停止后才销毁"的时序，
// 避免在销毁时访问已销毁模块的资源。
func (a *app) stop() {
	a.Lock()
	currentState := a.state.Load()
	if currentState == AppStateStop {
		a.Unlock()
		slog.Warn("application already stopping")
		return
	}
	if currentState == AppStateNone {
		a.Unlock()
		slog.Warn("application is not running")
		return
	}
	a.setState(AppStateStop)
	a.Unlock()

	slog.Info("application shutdown initiated")

	// 先关闭动态模块，它们通常依赖静态模块提供的服务
	a.removeAllDynamicModules()

	// 按逆序关闭静态模块，保证依赖关系正确解除（后启动的先关闭）
	a.RLock()
	moduleCount := len(a.modules)
	a.RUnlock()

	for i := moduleCount - 1; i >= 0; i-- {
		a.shutdownModule(a.modules[i])
	}

	a.Lock()
	a.setState(AppStateNone)
	a.Unlock()
	slog.Info("application shutdown complete")
}

// shutdownModule 优雅关闭单个静态模块，完整流程为：调用 OnDestroy → 发送停止信号 → 等待 goroutine 退出（含超时保护）。
//
// 超时保护通过独立 goroutine + done channel 实现，而非直接阻塞，
// 原因是 wg.Wait 本身不支持超时，需要借助 select 和 timer 组合。
// 超时后不强制退出，仅记录错误，因为强制终止可能导致数据损坏（如正在写数据库）。
func (a *app) shutdownModule(wrapper *moduleWrapper) {
	slog.Info("destroying module", "module", wrapper.Name())
	a.destroyModule(wrapper)

	// 通过 context 取消向模块的 OnRun 发送停止信号
	wrapper.cancel()
	// 在辅助 goroutine 中等待模块退出，配合 select + timer 实现超时保护
	done := make(chan struct{})
	go func() {
		wrapper.wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(a.shutdownTimeout)
	defer timer.Stop()
	select {
	case <-done:
		slog.Info("module goroutine exited", "module", wrapper.Name())
	case <-timer.C:
		slog.Error("module shutdown timeout", "module", wrapper.Name())
		return
	}
	slog.Info("module shutdown complete", "module", wrapper.Name())
}

// destroyModule 调用模块的 OnDestroy 并捕获其中可能发生的 panic。
//
// 防御性 panic 捕获的必要性：在关闭流程中，部分资源可能已半释放，
// 若某模块的 OnDestroy 因访问已释放资源而 panic，必须隔离该 panic，
// 确保其他模块的关闭流程不受影响，避免资源泄漏。
func (a *app) destroyModule(wrapper *moduleWrapper) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("module destroy panic recovered", "module", wrapper.Name(), "panic", r, "stack", string(debug.Stack()))
		}
	}()

	wrapper.OnDestroy()
}

// DynamicModules 返回当前所有动态模块的名称列表，用于监控和管理。
func (a *app) DynamicModules() (res []string) {
	a.dynamicModules.Range(func(key, value any) bool {
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
//   - panic 不会导致进程退出，仅记录日志（onStartModule 的 dynamic=true 参数控制）
//   - 支持通过 RemoveDynamicModule 单独卸载，不影响其他模块
//   - 模块按传入顺序依次初始化，任一失败不会中止后续模块的初始化尝试
//     （已成功初始化的模块不会因后面某个模块失败而回滚）
//
// 返回值为每个非 nil 模块的处理结果列表，调用方可据此精确判断哪些模块
// 成功、哪些失败及失败原因；若全部成功，err 为 nil，否则 err 汇总了
// 所有失败模块的名称，便于快速定位问题而无需遍历 results。
func (a *app) AddDynamicModules(mods ...IModule) (results []AddDynamicModuleResult, err error) {
	var failedNames []string

	for _, mod := range mods {
		if mod == nil {
			continue
		}
		wrapper := newModuleWrapper(mod)
		name := wrapper.Name()

		if initErr := wrapper.OnInit(); initErr != nil {
			slog.Error("module init error", "module", name, "err", initErr)
			results = append(results, AddDynamicModuleResult{Name: name, Err: initErr})
			failedNames = append(failedNames, name)
			continue
		}

		wrapper.wg.Add(1)
		go a.serveModule(wrapper, true) // dynamic=true：panic 不会退出进程
		a.dynamicModules.Store(name, wrapper)
		results = append(results, AddDynamicModuleResult{Name: name})
	}

	if len(failedNames) > 0 {
		err = fmt.Errorf("dynamic modules init failed: %s", strings.Join(failedNames, ", "))
	}
	return results, err
}

// RemoveDynamicModule 同步移除并销毁指定名称的动态模块。
//
// 完整操作序列：
//  1. OnDestroy：调用销毁钩子释放模块资源
//  2. cancel：向模块发送停止信号，通知 OnRun 退出主循环
//  3. wg.Wait：阻塞等待 OnRun goroutine 完全退出
//  4. Delete：从 dynamicModules 移除，释放引用
//
// 该操作是同步阻塞的，调用方会等待模块完全停止后才返回，
// 确保模块的所有资源在函数返回前已被完整清理，避免悬挂的 goroutine 或资源泄漏。
func (a *app) RemoveDynamicModule(name string) bool {
	value, ok := a.dynamicModules.Load(name)
	if !ok {
		return false
	}

	wrapper, ok := value.(*moduleWrapper)
	if !ok {
		return false
	}

	a.destroyModule(wrapper)

	wrapper.cancel()  // 发送停止信号，通知模块 OnRun 退出
	wrapper.wg.Wait() // 等待 OnRun goroutine 完全退出后再继续

	a.dynamicModules.Delete(name)

	return true
}

// removeAllDynamicModules 收集所有动态模块名称后逐一移除。
//
// 先收集名称快照再逐一移除，而非在 Range 回调中直接移除：
// sync.Map 的文档说明 Range 期间调用 Delete 是安全的，但先收集快照能使逻辑更清晰，
// 且避免在 Range 内部嵌套 RemoveDynamicModule（其中包含 wg.Wait）可能引发的潜在问题。
func (a *app) removeAllDynamicModules() {
	var moduleNames []string

	a.dynamicModules.Range(func(key, value any) bool {
		moduleNames = append(moduleNames, key.(string))
		return true
	})

	for _, name := range moduleNames {
		a.RemoveDynamicModule(name)
	}
}

// RegisterSignal 注册信号。
//
// 同一信号可多次注册；收到信号时会为每个处理器启动 goroutine 并等待全部完成，不保证完成顺序：
//   - SIGHUP：可叠加多个热重载逻辑，每个处理器独立执行
//
// SIGINT / SIGTERM 为可捕获的框架保留信号；SIGKILL 不可被进程捕获，也作为保留信号禁止业务注册。
//
// 示例（在游戏模块 OnInit 中注册 SIGHUP 热重载）：
//
//	if err := core.RegisterSignal(func() {
//	    slog.Info("收到 SIGHUP，重新加载配置")
//	    reloadConfig()
//	}, syscall.SIGHUP); err != nil {
//	    return err
//	}
func (a *app) RegisterSignal(trap SignalTrap, sigs ...os.Signal) error {
	return a.sm.Register(trap, sigs...)
}
