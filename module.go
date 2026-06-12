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
	Name() string              // 模块唯一名称，用于日志标识和跨模块 RPC 寻址
	OnInit() error             // 模块初始化，任一模块失败则终止整个应用启动流程
	OnRun(ctx context.Context) // 模块主循环，应监听 ctx.Done() 并在收到取消信号时退出
	OnDestroy()                // 模块销毁，在 goroutine 完全退出前调用，负责释放所有资源
	ChanRPC() *chanrpc.Server  // 返回模块的 ChanRPC 服务端，nil 表示该模块不接受外部 RPC 调用
}

// 应用全局状态常量，表示应用生命周期的各个阶段。
const (
	AppStateNone = iota // 应用未启动或已完全停止，可安全重新启动
	AppStateInit        // 应用正在初始化，所有模块的 OnInit 正在按序执行
	AppStateRun         // 应用运行中，所有模块已成功启动并处于活跃状态
	AppStateStop        // 应用正在优雅关闭，模块正按逆序依次停止
)

const (
	// defaultShutdownTimeout 单个模块优雅关闭的最大等待时间。
	// 设置为 30 分钟是为了兼容可能持有长时间锁或大批量数据落盘的模块，
	// 超时后记录错误日志但不强制终止，避免数据损坏，由运维介入处理。
	defaultShutdownTimeout = 30 * time.Minute
)

// moduleWrapper 为 IModule 附加框架运行时所需的控制元数据。
//
// ctx/cancel 构成模块停止信号通道：框架通过调用 cancel 通知模块 OnStart 应退出主循环；
// wg 用于等待模块 goroutine 完全退出后再调用 OnDestroy，保证资源清理的时序正确。
type moduleWrapper struct {
	IModule
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// app 是应用框架的核心结构，统一管理静态模块列表和动态模块集合。
//
// 并发安全说明：
//   - modules 切片通过嵌入的 RWMutex 保护，启动后仅读
//   - dynamicModules 使用 sync.Map，原生支持并发增删改查
//   - state 使用原子操作读写
//   - 信号注册与分发由 SignalManager 内部的 RWMutex 保护
type app struct {
	sync.RWMutex                    // 保护 modules 切片
	modules        []*moduleWrapper // 静态模块列表，按优先级排序，启动后不允许修改
	dynamicModules sync.Map         // 动态模块集合，key 为模块名，支持运行时热加载
	state          int32            // 应用全局状态，使用 atomic 操作保证原子性
	sm             *SignalManager
}

// newApp 创建新的应用框架实例，初始状态为 AppStateNone。
func newApp() *app {
	return &app{
		sm:      NewSignalManager(),
		state:   AppStateNone,
		modules: make([]*moduleWrapper, 0),
	}
}

// setState 通过原子写入更新应用状态，确保状态变更对所有 goroutine 立即可见。
func (a *app) setState(state int32) {
	atomic.StoreInt32(&a.state, state)
}

// State 通过原子读取获取应用当前状态，可在任意 goroutine 中安全调用。
func (a *app) State() int32 {
	return atomic.LoadInt32(&a.state)
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
func (a *app) appendModuleStats(builder *strings.Builder, moduleType string, wrapper *moduleWrapper) {
	rpcServer := wrapper.ChanRPC()

	if rpcServer != nil {
		channelLen := len(rpcServer.ChanCall)
		builder.WriteString(fmt.Sprintf("%s: %s, rpc_queue_length: %d\n",
			moduleType, wrapper.Name(), channelLen))
	} else {
		builder.WriteString(fmt.Sprintf("%s: %s, rpc_queue_length: N/A\n",
			moduleType, wrapper.Name()))
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
func (a *app) Register(mods ...IModule) error {
	if a.State() != AppStateNone {
		return fmt.Errorf("application is already running")
	}

	for _, mod := range mods {
		a.Lock()
		wrapper := &moduleWrapper{
			IModule: mod,
		}
		wrapper.ctx, wrapper.cancel = context.WithCancel(context.Background())
		a.modules = append(a.modules, wrapper)
		a.Unlock()
	}

	return nil
}

// Run 注册并启动所有模块，阻塞至所有信号处理完毕（通常是收到 SIGINT/SIGKILL/SIGTERM 后优雅关闭）。
//
// 框架默认注册 SIGINT/SIGKILL/SIGTERM → 优雅关闭，SIGHUP → 仅记录日志继续运行。
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
//  1. 状态检查，防止重复启动
//  2. 将 Run 参数中的模块追加到 modules 列表（支持 Register + Run 两种注册方式）
//  3. 依次调用 OnInit，任一失败则中止启动并返回 false
//  4. 为每个模块启动独立 goroutine 并运行 OnStart
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
	currentState := a.State()
	if currentState != AppStateNone {
		slog.Error("application cannot start twice", "current_state", currentState)
		return false
	}

	a.Lock()
	for _, mod := range mods {
		if mod == nil {
			continue
		}
		wrapper := &moduleWrapper{
			IModule: mod,
		}
		wrapper.ctx, wrapper.cancel = context.WithCancel(context.Background())
		a.modules = append(a.modules, wrapper)
	}
	a.Unlock()

	if len(a.modules) == 0 {
		slog.Warn("no modules provided to start")
		return false
	}

	a.setState(AppStateInit)
	slog.Info("application starting", "module_count", len(a.modules))
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
		go a.onRunModule(wrapper, false)
	}

	a.setState(AppStateRun)
	slog.Info("application started successfully")
	return true
}

// onRunModule 在独立 goroutine 中运行模块的 OnStart 主循环。
//
// runtime.LockOSThread 将 goroutine 绑定到专用系统线程：
//   - 保证某些依赖线程本地状态的库（如 OpenGL、部分 CGO 库）能正常工作
//   - 代价是增加系统线程数，对纯 Go 模块而言可考虑移除此调用以减少线程开销
//
// panic 处理策略差异：
//   - 静态模块（dynamic=false）panic 后调用 os.Exit(255)，确保进程不在不确定状态下运行
//   - 动态模块（dynamic=true）panic 仅记录日志，不影响其他模块和进程的正常运行
func (a *app) onRunModule(wrapper *moduleWrapper, dynamic bool) {
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

	wrapper.OnRun(wrapper.ctx)
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
	if a.State() == AppStateStop {
		slog.Warn("application already stopping")
		return
	}

	a.setState(AppStateStop)
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

	a.setState(AppStateNone)
	slog.Info("application shutdown complete")
}

// shutdownModule 优雅关闭单个模块，完整流程为：发送停止信号 → 等待 goroutine 退出（含超时保护）→ 调用 OnDestroy。
//
// 超时保护通过独立 goroutine + done channel 实现，而非直接阻塞，
// 原因是 wg.Wait 本身不支持超时，需要借助 select 和 timer 组合。
// 超时后不强制退出，仅记录错误，因为强制终止可能导致数据损坏（如正在写数据库）。
func (a *app) shutdownModule(wrapper *moduleWrapper) {
	slog.Info("signaling module shutdown", "module", wrapper.Name())

	slog.Info("destroying module", "module", wrapper.Name())
	a.destroyModule(wrapper)

	// 通过 context 取消向模块的 OnStart 发送停止信号
	wrapper.cancel()
	// 在辅助 goroutine 中等待模块退出，配合 select + timer 实现超时保护
	done := make(chan struct{})
	go func() {
		wrapper.wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(defaultShutdownTimeout)
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

// AddDynamicModules 在运行时动态添加并启动一批模块，支持热加载。
//
// 与静态模块相比，动态模块的特殊之处：
//   - panic 不会导致进程退出，仅记录日志（onStartModule 的 dynamic=true 参数控制）
//   - 支持通过 RemoveDynamicModule 单独卸载，不影响其他模块
//   - 模块按传入顺序依次初始化，任一失败则停止并返回错误（已成功初始化的模块不自动回滚）
func (a *app) AddDynamicModules(mods ...IModule) error {
	var wrappers []*moduleWrapper
	for _, mod := range mods {
		if mod == nil {
			continue
		}
		wrapper := &moduleWrapper{
			IModule: mod,
		}
		wrapper.ctx, wrapper.cancel = context.WithCancel(context.Background())
		wrappers = append(wrappers, wrapper)
	}

	for _, wrapper := range wrappers {
		if err := wrapper.OnInit(); err != nil {
			slog.Error("module init error", "module", wrapper.Name(), "err", err)
			return fmt.Errorf("module %s init failed: %w", wrapper.Name(), err)
		}
		wrapper.wg.Add(1)
		go a.onRunModule(wrapper, true) // dynamic=true：panic 不会退出进程
		a.dynamicModules.Store(wrapper.Name(), wrapper)
	}
	return nil
}

// RemoveDynamicModule 同步移除并销毁指定名称的动态模块。
//
// 完整操作序列：
//  1. cancel：向模块发送停止信号，通知 OnStart 退出主循环
//  2. wg.Wait：阻塞等待 OnStart goroutine 完全退出
//  3. OnDestroy：调用销毁钩子释放模块资源
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

	wrapper.cancel()  // 发送停止信号，通知模块 OnStart 退出
	wrapper.wg.Wait() // 等待 OnStart goroutine 完全退出后再继续

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

// RegisterSignal 注册信号
//
// 同一信号可多次注册，收到信号时按注册顺序依次调用所有处理器，处理器间互不影响：
//   - SIGHUP：可叠加多个热重载逻辑，每个处理器独立执行
//
// SIGINT / SIGKILL / SIGTERM 由框架独占管理（固定触发优雅关闭），传入时返回错误。
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
