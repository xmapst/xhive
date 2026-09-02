package xhive

import (
	"cmp"
	"context"
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
)

// IModule 定义应用模块的完整生命周期接口。
//
// 框架通过此接口管理模块从初始化到销毁的全过程，每个模块代表一个独立的业务单元，
// 拥有独立的 goroutine、RPC 服务端和定时器管理器。
// 模块之间通过 ChanRPC 通信，天然隔离内部状态，无需跨模块加锁。
type IModule interface {
	// Name 返回模块唯一名称，用于日志标识和跨模块 RPC 寻址。
	Name() string
	// Priority 返回模块启动优先级，值越小越早初始化；优先级相同时保留注册/添加顺序
	// （静态模块为 Register/start 调用顺序，动态模块为 AddDynamicModules 传参顺序），
	// 关闭顺序与之完全相反（LIFO）。
	Priority() uint
	// OnInit 执行模块初始化，任一模块失败则终止整个应用启动流程。
	//
	// **返回 error 之前必须自行回滚本方法已经分配的资源。** 框架不会对
	// OnInit 失败的模块调用 OnDestroy 或 Close——它拿不到"OnInit 走到第几步、
	// 已经开了哪些东西"这类信息，只有 OnInit 自己知道该回滚什么。
	// 静态模块与动态模块在这一点上语义完全一致。
	//
	// 同理，某个模块 OnInit 失败导致启动中止时，排在它之后、OnInit 一次都
	// 没被调用过的模块也不会收到 OnDestroy/Close：它们没有任何资源需要释放。
	// 排在它之前、OnInit 已经成功的模块则会照常收到 OnDestroy 与 Close。
	OnInit() error
	// Serve 执行模块主循环，应监听 ctx.Done() 并在收到取消信号时退出。
	Serve(ctx context.Context)
	// Ready 返回就绪信号 channel，框架在此 channel 关闭后才允许其他模块向本模块发起调用。
	Ready() <-chan struct{}
	// OnDestroy 执行模块销毁，在模块 goroutine 完全退出之后调用，负责释放所有资源。
	//
	// **调用前提：OnInit 已经成功返回。** 框架为每个模块维护显式的生命周期
	// 状态，只对 OnInit 成功的模块调用 OnDestroy；OnInit 从未被调用、
	// 或返回了 error 的模块不会走到这里（见 OnInit 与 shutdownModule 的说明）。
	// 因此 OnDestroy 里可以直接解引用 OnInit 中创建的字段，不必写防御式判空。
	//
	// 注意：不能自投递。执行到这里时本模块自己的 Server 已经 Close，
	// 对它的 Cast/Call/AsyncCall（包括 m.Call(m.Name(), ...) 这种自调用）
	// 都会因为投递到已关闭队列而失败——同步 Call 会拿到错误响应，不会
	// 挂起，但如果指望它能把处理逻辑重新投递回本模块的事件循环，这个
	// 逻辑就悄悄失效了。OnDestroy 需要的收尾工作应该直接写成同步函数
	// 调用/循环（例如遍历并落地业务数据），而不是通过消息投递给自己——
	// 此刻事件循环已经不存在，没有人能消费这条自投递。
	// 给别的、还没轮到关闭的模块投递数据不受影响，见 shutdownModule 的说明。
	//
	// **跨模块投递请用 Cast/AsyncCall，或带超时的 CallWithContext，不要用同步 Call。**
	// 启动中途失败时所有模块的事件循环一次都没跑过，此时对一个"更早注册、
	// LIFO 里还没轮到销毁"的模块发起同步 Call，对端没有事件循环来消费这条请求，
	// 回包永远不会产生，调用方就永久卡在这里——而 WithShutdownTimeout 只覆盖
	// "等待模块 goroutine 退出"这一步，覆盖不到 OnDestroy 自身的执行。
	// 框架只能保证已经轮到销毁的模块其 Server 一定已关闭（投递会立刻失败返回），
	// 保证不了尚未轮到的那些。
	OnDestroy()
	// ChanRPC 返回模块的 ChanRPC 服务端，nil 表示该模块不接受外部 RPC 调用。
	ChanRPC() *chanrpc.Server
	// Close 释放模块的出站 client 资源，由框架在 OnDestroy 返回之后调用，
	// 静态模块与动态模块（含 RemoveDynamicModule 卸载）都走这条路径；
	// 见 Skeleton.Close 的说明。
	//
	// 与 OnDestroy 成对：只有收到过 OnDestroy 的模块才会收到 Close。
	// 其中的 panic 由框架 recover 并记录日志，不会中断其余模块的关闭流程。
	Close() error
}

const (
	// AppStateNone 表示应用未启动或已完全停止。
	//
	// 注意：**已停止的应用不支持再次启动。** stop 不重建 modules 里的 wrapper，
	// 它们的 ctx 已被 cancel、模块自身的一次性资源（如 Skeleton.ready 这个只
	// make 一次的 channel）也已消耗。start 会检查模块的生命周期状态并明确拒绝
	// 重启，返回 false 而不是产出一个 Serve 立即退出的僵尸应用。
	// 需要"重启"时请新建模块实例与新的 app。
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
	// 容忍度差异很大，写死的全局常量缺乏灵活性，因此保留了
	// WithShutdownTimeout 这个覆盖入口。
	//
	// 注意：该入口目前**包外不可达**——AppOption 的唯一消费者是 newApp，
	// 而 newApp 与 app 都未导出，包级单例 defaultApp 也不带任何选项构造。
	// 也就是说包外用户实际只能使用这里的默认值。要让它真正可配置，
	// 需要另外提供一个导出的构造/配置入口。
	defaultShutdownTimeout = 30 * time.Minute

	// defaultStartupSettleTimeout stop 等待仍在进行的 start 收敛的最大时间。
	//
	// 刻意与 shutdownTimeout 分开、且短得多：这段等待发生在"收到停止信号但
	// start 还没跑完"的窗口里，等的是 OnInit（通常是连接依赖、加载配置，秒级），
	// 而不是 shutdownTimeout 所针对的"大批量落盘"。若复用 30 分钟，启动期卡住
	// 一个 OnInit 就会让 SIGTERM 之后的停机整整空等半小时，早已被 k8s 的
	// terminationGracePeriodSeconds（默认 30 秒）强杀。
	//
	// 超时后不是强行并发销毁：兜底放行只是让 stop 继续往下走，仍在执行 OnInit
	// 的那个模块停在 lifecycleIniting，shutdownModule 会跳过它，不会退化成
	// 与 OnInit 并发读写同一批业务内存。
	defaultStartupSettleTimeout = 30 * time.Second
)

// AppOption 用于自定义 app 实例的可选行为。
//
// 注意：AppOption 虽是导出类型，但它唯一的消费者 newApp 与目标类型 app
// 都未导出，包级单例 defaultApp 也是无选项构造的，因此包外目前拿不到
// 任何可以传入 AppOption 的入口——这些选项实际只在包内（含测试）生效。
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

// moduleLifecycle 描述单个模块在框架中所处的生命周期阶段。
//
// 引入模块级状态的原因：框架此前只有 app 级别的 AppStateXxx，模块级别没有
// 任何"这个模块走到第几步"的记账。于是启动中途失败时 stop 无法区分
// "OnInit 已成功、资源已分配"与"OnInit 一次都没被调用过"两类模块，
// 只能无差别地对所有模块执行 OnDestroy/Close——对后者而言，那是在一个
// 字段全为零值的对象上执行"释放所有资源"，属于契约违反。
//
// 状态跃迁（单向，不可回退）：
//
//	Registered ──OnInit 开始──> Initing ──成功──> Inited ──启动 Serve──> Serving
//	                               │                  │                    │
//	                               └──失败──> InitFailed                   │
//	                                                  │                    │
//	                                        shutdownModule 抢占            │
//	                                                  ▼                    ▼
//	                                            Destroying ──完成──> Destroyed
type moduleLifecycle int32

const (
	// lifecycleRegistered 模块已登记进框架，OnInit 尚未开始执行。
	// 模块此时没有任何自己分配的资源，框架不得调用 OnDestroy/Close。
	lifecycleRegistered moduleLifecycle = iota
	// lifecycleIniting OnInit 正在执行且尚未返回。
	// 此状态下模块内存正被 OnInit 所在的 goroutine 写入，
	// 关闭流程绝不能并发调用 OnDestroy，只能放弃销毁（理由同 shutdownModule 的超时分支）。
	lifecycleIniting
	// lifecycleInitFailed OnInit 返回了 error。
	// 按 IModule.OnInit 的契约，OnInit 必须在返回 error 之前自行回滚已分配的部分资源，
	// 因此框架不再调用 OnDestroy/Close——与动态模块路径的既有行为一致。
	lifecycleInitFailed
	// lifecycleInited OnInit 已成功返回，但 Serve goroutine 尚未启动
	// （典型场景：排在后面的模块 OnInit 失败，导致整个启动流程中止）。
	// 模块资源已分配，框架必须调用 OnDestroy/Close；但它的事件循环从未存在，
	// 既不需要等待 goroutine 退出，也没有人会替它关闭 ChanRPC 服务端。
	lifecycleInited
	// lifecycleServing Serve goroutine 已启动（可能仍在运行，也可能已经退出）。
	// 完整走 cancel → 等待 goroutine 退出 → OnDestroy → Close。
	lifecycleServing
	// lifecycleDestroying 已被 shutdownModule 通过 CAS 抢占，销毁进行中。
	// 该状态使"销毁权"具有唯一归属：并发的 stop / RemoveDynamicModule 只有一个能进入销毁流程。
	lifecycleDestroying
	// lifecycleDestroyed OnDestroy 与 Close 均已完整执行完毕，模块不可再使用。
	// 关闭超时被跳过销毁的模块停在 lifecycleDestroying，不会进入本状态。
	lifecycleDestroyed
)

// String 返回状态名，仅用于日志可读性。
func (l moduleLifecycle) String() string {
	switch l {
	case lifecycleRegistered:
		return "registered"
	case lifecycleIniting:
		return "initing"
	case lifecycleInitFailed:
		return "init_failed"
	case lifecycleInited:
		return "inited"
	case lifecycleServing:
		return "serving"
	case lifecycleDestroying:
		return "destroying"
	case lifecycleDestroyed:
		return "destroyed"
	default:
		return "unknown"
	}
}

// moduleWrapper 为 IModule 附加框架运行时所需的控制元数据。
//
// ctx/cancel 构成模块停止信号通道：框架通过调用 cancel 通知模块 Serve 应退出主循环；
// wg 用于等待模块 goroutine 完全退出，保证关闭流程可同步等待完成。
//
// exited 在 serveModule 返回时关闭，用于把"模块 goroutine 已经结束"这件事
// 变成可 select 的信号：等待就绪只看 Ready() 是不够的，Serve 若在 close(ready)
// 之前 panic 或提前 return，ready 永远不会被关闭，等待方会永久挂死。
//
// lifecycle 存放 moduleLifecycle，用原子量而非互斥量：start 在独立 goroutine
// 上运行（见 Run），stop 可能与它并发，销毁权必须靠 CAS 原子抢占。
//
// seq 仅动态模块使用：sync.Map.Range 不保证遍历顺序，removeAllDynamicModules
// 无法像静态模块那样直接依赖切片下标复原"同优先级按 Add 调用顺序"这一约定，
// 因此在 AddDynamicModules 成功启动时记录一个单调递增序号，倒序关闭时以
// (Priority, seq) 排序重建真实的添加顺序。静态模块始终为零值，未参与排序。
type moduleWrapper struct {
	IModule
	ctx       context.Context
	cancel    context.CancelFunc
	exited    chan struct{}
	wg        sync.WaitGroup
	lifecycle atomic.Int32 // moduleLifecycle，零值即 lifecycleRegistered
	seq       uint64
}

// lifecycleState 原子读取模块当前生命周期状态。
func (w *moduleWrapper) lifecycleState() moduleLifecycle {
	return moduleLifecycle(w.lifecycle.Load())
}

// setLifecycle 原子推进模块生命周期状态，仅由启动路径（单写者）调用。
func (w *moduleWrapper) setLifecycle(state moduleLifecycle) {
	w.lifecycle.Store(int32(state))
}

// claimShutdown 通过 CAS 抢占模块的销毁权，返回抢占前的状态。
//
// 返回 lifecycleDestroying / lifecycleDestroyed 表示销毁权已被别的调用方
// 拿走（重复 stop、stop 与 RemoveDynamicModule 并发、并发卸载同名动态模块），
// 调用方必须直接返回，不得再执行 OnDestroy——这使"每个模块的 OnDestroy 与
// Close 至多各执行一次"成为结构性保证，而不再依赖外层 sync.Once 与 app 状态闸门。
func (w *moduleWrapper) claimShutdown() moduleLifecycle {
	for {
		state := w.lifecycleState()
		if state == lifecycleDestroying || state == lifecycleDestroyed {
			return state
		}
		if w.lifecycle.CompareAndSwap(int32(state), int32(lifecycleDestroying)) {
			return state
		}
	}
}

// waitReady 等待模块事件循环就绪，同时监视 Serve goroutine 是否已经退出。
//
// 返回 false 表示 goroutine 已经结束而 ready 从未被关闭——Serve 在
// close(ready) 之前 panic 或提前 return 时就是这种情况。只 select ready 会让
// 调用方永久阻塞：静态路径因 serveModule 的 os.Exit(255) 掩盖了这一点，
// 动态路径的 panic 不退出进程，直接表现为 AddDynamicModules 调用方卡死。
func (w *moduleWrapper) waitReady() bool {
	select {
	case <-w.Ready():
		return true
	case <-w.exited:
		// goroutine 已退出，但它可能是在关闭 ready 之后才正常返回的，
		// 两个 channel 同时就绪时 select 会随机选一个，这里补一次确定性判定。
		select {
		case <-w.Ready():
			return true
		default:
			return false
		}
	}
}

// newModuleWrapper 构造 moduleWrapper 并初始化其停止信号 context。
//
// 该构造逻辑原先在 Register 与 start 中各自重复实现一次，收敛到此处
// 之后两处调用方只需一行代码即可复用，避免后续新增字段时两处遗漏同步修改。
func newModuleWrapper(mod IModule) *moduleWrapper {
	wrapper := &moduleWrapper{IModule: mod, exited: make(chan struct{})}
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
	dynSeq          atomic.Uint64    // 动态模块添加序号生成器，见 moduleWrapper.seq 的说明
	startDone       chan struct{}    // start 进行中时非 nil，start 返回前关闭；受 RWMutex 保护，见 stop 的启动交接说明
	shutdownTimeout time.Duration    // 单个模块优雅关闭的最大等待时间，可通过 WithShutdownTimeout 自定义
	startupSettle   time.Duration    // stop 等待仍在进行的 start 收敛的最大时间，见 defaultStartupSettleTimeout
	state           atomic.Int32     // 应用全局状态，读写均在持锁状态下进行，Stats/State 对外仍以原子读保证快照一致
	stopRequested   atomic.Bool      // stop 已被请求；start 在各阶段之间检查它并中止启动，避免关闭完成后又把状态改回 AppStateRun
	runInFlight     atomic.Bool      // Run 正在执行；同一个 app 上不允许并发 Run，见 Run 的说明
	sync.RWMutex                     // 保护 modules 切片，并使状态变更与 modules 写入保持原子性
}

// newApp 创建新的应用框架实例，初始状态为 AppStateNone。
func newApp(opts ...AppOption) *app {
	a := &app{
		sm:              NewSignalManager(),
		modules:         make([]*moduleWrapper, 0),
		shutdownTimeout: defaultShutdownTimeout,
		startupSettle:   defaultStartupSettleTimeout,
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
// 输出格式："{static|dynamic}: {模块名}, rpc_queue_length: {队列长度}, rpc_queue_cap: {队列容量}"
// rpc_queue_length 反映模块消息积压程度，与 rpc_queue_cap 相除即得水位，
// 是性能瓶颈和消息处理速率的重要观测指标，也是可直接用于告警的量。
// 两者同时为 N/A 表示该模块未配置 ChanRPC 服务端（如纯定时器模块）。
func (a *app) Stats() string {
	var builder strings.Builder

	// 遍历静态模块
	a.RLock()
	staticModules := slices.Clone(a.modules)
	a.RUnlock()
	for _, wrapper := range staticModules {
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
func (a *app) ChanRPC(name string) *chanrpc.Server {
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
//
// 同一个 app 上不允许并发或重入调用 Run。start 有多种返回 false 的原因，其中
// "应用已经启动"是一种**不该触发关闭**的失败：它什么都没改，只是拒绝了这次启动。
// 但下面的 select 看到 errCh 就会无条件调 stopFn，把那个健康运行中的应用连同
// 全部模块一起销毁掉；更糟的是关闭由第二个 Run 的 stopFn 完成，第一个 Run 的
// stopped 永远不会被 close，它的 select 永久阻塞（通常就是 main goroutine）。
// 因此这里直接拒绝第二次 Run，而不是让它走进那条会误伤的失败分支。
func (a *app) Run(mods ...IModule) {
	if !a.runInFlight.CompareAndSwap(false, true) {
		slog.Error("application is already running, Run must not be called concurrently or reentrantly")
		return
	}
	defer a.runInFlight.Store(false)

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
//  3. 为每个模块启动独立 goroutine 并运行 Serve
//
// 顶层 panic recover：捕获启动过程中的意外 panic，记录完整堆栈后以退出码 255 终止进程，
// 防止进程在不确定状态下继续运行造成数据损坏。
func (a *app) start(mods ...IModule) bool {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("application panic recovered", slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
			os.Exit(255)
		}
	}()

	a.Lock()
	currentState := a.state.Load()
	if currentState != AppStateNone {
		a.Unlock()
		slog.Error("application cannot start twice", slog.Int64("current_state", int64(currentState)))
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
			return false
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

	a.setState(AppStateInit)
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
	for _, wrapper := range staticModules {
		if a.stopRequested.Load() {
			slog.Warn("application start aborted by shutdown request", slog.String("module", wrapper.Name()))
			return false
		}
		wrapper.setLifecycle(lifecycleIniting)
		if err := wrapper.OnInit(); err != nil {
			wrapper.setLifecycle(lifecycleInitFailed)
			slog.Error("module initialization failed", slog.String("module", wrapper.Name()), slog.Any("error", err))
			return false
		}
		wrapper.setLifecycle(lifecycleInited)
	}

	// 拉起 goroutine 之前再复查一道：上面的 OnInit 循环只在每次迭代开头检查
	// stopRequested，全部 OnInit 成功时不会再进入循环体，检查也就不会发生。
	// 而 stop 若走了 startupSettle 的兜底分支，此刻可能已经把这批模块销毁完毕
	// 并把状态复位成 AppStateNone。继续往下会给已经 OnDestroy 过的模块重新
	// 拉起 Serve，并用 lifecycleServing 覆盖掉它们的 lifecycleDestroyed。
	if a.stopRequested.Load() {
		slog.Warn("application start aborted by shutdown request before serving")
		return false
	}

	// 所有模块初始化完成后，并发启动各自的 goroutine
	for _, wrapper := range staticModules {
		a.startServing(wrapper, false)
	}

	// 等待所有模块的事件循环（Serve）进入 select 就绪，避免 Serve 中跨模块 Call
	// 时目标模块尚未监听 ChanCall 导致 message_id not registered。
	// waitReady 同时监视 goroutine 是否已经退出：Serve 若在 close(ready) 之前
	// panic 或提前 return，只等 ready 会让 start 永久挂死。
	for _, wrapper := range staticModules {
		if !wrapper.waitReady() {
			slog.Error("module serve exited before becoming ready", slog.String("module", wrapper.Name()))
			return false
		}
	}

	a.Lock()
	// 启动尾声再确认一次：期间若已收到关闭请求，绝不能把一个正在/已经关闭的
	// 应用重新标记为 AppStateRun。
	if a.stopRequested.Load() || a.state.Load() != AppStateInit {
		a.Unlock()
		slog.Warn("application start aborted by shutdown request")
		return false
	}
	a.setState(AppStateRun)
	a.Unlock()
	slog.Info("application started successfully")
	return true
}

// startServing 把模块推进到 lifecycleServing 并拉起它的事件循环 goroutine。
//
// 状态先于 goroutine 置位：关闭流程只对 lifecycleServing 的模块执行 wg.Wait，
// 若先起 goroutine 再置位，并发的 stop 可能读到 lifecycleInited 而跳过等待。
// wg.Go 在内部完成 Add，与 goroutine 的创建是原子的，无需外部 Add/Done 配对。
func (a *app) startServing(wrapper *moduleWrapper, dynamic bool) {
	wrapper.setLifecycle(lifecycleServing)
	wrapper.wg.Go(func() {
		a.serveModule(wrapper, dynamic)
	})
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
func (a *app) serveModule(wrapper *moduleWrapper, dynamic bool) {
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
	// 先向 start 公示关闭意图，再决定是否需要等它跑完。
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
	if currentState == AppStateInit && startDone != nil {
		timer := time.NewTimer(a.startupSettle)
		select {
		case <-startDone:
		case <-timer.C:
			slog.Error("application start did not finish before startup settle timeout, proceeding",
				slog.Duration("startup_settle_timeout", a.startupSettle))
		}
		timer.Stop()
	}

	a.Lock()
	// 重取锁后复查：等待 startDone 的这段时间里锁是放开的，另一个 stop 可能
	// 已经越过上面的状态闸门。销毁本身由 moduleWrapper.claimShutdown 保证只做
	// 一次，这里再拦一道，避免两条关闭流程重复扫动态模块、刷重复日志。
	if a.state.Load() == AppStateStop {
		a.Unlock()
		slog.Warn("application already stopping")
		return
	}
	a.setState(AppStateStop)
	staticModules := slices.Clone(a.modules)
	a.Unlock()

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
	a.setState(AppStateNone)
	a.Unlock()
	slog.Info("application shutdown complete")
}

// shutdownModule 依模块自身的生命周期状态关闭单个模块，静态与动态模块共用同一条路径。
//
// 销毁权通过 claimShutdown 原子抢占：每个模块的 OnDestroy 与 Close 至多各执行
// 一次，这是结构性保证，不再依赖 Run 的 sync.Once 与 app 的状态闸门。
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
// 分支里，与"停事件循环"是同一步（见 Skeleton.Serve / chanrpc.Server.Close）。
// 事件循环从未运行过的模块没有人替它做这件事，由 closeIdleServer 代劳，
// 使「OnDestroy 执行时本模块 Server 已关闭」这条不变量在所有路径上都成立。
//
// **client 的释放不能收在那里**：OnDestroy 通常要用 client 把停机前的
// 最后一批状态投递给其它模块（例如汇总数据后转交给专门负责收尾处理的
// 模块），提前关掉 client 会让这些投递全部失败。因此 client 由这里在 OnDestroy
// 返回之后单独关闭（见 Skeleton.Close）。
// OnDestroy 若想给别的模块投递最后一批数据，对方模块必须还没轮到自己
// 关闭——这正是本函数外层 LIFO 停机顺序要保证的前提。
//
// **但不能给自己投递。** OnDestroy 执行到这一步时，本模块自己的 Server
// 一定已经关闭（Serving 模块由 Serve 退出时关闭，Inited 模块由 closeIdleServer
// 关闭），Cast/Call/AsyncCall 到自己（包括 m.Call(m.Name(), ...) 这种自调用）
// 都会因为投递到已关闭队列而失败。OnDestroy 需要的收尾工作应该直接写成同步
// 函数调用/循环，不要指望能把处理逻辑重新投递回本模块的事件循环——此刻事件
// 循环已经不存在，没有人能消费这条自投递（详见 IModule.OnDestroy 的说明）。
func (a *app) shutdownModule(wrapper *moduleWrapper) {
	state := wrapper.claimShutdown()
	if state == lifecycleDestroying || state == lifecycleDestroyed {
		// 销毁权已被别的调用方拿走（重复 stop、stop 与 RemoveDynamicModule 并发、
		// 并发卸载同名动态模块），直接返回，保证销毁严格只发生一次。
		return
	}

	slog.Info("stopping module", slog.String("module", wrapper.Name()), slog.String("module_state", state.String()))

	// 停止信号总要发出：即便 Serve 从未启动，cancel 也要调用，
	// 以释放 context 树上这个节点，并让任何持有该 ctx 的业务逻辑及时脱身。
	wrapper.cancel()

	switch state {
	case lifecycleIniting:
		// OnInit 仍在另一条 goroutine 上执行，此刻碰它的业务内存必然是并发读写。
		// 宁可漏掉一次销毁，也不要 fatal——与关闭超时分支同一条取舍。
		slog.Error("module still initializing, destroy skipped", slog.String("module", wrapper.Name()))
		return
	case lifecycleRegistered, lifecycleInitFailed:
		// OnInit 从未成功，模块没有由框架托管的资源，不调 OnDestroy / Close。
		// 但仍要关掉它的 Server：否则别的模块向它投递时 IsClosed 为 false，
		// Cast/AsyncCall 会"成功"入队后静默蒸发，同步 Call 更是永久阻塞。
		a.closeIdleServer(wrapper)
		slog.Info("module destroy skipped, OnInit never succeeded",
			slog.String("module", wrapper.Name()), slog.String("module_state", state.String()))
		wrapper.setLifecycle(lifecycleDestroyed)
		return
	case lifecycleInited:
		// 资源已分配但事件循环从未启动：没有 goroutine 需要等待，
		// 也没有人会替它关闭 Server，由框架代劳后再进入销毁。
		a.closeIdleServer(wrapper)
	case lifecycleServing:
		if !a.waitModuleExit(wrapper) {
			// 超时说明 Serve 还没退出，此时调 OnDestroy 就回到了并发读写的老问题上，
			// 因此直接放弃本模块的销毁：宁可漏掉一次落地，也不要 fatal。
			// 状态停在 lifecycleDestroying，不会被标记为已完成销毁。
			return
		}
	default:
		// 兜底而非省略：状态机是这套关闭逻辑的唯一依据，日后新增一个
		// moduleLifecycle 却忘了在上面加分支时，模块会落到这里被明确记录并跳过，
		// 而不是静默穿过 switch 去对一个未知状态的模块执行 OnDestroy。
		// （lifecycleDestroying / lifecycleDestroyed 已在函数开头拦下，到不了这里。）
		slog.Error("unexpected module lifecycle state, destroy skipped",
			slog.String("module", wrapper.Name()), slog.String("module_state", state.String()))
		return
	}

	// 事件循环已停（或从未存在），业务内存此刻只有本 goroutine 访问，
	// OnDestroy 可以安全遍历；client 仍然打开，OnDestroy 里的
	// Cast/Call/AsyncCall 仍然可以正常投递给尚未关闭的模块。
	slog.Info("destroying module", slog.String("module", wrapper.Name()))
	a.destroyModule(wrapper)

	// 最后释放 client：OnDestroy 里的投递到此已全部发出
	a.closeModule(wrapper)
	wrapper.setLifecycle(lifecycleDestroyed)
	slog.Info("module shutdown complete", slog.String("module", wrapper.Name()))
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
func (a *app) waitModuleExit(wrapper *moduleWrapper) bool {
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

// closeIdleServer 关闭一个事件循环从未运行过的模块的 ChanRPC 服务端。
//
// chanrpc.Server.Close 的前提是"必须在所属模块自己的事件循环 goroutine 上调用"，
// 正常路径由 Skeleton.Serve 的 ctx.Done 分支满足。启动中途失败时这些模块的
// Serve 一次都没跑过，没有任何 goroutine 会替它们关 server，于是：
//   - 别的模块 OnDestroy 里的 Cast/AsyncCall 因为 IsClosed 为 false 而"投递成功"，
//     然后随进程退出静默蒸发，没有任何错误或日志；
//   - 同步 Call 更糟：对端事件循环根本不存在，回包永远不会产生，
//     调用方永久阻塞在 chanrpc 的等待循环里（只每 5 秒打一条 warn），
//     而 shutdownTimeout 只覆盖 wg.Wait，覆盖不到 OnDestroy 本身。
//
// 由框架在这里代关是安全的：该模块从来没有事件循环，此刻唯一的访问者就是
// 停机 goroutine，不存在 Close 注释所担心的并发访问。队列里若有 OnInit 阶段
// 投递进来的积压请求，Close 会照常排空执行；对 OnInit 从未成功的模块，
// handler 尚未注册，exec 会回一个 "not registered" 错误包，
// 调用方拿到的是明确错误而不是永久等待。
func (a *app) closeIdleServer(wrapper *moduleWrapper) {
	server := wrapper.ChanRPC()
	if server == nil || server.IsClosed() {
		return
	}
	server.Close()
}

// closeModule 调用模块的 Close 并捕获其中可能发生的 panic。
//
// 与 destroyModule 对称：Close 同样是 IModule 的公开钩子，业务普遍会重写它来
// 释放出站连接池、生产者、文件句柄。此前它裸调用在 recover 之外，一次 panic
// 会一路冲出 shutdownModule → stop 的 LIFO 循环 → Run，排在后面（即更早注册、
// 通常是持久化/收尾）的所有模块整段拿不到 OnDestroy。
func (a *app) closeModule(wrapper *moduleWrapper) {
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
func (a *app) destroyModule(wrapper *moduleWrapper) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("module destroy panic recovered", slog.String("module", wrapper.Name()), slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
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
//   - panic 不会导致进程退出，仅记录日志（serveModule 的 dynamic=true 参数控制）
//   - 支持通过 RemoveDynamicModule 单独卸载，不影响其他模块
//   - 模块按 Priority 升序初始化，同优先级时保留传入 mods 的参数顺序（稳定排序），
//     任一失败不会中止后续模块的初始化尝试（已成功初始化的模块不会因后面某个模块失败而回滚）
//
// 返回值为每个非 nil 模块的处理结果列表，调用方可据此精确判断哪些模块
// 成功、哪些失败及失败原因；若全部成功，err 为 nil，否则 err 汇总了
// 所有失败模块的名称，便于快速定位问题而无需遍历 results。
func (a *app) AddDynamicModules(mods ...IModule) (results []AddDynamicModuleResult, err error) {
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
		wrappers = append(wrappers, newModuleWrapper(mod))
	}

	// 动态模块同样按 Priority 稳定排序：同优先级时保留传入顺序，
	// 使调用方仍可通过参数顺序表达同优先级模块间的依赖关系（与 start 的排序规则一致，
	// 不同优先级之间会重新排序，只有同优先级组内部不被打乱，示例见 start 中的注释）。
	slices.SortStableFunc(wrappers, func(i, j *moduleWrapper) int {
		return cmp.Compare(i.Priority(), j.Priority())
	})
	for _, wrapper := range wrappers {
		name := wrapper.Name()
		// 重名直接拒绝：Store 会静默覆盖同名 key，被覆盖的旧 wrapper 从此再无
		// 任何引用能触及——cancel 永不调用、goroutine 永久驻留、OnDestroy/Close
		// 永不执行，removeAllDynamicModules 按名字遍历也看不见它。
		if _, exists := a.dynamicModules.Load(name); exists {
			initErr := fmt.Errorf("dynamic module %q already exists", name)
			wrapper.cancel()
			slog.Error("module init error", slog.String("module", name), slog.Any("error", initErr))
			results = append(results, AddDynamicModuleResult{Name: name, Err: initErr})
			failedNames = append(failedNames, name)
			continue
		}

		wrapper.setLifecycle(lifecycleIniting)
		if initErr := wrapper.OnInit(); initErr != nil {
			wrapper.setLifecycle(lifecycleInitFailed)
			// 释放 context 节点：wrapper 到此被彻底丢弃，不会再有任何路径能触及它。
			// OnInit 已分配的业务资源由 OnInit 自己在返回 error 前回滚，
			// 框架不调用 OnDestroy/Close——与静态路径的契约完全一致。
			wrapper.cancel()
			slog.Error("module init error", slog.String("module", name), slog.Any("error", initErr))
			results = append(results, AddDynamicModuleResult{Name: name, Err: initErr})
			failedNames = append(failedNames, name)
			continue
		}
		a.startServing(wrapper, true) // dynamic=true：panic 不退出进程
		// 等待事件循环就绪后再存入，保证 ChanRPC 可接收。
		// waitReady 同时监视 goroutine 退出：Serve 若在 close(ready) 之前 panic，
		// 动态模块的 panic 不会退出进程，只等 ready 会让调用方（常是 HTTP 热加载
		// 接口的处理协程）永久挂死。
		if !wrapper.waitReady() {
			initErr := fmt.Errorf("dynamic module %q serve exited before becoming ready", name)
			slog.Error("module serve exited before becoming ready", slog.String("module", name))
			a.shutdownModule(wrapper)
			results = append(results, AddDynamicModuleResult{Name: name, Err: initErr})
			failedNames = append(failedNames, name)
			continue
		}
		// 记录添加序号：sync.Map.Range 不保证遍历顺序，removeAllDynamicModules
		// 靠这个单调递增序号在同优先级内重建真实的添加顺序，见 moduleWrapper.seq 的说明。
		wrapper.seq = a.dynSeq.Add(1)
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
//  1. LoadAndDelete：原子摘除，模块立即对 ChanRPC 不可见
//  2. shutdownModule：与静态模块完全相同的关闭路径，
//     即 cancel → 等待 goroutine 退出（含 shutdownTimeout 超时保护）→ OnDestroy → Close
//
// **先 Delete 再销毁**：此前 Delete 排在整个销毁流程最后，从 OnDestroy 开始到
// wg.Wait 返回的整个窗口内 ChanRPC(name) 仍能查到这个模块，别的模块照常投递，
// 而 cancel 又排在 OnDestroy 之后、事件循环还活着，于是"模块已执行完 OnDestroy、
// 业务上已释放资源"与"模块仍在对外提供服务并执行 handler"同时成立（use-after-destroy）。
// LoadAndDelete 同时解决了 Load 与 Delete 之间的窗口：并发卸载同名模块时，
// 此前每个调用方都会 Load 到同一个 wrapper 并各自执行一遍 OnDestroy。
//
// **销毁顺序与静态模块一致**：此前这里是 OnDestroy → cancel → wg.Wait，
// OnDestroy 跑在事件循环仍在消费消息的时刻，直接违反 shutdownModule 论证的
// 不变量（并发读写业务 map → Go 运行时 fatal 且不可 recover）。改为共用
// shutdownModule 之后，动态模块也拿到了 Close 调用、wg.Wait 超时保护，
// 以及"OnDestroy 执行时本模块 Server 已关闭"这条与静态模块相同的语义。
//
// 该操作是同步阻塞的，调用方会等待模块完全停止后才返回，
// 确保模块的所有资源在函数返回前已被完整清理，避免悬挂的 goroutine 或资源泄漏。
func (a *app) RemoveDynamicModule(name string) bool {
	value, ok := a.dynamicModules.LoadAndDelete(name)
	if !ok {
		return false
	}

	wrapper, ok := value.(*moduleWrapper)
	if !ok {
		return false
	}

	a.shutdownModule(wrapper)

	return true
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
func (a *app) removeAllDynamicModules() {
	var wrappers []*moduleWrapper

	a.dynamicModules.Range(func(key, value any) bool {
		if wrapper, ok := value.(*moduleWrapper); ok {
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
// 示例（在某个模块的 OnInit 中注册 SIGHUP 热重载）：
//
//	if err := xhive.RegisterSignal(func() {
//	    slog.Info("收到 SIGHUP，重新加载配置")
//	    reloadConfig()
//	}, syscall.SIGHUP); err != nil {
//	    return err
//	}
func (a *app) RegisterSignal(trap SignalTrap, sigs ...os.Signal) error {
	return a.sm.Register(trap, sigs...)
}
