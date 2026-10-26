package app

import (
	"context"
	"sync"
	"sync/atomic"

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
	// （静态模块为 Register/Run 的注册顺序，动态模块为 AddDynamicModules 传参顺序），
	// 关闭顺序与之完全相反（LIFO）。
	Priority() uint
	// OnInit 执行模块初始化。静态模块失败时终止应用启动；
	// 动态模块失败记录在 AddDynamicModules 的结果中，并继续尝试后续模块。
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
	// Ready 返回就绪信号 channel，应在 Serve 进入事件循环前关闭。
	// 所有静态模块就绪后应用才进入 StateRun；动态模块就绪后才对 ChanRPC 可见。
	// 此信号不限制静态模块之间在启动期间提前投递消息。
	Ready() <-chan struct{}
	// OnDestroy 执行模块销毁，在模块 goroutine 完全退出之后调用，负责释放所有资源。
	//
	// **调用前提：OnInit 已经成功返回。** 框架为每个模块维护显式的生命周期
	// 状态，只对 OnInit 成功的模块调用 OnDestroy；OnInit 从未被调用、
	// 或返回了 error 的模块不会走到这里（见 OnInit 与 shutdownModule 的说明）。
	// 因此 OnDestroy 里可以直接解引用 OnInit 中创建的字段，不必写防御式判空。
	//
	// 注意：不能自投递。执行到这里时本模块自己的 Server 已经 Close，
	// 对它的 Cast/Call/AsyncCall（包括 m.Call(m.ChanRPC(), ...) 这种自调用）
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
	// 见 xhive.Skeleton.Close 的说明。
	//
	// 与 OnDestroy 成对：只有收到过 OnDestroy 的模块才会收到 Close。
	// 其中的 panic 由框架 recover 并记录日志，不会中断其余模块的关闭流程。
	Close() error
}

// moduleLifecycle 描述单个模块在框架中所处的生命周期阶段。
//
// 引入模块级状态的原因：框架此前只有 App 级别的 AppStateXxx，模块级别没有
// 任何"这个模块走到第几步"的记账。于是启动中途失败时 stop 无法区分
// "OnInit 已成功、资源已分配"与"OnInit 一次都没被调用过"两类模块，
// 只能无差别地对所有模块执行 OnDestroy/Close——对后者而言，那是在一个
// 字段全为零值的对象上执行"释放所有资源"，属于契约违反。
//
// 状态跃迁：
//
//	Registered ──OnInit 开始──> Initing ──成功──> Inited ──启动 Serve──> Serving
//	                               │                  │                    │
//	                               └──失败──> InitFailed                   │
//	                                                  │                    │
//	                                        claimShutdown 抢占（任意状态）  │
//	                                                  ▼                    ▼
//	                                            Destroying ──完成──> Destroyed
//
// 两条写入路径的分工是这套状态机能成立的关键：
//   - 启动路径（start / AddDynamicModules）只用 advanceLifecycle 做 CAS 推进。
//     它随时可能被一个并发的 stop 抢走模块，推进失败就意味着"已经不归我管了"。
//   - 关闭路径先用 claimShutdown 把状态 CAS 成 Destroying 拿下销毁权，
//     此后这个 wrapper 归它独占，可以直接 setLifecycle 直写。
//
// 所以 Destroying 之后不会再回到任何"活着"的状态：启动路径的 CAS 前置条件
// 是它推进前读到的那个状态，一旦被抢占就必然失败。反过来，Registered →
// Initing → Inited → Serving 这条主链本身是单向的。
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
	visible   atomic.Bool  // 动态模块是否已完成登记、可被 ChanRPC 寻址，见 AddDynamicModules
	seq       uint64
}

// lifecycleState 原子读取模块当前生命周期状态。
func (w *moduleWrapper) lifecycleState() moduleLifecycle {
	return moduleLifecycle(w.lifecycle.Load())
}

// setLifecycle 无条件写入模块生命周期状态。
//
// 只有关闭路径可以这样写：它先通过 claimShutdown 拿到了销毁权，此后这个
// wrapper 的状态归它独占。启动路径必须改用 advanceLifecycle——那里随时可能
// 有一个并发的 stop 已经接管了模块，无条件覆盖会把它拉回"活着"的状态。
func (w *moduleWrapper) setLifecycle(state moduleLifecycle) {
	w.lifecycle.Store(int32(state))
}

// advanceLifecycle 仅当模块当前正处于 from 状态时，才把它推进到 to，
// 返回是否推进成功。
//
// 启动路径全程用它而不是 setLifecycle，是为了让"销毁权"真正独占：
// 启动期收到停止信号时，stop 会对仍在 OnInit 的模块 claimShutdown
// （Initing → Destroying）并跳过销毁；此刻 OnInit 若返回 nil，用
// setLifecycle 直写就会把 Destroying 覆盖成 Inited——模块从此停在一个
// "资源已分配、框架必须销毁"的状态上，而 stop 的 LIFO 循环早已走过它，
// 再也不会有人来销毁，OnDestroy/Close 永远不执行。
// 用 CAS 推进则会失败，调用方据此知道模块已被接管，就此收手。
func (w *moduleWrapper) advanceLifecycle(from, to moduleLifecycle) bool {
	return w.lifecycle.CompareAndSwap(int32(from), int32(to))
}

// claimShutdown 通过 CAS 抢占模块的销毁权，返回抢占前的状态。
//
// 返回 lifecycleDestroying / lifecycleDestroyed 表示销毁权已被别的调用方
// 拿走（重复 stop、stop 与 RemoveDynamicModule 并发、并发卸载同名动态模块），
// 调用方必须直接返回，不得再执行 OnDestroy——这使"每个模块的 OnDestroy 与
// Close 至多各执行一次"成为结构性保证，而不再依赖外层 sync.Once 与 App 状态闸门。
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
// Register、start 和 AddDynamicModules 共用此构造逻辑，
// 确保静态与动态模块的 context、退出信号及初始生命周期状态一致。
func newModuleWrapper(mod IModule) *moduleWrapper {
	wrapper := &moduleWrapper{IModule: mod, exited: make(chan struct{})}
	wrapper.ctx, wrapper.cancel = context.WithCancel(context.Background())
	return wrapper
}
