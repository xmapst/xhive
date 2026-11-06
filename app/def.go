// Package app 管理 Actor 模块的注册、生命周期、RPC 寻址和应用退出信号。
// 包级函数操作默认应用实例，也可通过 New 创建独立的 App。
package app

import (
	"os"
	"time"

	"github.com/xmapst/xhive/chanrpc"
	"github.com/xmapst/xhive/signal"
)

// defaultApp 是包级函数共用的默认应用实例。
// main 函数可直接调用 app.Run；需要自定义选项时，通过 app.New 创建独立实例。
var defaultApp = New()

// Register 向全局默认应用实例注册静态模块。
//
// 必须在 Run 调用之前完成注册。若应用已处于运行状态则返回错误。
// 模块名称须在本 App 内唯一（包含动态模块）；名称冲突时整批注册失败。
// 运行中需要动态增减模块请使用 AddDynamicModules 和 RemoveDynamicModule。
func Register(mods ...IModule) error {
	return defaultApp.Register(mods...)
}

// Run 向全局默认应用实例注册模块并启动，同时监听系统退出信号（SIGINT/SIGTERM）。
//
// 函数会阻塞当前 goroutine 直至收到退出信号，收到后执行优雅关闭流程。
// 适合在 main 函数中直接调用，是应用的主入口点。
// SIGHUP 信号不会触发关闭，可用于通知应用重新加载配置（业务层自行监听处理）。
// SIGKILL 不可被进程捕获，仅作为框架保留信号禁止业务注册。
//
// 启动失败（如某个模块 OnInit 返回错误）或重复调用时返回非 nil error，
// 正常收到退出信号并完成优雅关闭时返回 nil。
// 模块名称冲突会在初始化前返回错误，包含与 Register 或动态加载模块的冲突。
func Run(mods ...IModule) error {
	return defaultApp.Run(mods...)
}

// State 获取全局默认应用实例的当前运行状态。
//
// 返回值为 StateNone/AppStateInit/AppStateRun/AppStateStop 之一，
// 可用于在关闭信号处理逻辑中判断当前是否可以安全发起操作。
func State() int32 {
	return defaultApp.State()
}

// Stats 获取全局默认应用实例中所有模块（静态 + 动态）的 RPC 队列积压状态统计字符串。
//
// 输出内容可直接用于监控告警或定期日志打印，帮助快速定位消息积压瓶颈。
func Stats() string {
	return defaultApp.Stats()
}

// ChanRPC 通过模块名称获取对应模块的 ChanRPC 服务端，用于跨模块消息投递。
//
// 优先在静态模块中查找，未命中则查找动态模块。
// 返回 nil 表示对应模块不存在或不支持 RPC，调用方可据此做降级处理（如 Cast 时静默丢弃）。
func ChanRPC(name string) *chanrpc.Server {
	return defaultApp.ChanRPC(name)
}

// DynamicModules 返回全局默认应用实例中当前所有动态模块的名称列表。
//
// 用于运维监控、热加载管理等场景，列表顺序不保证与注册顺序一致（sync.Map 无序）。
func DynamicModules() []string {
	return defaultApp.DynamicModules()
}

// AddDynamicModules 向运行中的全局默认应用实例动态添加并启动一批模块。
//
// 动态模块支持热加载，模块在 OnInit 成功后立即启动，无需重启整个应用。
// 动态模块可通过 RemoveDynamicModule 单独卸载；Serve、OnDestroy 与 Close 中的
// panic 仅记录日志。OnInit 默认向调用方传播 panic，WithPanicExit(false) 将其转为添加失败。
//
// 返回值 results 记录每个模块的处理结果（成功或失败原因），调用方可据此
// 精确判断哪些模块成功、哪些失败，而不必因为 err 非 nil 就误以为全部失败；
// err 非 nil 时通常表示至少有一个模块添加失败，具体失败列表见 results。
//
// 有一个例外：应用已进入关闭流程时整批调用会被直接拒绝，此时 err 非 nil 而
// results 为 nil——按 results 遍历上报的调用方在这条路径上会什么都上报不出来，
// 需要单独处理 err。
//
// 两项拒绝规则：
//   - 名称已被静态或动态模块占用时会被拒绝，不会覆盖已有模块（此前是静默覆盖，
//     被覆盖的模块从此无人可达，goroutine 与资源永久泄漏）；
//   - 应用已经开始关闭（或已关闭）时整批拒绝，避免登记出无人负责销毁的模块。
//     初始化期间才开始关闭的模块会被就地回滚（执行 OnDestroy → Close）并计为失败。
func AddDynamicModules(mods ...IModule) (results []AddDynamicModuleResult, err error) {
	return defaultApp.AddDynamicModules(mods...)
}

// RemoveDynamicModule retires a module in the default application.
// A timeout returns false; cleanup and name reservation continue in the background.
func RemoveDynamicModule(name string) bool {
	return defaultApp.RemoveDynamicModule(name)
}

// RegisterSignal 向全局默认应用实例追加信号处理器，可在 Run 调用前后的任意时刻安全调用。
// 同一信号的多个处理器会并发执行；SIGINT/SIGTERM 为可捕获的框架保留信号；SIGKILL 不可捕获，也作为保留信号禁止业务注册。
func RegisterSignal(trap signal.Trap, sigs ...os.Signal) error {
	return defaultApp.RegisterSignal(trap, sigs...)
}

const (
	// StateNone 表示应用未启动或已完全停止。
	//
	// 注意：**已停止的应用不支持再次启动。** stop 不重建 modules 里的 wrapper，
	// 它们的 ctx 已被 cancel、模块自身的一次性资源（如 Ready 返回的就绪
	// channel）也已消耗。start 会检查模块的生命周期状态并明确拒绝
	// 重启，返回 error 而不是产出一个 Serve 立即退出的僵尸应用。
	// 需要"重启"时请新建模块实例与新的 App。
	StateNone = iota
	// StateInit 表示应用正在初始化，所有模块的 OnInit 正在按序执行。
	StateInit
	// StateRun 表示应用运行中，所有模块已成功启动并处于活跃状态。
	StateRun
	// StateStop 表示应用正在优雅关闭，模块正按逆序依次停止。
	StateStop
)

const (
	// defaultShutdownTimeout 单个模块优雅关闭的默认最大等待时间。
	// 设置为 30 分钟是为了兼容可能持有长时间锁或大批量数据落盘的模块，
	// 超时后记录错误日志但不强制终止，避免数据损坏，由运维介入处理。
	//
	// 该值作为 App.shutdownTimeout 的默认值。调用方可通过
	// New(WithShutdownTimeout(d)) 为独立应用实例覆盖；包级函数使用的
	// defaultApp 以默认选项构造，仍使用此值。
	defaultShutdownTimeout = 30 * time.Minute

	// defaultStartupSettleTimeout stop 等待仍在进行的 start 收敛的最大时间。
	//
	// 刻意与 shutdownTimeout 分开、且短得多：这段等待发生在"收到停止信号但
	// start 还没跑完"的窗口里，等的是 OnInit（通常是连接依赖、加载配置，秒级），
	// 而不是 shutdownTimeout 所针对的"大批量落盘"。若复用 30 分钟，启动期卡住
	// 一个 OnInit 就会让 SIGTERM 之后的停机整整空等半小时。
	//
	// 取 5 秒而不是 30 秒，是因为这段等待完全落在停机的关键路径上，而它之后
	// 才轮到真正要花时间的事——逐个模块 OnDestroy 落盘。k8s 的
	// terminationGracePeriodSeconds 默认就是 30 秒，等满 30 秒等于把整个宽限期
	// 用在"等一个卡住的 OnInit"上，已经 OnInit 成功的那些持久化模块一个都轮不上。
	// 5 秒足够覆盖正常的 OnInit 收敛，剩下的时间留给真正的落地工作。
	//
	// 超时后进入各模块的独立清理屏障；仍在执行 OnInit 的模块等待 initDone，
	// 不并发销毁，也不在其资源释放前关闭更早启动的依赖。
	defaultStartupSettleTimeout = 5 * time.Second
)

// Option 用于自定义 New 创建的 App 实例。
//
// 选项仅影响传给 New 的那个实例，不改变包级函数使用的默认应用。
type Option func(*App)

// WithShutdownTimeout 自定义排空和关闭的单次等待时间，传给 New 使用。
//
// 超时作为排空、退出等待的日志周期及同步卸载调用的等待上限。
// 后台清理继续保留名称与依赖，不并发销毁活跃模块；全应用停止等待实际清理完成。
// d <= 0 时该选项不生效，沿用 defaultShutdownTimeout。
func WithShutdownTimeout(d time.Duration) Option {
	return func(a *App) {
		if d > 0 {
			a.shutdownTimeout = d
		}
	}
}
