// Package xhive provides an actor-style application framework with module lifecycle,
// ChanRPC messaging, timers, and signal handling.
package xhive

import (
	"os"

	"github.com/xmapst/xhive/chanrpc"
)

// defaultApp 全局默认应用实例（包级单例），供包级函数直接使用。
// 单例模式避免调用方持有实例引用，简化了典型场景下的使用方式（main 函数直接调用 core.Run）。
var defaultApp = newApp()

// Register 向全局默认应用实例注册静态模块。
//
// 必须在 Run 调用之前完成注册。若应用已处于运行状态则返回错误。
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
func Run(mods ...IModule) {
	defaultApp.Run(mods...)
}

// State 获取全局默认应用实例的当前运行状态。
//
// 返回值为 AppStateNone/AppStateInit/AppStateRun/AppStateStop 之一，
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
// 动态模块的 panic 不会导致进程退出，且可通过 RemoveDynamicModule 单独卸载。
//
// 返回值 results 记录每个模块的处理结果（成功或失败原因），调用方可据此
// 精确判断哪些模块成功、哪些失败，而不必因为 err 非 nil 就误以为全部失败；
// err 非 nil 时仅表示至少有一个模块初始化失败，具体失败列表见 results。
func AddDynamicModules(mods ...IModule) (results []AddDynamicModuleResult, err error) {
	return defaultApp.AddDynamicModules(mods...)
}

// RemoveDynamicModule 从全局默认应用实例中同步移除并销毁指定名称的动态模块。
//
// 操作为同步阻塞：OnDestroy（清理资源）→ cancel（发停止信号）→ wg.Wait（等待 goroutine 退出）→ 从 map 移除。
// 调用方会等待模块完全停止后才返回，确保所有资源在函数返回前已被完整清理。
func RemoveDynamicModule(name string) bool {
	return defaultApp.RemoveDynamicModule(name)
}

// RegisterSignal 向全局默认应用实例追加信号处理器，可在 Run 调用前后的任意时刻安全调用。
// 同一信号的多个处理器会并发执行；SIGINT/SIGTERM 为可捕获的框架保留信号；SIGKILL 不可捕获，也作为保留信号禁止业务注册。
func RegisterSignal(trap SignalTrap, sigs ...os.Signal) error {
	return defaultApp.RegisterSignal(trap, sigs...)
}
