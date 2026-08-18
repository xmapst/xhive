// Package xtime 是全进程唯一的时间源。
//
// 它放在框架里而不是业务侧，是因为框架自己就是时间的消费者：timer 的时间轮按
// 当前时刻推进、按 deadline 判定到期。时间源若归业务所有，框架就只能留一个注入
// 接缝，而「忘了注入」会让时间轮与业务跑在两根轴上——业务把时间调到明天之后，
// 跨天重置、周期性任务这类靠定时器驱动的逻辑全都不触发，且不报任何错。
// 由框架持有时间源，这条接缝连同它的失效模式一起消失。
//
// # 为什么需要它
//
// 服务端的绝大多数状态都是「绝对时间戳 + 纯函数」：一个随时间线性变化的量是
// (起止时刻, 当前时刻) 的插值，一个到期判定是两个绝对时刻的比较。这类模型
// 有一条隐含前提——**所有生产者与消费者必须读同一根时间轴**。一旦一半代码读
// 墙上时钟、另一半读平移后的时钟，状态会瞬移、结算会重复，而且不报任何错。
//
// 直接散着调 time.Now() 就没有任何东西能保证这一点，也没法在测试里
// 把时间推到明天去验证跨天逻辑。
//
// # 两根时间轴，别混用
//
//	Now / NowMs   业务时间。参与业务逻辑的一切时刻都用它：进度插值、
//	              冷却、存活截止、任务完成判定。它会被时间平移影响。
//	Wall / WallMs 物理时间。日志时间戳、RPC 超时、性能统计、连接保活用它。
//	              它永远等于真实的墙上时钟，不受平移影响。
//
// 判断标准很简单：**这个时刻会不会被拿去和另一个业务时刻作差或比较？**
// 会，就是业务时间；只是「记录一下现在几点」或「多久之后放弃等待」，就是物理时间。
//
// # 时区固定 UTC
//
// 两条路径都返回 UTC，没有开关。「今天零点」是按 Year/Month/Day 算的——
// 于是开不开时间平移，跨天边界会差好几个小时，测试环境与线上行为不一致。
//
// # 并发口径
//
// 平移量用原子量保存，可从任意 goroutine 读；开关只在启动期设置一次，
// 之后只读。各业务模块是单 goroutine actor，模块内无需再加锁。
package xtime

import (
	"sync/atomic"
	"time"
)

// shiftEnabled 表示是否启用时间平移。
//
// **只在启动期设置一次**，之后只读，因此用普通 bool 即可。
// 生产环境应保持关闭：关闭时 Now 退化成一次 time.Now().UTC()，零额外开销，
// 也就不存在「线上被人把时间调走」这条风险。
var shiftEnabled bool

// shift 是业务时间相对物理时间的偏移量（纳秒）。
var shift atomic.Int64

// SetShiftEnabled 设置是否启用时间平移，**只能在启动期调用一次**。
//
// 只应在非生产环境打开（例如调试/测试模式下允许运维把业务时间人为
// 调快调慢），生产环境保持关闭。
func SetShiftEnabled(v bool) { shiftEnabled = v }

// ShiftEnabled 返回当前是否启用平移。
func ShiftEnabled() bool { return shiftEnabled }

// Shift 返回当前的平移量。未启用时恒为 0。
func Shift() time.Duration {
	if !shiftEnabled {
		return 0
	}
	return time.Duration(shift.Load())
}

// SetShift 设置平移量。未启用平移时为空操作。
//
// # 调用方必须保证的事
//
// 平移量**必须落库，并在读取任何存档之前重放**。库里存的是绝对业务时间戳，
// 它们是在平移后的时间轴上写下的。重启后若不先把平移量恢复回来就去读存档，
// 这些时刻会被当成新时间轴上的值解释：一个还有 10 分钟到期的任务可能立刻
// 被判定为已到期，或者永远不会到期。
//
// 本包不碰数据库，这条约束只能由调用方履行——接入平移功能之前必须先把
// 持久化与重放做掉，否则宁可让平移量恒为 0。
func SetShift(d time.Duration) {
	if !shiftEnabled {
		return
	}
	shift.Store(int64(d))
}

// AddShift 在当前平移量上叠加。
func AddShift(d time.Duration) {
	if !shiftEnabled {
		return
	}
	shift.Add(int64(d))
}

// ShiftTo 把业务时间平移到目标时刻。
func ShiftTo(t time.Time) { SetShift(t.Sub(time.Now().UTC())) }

// ClearShift 清除平移，业务时间回到墙上时钟。
func ClearShift() { shift.Store(0) }

// Now 返回业务时间（UTC）。参与游戏逻辑的一切时刻都应取自这里。
func Now() time.Time {
	if !shiftEnabled {
		return time.Now().UTC()
	}
	return time.Now().UTC().Add(time.Duration(shift.Load()))
}

// NowMs 返回业务时间的 Unix 毫秒戳，是全项目最常用的时间取法。
func NowMs() int64 { return Now().UnixMilli() }

// Since 返回从 t 到现在经过的**业务时间**。
//
// 提供它是为了让业务代码不必去够标准库的 time.Since——那个隐式读墙上时钟，
// 拿去减一个业务时刻就会悄悄混轴，而且看不出任何异常。
// 量真实耗时（性能统计、超时）请继续用标准库的 time.Since。
func Since(t time.Time) time.Duration { return Now().Sub(t) }

// Until 返回从现在到 t 还剩多少**业务时间**，理由同 Since。
func Until(t time.Time) time.Duration { return t.Sub(Now()) }

// SinceMs 返回从毫秒时间戳 ms 到现在经过的业务毫秒数。
// 本项目的业务时刻绝大多数以毫秒戳保存，这条比 Since 更常用。
func SinceMs(ms int64) int64 { return NowMs() - ms }

// Wall 返回物理时间（UTC），不受时间平移影响。
// 日志时间戳、RPC 超时、性能统计、连接保活用它。
func Wall() time.Time { return time.Now().UTC() }

// WallMs 返回物理时间的 Unix 毫秒戳。
func WallMs() int64 { return Wall().UnixMilli() }
