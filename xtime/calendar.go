package xtime

import "time"

// 日历与时间边界工具。
//
// 全部以 **UTC** 为口径，且**只接受毫秒时间戳、只返回毫秒时间戳**——
// 边界计算最容易出错的地方就是单位与时区在调用链上悄悄换了一次，
// 把两者都钉死在签名里，误用就变成编译错误而不是线上跨天时刻差几小时。
//
// 想按业务时间取「今天」，写 DayStartMs(NowMs())；
// 需要固定某个时刻算，就把那个时刻传进来。函数本身不读时钟，因此可测。
//
// 这里只放真正用得上的几个。其中
// Ms2Sec / Sec2Ms / Min2Ms / Hour2Ms 这类纯乘除是噪音，
// TodayStartTs 与 DayStartTs 更是「同一件事两个函数」——不搬。

// dayMs 是一天的毫秒数。
const dayMs int64 = 24 * 60 * 60 * 1000

// DayStartMs 返回 ms 所在那天的 UTC 零点（毫秒）。
//
// 用整除而不是 time.Date：UTC 下每天恒为 86400000 毫秒，没有夏令时缺口，
// 整除既更快也不会因为 Location 传错而算到别的时区去。
// 注意 Go 的整除向零取整，负时间戳（1970 年之前）需要额外向下取整——
// 业务上不会出现，但让函数在全定义域内正确比留个坑强。
func DayStartMs(ms int64) int64 {
	d := ms / dayMs
	if ms < 0 && ms%dayMs != 0 {
		d--
	}
	return d * dayMs
}

// NextDayStartMs 返回 ms 之后的下一个 UTC 零点（毫秒）。
// ms 恰好是零点时返回第二天零点，而不是它自己——「下一个」不含当前时刻。
func NextDayStartMs(ms int64) int64 { return DayStartMs(ms) + dayMs }

// IsSameDay 判断两个毫秒时间戳是否落在同一个 UTC 日。
func IsSameDay(a, b int64) bool { return DayStartMs(a) == DayStartMs(b) }

// DaysBetween 返回从 a 到 b 跨越了几个 UTC 日界。
// 同一天为 0，b 在 a 之前为负。用于「跨天补发」这类判定。
func DaysBetween(a, b int64) int64 { return (DayStartMs(b) - DayStartMs(a)) / dayMs }

// WeekStartMs 返回 ms 所在那一周的**周一** UTC 零点（毫秒）。
//
// 周起始定为周一。这条必须显式声明：
// Go 的 time.Weekday 里 Sunday==0，直接拿它做偏移会得到周日起始。
func WeekStartMs(ms int64) int64 {
	dayStart := DayStartMs(ms)
	// Weekday: Sunday=0, Monday=1 ... Saturday=6。转成「距本周一过了几天」：
	// 周一→0，周二→1，…，周日→6。
	offset := (int64(Ms2Time(dayStart).Weekday()) + 6) % 7
	return dayStart - offset*dayMs
}

// Ms2Time 把毫秒时间戳转成 UTC 时间。
func Ms2Time(ms int64) time.Time { return time.UnixMilli(ms).UTC() }

// Time2Ms 把时间转成毫秒时间戳。
func Time2Ms(t time.Time) int64 { return t.UnixMilli() }
