package xtime

import (
	"testing"
	"time"
)

// 平移开关是包级状态，用例之间必须还原，否则先跑的用例会污染后跑的。
func withShift(t *testing.T, d time.Duration) {
	t.Helper()
	prev := shiftEnabled
	SetShiftEnabled(true)
	SetShift(d)
	t.Cleanup(func() {
		ClearShift()
		SetShiftEnabled(prev)
	})
}

// TestShiftDisabledByDefault 平移默认关闭：生产环境不应该存在「被人把时间调走」这条路径。
func TestShiftDisabledByDefault(t *testing.T) {
	if ShiftEnabled() {
		t.Fatal("shift is enabled by default; production must not be able to move game time")
	}
	SetShift(time.Hour) // 未启用时必须是空操作
	if Shift() != 0 {
		t.Errorf("Shift() = %v after SetShift while disabled, want 0", Shift())
	}
	if drift := Now().Sub(Wall()); drift > time.Second || drift < -time.Second {
		t.Errorf("Now() drifted from Wall() by %v while shift is disabled", drift)
	}
}

// TestNowFollowsShiftAndWallDoesNot 业务时间跟随平移，物理时间不跟随。
//
// 两者混用是这套设计最主要的失效形态：日志按平移后的时间打、超时按平移后的时间算，
// 或者反过来——一部分业务状态按墙上时钟推进而另一部分按平移后的时钟结算。
func TestNowFollowsShiftAndWallDoesNot(t *testing.T) {
	withShift(t, 48*time.Hour)

	if d := Now().Sub(Wall()); d < 47*time.Hour || d > 49*time.Hour {
		t.Errorf("Now()-Wall() = %v, want about 48h; business time must follow the shift", d)
	}
	if d := Wall().Sub(time.Now().UTC()); d > time.Second || d < -time.Second {
		t.Errorf("Wall() drifted from the real clock by %v; physical time must not shift", d)
	}
	if NowMs()-WallMs() < int64(47*time.Hour/time.Millisecond) {
		t.Error("NowMs/WallMs disagree with Now/Wall")
	}
}

// TestNowIsUTCRegardlessOfShift 两条分支都必须是 UTC。
//
// 「今天零点」按 Year/Month/Day 算——于是开不开平移，跨天边界差好几个小时。
func TestNowIsUTCRegardlessOfShift(t *testing.T) {
	if loc := Now().Location(); loc != time.UTC {
		t.Errorf("Now() location = %v while shift is disabled, want UTC", loc)
	}
	withShift(t, time.Hour)
	if loc := Now().Location(); loc != time.UTC {
		t.Errorf("Now() location = %v while shift is enabled, want UTC; "+
			"a zone that depends on the shift makes day boundaries differ between envs", loc)
	}
}

// TestShiftTo 平移到指定时刻。
func TestShiftTo(t *testing.T) {
	withShift(t, 0)
	target := time.Date(2030, 6, 1, 12, 0, 0, 0, time.UTC)
	ShiftTo(target)
	if d := Now().Sub(target); d > time.Second || d < -time.Second {
		t.Errorf("Now() = %v after ShiftTo(%v), off by %v", Now(), target, d)
	}
}

// ─── 日历 ────────────────────────────────────────────────────────────────────

func TestDayStartMs(t *testing.T) {
	// 2026-08-18T05:23:45.678Z
	ms := time.Date(2026, 8, 18, 5, 23, 45, 678_000_000, time.UTC).UnixMilli()
	want := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC).UnixMilli()
	if got := DayStartMs(ms); got != want {
		t.Errorf("DayStartMs = %d (%v), want %d (%v)", got, Ms2Time(got), want, Ms2Time(want))
	}
	// 零点本身是不动点
	if got := DayStartMs(want); got != want {
		t.Errorf("DayStartMs is not idempotent at midnight: %d", got)
	}
	// 1970 年之前：整除向零取整会算到后一天去
	neg := time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC).UnixMilli()
	wantNeg := time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC).UnixMilli()
	if got := DayStartMs(neg); got != wantNeg {
		t.Errorf("DayStartMs(negative) = %v, want %v", Ms2Time(got), Ms2Time(wantNeg))
	}
}

// TestNextDayStartMs 「下一个」零点不含当前时刻——零点传进去要拿到第二天。
func TestNextDayStartMs(t *testing.T) {
	midnight := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC).UnixMilli()
	want := time.Date(2026, 8, 19, 0, 0, 0, 0, time.UTC).UnixMilli()
	if got := NextDayStartMs(midnight); got != want {
		t.Errorf("NextDayStartMs(midnight) = %v, want %v; "+
			"returning the same instant would make a daily timer fire immediately in a loop",
			Ms2Time(got), Ms2Time(want))
	}
}

func TestIsSameDayAndDaysBetween(t *testing.T) {
	a := time.Date(2026, 8, 18, 23, 59, 59, 0, time.UTC).UnixMilli()
	b := time.Date(2026, 8, 19, 0, 0, 1, 0, time.UTC).UnixMilli()
	if IsSameDay(a, b) {
		t.Error("instants one second apart across midnight reported as the same day")
	}
	if got := DaysBetween(a, b); got != 1 {
		t.Errorf("DaysBetween across midnight = %d, want 1", got)
	}
	if got := DaysBetween(b, a); got != -1 {
		t.Errorf("DaysBetween backwards = %d, want -1", got)
	}
	if got := DaysBetween(a, a); got != 0 {
		t.Errorf("DaysBetween(same) = %d, want 0", got)
	}
}

// TestWeekStartMsIsMonday 周起始必须是周一。
//
// Go 的 time.Weekday 里 Sunday==0，直接拿它做偏移会得到周日起始，
// 而所有周常玩法的重置时刻都会因此差一天。
func TestWeekStartMsIsMonday(t *testing.T) {
	// 2026-08-18 是周二
	tue := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	if tue.Weekday() != time.Tuesday {
		t.Fatalf("test fixture is wrong: %v is %v", tue, tue.Weekday())
	}
	wantMon := time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC).UnixMilli()
	if got := WeekStartMs(tue.UnixMilli()); got != wantMon {
		t.Errorf("WeekStartMs(Tue) = %v, want %v (Monday)", Ms2Time(got), Ms2Time(wantMon))
	}

	// 周日必须归到**上一个**周一，这是 Sunday==0 最容易出错的一天
	sun := time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC)
	if sun.Weekday() != time.Sunday {
		t.Fatalf("test fixture is wrong: %v is %v", sun, sun.Weekday())
	}
	if got := WeekStartMs(sun.UnixMilli()); got != wantMon {
		t.Errorf("WeekStartMs(Sun) = %v, want %v; Sunday must belong to the week that "+
			"started on the previous Monday", Ms2Time(got), Ms2Time(wantMon))
	}

	// 周一零点是不动点
	if got := WeekStartMs(wantMon); got != wantMon {
		t.Errorf("WeekStartMs is not idempotent at week start: %v", Ms2Time(got))
	}
}

func TestMsTimeRoundTrip(t *testing.T) {
	ms := time.Date(2026, 8, 18, 5, 23, 45, 678_000_000, time.UTC).UnixMilli()
	if got := Time2Ms(Ms2Time(ms)); got != ms {
		t.Errorf("round trip = %d, want %d", got, ms)
	}
	if loc := Ms2Time(ms).Location(); loc != time.UTC {
		t.Errorf("Ms2Time location = %v, want UTC", loc)
	}
}

// TestSinceAndUntilUseBusinessClock Since/Until 必须走业务时钟。
//
// 标准库的 time.Since 隐式读墙上时钟：拿它去减一个业务时刻，
// 在启用平移之后会得到一个荒谬的差值（相差整个平移量），且不报任何错。
func TestSinceAndUntilUseBusinessClock(t *testing.T) {
	withShift(t, 24*time.Hour)

	past := Now().Add(-time.Hour)
	if d := Since(past); d < 59*time.Minute || d > 61*time.Minute {
		t.Errorf("Since(business past) = %v, want about 1h", d)
	}
	future := Now().Add(2 * time.Hour)
	if d := Until(future); d < 119*time.Minute || d > 121*time.Minute {
		t.Errorf("Until(business future) = %v, want about 2h", d)
	}
	if d := SinceMs(NowMs() - 5000); d < 4000 || d > 6000 {
		t.Errorf("SinceMs = %d, want about 5000", d)
	}

	// 对照组：标准库的 time.Since 拿业务时刻去减，结果会整整差掉一个平移量。
	// 这条断言把「为什么需要 xtime.Since」钉在测试里——
	// 两者之差恰好等于平移量，而不是某个看起来还合理的数。
	if diff := Since(past) - time.Since(past); diff < 23*time.Hour || diff > 25*time.Hour {
		t.Errorf("xtime.Since - time.Since = %v, want about 24h (the shift); "+
			"if these two agree, business code could use time.Since without noticing", diff)
	}
}
