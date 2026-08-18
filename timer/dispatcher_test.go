package timer

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitEvent(t *testing.T, ch <-chan Event, timeout time.Duration) Event {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("event channel closed")
		}
		return ev
	case <-time.After(timeout):
		t.Fatalf("timeout waiting event after %v", timeout)
		return nil
	}
}

func assertNoEvent(t *testing.T, ch <-chan Event, timeout time.Duration) {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			return
		}
		t.Fatalf("unexpected event: %s", ev.Name())
	case <-time.After(timeout):
	}
}

func countSlots(disp *dispatcher) int {
	count := 0
	for _, slot := range disp.timerSlots {
		count += len(slot)
	}
	return count
}

func TestDispatcherTimerCallbackSkipsCanceledRecoversPanicAndClearsCallback(t *testing.T) {
	var canceled sync.Map
	var called atomic.Int32
	timer := &dispatcherTimer{id: 1, cb: func(int64) { called.Add(1) }, canceled: &canceled}
	canceled.Store(timer.id, struct{}{})
	timer.Callback()
	if got := called.Load(); got != 0 {
		t.Fatalf("canceled callback called %d times, want 0", got)
	}
	if timer.cb != nil {
		t.Fatal("canceled callback should be cleared")
	}

	panicked := &dispatcherTimer{id: 2, cb: func(int64) { panic("boom") }}
	panicked.Callback()
	if panicked.cb != nil {
		t.Fatal("panicked callback should be cleared")
	}
}

func TestDispatcherPlaceSelectsLevelsAndImmediateFires(t *testing.T) {
	disp := newDispatcher(4)
	now := time.Now()

	immediate := &dispatcherTimer{id: 1, name: "immediate", deadline: now.Add(-time.Millisecond), cb: func(int64) {}}
	disp.place(immediate)
	if ev := waitEvent(t, disp.chanFired.Out(), 100*time.Millisecond); ev.Name() != "immediate" {
		t.Fatalf("event name = %q, want immediate", ev.Name())
	}

	near := &dispatcherTimer{id: 2, name: "near", deadline: time.Now().Add(timerTick), cb: func(int64) {}}
	disp.place(near)
	if _, ok := disp.timerSlots[0][near.id]; !ok {
		t.Fatal("near timer should be placed into level 0")
	}
}

// TestDispatcherPlaceKeepsScanInvariant 守护选层与扫描节奏之间的不变式：
// 第 i 层每 2^i 个 tick 才被 doTick 扫一次，因此放进第 i 层的定时器
// **剩余时间必须 >= 2^i 个 tick**，否则它会在自己那层安静地过期，
// 一直等到下一次该层扫描才被发现。
//
// 写反成「取最小的 i 使 diff <= 2^i」不会有任何报错，只会让长定时器稳定迟到最多一倍：
// 15 分钟的周期变成 17 分 28 秒，而且自调度 ticker 会把这个偏差永久锁死。
func TestDispatcherPlaceKeepsScanInvariant(t *testing.T) {
	disp := newDispatcher(4)

	for _, ticks := range []int64{1, 2, 3, 4, 5, 7, 8, 9, 100, 1000, 14063} {
		d := time.Duration(ticks) * timerTick
		tm := &dispatcherTimer{id: ticks, name: "t", deadline: time.Now().Add(d), cb: func(int64) {}}
		disp.place(tm)

		level := -1
		for i := range timerLevel {
			if _, ok := disp.timerSlots[i][tm.id]; ok {
				level = i
				break
			}
		}
		if level < 0 {
			t.Fatalf("timer of %d ticks was not placed into any level (silently dropped)", ticks)
		}
		// place 内部按 time.Now() 算 diff，构造与调用之间会流逝一点时间，
		// 因此实际 diff 可能比标称少不到一个 tick。断言用标称值即可：
		// 正确实现给出的层满足 2^level <= ticks，错误实现会给出 2^level > ticks。
		if int64(1)<<uint(level) > ticks {
			t.Errorf("timer of %d ticks placed into level %d (scanned every %d ticks); "+
				"it would expire before that level is next scanned",
				ticks, level, int64(1)<<uint(level))
		}
	}
}

// TestDispatcherPlaceAcceptsVeryLongTimers 超过最高层扫描周期的定时器不得被静默丢弃。
//
// 升序选层在 diff > 2^19 个 tick（约 9.32 小时）时一个分支都不命中，
// 定时器既不入层也不投触发队列，永远不会响，而 Manager 里的元数据还会永久残留。
func TestDispatcherPlaceAcceptsVeryLongTimers(t *testing.T) {
	disp := newDispatcher(4)

	// 2^19 + 1 个 tick，刚好越过最高层的扫描周期
	tm := &dispatcherTimer{id: 999, name: "very-long",
		deadline: time.Now().Add(time.Duration(int64(1)<<19+1) * timerTick), cb: func(int64) {}}
	disp.place(tm)

	for i := range timerLevel {
		if _, ok := disp.timerSlots[i][tm.id]; ok {
			return // 落进了某一层即可，后续由 trigger 逐层级联
		}
	}
	t.Fatal("a timer longer than the top level's scan period was silently dropped; " +
		"it will never fire and its metadata leaks in Manager.timers")
}

// TestDispatcherDoTickRebasesOnBackwardClockAndKeepsFiring 钉住时钟回退后的行为：
// 基准必须跟着回退，且回退之后时间轮要照常触发定时器。
//
// 这里曾经保持 lastTick 不动。那样一来基准停在未来，后续每次 doTick 都因
// nowTick <= lastTick 直接返回、一层都不扫，全进程定时器停摆到时钟走满差值为止。
// 业务把时间前跳一天再调回来，就会让整个进程的定时器停一天。
func TestDispatcherDoTickRebasesOnBackwardClockAndKeepsFiring(t *testing.T) {
	disp := newDispatcher(8)
	futureTick := time.Now().UnixNano()/int64(timerTick) + 100000
	nowTick := futureTick - 100000

	// 时钟回退：基准应当跟着退到 nowTick。
	got := disp.doTick(time.Unix(0, nowTick*int64(timerTick)), futureTick)
	if got != nowTick {
		t.Fatalf("doTick after backward clock = %d, want %d (base must follow the clock back)", got, nowTick)
	}

	// 回退之后照常触发：挂一个 2 格后到期的定时器，推进到它的 deadline 必须响。
	deadline := time.Unix(0, (nowTick+2)*int64(timerTick))
	var called atomic.Int32
	disp.place(&dispatcherTimer{id: 1, name: "after-rewind", deadline: deadline,
		cb: func(int64) { called.Add(1) }, canceled: &disp.canceledTimers})

	disp.doTick(deadline, got)
	ev := waitEvent(t, disp.chanFired.Out(), 100*time.Millisecond)
	if ev.Name() != "after-rewind" {
		t.Fatalf("event name = %q, want after-rewind", ev.Name())
	}
	ev.Callback()
	if called.Load() != 1 {
		t.Fatalf("callback count = %d, want 1", called.Load())
	}
}

// TestDispatcherRescanDoesNotDoubleFire 证明「基准回退会导致重复触发」这个担心不成立——
// 它正是上面那条旧行为的全部理由。trigger 在投递的同时就把定时器 delete 出槽位，
// 因此同一段 tick 区间被重复扫描时，已触发的定时器扫不到，重扫是幂等的。
func TestDispatcherRescanDoesNotDoubleFire(t *testing.T) {
	disp := newDispatcher(8)
	baseTick := time.Now().UnixNano() / int64(timerTick)
	deadline := time.Unix(0, (baseTick+2)*int64(timerTick))
	var called atomic.Int32
	disp.place(&dispatcherTimer{id: 1, name: "once", deadline: deadline,
		cb: func(int64) { called.Add(1) }, canceled: &disp.canceledTimers})

	// 第一次推进：触发。
	disp.doTick(deadline, baseTick)
	ev := waitEvent(t, disp.chanFired.Out(), 100*time.Millisecond)
	ev.Callback()

	// 把基准退回去，再把同一段区间重扫一遍：不应再有事件。
	disp.doTick(deadline, baseTick)
	assertNoEvent(t, disp.chanFired.Out(), 50*time.Millisecond)
	if called.Load() != 1 {
		t.Fatalf("callback count = %d, want 1 (rescan must not re-fire)", called.Load())
	}
}

func TestDispatcherDoTickMovesForwardAndStopsAtCurrentTick(t *testing.T) {
	disp := newDispatcher(8)
	nowTick := time.Now().UnixNano() / int64(timerTick)
	startTick := nowTick - 4
	deadline := time.Unix(0, (startTick+2)*int64(timerTick))
	var called atomic.Int32
	disp.place(&dispatcherTimer{id: 1, name: "due", deadline: deadline, cb: func(int64) { called.Add(1) }, canceled: &disp.canceledTimers})

	got := disp.doTick(time.Unix(0, nowTick*int64(timerTick)), startTick)
	if got != nowTick {
		t.Fatalf("doTick returned %d, want %d", got, nowTick)
	}
	ev := waitEvent(t, disp.chanFired.Out(), 100*time.Millisecond)
	if ev.Name() != "due" {
		t.Fatalf("event name = %q, want due", ev.Name())
	}
	ev.Callback()
	if called.Load() != 1 {
		t.Fatalf("callback count = %d, want 1", called.Load())
	}
	assertNoEvent(t, disp.chanFired.Out(), 20*time.Millisecond)
}

func TestDispatcherNewFiresAndCallbackReceivesGeneratedID(t *testing.T) {
	disp := newDispatcher(4)
	disp.Run()
	defer disp.Stop()

	var gotID atomic.Int64
	id := disp.New("near", 0, time.Now().Add(10*time.Millisecond), func(id int64) {
		gotID.Store(id)
	})
	if id == 0 {
		t.Fatal("expected non-zero timer id")
	}

	ev := waitEvent(t, disp.chanFired.Out(), 300*time.Millisecond)
	if ev.Name() != "near" {
		t.Fatalf("event name = %q, want near", ev.Name())
	}
	ev.Callback()
	if got := gotID.Load(); got != id {
		t.Fatalf("callback id = %d, want %d", got, id)
	}
}

func TestDispatcherCancelBeforeCallbackSuppressesEvent(t *testing.T) {
	disp := newDispatcher(4)
	var called atomic.Int32
	id := int64(100)
	timer := &dispatcherTimer{id: id, name: "cancel", deadline: time.Now().Add(-time.Millisecond), cb: func(int64) { called.Add(1) }, canceled: &disp.canceledTimers}
	disp.place(timer)
	disp.Cancel("cancel", id)
	ev := waitEvent(t, disp.chanFired.Out(), 100*time.Millisecond)
	ev.Callback()
	if got := called.Load(); got != 0 {
		t.Fatalf("callback called %d times after cancel", got)
	}
}

func TestDispatcherUpdateReplacesDeadlineAndCancelSuppresses(t *testing.T) {
	disp := newDispatcher(16)
	disp.Run()
	defer disp.Stop()

	var called atomic.Int32
	id := disp.New("update", 0, time.Now().Add(200*time.Millisecond), func(int64) {
		called.Add(1)
	})
	if id == 0 {
		t.Fatal("expected non-zero timer id")
	}
	disp.Update("update", id, time.Now().Add(25*time.Millisecond))
	disp.Update("update", id, time.Now().Add(150*time.Millisecond))
	disp.Cancel("update", id)

	assertNoEvent(t, disp.chanFired.Out(), 220*time.Millisecond)
	if got := called.Load(); got != 0 {
		t.Fatalf("callback called %d times after update/cancel chain", got)
	}
}

func TestDispatcherSameIDReplacementOnlyNewCallbackRuns(t *testing.T) {
	disp := newDispatcher(16)
	disp.Run()
	defer disp.Stop()

	const id int64 = 1001
	var oldCalled atomic.Int32
	var newCalled atomic.Int32
	if got := disp.New("same-id-old", id, time.Now().Add(10*time.Millisecond), func(int64) {
		oldCalled.Add(1)
	}); got != id {
		t.Fatalf("old timer id = %d, want %d", got, id)
	}
	if got := disp.New("same-id-new", id, time.Now().Add(40*time.Millisecond), func(int64) {
		newCalled.Add(1)
	}); got != id {
		t.Fatalf("new timer id = %d, want %d", got, id)
	}

	ev := waitEvent(t, disp.chanFired.Out(), 300*time.Millisecond)
	if ev.Name() != "same-id-new" {
		t.Fatalf("event name = %q, want same-id-new", ev.Name())
	}
	ev.Callback()
	assertNoEvent(t, disp.chanFired.Out(), 80*time.Millisecond)
	if got := oldCalled.Load(); got != 0 {
		t.Fatalf("old callback called %d times, want 0", got)
	}
	if got := newCalled.Load(); got != 1 {
		t.Fatalf("new callback called %d times, want 1", got)
	}
}

func TestDispatcherDeleteRemovesOnlyOneTimer(t *testing.T) {
	disp := newDispatcher(4)
	now := time.Now()
	disp.place(&dispatcherTimer{id: 1, name: "one", deadline: now.Add(20 * time.Millisecond)})
	disp.place(&dispatcherTimer{id: 2, name: "two", deadline: now.Add(40 * time.Millisecond)})
	if countSlots(disp) != 2 {
		t.Fatalf("slot count = %d, want 2", countSlots(disp))
	}
	if old := disp.delete(1); old == nil || old.id != 1 {
		t.Fatalf("delete returned %+v, want timer 1", old)
	}
	if countSlots(disp) != 1 {
		t.Fatalf("slot count = %d, want 1", countSlots(disp))
	}
}
