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

	far := &dispatcherTimer{id: 3, name: "far", deadline: time.Now().Add(timerTick * 4), cb: func(int64) {}}
	disp.place(far)
	if _, ok := disp.timerSlots[2][far.id]; !ok {
		t.Fatal("far timer should be placed into level 2")
	}
}

func TestDispatcherDoTickDoesNotMoveLastTickBackward(t *testing.T) {
	disp := newDispatcher(4)
	lastTick := time.Now().UnixNano() / int64(timerTick)
	backwardNow := time.Unix(0, (lastTick-100)*int64(timerTick))
	if got := disp.doTick(backwardNow, lastTick); got != lastTick {
		t.Fatalf("lastTick moved backward to %d, want %d", got, lastTick)
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
