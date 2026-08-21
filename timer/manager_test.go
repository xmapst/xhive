package timer

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestManagerNewValidationFindAndMetadataRange(t *testing.T) {
	tm := NewManager(4)
	tm.Run()
	defer tm.Stop()

	if id := tm.New("missing", time.Millisecond); id != 0 {
		t.Fatalf("New without handler id = %d, want 0", id)
	}

	tm.Register("kind", func(int64, map[string]string) {})
	metadata := map[string]string{"a": "1", "b": "2"}
	id := tm.New("kind", time.Second, WithID(99), WithMetadata(metadata))
	if id != 99 {
		t.Fatalf("id = %d, want 99", id)
	}
	if dup := tm.New("kind", time.Second, WithID(99)); dup != 0 {
		t.Fatalf("duplicate id = %d, want 0", dup)
	}

	found := tm.Find(id)
	if found == nil {
		t.Fatal("Find returned nil")
	}
	if found.ID() != id || found.Name() != "kind" || found.IsTicker() {
		t.Fatalf("unexpected timer metadata: %+v", found)
	}
	if found.StartAt().IsZero() {
		t.Fatal("StartAt should be set")
	}
	if !found.Deadline().After(found.StartAt()) {
		t.Fatalf("Deadline = %v should be after StartAt = %v", found.Deadline(), found.StartAt())
	}
	if byName := tm.FindByName("kind"); byName != found {
		t.Fatalf("FindByName returned %+v, want %+v", byName, found)
	}
	if byName := tm.FindByName("missing"); byName != nil {
		t.Fatalf("FindByName missing returned %+v, want nil", byName)
	}

	seen := map[string]string{}
	found.RangeMetadata(func(k, v string) bool {
		seen[k] = v
		return false
	})
	if len(seen) != 1 {
		t.Fatalf("RangeMetadata early stop len = %d, want 1", len(seen))
	}
	for k, v := range seen {
		if metadata[k] != v {
			t.Fatalf("metadata[%q] = %q, want %q", k, v, metadata[k])
		}
	}

	tm.Cancel(0)
	if tm.Find(id) == nil {
		t.Fatal("Cancel(0) should not remove valid timer")
	}
	tm.Cancel(id)
	if tm.Find(id) != nil {
		t.Fatal("timer should be removed after Cancel")
	}
	tm.Cancel(id)
}

func TestManagerOneShotCallbackPassesMetadataAndRemovesTimer(t *testing.T) {
	tm := NewManager(8)
	tm.Run()
	defer tm.Stop()

	done := make(chan map[string]string, 1)
	tm.Register("oneshot", func(_ int64, metadata map[string]string) {
		done <- metadata
	})
	metadata := map[string]string{"k": "v"}
	id := tm.New("oneshot", 10*time.Millisecond, WithMetadata(metadata))
	if id == 0 {
		t.Fatal("expected non-zero timer id")
	}

	ev := waitEvent(t, tm.Event(), 300*time.Millisecond)
	if ev.Name() != "oneshot" {
		t.Fatalf("event name = %q, want oneshot", ev.Name())
	}
	ev.Callback()
	select {
	case got := <-done:
		if got["k"] != "v" {
			t.Fatalf("metadata mismatch: %#v", got)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting manager handler")
	}
	if tm.Find(id) != nil {
		t.Fatal("one-shot timer metadata should be removed after callback")
	}
}

func TestManagerTickerReschedulesAndCancelStops(t *testing.T) {
	tm := NewManager(16)
	tm.Run()
	defer tm.Stop()

	var count atomic.Int32
	tm.Register("ticker", func(id int64, _ map[string]string) {
		if count.Add(1) >= 2 {
			tm.Cancel(id)
		}
	})
	id := tm.New("ticker", 10*time.Millisecond, WithTicker())
	if id == 0 {
		t.Fatal("expected non-zero timer id")
	}
	if timer := tm.Find(id); timer == nil || !timer.IsTicker() {
		t.Fatalf("ticker metadata = %+v, want ticker", timer)
	}

	deadline := time.After(500 * time.Millisecond)
	for count.Load() < 2 {
		select {
		case ev, ok := <-tm.Event():
			if !ok {
				t.Fatal("event channel closed")
			}
			ev.Callback()
		case <-deadline:
			t.Fatalf("ticker fired %d times, want at least 2", count.Load())
		}
	}

	if tm.Find(id) != nil {
		t.Fatal("ticker metadata should be removed after cancel")
	}
	assertNoEvent(t, tm.Event(), 80*time.Millisecond)
}

func TestManagerTickerRescheduleKeepsStableInterval(t *testing.T) {
	tm := NewManager(4)
	tm.Register("stable", func(int64, map[string]string) {})
	startAt := time.Now().Add(-100 * time.Millisecond)
	deadline := startAt.Add(30 * time.Millisecond)
	tm.timers[7] = &Timer{id: 7, name: "stable", startAt: startAt, deadline: deadline, isTicker: true}

	tm.commonCb(7)

	timer := tm.Find(7)
	if timer == nil {
		t.Fatal("ticker should still exist")
	}
	if !timer.StartAt().Equal(deadline) {
		t.Fatalf("ticker StartAt = %v, want %v", timer.StartAt(), deadline)
	}
	if !timer.Deadline().Equal(deadline.Add(30 * time.Millisecond)) {
		t.Fatalf("ticker Deadline = %v, want %v", timer.Deadline(), deadline.Add(30*time.Millisecond))
	}
}

func TestManagerAdjustValidation(t *testing.T) {
	if _, err := adjustAbs(time.Second, 0, true); err == nil {
		t.Fatal("adjustAbs zero duration should fail")
	}
	if got, err := adjustAbs(100*time.Millisecond, 150*time.Millisecond, true); err != nil || got != 0 {
		t.Fatalf("adjustAbs acc clamp got=%v err=%v, want 0 nil", got, err)
	}
	if got, err := adjustAbs(100*time.Millisecond, 50*time.Millisecond, false); err != nil || got != 150*time.Millisecond {
		t.Fatalf("adjustAbs delay got=%v err=%v", got, err)
	}
	for _, pct := range []int64{0, -1, PctBase + 1} {
		if _, err := adjustPct(time.Second, pct, true); err == nil {
			t.Fatalf("adjustPct pct %d should fail", pct)
		}
	}
	if got, err := adjustPct(time.Second, PctBase/2, true); err != nil || got != 500*time.Millisecond {
		t.Fatalf("adjustPct acc got=%v err=%v", got, err)
	}
	if got, err := adjustPct(time.Second, PctBase/2, false); err != nil || got != 1500*time.Millisecond {
		t.Fatalf("adjustPct delay got=%v err=%v", got, err)
	}
}

func TestManagerRescheduleUpdatesDeadlineWithoutChangingStartAt(t *testing.T) {
	tm := NewManager(16)
	tm.Run()
	defer tm.Stop()

	tm.Register("adjust", func(int64, map[string]string) {})
	id := tm.New("adjust", 300*time.Millisecond)
	if id == 0 {
		t.Fatal("expected timer id")
	}
	before := tm.Find(id).Deadline()
	startBefore := tm.Find(id).StartAt()
	if err := tm.AccPct(id, PctBase/2); err != nil {
		t.Fatalf("AccPct failed: %v", err)
	}
	accDeadline := tm.Find(id).Deadline()
	if !accDeadline.Before(before) {
		t.Fatalf("AccPct deadline = %v, should before %v", accDeadline, before)
	}
	if !tm.Find(id).StartAt().Equal(startBefore) {
		t.Fatalf("StartAt changed from %v to %v", startBefore, tm.Find(id).StartAt())
	}
	if err := tm.DelayPct(id, PctBase/2); err != nil {
		t.Fatalf("DelayPct failed: %v", err)
	}
	if !tm.Find(id).Deadline().After(accDeadline) {
		t.Fatalf("DelayPct deadline = %v, should after %v", tm.Find(id).Deadline(), accDeadline)
	}
	tm.Cancel(id)
}

func TestManagerUpdateSetsDeadline(t *testing.T) {
	tm := NewManager(4)
	tm.Run()
	defer tm.Stop()

	tm.Register("update", func(int64, map[string]string) {})
	id := tm.New("update", time.Second)
	if id == 0 {
		t.Fatal("expected timer id")
	}
	timer := tm.Find(id)
	startAt := timer.StartAt()
	newDeadline := time.Now().Add(100 * time.Millisecond)
	tm.Update(id, newDeadline)
	if !timer.Deadline().Equal(newDeadline) {
		t.Fatalf("deadline = %v, want %v", timer.Deadline(), newDeadline)
	}
	if !timer.StartAt().Equal(startAt) {
		t.Fatalf("StartAt changed from %v to %v", startAt, timer.StartAt())
	}
	tm.Cancel(id)
}

func TestManagerMissingTimerErrorsAndNoops(t *testing.T) {
	tm := NewManager(1)
	if err := tm.AccAbs(123456, time.Millisecond); err == nil {
		t.Fatal("AccAbs missing timer should fail")
	}
	if err := tm.DelayPct(123456, 1); err == nil {
		t.Fatal("DelayPct missing timer should fail")
	}
	tm.Update(123456, time.Now())
	tm.Cancel(123456)
}
