package timer

import (
	"testing"
	"time"
)

func TestMgrNewFindAndCancel(t *testing.T) {
	mgr := NewManager(8)
	mgr.Register("once", func(_ int64, _ map[string]string) {})
	mgr.Run()
	defer mgr.Stop()

	id := mgr.New("once", 50*time.Millisecond, WithMetadata(map[string]string{"k": "v"}))
	if id == 0 {
		t.Fatal("New() returned 0")
	}
	tm := mgr.Find(id)
	if tm == nil {
		t.Fatal("Find() = nil")
	}
	if tm.Name() != "once" {
		t.Fatalf("Name() = %q, want %q", tm.Name(), "once")
	}
	if tm.ID() != id {
		t.Fatalf("ID() = %d, want %d", tm.ID(), id)
	}
	if tm.IsTicker() {
		t.Fatal("IsTicker() = true, want false")
	}
	meta := map[string]string{}
	tm.RangeMetadata(func(k, v string) bool {
		meta[k] = v
		return true
	})
	if meta["k"] != "v" {
		t.Fatalf("metadata = %#v, want key k=v", meta)
	}
	if got := mgr.FindByName("once"); got == nil || got.ID() != id {
		t.Fatalf("FindByName() = %#v, want timer id %d", got, id)
	}
	mgr.Cancel(id)
	if mgr.Find(id) != nil {
		t.Fatal("Find() after Cancel != nil")
	}
	mgr.Cancel(id)
	mgr.Cancel(0)
}

func TestMgrTickerCommonCallbackAndEvent(t *testing.T) {
	mgr := NewManager(8)
	count := 0
	fired := make(chan struct{}, 2)
	mgr.Register("tick", func(_ int64, metadata map[string]string) {
		count++
		if metadata["mode"] != "ticker" {
			t.Fatalf("metadata = %#v, want mode=ticker", metadata)
		}
		fired <- struct{}{}
	})
	mgr.Run()
	defer mgr.Stop()

	id := mgr.New("tick", 10*time.Millisecond, WithTicker(), WithMetadata(map[string]string{"mode": "ticker"}))
	if id == 0 {
		t.Fatal("New() returned 0")
	}
	for i := 0; i < 2; i++ {
		select {
		case ev := <-mgr.Event():
			ev.Callback()
		case <-time.After(2 * time.Second):
			t.Fatal("wait Event() timeout")
		}
		select {
		case <-fired:
		case <-time.After(2 * time.Second):
			t.Fatal("wait handler timeout")
		}
	}
	if count < 2 {
		t.Fatalf("count = %d, want >= 2", count)
	}
	mgr.Cancel(id)
}

func TestMgrAccDelayUpdateAndAdjust(t *testing.T) {
	mgr := NewManager(8)
	mgr.Register("once", func(_ int64, _ map[string]string) {})
	mgr.Run()
	defer mgr.Stop()

	id := mgr.New("once", 100*time.Millisecond)
	if id == 0 {
		t.Fatal("New() returned 0")
	}
	before := mgr.Find(id).Deadline()
	if err := mgr.AccAbs(id, 10*time.Millisecond); err != nil {
		t.Fatalf("AccAbs() error = %v", err)
	}
	afterAcc := mgr.Find(id).Deadline()
	if afterAcc.After(before) {
		t.Fatalf("AccAbs() deadline = %v, want <= %v", afterAcc, before)
	}
	if err := mgr.DelayAbs(id, 20*time.Millisecond); err != nil {
		t.Fatalf("DelayAbs() error = %v", err)
	}
	afterDelay := mgr.Find(id).Deadline()
	if afterDelay.Before(afterAcc) {
		t.Fatalf("DelayAbs() deadline = %v, want >= %v", afterDelay, afterAcc)
	}
	if err := mgr.AccPct(id, 1000); err != nil {
		t.Fatalf("AccPct() error = %v", err)
	}
	if err := mgr.DelayPct(id, 1000); err != nil {
		t.Fatalf("DelayPct() error = %v", err)
	}
	mgr.Update(id, time.Now().Add(5*time.Millisecond))

	// adjustAbs：非法时长、加速、延迟。
	if _, err := adjustAbs(100, 0, true); err == nil {
		t.Fatal("adjustAbs invalid duration should error")
	}
	if got, err := adjustAbs(100, 10, true); err != nil || got != 90 {
		t.Fatalf("adjustAbs acc = (%d,%v), want (90,nil)", got, err)
	}
	if got, err := adjustAbs(100, 10, false); err != nil || got != 110 {
		t.Fatalf("adjustAbs delay = (%d,%v), want (110,nil)", got, err)
	}
	// 加速不允许早于当前时刻：remain-d 为负时归零。
	if got, err := adjustAbs(10, 20, true); err != nil || got != 0 {
		t.Fatalf("adjustAbs acc clamp = (%d,%v), want (0,nil)", got, err)
	}

	// adjustPct：加速、延迟、越界。
	if got, err := adjustPct(100, 1000, true); err != nil || got != 90 {
		t.Fatalf("adjustPct acc = (%d,%v), want (90,nil)", got, err)
	}
	if got, err := adjustPct(100, 1000, false); err != nil || got != 110 {
		t.Fatalf("adjustPct delay = (%d,%v), want (110,nil)", got, err)
	}
	if _, err := adjustPct(100, PctBase+1, true); err == nil {
		t.Fatal("adjustPct over-range should error")
	}
	if _, err := adjustPct(100, 0, true); err == nil {
		t.Fatal("adjustPct zero pct should error")
	}

	// 缺失定时器的错误分支。
	if err := mgr.AccAbs(999, time.Millisecond); err == nil {
		t.Fatal("AccAbs(missing) error = nil, want non-nil")
	}
	if err := mgr.DelayAbs(999, time.Millisecond); err == nil {
		t.Fatal("DelayAbs(missing) error = nil, want non-nil")
	}
	if err := mgr.AccPct(999, 1); err == nil {
		t.Fatal("AccPct(missing) error = nil, want non-nil")
	}
	if err := mgr.DelayPct(999, 1); err == nil {
		t.Fatal("DelayPct(missing) error = nil, want non-nil")
	}
}

func TestMgrNewFailureBranches(t *testing.T) {
	mgr := NewManager(8)
	if got := mgr.New("missing", 10*time.Millisecond); got != 0 {
		t.Fatalf("New(missing handler) = %d, want 0", got)
	}
	mgr.Register("once", func(_ int64, _ map[string]string) {})
	mgr.Run()
	defer mgr.Stop()
	if got := mgr.New("once", 10*time.Millisecond, WithID(7)); got != 7 {
		t.Fatalf("New(with id) = %d, want 7", got)
	}
	if got := mgr.New("once", 10*time.Millisecond, WithID(7)); got != 0 {
		t.Fatalf("New(duplicate id) = %d, want 0", got)
	}
}

// TestDispatcherNewFireCancelAndStop 覆盖 New→堆→fire→chanFired 路径、Cancel 的事件过滤、
// 同 ID 重建，以及 Stop 幂等。
func TestDispatcherNewFireCancelAndStop(t *testing.T) {
	disp := newDispatcher(4)
	disp.Run()
	hit := make(chan int64, 2)

	// 立即到期：派发循环应将其投递为 Event，消费后回调执行。
	id := disp.New("ready", 0, time.Now(), func(tid int64) { hit <- tid })
	select {
	case ev := <-disp.chanFired:
		ev.Callback()
	case <-time.After(2 * time.Second):
		t.Fatal("ready timer not fired")
	}
	select {
	case got := <-hit:
		if got != id {
			t.Fatalf("fired id = %d, want %d", got, id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("callback not executed")
	}

	// New 同步登记句柄表；Cancel 同步置取消标记，使已排队事件在 Callback 中被过滤。
	id2 := disp.New("late", 0, time.Now().Add(time.Hour), func(int64) {})
	disp.mu.Lock()
	dt := disp.entries[id2]
	disp.mu.Unlock()
	if dt == nil {
		t.Fatal("New() did not register entry in handle table")
	}
	disp.Cancel(id2)
	if !dt.canceled.Load() {
		t.Fatal("Cancel() did not set canceled flag")
	}
	dt.callback = func(int64) { t.Fatal("canceled timer callback must not run") }
	dt.Callback() // 已取消，应被过滤，不触发回调

	// 同 ID 重建：旧节点被标记取消，句柄表同步替换为新节点。
	id3 := disp.New("dup", 0, time.Now().Add(time.Hour), func(int64) {})
	disp.mu.Lock()
	old := disp.entries[id3]
	disp.mu.Unlock()
	_ = disp.New("dup", id3, time.Now().Add(2*time.Hour), func(int64) {})
	disp.mu.Lock()
	cur := disp.entries[id3]
	disp.mu.Unlock()
	if cur == old {
		t.Fatal("rebuild with same id did not replace entry in handle table")
	}
	if !old.canceled.Load() {
		t.Fatal("rebuild did not mark old entry canceled")
	}

	disp.Stop()
	disp.Stop() // 幂等：重复 Stop 不应 panic
}

// TestDispatcherUpdateAndPanicRecover 覆盖 Update 重置到期时刻、Update 缺失定时器的错误分支，
// 以及 Callback 的 panic 恢复与回调引用释放。
func TestDispatcherUpdateAndPanicRecover(t *testing.T) {
	disp := newDispatcher(4)
	disp.Run()
	defer disp.Stop()

	id := disp.New("n", 0, time.Now().Add(time.Hour), func(int64) {})
	if id == 0 {
		t.Fatal("New() generated id 0")
	}
	disp.Update(id, time.Now().Add(2*time.Hour))   // 命中分支
	disp.Update(999999, time.Now().Add(time.Hour)) // 缺失分支，仅记录日志

	panicTimer := &entry{name: "panic", id: 9, callback: func(int64) { panic("boom") }}
	panicTimer.Callback()
	if panicTimer.callback != nil {
		t.Fatal("Callback() should nil out callback")
	}
	if panicTimer.Name() != "panic" {
		t.Fatalf("Name() = %q, want panic", panicTimer.Name())
	}
}

// TestTimerAccessors 覆盖 Timer 的 StartAt/Deadline 等访问器以及 RangeMetadata 的提前终止分支。
func TestTimerAccessors(t *testing.T) {
	mgr := NewManager(8)
	mgr.Register("acc", func(_ int64, _ map[string]string) {})
	mgr.Run()
	defer mgr.Stop()

	id := mgr.New("acc", time.Second, WithMetadata(map[string]string{"a": "1", "b": "2"}))
	tm := mgr.Find(id)
	if tm == nil {
		t.Fatal("Find() = nil")
	}
	if tm.StartAt().IsZero() {
		t.Fatal("StartAt() is zero, want non-zero")
	}
	if !tm.Deadline().Equal(tm.StartAt().Add(time.Second)) {
		t.Fatalf("Deadline() = %v, want StartAt()+1s = %v", tm.Deadline(), tm.StartAt().Add(time.Second))
	}

	// RangeMetadata 在回调首次返回 false 时应立即终止遍历，visited 至多为 1。
	visited := 0
	tm.RangeMetadata(func(_, _ string) bool {
		visited++
		return false
	})
	if visited != 1 {
		t.Fatalf("RangeMetadata visited = %d, want 1 (early stop)", visited)
	}
}

// TestFindByNameMiss 覆盖 FindByName 未命中返回 nil 的分支。
func TestFindByNameMiss(t *testing.T) {
	mgr := NewManager(8)
	if got := mgr.FindByName("does-not-exist"); got != nil {
		t.Fatalf("FindByName(miss) = %#v, want nil", got)
	}
}

// TestUpdateMissingTimer 覆盖 Manager.Update 在定时器不存在时的提前返回分支。
func TestUpdateMissingTimer(t *testing.T) {
	mgr := NewManager(8)
	mgr.Run()
	defer mgr.Stop()
	// 不应 panic，且无副作用。
	mgr.Update(123456, time.Now().Add(time.Second))
}

// TestCommonCallbackHandlerMissing 覆盖 commonCallback 在 handler 缺失时记录错误并返回的分支。
// 通过手动构造一个 timers 中存在、但 handlers 中无对应 name 的定时器触发。
func TestCommonCallbackHandlerMissing(t *testing.T) {
	mgr := NewManager(8)
	mgr.timers[1] = &Timer{
		id:       1,
		name:     "no-handler",
		deadline: time.Now().Add(-10 * time.Millisecond),
	}
	// 不应 panic；handler 不存在时仅记录日志后返回。
	mgr.commonCallback(1)
}

// TestCommonCallbackTimerMissing 覆盖 commonCallback 在定时器元数据不存在时的提前返回分支。
func TestCommonCallbackTimerMissing(t *testing.T) {
	mgr := NewManager(8)
	mgr.commonCallback(99999) // timers 中无此 ID，应安全返回。
}

// TestCommonCallbackTickerReschedule 覆盖 Ticker 在 commonCallback 中的自动续期分支（deadline/startAt 更新）。
func TestCommonCallbackTickerReschedule(t *testing.T) {
	mgr := NewManager(8)
	mgr.Register("tick", func(_ int64, _ map[string]string) {})
	mgr.Run()
	defer mgr.Stop()

	id := mgr.New("tick", 10*time.Millisecond, WithTicker())
	tm := mgr.Find(id)
	if tm == nil {
		t.Fatal("Find() = nil")
	}
	oldStart := tm.startAt
	oldEnd := tm.deadline

	mgr.commonCallback(id) // 直接触发一次续期。

	tm2 := mgr.Find(id)
	if tm2 == nil {
		t.Fatal("ticker should still exist after reschedule")
	}
	if !tm2.startAt.Equal(oldEnd) {
		t.Fatalf("after reschedule startAt = %v, want old deadline %v", tm2.startAt, oldEnd)
	}
	if !tm2.deadline.Equal(oldEnd.Add(oldEnd.Sub(oldStart))) {
		t.Fatalf("after reschedule deadline = %v, want %v", tm2.deadline, oldEnd.Add(oldEnd.Sub(oldStart)))
	}
}

// TestNewDispatcherDefaultLen 覆盖 newDispatcher 在 l<=0 时使用默认容量的分支。
func TestNewDispatcherDefaultLen(t *testing.T) {
	disp := newDispatcher(0)
	if cap(disp.chanFired) != 10000 {
		t.Fatalf("default cap = %d, want 10000", cap(disp.chanFired))
	}
	disp2 := newDispatcher(-5)
	if cap(disp2.chanFired) != 10000 {
		t.Fatalf("negative len cap = %d, want 10000", cap(disp2.chanFired))
	}
}

// TestDispatcherFullEndToEnd 通过运行中的分发器走完整 New→tick→fire 路径，覆盖 run 主循环。
func TestDispatcherFullEndToEnd(t *testing.T) {
	disp := newDispatcher(8)
	disp.Run()
	defer disp.Stop()

	fired := make(chan int64, 1)
	id := disp.New("e2e", 0, time.Now().Add(12*time.Millisecond), func(tid int64) {
		fired <- tid
	})

	select {
	case ev := <-disp.chanFired:
		ev.Callback()
	case <-time.After(2 * time.Second):
		t.Fatal("dispatcher did not fire timer via run loop")
	}
	select {
	case got := <-fired:
		if got != id {
			t.Fatalf("fired id = %d, want %d", got, id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("callback not executed")
	}
}
