package timer

import (
    "testing"
    "time"
)

func TestTimerConstants(t *testing.T) {
    if SecMs != 1000 || MinMs != 60*SecMs || HourMs != 60*MinMs || DayMs != 24*HourMs {
        t.Fatalf("time constants mismatch")
    }
}

func TestMgrNewFindAndCancel(t *testing.T) {
    mgr := NewMgr(8)
    mgr.Register("once", func(_ int64, _ map[string]string) {})
    mgr.Run()
    defer mgr.Stop()
    
    id := mgr.New("once", 50, WithMetadata(map[string]string{"k": "v"}))
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
    mgr := NewMgr(8)
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
    
    id := mgr.New("tick", 10, WithTicker(), WithMetadata(map[string]string{"mode": "ticker"}))
    if id == 0 {
        t.Fatal("New() returned 0")
    }
    for i := 0; i < 2; i++ {
        select {
        case ev := <-mgr.Event():
            ev.Cb()
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

func TestMgrAccDelayUpdateAndCalcNewRemain(t *testing.T) {
    mgr := NewMgr(8)
    mgr.Register("once", func(_ int64, _ map[string]string) {})
    mgr.Run()
    defer mgr.Stop()
    
    id := mgr.New("once", 100)
    if id == 0 {
        t.Fatal("New() returned 0")
    }
    before := mgr.Find(id).EndTs()
    if err := mgr.Acc(id, AccAbs, 10); err != nil {
        t.Fatalf("Acc() error = %v", err)
    }
    afterAcc := mgr.Find(id).EndTs()
    if afterAcc > before {
        t.Fatalf("Acc() endTs = %d, want <= %d", afterAcc, before)
    }
    if err := mgr.Delay(id, AccAbs, 20); err != nil {
        t.Fatalf("Delay() error = %v", err)
    }
    afterDelay := mgr.Find(id).EndTs()
    if afterDelay < afterAcc {
        t.Fatalf("Delay() endTs = %d, want >= %d", afterDelay, afterAcc)
    }
    mgr.Update(id, time.Now().UnixMilli()+5)
    
    if _, err := mgr.calcNewRemain(100, AccAbs, 0, true); err == nil {
        t.Fatal("calcNewRemain invalid abs should error")
    }
    if got, err := mgr.calcNewRemain(100, AccAbs, 10, true); err != nil || got != 90 {
        t.Fatalf("calcNewRemain AccAbs acc = (%d,%v), want (90,nil)", got, err)
    }
    if got, err := mgr.calcNewRemain(100, AccAbs, 10, false); err != nil || got != 110 {
        t.Fatalf("calcNewRemain AccAbs delay = (%d,%v), want (110,nil)", got, err)
    }
    if got, err := mgr.calcNewRemain(100, AccPct, 1000, true); err != nil || got != 90 {
        t.Fatalf("calcNewRemain AccPct acc = (%d,%v), want (90,nil)", got, err)
    }
    if got, err := mgr.calcNewRemain(100, AccPct, 1000, false); err != nil || got != 110 {
        t.Fatalf("calcNewRemain AccPct delay = (%d,%v), want (110,nil)", got, err)
    }
    if _, err := mgr.calcNewRemain(100, AccPct, PctBase+1, true); err == nil {
        t.Fatal("calcNewRemain invalid pct should error")
    }
    if _, err := mgr.calcNewRemain(100, AccKind(99), 1, true); err == nil {
        t.Fatal("calcNewRemain unknown kind should error")
    }
    
    if err := mgr.Acc(999, AccAbs, 1); err == nil {
        t.Fatal("Acc(missing) error = nil, want non-nil")
    }
    if err := mgr.Delay(999, AccAbs, 1); err == nil {
        t.Fatal("Delay(missing) error = nil, want non-nil")
    }
}

func TestMgrNewFailureBranches(t *testing.T) {
    mgr := NewMgr(8)
    if got := mgr.New("missing", 10); got != 0 {
        t.Fatalf("New(missing handler) = %d, want 0", got)
    }
    mgr.Register("once", func(_ int64, _ map[string]string) {})
    mgr.Run()
    defer mgr.Stop()
    if got := mgr.New("once", 10, WithID(7)); got != 7 {
        t.Fatalf("New(with id) = %d, want 7", got)
    }
    if got := mgr.New("once", 10, WithID(7)); got != 0 {
        t.Fatalf("New(duplicate id) = %d, want 0", got)
    }
}

func TestDispatcherPlaceTriggerCancelAndStop(t *testing.T) {
    disp := newDispatcher(4)
    hit := make(chan int64, 1)
    late := &dispatcherTimer{name: "late", id: 1, endTs: time.Now().Add(10 * time.Millisecond).UnixMilli(), cb: func(id int64) { hit <- id }}
    disp.place(late)
    found := false
    for _, slot := range disp.timerSlots {
        if _, ok := slot[1]; ok {
            found = true
            break
        }
    }
    if !found {
        t.Fatal("place() did not store timer in any slot")
    }
    
    ready := &dispatcherTimer{name: "ready", id: 2, endTs: time.Now().Add(-time.Millisecond).UnixMilli(), cb: func(id int64) { hit <- id }}
    disp.place(ready)
    select {
    case ev := <-disp.chanFired:
        ev.Cb()
    case <-time.After(2 * time.Second):
        t.Fatal("ready timer not fired")
    }
    select {
    case id := <-hit:
        if id != 2 {
            t.Fatalf("fired id = %d, want 2", id)
        }
    case <-time.After(2 * time.Second):
        t.Fatal("callback not executed")
    }
    
    disp.Cancel("late", 1)
    if _, ok := disp.canceledTimers.Load(int64(1)); !ok {
        t.Fatal("Cancel() did not mark canceled timer")
    }
    if got := disp.delete(1); got == nil {
        t.Fatal("delete() = nil, want timer")
    }
    if got := disp.delete(999); got != nil {
        t.Fatalf("delete(missing) = %#v, want nil", got)
    }
    
    if !disp.doOp(&dispatcherTimer{name: "new", id: 3, endTs: time.Now().Add(time.Second).UnixMilli(), cb: func(int64) {}}) {
        t.Fatal("doOp(new) = false, want true")
    }
    if !disp.doOp(&dispatcherTimer{name: "update", id: 3, endTs: time.Now().Add(2 * time.Second).UnixMilli()}) {
        t.Fatal("doOp(update) = false, want true")
    }
    if !disp.doOp(&dispatcherTimer{name: "cancel", id: 3, endTs: 0}) {
        t.Fatal("doOp(cancel) = false, want true")
    }
    if disp.doOp(&dispatcherTimer{name: "stop", id: 0, endTs: 1}) {
        t.Fatal("doOp(stop) = true, want false")
    }
}

func TestDispatcherTickUpdateNewAndPanicRecover(t *testing.T) {
    disp := newDispatcher(4)
    id := disp.New("n", 0, time.Now().Add(20*time.Millisecond).UnixMilli(), func(int64) {})
    if id == 0 {
        t.Fatal("New() generated id 0")
    }
    disp.Update("n", id, time.Now().Add(10*time.Millisecond).UnixMilli())
    if got := disp.doTick(time.Now().Add(20*time.Millisecond), time.Now().UnixMilli()/timerTick-1); got == 0 {
        t.Fatal("doTick() returned 0, want non-zero tick")
    }
    
    panicTimer := &dispatcherTimer{name: "panic", id: 9, cb: func(int64) { panic("boom") }}
    panicTimer.Cb()
    if panicTimer.cb != nil {
        t.Fatal("Cb() should nil out callback")
    }
    if panicTimer.Name() != "panic" {
        t.Fatalf("Name() = %q, want panic", panicTimer.Name())
    }
}

// TestTimerAccessors 覆盖 Timer 的 StartTs/EndTs 等访问器以及 RangeMetadata 的提前终止分支。
func TestTimerAccessors(t *testing.T) {
    mgr := NewMgr(8)
    mgr.Register("acc", func(_ int64, _ map[string]string) {})
    mgr.Run()
    defer mgr.Stop()
    
    id := mgr.New("acc", 1000, WithMetadata(map[string]string{"a": "1", "b": "2"}))
    tm := mgr.Find(id)
    if tm == nil {
        t.Fatal("Find() = nil")
    }
    if tm.StartTs() == 0 {
        t.Fatal("StartTs() = 0, want non-zero")
    }
    if tm.EndTs() != tm.StartTs()+1000 {
        t.Fatalf("EndTs() = %d, want StartTs()+1000 = %d", tm.EndTs(), tm.StartTs()+1000)
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
    mgr := NewMgr(8)
    if got := mgr.FindByName("does-not-exist"); got != nil {
        t.Fatalf("FindByName(miss) = %#v, want nil", got)
    }
}

// TestUpdateMissingTimer 覆盖 Mgr.Update 在定时器不存在时的提前返回分支。
func TestUpdateMissingTimer(t *testing.T) {
    mgr := NewMgr(8)
    mgr.Run()
    defer mgr.Stop()
    // 不应 panic，且无副作用。
    mgr.Update(123456, time.Now().UnixMilli()+1000)
}

// TestCommonCbHandlerMissing 覆盖 commonCb 在 handler 缺失时记录错误并返回的分支。
// 通过手动构造一个 timers 中存在、但 handlers 中无对应 name 的定时器触发。
func TestCommonCbHandlerMissing(t *testing.T) {
    mgr := NewMgr(8)
    mgr.timers[1] = &Timer{
        id:    1,
        name:  "no-handler",
        endTs: time.Now().UnixMilli() - 10,
    }
    // 不应 panic；handler 不存在时仅记录日志后返回。
    mgr.commonCb(1)
}

// TestCommonCbTimerMissing 覆盖 commonCb 在定时器元数据不存在时的提前返回分支。
func TestCommonCbTimerMissing(t *testing.T) {
    mgr := NewMgr(8)
    mgr.commonCb(99999) // timers 中无此 ID，应安全返回。
}

// TestCommonCbTickerReschedule 覆盖 Ticker 在 commonCb 中的自动续期分支（endTs/startTs 更新）。
func TestCommonCbTickerReschedule(t *testing.T) {
    mgr := NewMgr(8)
    mgr.Register("tick", func(_ int64, _ map[string]string) {})
    mgr.Run()
    defer mgr.Stop()
    
    id := mgr.New("tick", 10, WithTicker())
    tm := mgr.Find(id)
    if tm == nil {
        t.Fatal("Find() = nil")
    }
    oldStart := tm.startTs
    oldEnd := tm.endTs
    
    mgr.commonCb(id) // 直接触发一次续期。
    
    tm2 := mgr.Find(id)
    if tm2 == nil {
        t.Fatal("ticker should still exist after reschedule")
    }
    if tm2.startTs != oldEnd {
        t.Fatalf("after reschedule startTs = %d, want old endTs %d", tm2.startTs, oldEnd)
    }
    if tm2.endTs != oldEnd+(oldEnd-oldStart) {
        t.Fatalf("after reschedule endTs = %d, want %d", tm2.endTs, oldEnd+(oldEnd-oldStart))
    }
}

// TestDispatcherTriggerDemotesAndCancels 覆盖 trigger 的高层级降级路径与取消过滤路径。
func TestDispatcherTriggerDemotesAndCancels(t *testing.T) {
    disp := newDispatcher(8)
    now := time.Now().UnixMilli()
    
    // 放置一个剩余时间较长的定时器，使其落在较高层级，随后 trigger 时被降级。
    high := &dispatcherTimer{name: "high", id: 1, endTs: now + 100, cb: func(int64) {}}
    disp.place(high)
    
    // 找到 high 所在层级。
    level := -1
    for i := range disp.timerSlots {
        if _, ok := disp.timerSlots[i][1]; ok {
            level = i
            break
        }
    }
    if level <= 0 {
        t.Fatalf("high timer placed at level %d, want > 0 for demotion test", level)
    }
    
    // 用一个远大于其剩余时间跨度的 nowMs 调用 trigger，触发降级到 level-1。
    disp.trigger(now+90, level)
    if _, ok := disp.timerSlots[level][1]; ok {
        t.Fatal("timer not removed from original level after trigger")
    }
    if _, ok := disp.timerSlots[level-1][1]; !ok {
        t.Fatal("timer not demoted to lower level")
    }
    
    // 取消过滤路径：标记取消后 trigger 应将其从槽位删除。
    canceled := &dispatcherTimer{name: "cancel", id: 2, endTs: now + 100, cb: func(int64) {}}
    disp.place(canceled)
    disp.canceledTimers.Store(int64(2), struct{}{})
    for i := range disp.timerSlots {
        if _, ok := disp.timerSlots[i][2]; ok {
            disp.trigger(now, i)
        }
    }
    for i := range disp.timerSlots {
        if _, ok := disp.timerSlots[i][2]; ok {
            t.Fatal("canceled timer not removed by trigger")
        }
    }
}

// TestDispatcherPlaceCanceledSkips 覆盖 place 在定时器已被取消时直接返回的分支。
func TestDispatcherPlaceCanceledSkips(t *testing.T) {
    disp := newDispatcher(8)
    disp.canceledTimers.Store(int64(5), struct{}{})
    disp.place(&dispatcherTimer{id: 5, endTs: time.Now().UnixMilli() + 1000, cb: func(int64) {}})
    for i := range disp.timerSlots {
        if _, ok := disp.timerSlots[i][5]; ok {
            t.Fatal("canceled timer should not be placed into any slot")
        }
    }
}

// TestDispatcherDoOpUpdateMissing 覆盖 doOp 更新一个不存在定时器时的错误日志分支。
func TestDispatcherDoOpUpdateMissing(t *testing.T) {
    disp := newDispatcher(8)
    // endTs != 0 且 cb == nil 为更新操作；id 不在轮中走 oldt == nil 分支。
    if !disp.doOp(&dispatcherTimer{name: "u", id: 42, endTs: time.Now().UnixMilli() + 1000}) {
        t.Fatal("doOp(update missing) = false, want true")
    }
}

// TestDispatcherDoOpCanceledThenRebuild 覆盖 doOp 在已取消标记存在时忽略新建/更新的竞态保护分支。
func TestDispatcherDoOpCanceledThenRebuild(t *testing.T) {
    disp := newDispatcher(8)
    disp.canceledTimers.Store(int64(7), struct{}{})
    // 已取消标记存在时，新建操作应被忽略（返回 true 但不放入槽位）。
    if !disp.doOp(&dispatcherTimer{name: "n", id: 7, endTs: time.Now().UnixMilli() + 1000, cb: func(int64) {}}) {
        t.Fatal("doOp = false, want true")
    }
    for i := range disp.timerSlots {
        if _, ok := disp.timerSlots[i][7]; ok {
            t.Fatal("canceled timer should not be rebuilt while cancel mark present")
        }
    }
}

// TestNewDispatcherDefaultLen 覆盖 newDispatcher 在 l<=0 时使用默认容量的分支。
func TestNewDispatcherDefaultLen(t *testing.T) {
    disp := newDispatcher(0)
    if cap(disp.chanOp) != 10000 || cap(disp.chanFired) != 10000 {
        t.Fatalf("default cap = (%d,%d), want (10000,10000)", cap(disp.chanOp), cap(disp.chanFired))
    }
    disp2 := newDispatcher(-5)
    if cap(disp2.chanOp) != 10000 {
        t.Fatalf("negative len cap = %d, want 10000", cap(disp2.chanOp))
    }
}

// TestDispatcherFullEndToEnd 通过运行中的分发器走完整 New→tick→fire 路径，覆盖 run 主循环。
func TestDispatcherFullEndToEnd(t *testing.T) {
    disp := newDispatcher(8)
    disp.Run()
    defer disp.Stop()
    
    fired := make(chan int64, 1)
    id := disp.New("e2e", 0, time.Now().Add(12*time.Millisecond).UnixMilli(), func(tid int64) {
        fired <- tid
    })
    
    select {
    case ev := <-disp.chanFired:
        ev.Cb()
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
