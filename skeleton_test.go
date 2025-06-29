package xhive

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xmapst/xhive/chanrpc"
)

// skMsg 是 Skeleton 测试专用的本地 RPC 消息类型，避免依赖其他包的导出类型。
type skMsg struct {
	Value string
}

// skModule 将 Skeleton 补齐为完整 IModule，便于注册到 defaultApp 进行跨模块 RPC 测试。
type skModule struct {
	*Skeleton
}

func (m *skModule) OnInit() error { return nil }
func (m *skModule) OnDestroy()    {}

// runSkeleton 在后台启动 Skeleton 的事件循环，返回 cancel 与等待退出的 channel。
func runSkeleton(s *Skeleton) (context.CancelFunc, <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.OnRun(ctx)
		close(done)
	}()
	return cancel, done
}

// TestSkeletonOnRunProcessesRPCAndTimer 启动 OnRun 事件循环，验证它能消费
// server.ChanCall（RPC 调用）与 timer.Event（定时器），并在 ctx 取消后干净退出（覆盖 close）。
//
// 注意：timer.Mgr 设计为仅在 OnRun 所在 goroutine 内无锁访问，因此定时器的创建必须
// 发生在事件循环内部——本测试在 RPC handler（由 OnRun goroutine 执行）中调用 NewTimer，
// 避免跨 goroutine 访问 Mgr 内部 map 触发数据竞争。
func TestSkeletonOnRunProcessesRPCAndTimer(t *testing.T) {
	s := NewSkeleton("sk-self")
	mod := &skModule{Skeleton: s}
	if err := defaultApp.Register(mod); err != nil {
		t.Fatalf("register self module error = %v", err)
	}

	timerHit := make(chan int64, 1)
	s.RegisterTimer("evt", func(id int64, _ map[string]string) {
		timerHit <- id
	})

	rpcHit := make(chan string, 1)
	if err := s.RegisterChanRPC(skMsg{}, func(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
		rpcHit <- ci.Request.(skMsg).Value
		// 在 OnRun goroutine 内创建定时器，符合 timer.Mgr 的单 goroutine 访问约定。
		s.NewTimer("evt", 8*time.Millisecond)
		return &chanrpc.RetInfo{Ack: skMsg{Value: "ack"}}
	}); err != nil {
		t.Fatalf("RegisterChanRPC error = %v", err)
	}

	cancel, done := runSkeleton(s)
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("OnRun did not exit after cancel")
		}
	}()

	// 通过 Cast 把消息投递给自身模块，OnRun 应消费并执行 handler。
	s.Cast("sk-self", skMsg{Value: "hello"})
	select {
	case v := <-rpcHit:
		if v != "hello" {
			t.Fatalf("rpc handler got %q, want hello", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("rpc handler not invoked via OnRun")
	}

	// handler 内创建的定时器到期后，OnRun 应消费 timer.Event 并触发 timer handler。
	select {
	case <-timerHit:
	case <-time.After(2 * time.Second):
		t.Fatal("timer handler not invoked via OnRun")
	}
}

// TestSkeletonSyncCall 验证 Call 同步调用：目标模块运行事件循环并回包。
func TestSkeletonSyncCall(t *testing.T) {
	target := NewSkeleton("sk-call-target")
	mod := &skModule{Skeleton: target}
	if err := defaultApp.Register(mod); err != nil {
		t.Fatalf("register target error = %v", err)
	}
	if err := target.RegisterChanRPC(skMsg{}, func(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
		return &chanrpc.RetInfo{Ack: skMsg{Value: ci.Request.(skMsg).Value + "-done"}}
	}); err != nil {
		t.Fatalf("RegisterChanRPC error = %v", err)
	}
	cancel, done := runSkeleton(target)
	defer func() { cancel(); <-done }()

	caller := NewSkeleton("sk-caller")
	ret := caller.Call("sk-call-target", skMsg{Value: "sync"})
	if ret.Err != nil {
		t.Fatalf("Call() err = %v", ret.Err)
	}
	if got := ret.Ack.(skMsg).Value; got != "sync-done" {
		t.Fatalf("Call() ack = %q, want sync-done", got)
	}
}

// TestSkeletonAsyncCall 验证 AsyncCall：调用方运行事件循环消费 ChanAsyncRet 并执行回调。
func TestSkeletonAsyncCall(t *testing.T) {
	target := NewSkeleton("sk-async-target")
	if err := defaultApp.Register(&skModule{Skeleton: target}); err != nil {
		t.Fatalf("register target error = %v", err)
	}
	if err := target.RegisterChanRPC(skMsg{}, func(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
		return &chanrpc.RetInfo{Ack: skMsg{Value: ci.Request.(skMsg).Value + "-async"}}
	}); err != nil {
		t.Fatalf("RegisterChanRPC error = %v", err)
	}
	tCancel, tDone := runSkeleton(target)
	defer func() { tCancel(); <-tDone }()

	caller := NewSkeleton("sk-async-caller")
	cCancel, cDone := runSkeleton(caller)
	defer func() { cCancel(); <-cDone }()

	cbHit := make(chan string, 1)
	if err := caller.AsyncCall("sk-async-target", skMsg{Value: "go"}, func(ri *chanrpc.RetInfo) {
		if ri.Err != nil {
			cbHit <- "err:" + ri.Err.Error()
			return
		}
		cbHit <- ri.Ack.(skMsg).Value
	}); err != nil {
		t.Fatalf("AsyncCall() error = %v", err)
	}
	select {
	case got := <-cbHit:
		if got != "go-async" {
			t.Fatalf("async callback got %q, want go-async", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("async callback not invoked")
	}
}

// TestSkeletonRPCToMissingModule 覆盖目标模块不存在时三种调用的降级行为。
func TestSkeletonRPCToMissingModule(t *testing.T) {
	s := NewSkeleton("sk-missing-caller")

	ret := s.Call("no-such-module", skMsg{})
	if !errors.Is(ret.Err, chanrpc.ErrServerNil) {
		t.Fatalf("Call(missing) err = %v, want ErrServerNil", ret.Err)
	}

	err := s.AsyncCall("no-such-module", skMsg{}, func(*chanrpc.RetInfo) {})
	if !errors.Is(err, chanrpc.ErrServerNil) {
		t.Fatalf("AsyncCall(missing) err = %v, want ErrServerNil", err)
	}

	// Cast 对不存在模块应静默丢弃，不 panic。
	s.Cast("no-such-module", skMsg{})
}

// TestSkeletonTimerForwarders 覆盖 AccAbsTimer/AccPctTimer/DelayAbsTimer/DelayPctTimer/UpdateTimer/CancelTimer 的转发。
func TestSkeletonTimerForwarders(t *testing.T) {
	s := NewSkeleton("sk-timer-fwd")
	s.RegisterTimer("t", func(int64, map[string]string) {})
	s.timer.Run()
	defer s.timer.Stop()

	id := s.NewTimer("t", 5*time.Second)
	if id == 0 {
		t.Fatal("NewTimer() = 0")
	}
	if err := s.AccAbsTimer(id, 100*time.Millisecond); err != nil {
		t.Fatalf("AccAbsTimer() error = %v", err)
	}
	if err := s.DelayAbsTimer(id, 100*time.Millisecond); err != nil {
		t.Fatalf("DelayAbsTimer() error = %v", err)
	}
	if err := s.AccPctTimer(id, 1000); err != nil {
		t.Fatalf("AccPctTimer() error = %v", err)
	}
	if err := s.DelayPctTimer(id, 1000); err != nil {
		t.Fatalf("DelayPctTimer() error = %v", err)
	}
	s.UpdateTimer(id, time.Now().Add(9*time.Second))
	s.CancelTimer(id) // 幂等，再次取消安全。
	s.CancelTimer(id)

	// 对不存在定时器的转发应返回错误 / 安全无操作。
	if err := s.AccAbsTimer(999999, time.Millisecond); err == nil {
		t.Fatal("AccAbsTimer(missing) err = nil, want non-nil")
	}
	if err := s.DelayAbsTimer(999999, time.Millisecond); err == nil {
		t.Fatal("DelayAbsTimer(missing) err = nil, want non-nil")
	}
	if err := s.AccPctTimer(999999, 1); err == nil {
		t.Fatal("AccPctTimer(missing) err = nil, want non-nil")
	}
	if err := s.DelayPctTimer(999999, 1); err == nil {
		t.Fatal("DelayPctTimer(missing) err = nil, want non-nil")
	}
	s.UpdateTimer(999999, time.Now())
}

// TestSkeletonScheduleDumpTimer 直接覆盖 scheduleDumpTimer 的整点计算与抖动逻辑。
func TestSkeletonScheduleDumpTimer(t *testing.T) {
	s := NewSkeleton("sk-dump")
	s.RegisterTimer(timerKindDumpStat, func(int64, map[string]string) {})
	s.timer.Run()
	defer s.timer.Stop()

	s.scheduleDumpTimer()
	// 应当创建了一个 dump 统计定时器。
	if got := s.timer.FindByName(timerKindDumpStat); got == nil {
		t.Fatal("scheduleDumpTimer did not create a dump timer")
	}
}

// TestSkeletonRecordStatIgnoresNilName 验证 recordStat 在 stat 存在时正常记录。
func TestSkeletonRecordStatIgnoresNilName(t *testing.T) {
	s := NewSkeleton("sk-stat")
	var count atomic.Int32
	count.Add(1)
	s.recordStat("k", 5)
	if dump := s.DumpStat(10); dump == "" {
		t.Fatal("DumpStat() = empty after recordStat")
	}
}
