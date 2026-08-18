package xhive

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xmapst/xhive/chanrpc"
)

type skeletonRPCReq struct {
	Value string
}

type skeletonRPCAck struct {
	Value string
}

type skeletonTestModule struct {
	*Skeleton
	onInit    func(*skeletonTestModule) error
	onDestroy func()
}

func newSkeletonTestModule(name string) *skeletonTestModule {
	return &skeletonTestModule{Skeleton: NewSkeleton(name, WithTimerChanLen(8), WithServerChanLen(8), WithClientChanLen(8), WithStatCap(32))}
}

func (m *skeletonTestModule) OnInit() error {
	if m.onInit != nil {
		return m.onInit(m)
	}
	return nil
}

func (m *skeletonTestModule) OnDestroy() {
	if m.onDestroy != nil {
		m.onDestroy()
	}
}

func TestSkeletonRegisterChanRPCAndRPCWrappers(t *testing.T) {
	oldDefault := defaultApp
	defaultApp = newApp()
	defer func() {
		defaultApp.stop()
		defaultApp = oldDefault
	}()

	server := newSkeletonTestModule("server")
	castSeen := make(chan string, 1)
	server.onInit = func(m *skeletonTestModule) error {
		return m.RegisterChanRPC(skeletonRPCReq{}, func(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
			req := ci.Request.(skeletonRPCReq)
			if req.Value == "cast" {
				castSeen <- req.Value
				return nil
			}
			return &chanrpc.RetInfo{Ack: skeletonRPCAck{Value: req.Value + "-ack"}}
		})
	}
	if !defaultApp.start(server) {
		t.Fatal("default app start should succeed")
	}

	caller := NewSkeleton("caller", WithTimerChanLen(8), WithClientChanLen(8), WithServerChanLen(8), WithStatCap(32))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		caller.Serve(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		waitClosed(t, done, time.Second)
	}()

	ret := caller.Call("server", skeletonRPCReq{Value: "sync"}, chanrpc.WithMeta("trace", "sync-trace"))
	if ret.Err != nil {
		t.Fatalf("Skeleton Call err = %v", ret.Err)
	}
	if ack := ret.Ack.(skeletonRPCAck); ack.Value != "sync-ack" {
		t.Fatalf("Skeleton Call ack = %#v", ret.Ack)
	}
	if ret.Metadata["trace"] != "sync-trace" {
		t.Fatalf("Skeleton Call metadata = %#v", ret.Metadata)
	}

	asyncDone := make(chan *chanrpc.RetInfo, 1)
	if err := caller.AsyncCall("server", skeletonRPCReq{Value: "async"}, func(ri *chanrpc.RetInfo) {
		asyncDone <- ri
	}, chanrpc.WithMeta("trace", "async-trace")); err != nil {
		t.Fatalf("Skeleton AsyncCall failed: %v", err)
	}
	select {
	case ri := <-asyncDone:
		if ri.Err != nil {
			t.Fatalf("Skeleton AsyncCall err = %v", ri.Err)
		}
		if ack := ri.Ack.(skeletonRPCAck); ack.Value != "async-ack" {
			t.Fatalf("Skeleton AsyncCall ack = %#v", ri.Ack)
		}
		if ri.Metadata["trace"] != "async-trace" {
			t.Fatalf("Skeleton AsyncCall metadata = %#v", ri.Metadata)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting skeleton async callback")
	}

	caller.Cast("server", skeletonRPCReq{Value: "cast"})
	select {
	case got := <-castSeen:
		if got != "cast" {
			t.Fatalf("cast value = %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting skeleton cast")
	}
}

// runReq 把一个闭包投递进 Skeleton 自己的事件循环执行。
//
// timer.Manager 的文档约定很明确："所有方法均在调用方的单一 goroutine 中
// 执行（Skeleton 事件循环），无并发竞争，无需加锁"——tm.timers 是一个裸
// map，没有锁保护。直接从测试的 goroutine 调用 s.NewTimer/s.CancelTimer
// 等方法，同时 Serve 的 goroutine 又在通过 commonCb 处理到期定时器（同样
// 读写 tm.timers），两个 goroutine 并发touch 同一个 map，会触发 Go 运行时
// 的 fatal error: concurrent map writes（曾经在本文件复现过一次，间歇性，
// 取决于两个 goroutine 的调度时序）。runReq 通过 chanrpc 把所有定时器操作
// 转发到 Serve 的 goroutine 上执行，让测试代码遵守这条单 goroutine 约定，
// 而不是去改 timer.Manager 本身加锁——生产代码里所有定时器方法调用本来就
// 已经在 Serve 的 goroutine 上（chanrpc handler / timer 回调内部），只有
// 这里的测试写法违反了约定。
type runReq struct{ fn func() }

func TestSkeletonTimerWrappersAndStat(t *testing.T) {
	s := NewSkeleton("timer-wrapper", WithTimerChanLen(8), WithStatCap(8))
	if err := s.RegisterChanRPC(runReq{}, func(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
		ci.Request.(runReq).fn()
		return &chanrpc.RetInfo{Ack: true}
	}); err != nil {
		t.Fatalf("RegisterChanRPC failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Serve(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		waitClosed(t, done, time.Second)
	}()

	client := chanrpc.NewClient(chanrpc.WithClientInitialCapacity(4))
	run := func(fn func()) {
		t.Helper()
		if ret := client.Call(s.ChanRPC(), runReq{fn: fn}); ret.Err != nil {
			t.Fatalf("run: %v", ret.Err)
		}
	}

	fired := make(chan struct{}, 1)
	var id int64
	run(func() {
		s.RegisterTimer("adjustable", func(int64, map[string]string) { fired <- struct{}{} })
		id = s.NewTimer("adjustable", 200*time.Millisecond)
	})
	run(func() {
		if err := s.AccAbsTimer(id, 150*time.Millisecond); err != nil {
			t.Errorf("AccAbsTimer failed: %v", err)
		}
	})
	select {
	case <-fired:
	case <-time.After(time.Second):
		t.Fatal("timer should fire after acceleration")
	}

	run(func() {
		id = s.NewTimer("adjustable", 30*time.Millisecond)
		_ = s.DelayAbsTimer(id, 50*time.Millisecond)
		_ = s.DelayPctTimer(id, 100)
		_ = s.AccPctTimer(id, 100)
		s.UpdateTimer(id, time.Now().Add(time.Hour))
		s.CancelTimer(id)
	})
	select {
	case <-fired:
		t.Fatal("cancelled timer should not fire")
	case <-time.After(80 * time.Millisecond):
	}

	s.recordStat("manual", 12)
	if dump := s.DumpStat(10); !strings.Contains(dump, "manual") {
		t.Fatalf("DumpStat missing manual entry: %s", dump)
	}
}

func TestSkeletonOptionsIgnoreNonPositiveValues(t *testing.T) {
	s := NewSkeleton("defaults", WithTimerChanLen(0), WithServerChanLen(-1), WithClientChanLen(0), WithStatCap(-1))
	if s.Name() != "defaults" || s.timer == nil || s.server == nil || s.client == nil || s.stat == nil {
		t.Fatalf("skeleton not initialized correctly: %#v", s)
	}
	s.recordStat("x", 1)
}
