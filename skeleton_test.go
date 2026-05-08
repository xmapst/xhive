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

func TestSkeletonTimerWrappersAndStat(t *testing.T) {
	s := NewSkeleton("timer-wrapper", WithTimerChanLen(8), WithStatCap(8))
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

	fired := make(chan struct{}, 1)
	s.RegisterTimer("adjustable", func(int64, map[string]string) { fired <- struct{}{} })
	id := s.NewTimer("adjustable", 200*time.Millisecond)
	if err := s.AccAbsTimer(id, 150*time.Millisecond); err != nil {
		t.Fatalf("AccAbsTimer failed: %v", err)
	}
	select {
	case <-fired:
	case <-time.After(time.Second):
		t.Fatal("timer should fire after acceleration")
	}

	id = s.NewTimer("adjustable", 30*time.Millisecond)
	s.DelayAbsTimer(id, 50*time.Millisecond)
	s.DelayPctTimer(id, 100)
	s.AccPctTimer(id, 100)
	s.UpdateTimer(id, time.Now().Add(time.Hour))
	s.CancelTimer(id)
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
