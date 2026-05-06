package chanrpc

import (
	"errors"
	"testing"
	"time"
)

type pingReq struct {
	Value string
}

type pingAck struct {
	Value string
}

type customMsg struct{}

func (customMsg) ID() uint32 { return 424242 }

func waitCallInfo(t *testing.T, s *Server) *CallInfo {
	t.Helper()
	select {
	case ci, ok := <-s.Event():
		if !ok {
			t.Fatal("server event channel closed")
		}
		return ci
	case <-time.After(time.Second):
		t.Fatal("timeout waiting call info")
		return nil
	}
}

func waitRetInfo(t *testing.T, c *Client) *RetInfo {
	t.Helper()
	select {
	case ri, ok := <-c.Event():
		if !ok {
			t.Fatal("client event channel closed")
		}
		return ri
	case <-time.After(time.Second):
		t.Fatal("timeout waiting ret info")
		return nil
	}
}

func TestIDDefaultPointerAndCustom(t *testing.T) {
	id1 := ID(pingReq{})
	id2 := ID(&pingReq{})
	if id1 == 0 {
		t.Fatal("default ID should not be zero")
	}
	if id1 != id2 {
		t.Fatalf("value and pointer ID mismatch: %d != %d", id1, id2)
	}
	if got := ID(customMsg{}); got != 424242 {
		t.Fatalf("custom ID = %d, want 424242", got)
	}
	if got := ID(nil); got != 0 {
		t.Fatalf("nil ID = %d, want 0", got)
	}
}

func TestServerRegisterValidationAndDuplicate(t *testing.T) {
	s := NewServer(4)
	defer s.Close()

	if err := s.Register(nil, func(*CallInfo) *RetInfo { return nil }); !errors.Is(err, ErrRegisterMsgNil) {
		t.Fatalf("register nil msg err = %v", err)
	}
	if err := s.Register(pingReq{}, nil); !errors.Is(err, ErrRegisterHandlerNil) {
		t.Fatalf("register nil handler err = %v", err)
	}
	if err := s.Register(pingReq{}, func(*CallInfo) *RetInfo { return nil }); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	if err := s.Register(pingReq{}, func(*CallInfo) *RetInfo { return nil }); err == nil {
		t.Fatal("duplicate register should fail")
	}
}

// TestClientCallExecAndMetadata 覆盖同步 Call 的完整链路：Client 投递、Server.Event 出队、Exec 路由处理、
// RetInfo 回包以及 metadata 从请求透传到响应。
func TestClientCallExecAndMetadata(t *testing.T) {
	s := NewServer(4)
	defer s.Close()
	c := NewClient(4)
	defer c.Close()

	if err := s.Register(pingReq{}, func(ci *CallInfo) *RetInfo {
		req := ci.Request.(pingReq)
		if ci.metadata["trace"] != "abc" {
			t.Fatalf("metadata in call info = %#v", ci.metadata)
		}
		return &RetInfo{Ack: pingAck{Value: req.Value + "-ack"}}
	}); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	done := make(chan *RetInfo, 1)
	go func() {
		done <- c.Call(s, pingReq{Value: "hello"}, WithMeta("trace", "abc"))
	}()

	s.Exec(waitCallInfo(t, s))
	select {
	case ri := <-done:
		if ri.Err != nil {
			t.Fatalf("call err = %v", ri.Err)
		}
		ack, ok := ri.Ack.(pingAck)
		if !ok || ack.Value != "hello-ack" {
			t.Fatalf("unexpected ack: %#v", ri.Ack)
		}
		if ri.Metadata["trace"] != "abc" {
			t.Fatalf("ret metadata = %#v", ri.Metadata)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting sync call")
	}
}

// TestClientAsyncCallCallbackAndPending 验证 AsyncCall 的 pending 计数和回调执行语义：响应先进入客户端事件队列，
// 只有调用 AsyncCallback 后才真正执行业务 callback 并减少 pending。
func TestClientAsyncCallCallbackAndPending(t *testing.T) {
	s := NewServer(4)
	defer s.Close()
	c := NewClient(4)
	defer c.Close()

	if err := s.Register(pingReq{}, func(ci *CallInfo) *RetInfo {
		return &RetInfo{Ack: pingAck{Value: ci.Request.(pingReq).Value}}
	}); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	called := make(chan *RetInfo, 1)
	if err := c.AsyncCall(s, pingReq{Value: "async"}, func(ri *RetInfo) {
		called <- ri
	}, WithMeta("m", 7)); err != nil {
		t.Fatalf("AsyncCall failed: %v", err)
	}
	if c.PendingCount() != 1 || c.Idle() {
		t.Fatalf("pending/idle mismatch: pending=%d idle=%v", c.PendingCount(), c.Idle())
	}

	s.Exec(waitCallInfo(t, s))
	ri := waitRetInfo(t, c)
	c.AsyncCallback(ri)
	select {
	case got := <-called:
		if got.Err != nil {
			t.Fatalf("async err = %v", got.Err)
		}
		if got.Metadata["m"] != 7 {
			t.Fatalf("metadata = %#v", got.Metadata)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting async callback")
	}
	if c.PendingCount() != 0 || !c.Idle() {
		t.Fatalf("pending/idle after callback mismatch: pending=%d idle=%v", c.PendingCount(), c.Idle())
	}
}

func TestClientCast(t *testing.T) {
	s := NewServer(4)
	defer s.Close()
	c := NewClient(4)
	defer c.Close()

	seen := make(chan string, 1)
	if err := s.Register(pingReq{}, func(ci *CallInfo) *RetInfo {
		seen <- ci.Request.(pingReq).Value
		return &RetInfo{Ack: pingAck{Value: "ignored"}}
	}); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	c.Cast(s, pingReq{Value: "cast"})
	s.Exec(waitCallInfo(t, s))
	select {
	case got := <-seen:
		if got != "cast" {
			t.Fatalf("cast value = %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting cast handler")
	}
	select {
	case ri := <-c.Event():
		t.Fatalf("cast should not return event: %#v", ri)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestServerExecUnregisteredAndPanicReturnsError(t *testing.T) {
	for _, tc := range []struct {
		name     string
		register bool
		handler  Handler
	}{
		{name: "unregistered"},
		{name: "panic", register: true, handler: func(*CallInfo) *RetInfo { panic("boom") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewServer(4)
			defer s.Close()
			c := NewClient(4)
			defer c.Close()
			if tc.register {
				if err := s.Register(pingReq{}, tc.handler); err != nil {
					t.Fatalf("register failed: %v", err)
				}
			}

			done := make(chan *RetInfo, 1)
			go func() { done <- c.Call(s, pingReq{}) }()
			s.Exec(waitCallInfo(t, s))
			select {
			case ri := <-done:
				if ri.Err == nil {
					t.Fatal("expected error")
				}
			case <-time.After(time.Second):
				t.Fatal("timeout waiting error ret")
			}
		})
	}
}

func TestClientValidationAndClose(t *testing.T) {
	c := NewClient(4)
	if err := c.AsyncCall(nil, pingReq{}, func(*RetInfo) {}); !errors.Is(err, ErrServerNil) {
		t.Fatalf("AsyncCall nil server err = %v", err)
	}
	if err := c.AsyncCall(NewServer(1), pingReq{}, nil); !errors.Is(err, ErrCallbackNil) {
		t.Fatalf("AsyncCall nil callback err = %v", err)
	}
	c.Close()
	if !c.IsClosed() {
		t.Fatal("client should be closed")
	}
	s := NewServer(1)
	defer s.Close()
	if err := c.AsyncCall(s, pingReq{}, func(*RetInfo) {}); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("AsyncCall closed client err = %v", err)
	}
}

// TestServerCloseDrainsQueuedCalls 验证 Server.Close 的排空语义：队列里尚未执行的调用不能被静默丢弃，
// 必须向调用方返回 ErrServerClosed，避免同步或异步调用方永久等待。
func TestServerCloseDrainsQueuedCalls(t *testing.T) {
	s := NewServer(4)
	c := NewClient(4)
	defer c.Close()

	if err := c.AsyncCall(s, pingReq{}, func(*RetInfo) {}); err != nil {
		t.Fatalf("AsyncCall failed: %v", err)
	}
	s.Close()
	if !s.IsClosed() {
		t.Fatal("server should be closed")
	}
	ri := waitRetInfo(t, c)
	if !errors.Is(ri.Err, ErrServerClosed) {
		t.Fatalf("drained ret err = %v, want ErrServerClosed", ri.Err)
	}
	c.AsyncCallback(ri)
	if c.PendingCount() != 0 {
		t.Fatalf("pending after drained close = %d", c.PendingCount())
	}
	if err := c.AsyncCall(s, pingReq{}, func(*RetInfo) {}); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("AsyncCall closed server err = %v", err)
	}
}

func TestCallInfoRetOnlyOnceAndMetadataCopy(t *testing.T) {
	ch := newSyncRet()
	ci := &CallInfo{
		id:       ID(pingReq{}),
		chanRet:  ch,
		metadata: map[string]any{"trace": "t1", "seq": 12},
		callback: func(*RetInfo) {},
	}
	if err := ci.ret(&RetInfo{Ack: pingAck{Value: "first"}}); err != nil {
		t.Fatalf("first ret failed: %v", err)
	}
	if err := ci.ret(&RetInfo{Ack: pingAck{Value: "second"}}); err != nil {
		t.Fatalf("second ret is ignored and should not return error: %v", err)
	}

	select {
	case ri := <-ch:
		ack := ri.Ack.(pingAck)
		if ack.Value != "first" {
			t.Fatalf("ret ack = %s, want first", ack.Value)
		}
		if ri.Metadata["trace"] != "t1" || ri.Metadata["seq"] != 12 {
			t.Fatalf("metadata not copied: %#v", ri.Metadata)
		}
		if ri.callback == nil {
			t.Fatal("callback should be attached to RetInfo")
		}
	default:
		t.Fatal("expected one ret value")
	}
	select {
	case ri := <-ch:
		t.Fatalf("ret twice should not enqueue second value: %#v", ri)
	default:
	}
}

func TestServerCloseDrainsQueuedSyncCall(t *testing.T) {
	s := NewServer(4)
	c := NewClient(4)
	defer c.Close()

	done := make(chan *RetInfo, 1)
	go func() {
		done <- c.Call(s, pingReq{Value: "queued"})
	}()
	for s.Len() == 0 {
		time.Sleep(time.Millisecond)
	}
	s.Close()
	select {
	case ri := <-done:
		if !errors.Is(ri.Err, ErrServerClosed) {
			t.Fatalf("sync call err = %v, want ErrServerClosed", ri.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued sync call should be unblocked by Server.Close")
	}
}

func TestClientCloseDrainsPendingAsyncCallbacks(t *testing.T) {
	s := NewServer(4)
	defer s.Close()
	c := NewClient(4)

	if err := s.Register(pingReq{}, func(ci *CallInfo) *RetInfo {
		return &RetInfo{Ack: pingAck{Value: ci.Request.(pingReq).Value}}
	}); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	called := make(chan string, 1)
	if err := c.AsyncCall(s, pingReq{Value: "drain"}, func(ri *RetInfo) {
		called <- ri.Ack.(pingAck).Value
	}); err != nil {
		t.Fatalf("AsyncCall failed: %v", err)
	}
	s.Exec(waitCallInfo(t, s))
	c.Close()
	select {
	case got := <-called:
		if got != "drain" {
			t.Fatalf("callback value = %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Close should drain pending async callback")
	}
	if c.PendingCount() != 0 || !c.IsClosed() {
		t.Fatalf("client close state pending=%d closed=%v", c.PendingCount(), c.IsClosed())
	}
	c.Close()
}

func TestRetInfoIDAndCallOptions(t *testing.T) {
	if got := (&RetInfo{Ack: pingAck{}}).ID(); got != ID(pingAck{}) {
		t.Fatalf("RetInfo.ID = %d, want %d", got, ID(pingAck{}))
	}
	if got := (&RetInfo{Err: errors.New("x"), Ack: pingAck{}}).ID(); got != 0 {
		t.Fatalf("RetInfo.ID with error = %d, want 0", got)
	}
	if got := (&RetInfo{}).ID(); got != 0 {
		t.Fatalf("RetInfo.ID nil ack = %d, want 0", got)
	}

	c := NewClient(1)
	defer c.Close()
	o := c.applyOpts(WithMeta("a", 1), WithMeta("b", "two"))
	if o.metadata["a"] != 1 || o.metadata["b"] != "two" {
		t.Fatalf("call options metadata = %#v", o.metadata)
	}
}

func TestServerExecNilAndCallValidationErrors(t *testing.T) {
	s := NewServer(1)
	defer s.Close()
	s.Exec(nil)

	c := NewClient(1)
	defer c.Close()
	if ri := c.Call(nil, pingReq{}); !errors.Is(ri.Err, ErrServerNil) {
		t.Fatalf("Call nil server err = %v", ri.Err)
	}
	if ri := c.Call(s, nil); !errors.Is(ri.Err, ErrInvalidMsgType) {
		t.Fatalf("Call nil request err = %v", ri.Err)
	}
	if err := c.call(nil, &CallInfo{}); !errors.Is(err, ErrCallChannelNil) {
		t.Fatalf("raw call nil channel err = %v", err)
	}
	if err := c.call(s.chanCall, nil); !errors.Is(err, ErrCallInfoNil) {
		t.Fatalf("raw call nil CallInfo err = %v", err)
	}
}

func TestBKDRHashKnownValuesAndConsistency(t *testing.T) {
	if got := BKDRBytesHash(nil); got != 0 {
		t.Fatalf("BKDRBytesHash(nil) = %d, want 0", got)
	}
	if got := BKDRHashStr(""); got != 0 {
		t.Fatalf("BKDRHashStr(empty) = %d, want 0", got)
	}
	if got := BKDRHashStr("abc"); got != 1677554 {
		t.Fatalf("BKDRHashStr(abc) = %d, want 1677554", got)
	}
	if BKDRHashStr("same") != BKDRBytesHash([]byte("same")) {
		t.Fatal("string and bytes hash should be consistent")
	}
}

func TestClientCheckValidationMatrixAndCastNoPanic(t *testing.T) {
	c := NewClient(1)
	defer c.Close()
	if _, err := c.check(nil, pingReq{}); !errors.Is(err, ErrServerNil) {
		t.Fatalf("check nil server err = %v", err)
	}
	s := NewServer(1)
	if _, err := c.check(s, nil); !errors.Is(err, ErrInvalidMsgType) {
		t.Fatalf("check nil request err = %v", err)
	}
	s.Close()
	if _, err := c.check(s, pingReq{}); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("check closed server err = %v", err)
	}

	closedClient := NewClient(1)
	closedClient.Close()
	openServer := NewServer(1)
	defer openServer.Close()
	if _, err := closedClient.check(openServer, pingReq{}); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("check closed client err = %v", err)
	}
	closedClient.Cast(openServer, pingReq{})
	closedClient.Cast(nil, pingReq{})
}

func TestServerRegisterInvalidMessageID(t *testing.T) {
	s := NewServer(1)
	defer s.Close()
	if err := s.Register(customZeroIDMsg{}, func(*CallInfo) *RetInfo { return nil }); err == nil {
		t.Fatal("register zero custom message ID should fail")
	}
}

type customZeroIDMsg struct{}

func (customZeroIDMsg) ID() uint32 { return 0 }

func TestServerExecHandlerReturnsNilAndCastReturnPath(t *testing.T) {
	s := NewServer(2)
	defer s.Close()
	c := NewClient(2)
	defer c.Close()

	if err := s.Register(pingReq{}, func(*CallInfo) *RetInfo { return nil }); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	done := make(chan *RetInfo, 1)
	go func() { done <- c.Call(s, pingReq{}) }()
	s.Exec(waitCallInfo(t, s))
	select {
	case ri := <-done:
		if ri.Err != nil || ri.Ack != nil {
			t.Fatalf("nil handler ret should become empty RetInfo, got %#v", ri)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting nil handler ret")
	}

	c.Cast(s, pingReq{})
	s.Exec(waitCallInfo(t, s))
}

func TestAsyncCallbackRecoversPanicAndPendingDecrements(t *testing.T) {
	s := NewServer(1)
	defer s.Close()
	c := NewClient(1)
	defer c.Close()
	if err := s.Register(pingReq{}, func(*CallInfo) *RetInfo { return &RetInfo{Ack: pingAck{}} }); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	if err := c.AsyncCall(s, pingReq{}, func(*RetInfo) { panic("callback panic") }); err != nil {
		t.Fatalf("AsyncCall failed: %v", err)
	}
	s.Exec(waitCallInfo(t, s))
	c.AsyncCallback(waitRetInfo(t, c))
	if c.PendingCount() != 0 || !c.Idle() {
		t.Fatalf("pending after panic callback = %d idle=%v", c.PendingCount(), c.Idle())
	}
}

func TestCallInfoRetDroppedWhenSyncRetFullAndNoRetForCast(t *testing.T) {
	ch := newSyncRet()
	ch <- &RetInfo{Ack: pingAck{Value: "occupied"}}
	ci := &CallInfo{id: ID(pingReq{}), chanRet: ch}
	if err := ci.ret(&RetInfo{Ack: pingAck{Value: "dropped"}}); !errors.Is(err, ErrRetDropped) {
		t.Fatalf("ret full sync channel err = %v, want ErrRetDropped", err)
	}
	castCI := &CallInfo{id: ID(pingReq{})}
	if err := castCI.ret(&RetInfo{Ack: pingAck{}}); err != nil {
		t.Fatalf("cast ret should be ignored without error: %v", err)
	}
}

func TestClientCallPanicOnClosedQueueReturnsError(t *testing.T) {
	s := NewServer(1)
	c := NewClient(1)
	defer c.Close()
	s.chanCall.Close()
	err := c.call(s.chanCall, &CallInfo{id: ID(pingReq{}), Request: pingReq{}, chanRet: newSyncRet()})
	if err == nil {
		t.Fatal("call to closed queue should return panic error")
	}
}
