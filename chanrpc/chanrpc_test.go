package chanrpc

import (
	"errors"
	"testing"
	"time"
)

type testMsg struct {
	Value string
}

type customIDMsg struct{}

func (customIDMsg) ID() uint32 { return 777 }

func TestBKDRHashAndID(t *testing.T) {
	if got, want := BKDRBytesHash([]byte("abc")), BKDRHashStr("abc"); got != want {
		t.Fatalf("BKDRBytesHash/BKDRHashStr mismatch: got %d want %d", got, want)
	}
	if got := ID(nil); got != 0 {
		t.Fatalf("ID(nil) = %d, want 0", got)
	}
	if got := ID(customIDMsg{}); got != 777 {
		t.Fatalf("ID(customIDMsg) = %d, want 777", got)
	}
	if got1, got2 := ID(testMsg{}), ID(&testMsg{}); got1 == 0 || got1 != got2 {
		t.Fatalf("ID(testMsg) = %d, ID(*testMsg) = %d, want same non-zero", got1, got2)
	}
}

func TestWithMetaAndRetInfoID(t *testing.T) {
	c := NewClient(1)
	o := c.applyOpts(WithMeta("trace", "abc"), WithMeta("uid", 1))
	if got := o.metadata["trace"]; got != "abc" {
		t.Fatalf("metadata trace = %v, want abc", got)
	}
	if got := o.metadata["uid"]; got != 1 {
		t.Fatalf("metadata uid = %v, want 1", got)
	}

	ri := &RetInfo{Ack: &testMsg{Value: "ok"}}
	if ri.ID() == 0 {
		t.Fatal("RetInfo.ID() = 0, want non-zero")
	}
	if (&RetInfo{Err: errors.New("x")}).ID() != 0 {
		t.Fatal("RetInfo.ID() with err should be 0")
	}
}

func TestCallInfoRetCopiesMetadataAndPreventsDoubleReturn(t *testing.T) {
	ch := make(chan *RetInfo, 1)
	ci := &CallInfo{
		id:       1,
		chanRet:  ch,
		metadata: map[string]any{"trace": "abc"},
		callback: func(*RetInfo) {},
	}
	if err := ci.ret(&RetInfo{Metadata: map[string]any{"req": 1}}); err != nil {
		t.Fatalf("ret() error = %v", err)
	}
	ri := <-ch
	if got := ri.Metadata["trace"]; got != "abc" {
		t.Fatalf("Metadata[trace] = %v, want abc", got)
	}
	if got := ri.Metadata["req"]; got != 1 {
		t.Fatalf("Metadata[req] = %v, want 1", got)
	}
	if err := ci.ret(&RetInfo{}); err != nil {
		t.Fatalf("second ret() error = %v, want nil ignored", err)
	}
}

func TestServerRegisterExecAndClose(t *testing.T) {
	s := NewServer(2)
	if err := s.Register(testMsg{}, func(ci *CallInfo) *RetInfo {
		msg := ci.Request.(testMsg)
		return &RetInfo{Ack: testMsg{Value: msg.Value + "-ack"}}
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := s.Register(testMsg{}, func(ci *CallInfo) *RetInfo { return nil }); err == nil {
		t.Fatal("duplicate Register() error = nil, want non-nil")
	}
	if err := s.Register(nil, func(ci *CallInfo) *RetInfo { return nil }); !errors.Is(err, ErrRegisterMsgNil) {
		t.Fatalf("Register(nil) error = %v, want ErrRegisterMsgNil", err)
	}
	if err := s.Register(testMsg{}, nil); !errors.Is(err, ErrRegisterHandlerNil) {
		t.Fatalf("Register(nil handler) error = %v, want ErrRegisterHandlerNil", err)
	}

	ch := make(chan *RetInfo, 1)
	ci := &CallInfo{id: ID(testMsg{}), Request: testMsg{Value: "v"}, chanRet: ch}
	s.Exec(ci)
	ri := <-ch
	ack, ok := ri.Ack.(testMsg)
	if !ok || ack.Value != "v-ack" {
		t.Fatalf("Ack = %#v, want testMsg{Value: \"v-ack\"}", ri.Ack)
	}

	blocked := &CallInfo{id: ID(testMsg{}), Request: testMsg{Value: "queued"}, chanRet: make(chan *RetInfo, 1)}
	s.ChanCall <- blocked
	closed := make(chan struct{})
	go func() {
		s.Close()
		close(closed)
	}()
	if ri3 := <-blocked.chanRet; !errors.Is(ri3.Err, ErrServerClosed) {
		t.Fatalf("queued call err = %v, want ErrServerClosed", ri3.Err)
	}
	<-closed
	if !s.IsClosed() {
		t.Fatal("IsClosed() = false, want true")
	}
	s.Close()
}

func TestClientCallAsyncCallCastAndClose(t *testing.T) {
	s := NewServer(4)
	if err := s.Register(testMsg{}, func(ci *CallInfo) *RetInfo {
		msg := ci.Request.(testMsg)
		return &RetInfo{Ack: testMsg{Value: msg.Value + "-done"}}
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	c := NewClient(4)

	done := make(chan struct{})
	go func() {
		for ci := range s.ChanCall {
			s.Exec(ci)
		}
		close(done)
	}()

	callRet := c.Call(s, testMsg{Value: "sync"})
	if callRet.Err != nil {
		t.Fatalf("Call() err = %v", callRet.Err)
	}
	if ack := callRet.Ack.(testMsg); ack.Value != "sync-done" {
		t.Fatalf("Call() ack = %q, want %q", ack.Value, "sync-done")
	}

	asyncDone := make(chan *RetInfo, 1)
	if err := c.AsyncCall(s, testMsg{Value: "async"}, func(ri *RetInfo) {
		asyncDone <- ri
	}); err != nil {
		t.Fatalf("AsyncCall() error = %v", err)
	}
	ret := <-c.ChanAsyncRet
	c.AsyncCallback(ret)
	select {
	case got := <-asyncDone:
		if got.Err != nil {
			t.Fatalf("async callback err = %v", got.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("async callback timeout")
	}
	if c.PendingCount() != 0 || !c.Idle() {
		t.Fatalf("PendingCount() = %d Idle() = %v, want 0/true", c.PendingCount(), c.Idle())
	}

	c.Cast(s, testMsg{Value: "cast"})
	time.Sleep(20 * time.Millisecond)

	s.Close()
	<-done

	closeServer := NewServer(1)
	closeClient := NewClient(1)
	closeClient.pendingAsyncCall.Store(1)
	closeClient.ChanAsyncRet <- &RetInfo{callback: func(*RetInfo) {}}
	closeClient.Close()
	closeClient.Close()
	if !closeClient.IsClosed() {
		t.Fatal("client should be closed")
	}
	if closeClient.PendingCount() != 0 {
		t.Fatalf("PendingCount() after Close = %d, want 0", closeClient.PendingCount())
	}
	_ = closeServer
}

func TestClientErrorBranches(t *testing.T) {
	c := NewClient(1)
	if err := c.AsyncCall(nil, testMsg{}, func(*RetInfo) {}); !errors.Is(err, ErrServerNil) {
		t.Fatalf("AsyncCall(nil) error = %v, want ErrServerNil", err)
	}
	if err := c.AsyncCall(NewServer(1), testMsg{}, nil); !errors.Is(err, ErrCallbackNil) {
		t.Fatalf("AsyncCall(nil callback) error = %v, want ErrCallbackNil", err)
	}
	if ret := c.Call(nil, testMsg{}); !errors.Is(ret.Err, ErrServerNil) {
		t.Fatalf("Call(nil) err = %v, want ErrServerNil", ret.Err)
	}

	s := NewServer(1)
	s.Close()
	if err := c.AsyncCall(s, testMsg{}, func(*RetInfo) {}); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("AsyncCall(closed server) error = %v, want ErrServerClosed", err)
	}

	c2 := NewClient(1)
	c2.Close()
	if err := c2.AsyncCall(NewServer(1), testMsg{}, func(*RetInfo) {}); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("AsyncCall(closed client) error = %v, want ErrClientClosed", err)
	}
	if err := c2.call(nil, &CallInfo{}, false); !errors.Is(err, ErrCallChannelNil) {
		t.Fatalf("call(nil chan) error = %v, want ErrCallChannelNil", err)
	}
	if err := c2.call(make(chan *CallInfo, 1), nil, false); !errors.Is(err, ErrCallInfoNil) {
		t.Fatalf("call(nil ci) error = %v, want ErrCallInfoNil", err)
	}
}

func TestClientExecCallbackRecoverAndServerExecRecover(t *testing.T) {
	c := NewClient(1)
	c.execCallback(&RetInfo{callback: func(*RetInfo) { panic("callback panic") }})

	s := NewServer(1)
	if err := s.Register(testMsg{}, func(ci *CallInfo) *RetInfo {
		panic("handler panic")
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	ch := make(chan *RetInfo, 2)
	ci := &CallInfo{id: ID(testMsg{}), Request: testMsg{}, chanRet: ch}
	s.Exec(ci)
	if ri := <-ch; ri.Err == nil {
		t.Fatal("panic Exec() err = nil, want non-nil")
	}
}
