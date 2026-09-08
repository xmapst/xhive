package chanrpc

import (
	"context"
	"errors"
	"runtime"
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

// waitCallInfo 等一条待处理调用。先 Pop 再等信号：NotEmpty 是边沿信号且会
// 合并，上来就等会在调用早已入队、信号却已被上一轮取走时白等到超时。
func waitCallInfo(t *testing.T, s *Server) *CallInfo {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		if ci, ok := s.Pop(); ok {
			return ci
		}
		select {
		case <-s.NotEmpty():
		case <-deadline:
			t.Fatal("timeout waiting call info")
			return nil
		}
	}
}

func waitRetInfo(t *testing.T, c *Client) *RetInfo {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		if ri, ok := c.Pop(); ok {
			return ri
		}
		select {
		case <-c.NotEmpty():
		case <-deadline:
			t.Fatal("timeout waiting ret info")
			return nil
		}
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
	s := NewServer(WithChanLen(4))
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
	s := NewServer(WithChanLen(4))
	defer s.Close()
	c := NewClient(WithClientChanLen(4))
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
	s := NewServer(WithChanLen(4))
	defer s.Close()
	c := NewClient(WithClientChanLen(4))
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
	s := NewServer(WithChanLen(4))
	defer s.Close()
	c := NewClient(WithClientChanLen(4))
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
	case <-c.NotEmpty():
		if ri, ok := c.Pop(); ok {
			t.Fatalf("cast should not return event: %#v", ri)
		}
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
			s := NewServer(WithChanLen(4))
			defer s.Close()
			c := NewClient(WithClientChanLen(4))
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
	c := NewClient(WithClientChanLen(4))
	if err := c.AsyncCall(nil, pingReq{}, func(*RetInfo) {}); !errors.Is(err, ErrServerNil) {
		t.Fatalf("AsyncCall nil server err = %v", err)
	}
	if err := c.AsyncCall(NewServer(WithChanLen(1)), pingReq{}, nil); !errors.Is(err, ErrCallbackNil) {
		t.Fatalf("AsyncCall nil callback err = %v", err)
	}
	c.Close()
	if !c.IsClosed() {
		t.Fatal("client should be closed")
	}
	s := NewServer(WithChanLen(1))
	defer s.Close()
	if err := c.AsyncCall(s, pingReq{}, func(*RetInfo) {}); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("AsyncCall closed client err = %v", err)
	}
}

// TestServerCloseDrainsQueuedCalls 覆盖 Close 的核心约束：队列里已经排队但
// 还没被消费的调用，在 Close 时必须被真正执行（Exec），而不是被判
// ErrServerClosed 拒绝——调用方已经把请求当作"发出去了"，框架不能事后
// 让它凭空消失，见 Server.Close 的注释。
// Close **之后**发起的新调用仍然会被拒绝，这条不变。
func TestServerCloseDrainsQueuedCalls(t *testing.T) {
	s := NewServer(WithChanLen(4))
	c := NewClient(WithClientChanLen(4))
	defer c.Close()

	if err := s.Register(pingReq{}, func(ci *CallInfo) *RetInfo {
		return &RetInfo{Ack: pingAck{Value: ci.Request.(pingReq).Value}}
	}); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	if err := c.AsyncCall(s, pingReq{Value: "queued"}, func(*RetInfo) {}); err != nil {
		t.Fatalf("AsyncCall failed: %v", err)
	}
	s.Close()
	if !s.IsClosed() {
		t.Fatal("server should be closed")
	}
	ri := waitRetInfo(t, c)
	if ri.Err != nil {
		t.Fatalf("drained ret err = %v, want the call to have actually executed", ri.Err)
	}
	if ack := ri.Ack.(pingAck); ack.Value != "queued" {
		t.Fatalf("drained ret ack = %#v, want the queued request's real result", ri.Ack)
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
	// 第二次回包被丢弃，且必须**报错**而非静默返回 nil：
	// 延迟响应（见 CallInfo.Hold）下回包发生在 handler 之外的 goroutine，
	// 静默丢弃会让业务完全察觉不到自己的响应没送出去。
	if err := ci.ret(&RetInfo{Ack: pingAck{Value: "second"}}); !errors.Is(err, ErrAlreadyRet) {
		t.Fatalf("second ret err = %v, want ErrAlreadyRet", err)
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

// TestServerCloseDrainsQueuedSyncCall 是同步 Call 版本的
// TestServerCloseDrainsQueuedCalls：队列里排队的同步调用同样必须被
// Close 真正执行并把真实结果送回调用方，而不是回一个 ErrServerClosed。
func TestServerCloseDrainsQueuedSyncCall(t *testing.T) {
	s := NewServer(WithChanLen(4))
	c := NewClient(WithClientChanLen(4))
	defer c.Close()

	if err := s.Register(pingReq{}, func(ci *CallInfo) *RetInfo {
		return &RetInfo{Ack: pingAck{Value: ci.Request.(pingReq).Value}}
	}); err != nil {
		t.Fatalf("register failed: %v", err)
	}

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
		if ri.Err != nil {
			t.Fatalf("sync call err = %v, want the call to have actually executed", ri.Err)
		}
		if ack := ri.Ack.(pingAck); ack.Value != "queued" {
			t.Fatalf("sync call ack = %#v, want the queued request's real result", ri.Ack)
		}
	case <-time.After(time.Second):
		t.Fatal("queued sync call should be unblocked by Server.Close")
	}
}

// chainMsg 模拟"每次处理一批，剩余部分自己 Cast 回本 Server 继续"的分批
// 处理写法（游戏服里战斗结算、批量 tick 这类 handler 常见这个写法）。
type chainMsg struct{ Remaining int }

// TestServerCloseDrainsSelfCastChain 覆盖 Close 排空语义的核心场景：
// drain 过程中执行到的 handler 自己 Cast 回本 Server 产生的新调用，必须
// 被继续处理直到链条真正走完，而不是在 Close 一开始把 closed 置位后，
// 这类自投递被 client.check 的 IsClosed 判断直接腰斩。
func TestServerCloseDrainsSelfCastChain(t *testing.T) {
	s := NewServer(WithChanLen(4))
	c := NewClient(WithClientChanLen(4))
	defer c.Close()

	const chainLen = 5
	var processed []int
	if err := s.Register(chainMsg{}, func(ci *CallInfo) *RetInfo {
		msg := ci.Request.(chainMsg)
		processed = append(processed, msg.Remaining)
		if msg.Remaining > 0 {
			c.Cast(s, chainMsg{Remaining: msg.Remaining - 1})
		}
		return &RetInfo{Ack: true}
	}); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	c.Cast(s, chainMsg{Remaining: chainLen})
	s.Close()

	if !s.IsClosed() {
		t.Fatal("server should be closed once the self-cast chain finishes")
	}
	if len(processed) != chainLen+1 {
		t.Fatalf("processed = %v, want %d items (chain fully drained, not cut off)", processed, chainLen+1)
	}
	for i, v := range processed {
		if want := chainLen - i; v != want {
			t.Fatalf("processed[%d] = %d, want %d (chain order preserved)", i, v, want)
		}
	}

	// Close 之后新调用仍然会被拒绝，这条不变——用 AsyncCall（会先过
	// client.check 的 IsClosed 判断）而不是内部的 call，后者是给
	// check 通过之后使用的，绕过 check 直接调用不是真实调用路径。
	if err := c.AsyncCall(s, chainMsg{}, func(*RetInfo) {}); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("AsyncCall after chain-drained close err = %v, want ErrServerClosed", err)
	}
}

// TestServerCloseGivesUpOnRunawaySelfCastChain 覆盖 Close 排空的超时兜底：
// 一个永远不收敛的自投递（业务代码的 bug，或者设计上就是死循环）不应该
// 让 Close 永久阻塞——WithCloseDrainTimeout 设置的超时到期后必须放弃
// 剩余部分、正常返回并完成关闭；也不能让 chanCall 内部的转发 goroutine
// 泄漏（超时前修复中曾经漏掉这一步：没人再读 Out()，那个 goroutine
// 试图把剩余缓冲送进 Out() 时会因为没有接收方而永久阻塞）。
func TestServerCloseGivesUpOnRunawaySelfCastChain(t *testing.T) {
	s := NewServer(WithChanLen(4), WithCloseDrainTimeout(50*time.Millisecond))
	c := NewClient(WithClientChanLen(4))
	defer c.Close()

	type foreverMsg struct{}
	if err := s.Register(foreverMsg{}, func(ci *CallInfo) *RetInfo {
		c.Cast(s, foreverMsg{}) // 永远自投递，没有收敛条件
		return &RetInfo{Ack: true}
	}); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	c.Cast(s, foreverMsg{})

	runtime.GC()
	before := runtime.NumGoroutine()

	done := make(chan struct{})
	go func() {
		s.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close should give up once closeDrainTimeout elapses, not hang forever on a runaway self-cast chain")
	}
	if !s.IsClosed() {
		t.Fatal("server should be closed after giving up on the runaway chain")
	}

	// 超时分支起的丢弃 goroutine 需要一点时间把剩余积压排空退出，
	// 轮询等它稳定下来，确认没有把内部转发 goroutine 永久卡死。
	deadline := time.Now().Add(2 * time.Second)
	for {
		runtime.GC()
		after := runtime.NumGoroutine()
		if after <= before+1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine count did not settle after timeout close: before=%d after=%d", before, after)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestClientCloseDrainsPendingAsyncCallbacks(t *testing.T) {
	s := NewServer(WithChanLen(4))
	defer s.Close()
	c := NewClient(WithClientChanLen(4))

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

// TestClientCloseGivesUpOnStuckAsyncCall 覆盖 Client.Close 的超时兜底：
// 一个永远不回包的异步调用（对端 handler Hold 住之后忘了/没机会 Ret，
// 或者对端本身卡死）不应该让 Close 永久阻塞——WithClientCloseTimeout
// 设置的超时到期后必须强制清零 pendingAsyncCall、正常返回并完成关闭。
func TestClientCloseGivesUpOnStuckAsyncCall(t *testing.T) {
	s := NewServer()
	defer s.Close()
	stop := serveOnce(s)
	defer stop()

	type stuckMsg struct{}
	if err := s.Register(stuckMsg{}, func(ci *CallInfo) *RetInfo {
		ci.Hold() // 永远不回包，模拟一个卡死/忘了回包的异步调用
		return nil
	}); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	c := NewClient(WithClientCloseTimeout(50 * time.Millisecond))
	if err := c.AsyncCall(s, stuckMsg{}, func(*RetInfo) {}); err != nil {
		t.Fatalf("AsyncCall failed: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for c.PendingCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("async call was never picked up by the server")
		}
		time.Sleep(time.Millisecond)
	}

	done := make(chan struct{})
	go func() {
		c.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close should give up once WithClientCloseTimeout elapses, not hang forever on a stuck async call")
	}
	if !c.IsClosed() {
		t.Fatal("client should be closed after giving up on the stuck call")
	}
	if c.PendingCount() != 0 {
		t.Fatalf("PendingCount after timeout close = %d, want 0 (forced clear)", c.PendingCount())
	}
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

	c := NewClient(WithClientChanLen(1))
	defer c.Close()
	o := c.applyOpts(WithMeta("a", 1), WithMeta("b", "two"))
	if o.metadata["a"] != 1 || o.metadata["b"] != "two" {
		t.Fatalf("call options metadata = %#v", o.metadata)
	}
}

func TestServerExecNilAndCallValidationErrors(t *testing.T) {
	s := NewServer(WithChanLen(1))
	defer s.Close()
	s.Exec(nil)

	c := NewClient(WithClientChanLen(1))
	defer c.Close()
	if ri := c.Call(nil, pingReq{}); !errors.Is(ri.Err, ErrServerNil) {
		t.Fatalf("Call nil server err = %v", ri.Err)
	}
	if ri := c.Call(s, nil); !errors.Is(ri.Err, ErrInvalidMsgType) {
		t.Fatalf("Call nil request err = %v", ri.Err)
	}
	ctx := t.Context()
	if err := c.call(ctx, nil, &CallInfo{}, false); !errors.Is(err, ErrServerNil) {
		t.Fatalf("raw call nil server err = %v", err)
	}
	if err := c.call(ctx, &Server{}, &CallInfo{}, false); !errors.Is(err, ErrCallChannelNil) {
		t.Fatalf("raw call nil channel err = %v", err)
	}
	if err := c.call(ctx, s, nil, false); !errors.Is(err, ErrCallInfoNil) {
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
	c := NewClient(WithClientChanLen(1))
	defer c.Close()
	if _, err := c.check(nil, pingReq{}); !errors.Is(err, ErrServerNil) {
		t.Fatalf("check nil server err = %v", err)
	}
	s := NewServer(WithChanLen(1))
	if _, err := c.check(s, nil); !errors.Is(err, ErrInvalidMsgType) {
		t.Fatalf("check nil request err = %v", err)
	}
	s.Close()
	if _, err := c.check(s, pingReq{}); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("check closed server err = %v", err)
	}

	closedClient := NewClient(WithClientChanLen(1))
	closedClient.Close()
	openServer := NewServer(WithChanLen(1))
	defer openServer.Close()
	if _, err := closedClient.check(openServer, pingReq{}); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("check closed client err = %v", err)
	}
	closedClient.Cast(openServer, pingReq{})
	closedClient.Cast(nil, pingReq{})
}

func TestServerRegisterInvalidMessageID(t *testing.T) {
	s := NewServer(WithChanLen(1))
	defer s.Close()
	if err := s.Register(customZeroIDMsg{}, func(*CallInfo) *RetInfo { return nil }); err == nil {
		t.Fatal("register zero custom message ID should fail")
	}
}

type customZeroIDMsg struct{}

func (customZeroIDMsg) ID() uint32 { return 0 }

func TestServerExecHandlerReturnsNilAndCastReturnPath(t *testing.T) {
	s := NewServer(WithChanLen(2))
	defer s.Close()
	c := NewClient(WithClientChanLen(2))
	defer c.Close()

	if err := s.Register(pingReq{}, func(ci *CallInfo) *RetInfo {
		ci.ret(&RetInfo{Ack: pingAck{Value: "delayed"}})
		return nil
	}); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	done := make(chan *RetInfo, 1)
	go func() { done <- c.Call(s, pingReq{}) }()
	s.Exec(waitCallInfo(t, s))
	select {
	case ri := <-done:
		if ri.Err != nil {
			t.Fatalf("call err = %v", ri.Err)
		}
		ack, ok := ri.Ack.(pingAck)
		if !ok || ack.Value != "delayed" {
			t.Fatalf("delayed ret ack = %#v, want delayed", ri.Ack)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting delayed handler ret")
	}

	c.Cast(s, pingReq{})
	s.Exec(waitCallInfo(t, s))
}

func TestAsyncCallbackRecoversPanicAndPendingDecrements(t *testing.T) {
	s := NewServer(WithChanLen(1))
	defer s.Close()
	c := NewClient(WithClientChanLen(1))
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

// TestClientCallOnClosedQueueReturnsServerClosed 覆盖「队列已关闭后仍有调用
// 投递进来」这条路径。队列返回 ErrClosed，call 把它翻译成 ErrServerClosed：
// 过去这里是靠 send on closed channel 的 panic 再由 recover 兜成一个字符串
// 错误，调用方无从 errors.Is 判别，现在是一个确定的哨兵错误。
func TestClientCallOnClosedQueueReturnsServerClosed(t *testing.T) {
	s := NewServer(WithChanLen(1))
	c := NewClient(WithClientChanLen(1))
	defer c.Close()
	s.chanCall.Close()
	err := c.call(t.Context(), s, &CallInfo{id: ID(pingReq{}), Request: pingReq{}, chanRet: newSyncRet()}, false)
	if !errors.Is(err, ErrServerClosed) {
		t.Fatalf("call to closed queue = %v, want ErrServerClosed", err)
	}
	if got := s.pending.Load(); got != 0 {
		t.Fatalf("投递失败后 pending = %d, want 0（预加的那一笔必须回滚）", got)
	}
}

// serveOnce 在后台跑一个最小事件循环，把 Server 收到的调用逐个 Exec。
// 返回的 stop 用于结束该循环。
func serveOnce(s *Server) (stop func()) {
	done := make(chan struct{})
	go func() {
		for {
			if ci, ok := s.Pop(); ok {
				s.Exec(ci)
				continue
			}
			select {
			case <-s.NotEmpty():
			case <-done:
				return
			}
		}
	}()
	return func() { close(done) }
}

// TestHoldDefersResponse 验证延迟响应：handler 先 Hold 再返回 nil，
// 框架不得自动回包；调用方应当等到稍后那次真正的 Ret。
func TestHoldDefersResponse(t *testing.T) {
	s := NewServer(WithChanLen(4))
	c := NewClient(WithClientChanLen(4))
	defer s.Close()

	const delay = 60 * time.Millisecond
	if err := s.Register((*pingReq)(nil), func(ci *CallInfo) *RetInfo {
		reply := ci.Hold()
		go func() {
			time.Sleep(delay)
			if err := reply.Ret(&RetInfo{Ack: &pingAck{Value: "late"}}); err != nil {
				t.Errorf("deferred ret failed: %v", err)
			}
		}()
		return nil
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	stop := serveOnce(s)
	defer stop()

	start := time.Now()
	ri := c.Call(s, &pingReq{Value: "q"})
	elapsed := time.Since(start)

	if ri.Err != nil {
		t.Fatalf("call err: %v", ri.Err)
	}
	ack, ok := ri.Ack.(*pingAck)
	if !ok || ack.Value != "late" {
		t.Fatalf("want deferred ack %q, got %#v", "late", ri.Ack)
	}
	// 若框架仍在 handler 返回时兜底回包，Call 会立刻返回一个空 Ack，
	// 这里的耗时断言正是用来钉住那种回归。
	if elapsed < delay {
		t.Fatalf("call returned in %v, earlier than the deferred ret at %v", elapsed, delay)
	}
}

// TestNilRetWithoutHoldRepliesEmpty 验证未 Hold 而返回 nil 时框架必须兜底回包，
// 否则所有「无需响应」的 handler 都会让同步 Call 挂死。
func TestNilRetWithoutHoldRepliesEmpty(t *testing.T) {
	s := NewServer(WithChanLen(4))
	c := NewClient(WithClientChanLen(4))
	defer s.Close()

	if err := s.Register((*pingReq)(nil), func(*CallInfo) *RetInfo { return nil }); err != nil {
		t.Fatalf("register: %v", err)
	}
	stop := serveOnce(s)
	defer stop()

	done := make(chan *RetInfo, 1)
	go func() { done <- c.Call(s, &pingReq{}) }()

	select {
	case ri := <-done:
		if ri.Err != nil {
			t.Fatalf("want empty ack, got err %v", ri.Err)
		}
		if ri.Ack != nil {
			t.Fatalf("want nil ack, got %#v", ri.Ack)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("call hung: framework did not fall back to an empty reply")
	}
}

// TestHoldStillRepliesOnPanic 验证 handler 在 Hold 之后 panic 时框架仍会回包——
// handler 已经崩了，再守着「稍后会回」的承诺只会让调用方一直等下去。
func TestHoldStillRepliesOnPanic(t *testing.T) {
	s := NewServer(WithChanLen(4))
	c := NewClient(WithClientChanLen(4))
	defer s.Close()

	if err := s.Register((*pingReq)(nil), func(ci *CallInfo) *RetInfo {
		ci.Hold()
		panic("boom")
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	stop := serveOnce(s)
	defer stop()

	done := make(chan *RetInfo, 1)
	go func() { done <- c.Call(s, &pingReq{}) }()

	select {
	case ri := <-done:
		if ri.Err == nil {
			t.Fatal("want error after panic, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("call hung after handler panicked while held")
	}
}

// TestRetTwiceReportsAlreadyRet 验证重复回包会返回 ErrAlreadyRet 而非静默成功。
// 延迟响应下回包发生在别的 goroutine，静默丢弃会让业务完全察觉不到。
func TestRetTwiceReportsAlreadyRet(t *testing.T) {
	s := NewServer(WithChanLen(4))
	c := NewClient(WithClientChanLen(4))
	defer s.Close()

	second := make(chan error, 1)
	if err := s.Register((*pingReq)(nil), func(ci *CallInfo) *RetInfo {
		reply := ci.Hold()
		if err := reply.Ret(&RetInfo{Ack: &pingAck{Value: "first"}}); err != nil {
			t.Errorf("first ret failed: %v", err)
		}
		second <- reply.Ret(&RetInfo{Ack: &pingAck{Value: "second"}})
		return nil
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	stop := serveOnce(s)
	defer stop()

	ri := c.Call(s, &pingReq{})
	if ack, ok := ri.Ack.(*pingAck); !ok || ack.Value != "first" {
		t.Fatalf("want first ack, got %#v", ri.Ack)
	}
	select {
	case err := <-second:
		if !errors.Is(err, ErrAlreadyRet) {
			t.Fatalf("want ErrAlreadyRet on duplicate ret, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting duplicate ret result")
	}
}

// --- 有界队列语义 ---

// TestCastAndAsyncCallFailWhenChannelFull 验证队列打满后异步语义直接失败，
// 而不是把积压悄悄堆进内存：这正是从无界队列换成有界队列要买到的东西。
func TestCastAndAsyncCallFailWhenChannelFull(t *testing.T) {
	s := NewServer(WithChanLen(1))
	c := NewClient(WithClientChanLen(4))
	defer c.Close()
	if err := s.Register(pingReq{}, func(*CallInfo) *RetInfo { return nil }); err != nil {
		t.Fatalf("register: %v", err)
	}

	// 不启动事件循环，第一条占满容量为 1 的队列。
	c.Cast(s, pingReq{})
	if got := s.Len(); got != 1 {
		t.Fatalf("Len after first cast = %d, want 1", got)
	}
	if got := s.Cap(); got != 1 {
		t.Fatalf("Cap = %d, want 1", got)
	}

	if err := c.AsyncCall(s, pingReq{}, func(*RetInfo) {}); !errors.Is(err, ErrChanFull) {
		t.Fatalf("AsyncCall on full channel err = %v, want ErrChanFull", err)
	}
	// 投递失败必须回滚 pending 计数，否则 Close 会白等一个不会到来的回调。
	if got := c.PendingCount(); got != 0 {
		t.Fatalf("PendingCount after failed AsyncCall = %d, want 0", got)
	}
	// Cast 失败同样不能给服务端留下一笔永远不会被 Exec 的待办。
	c.Cast(s, pingReq{})
	if got := s.pending.Load(); got != 1 {
		t.Fatalf("server pending after failed cast = %d, want 1", got)
	}
}

// TestSyncCallBlocksUntilChannelHasRoom 验证同步调用在队列满时是等待而非失败：
// 调用方本来就在等结果，让它多等一会儿入队，比丢掉这次调用更符合预期。
func TestSyncCallBlocksUntilChannelHasRoom(t *testing.T) {
	s := NewServer(WithChanLen(1))
	c := NewClient(WithClientChanLen(4))
	defer c.Close()
	if err := s.Register(pingReq{}, func(*CallInfo) *RetInfo { return &RetInfo{Ack: pingAck{Value: "pong"}} }); err != nil {
		t.Fatalf("register: %v", err)
	}

	c.Cast(s, pingReq{}) // 占满队列，此时事件循环还没起来

	done := make(chan *RetInfo, 1)
	go func() { done <- c.Call(s, pingReq{}) }()

	select {
	case ri := <-done:
		t.Fatalf("Call returned before channel had room: %#v", ri)
	case <-time.After(50 * time.Millisecond):
	}

	stop := serveOnce(s)
	defer stop()

	select {
	case ri := <-done:
		if ri.Err != nil {
			t.Fatalf("Call err = %v", ri.Err)
		}
		if ack, ok := ri.Ack.(pingAck); !ok || ack.Value != "pong" {
			t.Fatalf("Call ack = %#v", ri.Ack)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for blocked Call to complete")
	}
}

// TestSyncCallCanceledWhileWaitingForRoom 验证 ctx 覆盖的是"入队"这一段：
// 队列满时调用方能靠 ctx 从等待中脱身，而不是连队列都没进去就永久卡死。
func TestSyncCallCanceledWhileWaitingForRoom(t *testing.T) {
	s := NewServer(WithChanLen(1))
	c := NewClient(WithClientChanLen(4))
	defer c.Close()
	if err := s.Register(pingReq{}, func(*CallInfo) *RetInfo { return nil }); err != nil {
		t.Fatalf("register: %v", err)
	}

	c.Cast(s, pingReq{}) // 占满队列

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	ri := c.CallWithContext(ctx, s, pingReq{})
	if !errors.Is(ri.Err, context.DeadlineExceeded) {
		t.Fatalf("CallWithContext err = %v, want DeadlineExceeded", ri.Err)
	}
	if got := s.pending.Load(); got != 1 {
		t.Fatalf("server pending after canceled call = %d, want 1", got)
	}
}

// TestAsyncRetDroppedWhenClientChannelFull 验证回包队列满时响应被丢弃，
// 且 pendingAsyncCall 被减回来——否则 Client.Close 会一直等一个永远不会
// 执行的回调，直到超时兜底才放弃。
func TestAsyncRetDroppedWhenClientChannelFull(t *testing.T) {
	s := NewServer(WithChanLen(4))
	c := NewClient(WithClientChanLen(1))
	defer c.Close()
	if err := s.Register(pingReq{}, func(*CallInfo) *RetInfo { return &RetInfo{Ack: pingAck{}} }); err != nil {
		t.Fatalf("register: %v", err)
	}

	// 两次异步调用，但调用方一直不消费 Event()，回包队列容量只有 1。
	for range 2 {
		if err := c.AsyncCall(s, pingReq{}, func(*RetInfo) {}); err != nil {
			t.Fatalf("AsyncCall: %v", err)
		}
	}
	for range 2 {
		s.Exec(waitCallInfo(t, s))
	}

	if got := c.Len(); got != 1 {
		t.Fatalf("client Len = %d, want 1", got)
	}
	// 一条留在队列里等回调（pending=1），另一条被丢弃并已回滚计数。
	if got := c.PendingCount(); got != 1 {
		t.Fatalf("PendingCount = %d, want 1", got)
	}
}

// TestServerQueueGrowsOnDemandAndKeepsHardLimit 锁定「动态扩缩容 + 硬上限」
// 这两件事同时成立，它们正是这套队列存在的理由：
//
//   - 上限不等于常驻内存：一个上限 4096 的 Server 空闲时只分配十几个槽位，
//     原生 channel 做不到这点（缓冲区在 make 时一次性分配且永不增长）；
//   - 但上限仍然是硬的：打满即拒绝，不会像无界队列那样一路吃内存到 OOM。
//
// 任何一半失效都会让这次重构变得没有意义，所以两半在同一个用例里断言。
func TestServerQueueGrowsOnDemandAndKeepsHardLimit(t *testing.T) {
	const limit = 4096
	// 空闲和排空后允许的分配上限，给收缩留出余量（收缩要求连续低水位）。
	const idleSlack = 64

	s := NewServer(WithChanLen(limit))
	c := NewClient(WithClientChanLen(4))
	defer c.Close()

	if err := s.Register(pingReq{}, func(*CallInfo) *RetInfo { return nil }); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	if got := s.chanCall.Alloc(); got > idleSlack {
		t.Fatalf("空闲时已分配 %d 个槽位（上限 %d）——队列不该按上限预留内存", got, limit)
	}
	if got := s.Cap(); got != limit {
		t.Fatalf("Cap() = %d, want %d", got, limit)
	}

	for range limit {
		c.Cast(s, pingReq{})
	}
	if got := s.Len(); got != limit {
		t.Fatalf("打满后 Len() = %d, want %d", got, limit)
	}
	if got := s.chanCall.Alloc(); got != limit {
		t.Fatalf("打满后分配 %d 个槽位, want %d——容量应随积压长到上限", got, limit)
	}

	// 上限是硬的：再投一条立即失败，而不是继续吃内存。
	err := c.call(t.Context(), s, &CallInfo{id: ID(pingReq{}), Request: pingReq{}}, false)
	if !errors.Is(err, ErrChanFull) {
		t.Fatalf("打满后投递 = %v, want ErrChanFull", err)
	}

	for i := range limit {
		ci, ok := s.Pop()
		if !ok {
			t.Fatalf("Pop 在第 %d 条就见底了, want %d 条", i, limit)
		}
		s.Exec(ci)
	}
	if got := s.chanCall.Alloc(); got > idleSlack {
		t.Fatalf("排空后仍占 %d 个槽位, 未收缩到 %d 以内", got, idleSlack)
	}
	if got := s.pending.Load(); got != 0 {
		t.Fatalf("全部 Exec 后 pending = %d, want 0", got)
	}

	s.Close()
}

// TestRequestAsAndAckAs 覆盖 Go 1.27 泛型方法版的类型解包：命中、类型不符、
// 以及字段为 nil 三种情况都不能 panic，这正是它相对裸断言的全部价值。
func TestRequestAsAndAckAs(t *testing.T) {
	ci := &CallInfo{Request: pingReq{Value: "ping"}}
	if req, ok := ci.RequestAs[pingReq](); !ok || req.Value != "ping" {
		t.Fatalf("RequestAs[pingReq]() = (%v, %v), want ({ping}, true)", req, ok)
	}
	if got, ok := ci.RequestAs[pingAck](); ok {
		t.Fatalf("类型不符时 RequestAs = (%v, true), want false", got)
	}
	if got, ok := ci.RequestAs[*pingReq](); ok {
		t.Fatalf("值类型不该匹配指针类型: (%v, true)", got)
	}
	if got, ok := (&CallInfo{}).RequestAs[pingReq](); ok || got.Value != "" {
		t.Fatalf("Request 为 nil 时 = (%v, %v), want (零值, false)", got, ok)
	}

	ri := &RetInfo{Ack: &pingAck{Value: "pong"}}
	if ack, ok := ri.AckAs[*pingAck](); !ok || ack.Value != "pong" {
		t.Fatalf("AckAs[*pingAck]() = (%v, %v), want ({pong}, true)", ack, ok)
	}
	// 错误响应上 Ack 通常是 nil，裸断言必 panic，这里必须安全返回。
	if got, ok := (&RetInfo{Err: ErrServerClosed}).AckAs[*pingAck](); ok || got != nil {
		t.Fatalf("错误响应 AckAs = (%v, %v), want (nil, false)", got, ok)
	}
}
