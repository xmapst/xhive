package chanrpc

import (
	"errors"
	"testing"
	"time"
)

// recvWithin 在限定时间内从 chanRet 取回包，超时即判定为死锁。
func recvWithin(t *testing.T, ch <-chan *RetInfo, d time.Duration) *RetInfo {
	t.Helper()
	select {
	case ri := <-ch:
		return ri
	case <-time.After(d):
		t.Fatal("调用方在超时内未收到回包，疑似死锁")
		return nil
	}
}

// TestExecHandlerPanicAlwaysReplies 回归测试：handler panic 时，exec 的 defer 必须向调用方回包错误。
//
// 历史 bug：defer 中先 CompareAndSwap 抢占了 hasRet，导致随后 ci.ret 内部的 CAS 失败而静默不发包，
// 使同步 Call 的调用方永久阻塞。本测试锁定该路径，防止回归。
func TestExecHandlerPanicAlwaysReplies(t *testing.T) {
	s := NewServer(1)
	if err := s.Register(testMsg{}, func(*CallInfo) *RetInfo { panic("handler boom") }); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	ch := make(chan *RetInfo, 1)
	s.Exec(&CallInfo{id: ID(testMsg{}), Request: testMsg{}, chanRet: ch})

	ri := recvWithin(t, ch, 2*time.Second)
	if ri.Err == nil {
		t.Fatal("panic 路径回包 Err = nil, want non-nil")
	}
}

// TestExecUnregisteredMessageAlwaysReplies 回归测试：消息未注册时同样必须回包错误而非死锁。
func TestExecUnregisteredMessageAlwaysReplies(t *testing.T) {
	s := NewServer(1)
	ch := make(chan *RetInfo, 1)
	// 构造一个未注册的消息类型 ID。
	s.Exec(&CallInfo{id: ID(customIDMsg{}), Request: customIDMsg{}, chanRet: ch})

	ri := recvWithin(t, ch, 2*time.Second)
	if ri.Err == nil {
		t.Fatal("未注册消息回包 Err = nil, want non-nil")
	}
}

// TestExecSuccessRepliesExactlyOnce 验证成功路径下 handler 已回包，
// defer 不再重复发送（chanRet 容量为 1，重复发送会阻塞或 panic）。
func TestExecSuccessRepliesExactlyOnce(t *testing.T) {
	s := NewServer(1)
	if err := s.Register(testMsg{}, func(ci *CallInfo) *RetInfo {
		return &RetInfo{Ack: testMsg{Value: ci.Request.(testMsg).Value + "-ok"}}
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	ch := make(chan *RetInfo, 1)
	s.Exec(&CallInfo{id: ID(testMsg{}), Request: testMsg{Value: "v"}, chanRet: ch})

	ri := recvWithin(t, ch, 2*time.Second)
	if ack, ok := ri.Ack.(testMsg); !ok || ack.Value != "v-ok" {
		t.Fatalf("Ack = %#v, want testMsg{Value: v-ok}", ri.Ack)
	}
	// 不应再有第二个回包。
	select {
	case extra := <-ch:
		t.Fatalf("收到重复回包 %#v, want 仅一次响应", extra)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestSyncCallSurvivesHandlerPanic 端到端验证：通过运行事件循环的 Server，
// 同步 Call 在对端 handler panic 时能收到错误而非永久阻塞。
func TestSyncCallSurvivesHandlerPanic(t *testing.T) {
	s := NewServer(2)
	if err := s.Register(testMsg{}, func(*CallInfo) *RetInfo { panic("remote boom") }); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case ci := <-s.ChanCall:
				if ci == nil {
					return
				}
				s.Exec(ci)
			case <-stop:
				return
			}
		}
	}()
	defer close(stop)

	c := NewClient(2)
	done := make(chan *RetInfo, 1)
	go func() { done <- c.Call(s, testMsg{Value: "x"}) }()

	select {
	case ri := <-done:
		if ri.Err == nil {
			t.Fatal("Call 对端 panic 后 Err = nil, want non-nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Call 在对端 handler panic 后永久阻塞（死锁回归）")
	}
}

// TestRetServerClosedError 验证 ret 在 chanRet 已就绪时正确投递 ErrServerClosed（Close 排空路径）。
func TestRetServerClosedError(t *testing.T) {
	ch := make(chan *RetInfo, 1)
	ci := &CallInfo{id: 1, chanRet: ch}
	if err := ci.ret(&RetInfo{Err: ErrServerClosed}); err != nil {
		t.Fatalf("ret() error = %v", err)
	}
	ri := recvWithin(t, ch, time.Second)
	if !errors.Is(ri.Err, ErrServerClosed) {
		t.Fatalf("ri.Err = %v, want ErrServerClosed", ri.Err)
	}
}
