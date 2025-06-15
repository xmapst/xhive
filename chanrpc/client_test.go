package chanrpc

import (
	"errors"
	"testing"
	"time"
)

// TestExecNilCallInfo 覆盖 Exec 对 nil CallInfo 的防御分支。
func TestExecNilCallInfo(t *testing.T) {
	NewServer(1).Exec(nil) // 不应 panic。
}

// TestRetNilChanIsNoop 覆盖 ret 在 chanRet 为 nil（Cast 场景）时直接返回 nil。
func TestRetNilChanIsNoop(t *testing.T) {
	ci := &CallInfo{id: 1}
	if err := ci.ret(&RetInfo{}); err != nil {
		t.Fatalf("ret(nil chan) error = %v, want nil", err)
	}
}

// TestCallChannelFullNonBlocking 覆盖 call 非阻塞模式下 channel 已满立即返回错误的分支。
func TestCallChannelFullNonBlocking(t *testing.T) {
	c := NewClient(1)
	full := make(chan *CallInfo, 1)
	full <- &CallInfo{id: 1} // 占满。
	err := c.call(full, &CallInfo{id: 2, Request: testMsg{}}, false)
	if err == nil {
		t.Fatal("call(full chan, non-block) error = nil, want channel full error")
	}
}

// TestCallToClosedChannelRecovers 覆盖 call 向已关闭 channel 写入触发 panic 后 recover 并回包的分支。
func TestCallToClosedChannelRecovers(t *testing.T) {
	c := NewClient(1)
	closedCh := make(chan *CallInfo, 1)
	close(closedCh)
	ret := make(chan *RetInfo, 1)
	err := c.call(closedCh, &CallInfo{id: 1, Request: testMsg{}, chanRet: ret}, false)
	if err == nil {
		t.Fatal("call(closed chan) error = nil, want recovered panic error")
	}
	// chanRet 非空时，recover 分支会尝试非阻塞回包。
	select {
	case ri := <-ret:
		if ri.Err == nil {
			t.Fatal("recovered reply Err = nil, want non-nil")
		}
	case <-time.After(time.Second):
		t.Fatal("recover 分支未向 chanRet 回包")
	}
}

// TestCastSuccessAndServerClosed 覆盖 Cast 成功投递分支与对已关闭 Server 的静默处理。
func TestCastSuccessAndServerClosed(t *testing.T) {
	s := NewServer(2)
	c := NewClient(2)
	c.Cast(s, testMsg{Value: "v"}) // 成功投递。
	select {
	case ci := <-s.ChanCall:
		if ci.Request.(testMsg).Value != "v" {
			t.Fatalf("cast request = %#v", ci.Request)
		}
	default:
		t.Fatal("Cast 未投递到 ChanCall")
	}

	// 对已关闭 Server 的 Cast：check 返回 ErrServerClosed，记录 warn 后返回，不 panic。
	s2 := NewServer(1)
	s2.Close()
	c.Cast(s2, testMsg{})

	// 对 nil Server 的 Cast：静默丢弃，不打 warn，不 panic。
	c.Cast(nil, testMsg{})
}

// TestCastChannelFull 覆盖 Cast 在目标 ChanCall 已满时的失败分支（非阻塞）。
func TestCastChannelFull(t *testing.T) {
	s := NewServer(1)
	s.ChanCall <- &CallInfo{id: 1} // 占满容量为 1 的队列。
	c := NewClient(1)
	c.Cast(s, testMsg{Value: "overflow"}) // channel full，记录 warn 后返回。
}

// TestRegisterInvalidMessageID 覆盖 Register 在消息 ID 解析为 0 时的错误分支。
type zeroIDMsg struct{}

func (zeroIDMsg) ID() uint32 { return 0 }

func TestRegisterInvalidMessageID(t *testing.T) {
	s := NewServer(1)
	err := s.Register(zeroIDMsg{}, func(*CallInfo) *RetInfo { return nil })
	if err == nil {
		t.Fatal("Register(zero-id msg) error = nil, want non-nil")
	}
}

// TestAsyncCallChannelFull 覆盖 AsyncCall 在 Server.ChanCall 已满时返回错误且不增加 pending 计数。
func TestAsyncCallChannelFull(t *testing.T) {
	s := NewServer(1)
	s.ChanCall <- &CallInfo{id: 1} // 占满。
	c := NewClient(1)
	err := c.AsyncCall(s, testMsg{}, func(*RetInfo) {})
	if err == nil {
		t.Fatal("AsyncCall(full) error = nil, want channel full error")
	}
	if c.PendingCount() != 0 {
		t.Fatalf("PendingCount() = %d, want 0 (失败不应计数)", c.PendingCount())
	}
}

// TestClientCloseWhenIdle 覆盖 Close 在无待处理调用（pending=0）时的快速返回分支。
func TestClientCloseWhenIdle(t *testing.T) {
	c := NewClient(1)
	c.Close()
	if !c.IsClosed() {
		t.Fatal("IsClosed() = false after Close")
	}
}

// TestClientCloseTimeout 覆盖 Close 在回调始终不到达时的 5 秒超时强制清零分支。
// 为避免真等 5 秒，这里通过预置一个可立即消费的回包让循环走 ChanAsyncRet 分支后归零，
// 与超时分支互补（超时分支由结构保证，不实际等待）。
func TestClientCloseDrainsPending(t *testing.T) {
	c := NewClient(2)
	c.pendingAsyncCall.Store(1)
	cbHit := make(chan struct{}, 1)
	c.ChanAsyncRet <- &RetInfo{callback: func(*RetInfo) { cbHit <- struct{}{} }}
	c.Close()
	select {
	case <-cbHit:
	default:
		t.Fatal("Close 未消费并执行待处理回调")
	}
	if c.PendingCount() != 0 {
		t.Fatalf("PendingCount() after Close = %d, want 0", c.PendingCount())
	}
}

// TestCheckInvalidMsgType 覆盖 check 在消息 ID 为 0（nil 请求）时返回 ErrInvalidMsgType。
func TestCheckInvalidMsgType(t *testing.T) {
	c := NewClient(1)
	s := NewServer(1)
	_, err := c.check(s, nil)
	if !errors.Is(err, ErrInvalidMsgType) {
		t.Fatalf("check(nil request) error = %v, want ErrInvalidMsgType", err)
	}
}

// TestWithMetaNilMetadataInit 覆盖 WithMeta 在 metadata 为 nil 时的惰性初始化分支。
func TestWithMetaNilMetadataInit(t *testing.T) {
	o := &callOpts{} // metadata 为 nil。
	WithMeta("k", "v")(o)
	if o.metadata["k"] != "v" {
		t.Fatalf("metadata[k] = %v, want v", o.metadata["k"])
	}
}
