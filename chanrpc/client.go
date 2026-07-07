package chanrpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xmapst/xhive/chanx"
)

// Client ChanRPC 客户端，向其他模块的 Server 发起 RPC 调用。
//
// 通过 pendingAsyncCall 原子计数器追踪所有未处理完毕的异步调用，
// 在 Close 时等待计数归零，确保模块关闭前所有回调均已执行，防止业务状态不一致。
// closed 标志在 CAS 语义下保证关闭操作的幂等性，防止关闭后再次发起调用。
type Client struct {
	chanAsyncRet     *chanx.Unbounded[*RetInfo] // 异步调用结果队列，发送方永不阻塞、永不失败
	pendingAsyncCall atomic.Int64               // 当前尚未处理完毕的异步调用数量，原子操作保证并发安全
	closed           atomic.Bool                // 关闭标志，防止关闭后继续发起新的调用
}

// NewClient 创建 ChanRPC 客户端。
//
// initCap 是内部环形缓冲区的初始容量提示，语义与 Server.chanCall 一致：
// 用于减少反复扩容，不是硬性上限。
func NewClient(initCap int) *Client {
	c := &Client{
		chanAsyncRet: chanx.NewUnbounded[*RetInfo](context.Background(),
			chanx.WithInitialCapacity(initCap)),
	}
	return c
}

// Event 返回异步调用响应队列的只读接收端，供调用方事件循环消费。
func (c *Client) Event() <-chan *RetInfo {
	return c.chanAsyncRet.Out()
}

// Len 返回异步调用响应队列的近似积压数量，用于监控和告警。
func (c *Client) Len() int64 {
	return c.chanAsyncRet.Len()
}

// IsClosed 检查客户端是否已关闭。
func (c *Client) IsClosed() bool {
	return c.closed.Load()
}

// check 在发起调用前执行统一的前置校验，并解析消息名。
//
// 将 nil 检查、关闭状态检查、消息名解析等公共逻辑收敛到此处，
// 避免在 Call/AsyncCall/Cast 三处入口中分散重复相同的校验代码。
func (c *Client) check(s *Server, request any) (uint32, error) {
	if s == nil {
		return 0, ErrServerNil
	}
	if s.IsClosed() {
		return 0, ErrServerClosed
	}
	if c.IsClosed() {
		return 0, ErrClientClosed
	}
	id := ID(request)
	if id == 0 {
		return 0, ErrInvalidMsgType
	}
	return id, nil
}

// Call 向指定 Server 发起同步 RPC 调用，阻塞等待处理结果后返回。
//
// 每次调用创建独立的一次性 syncRet，而非共用 chanAsyncRet，
// 目的是隔离并发 Call 的响应通道，防止多个同时进行的 Call 互相"抢包"。
// syncRet 是普通 channel，无需显式回收，用完由 GC 自动处理。
//
// 等待响应采用“无限等待 + 周期告警”策略：投递到 Server.chanCall 本身不会阻塞（无界队列），
// 真正的等待风险在于 Server 迟迟不处理，因此每 5 秒记录一次告警用于诊断，
// 但不会主动超时返回；调用方仍需避免在事件循环中形成循环等待。
//
// 警告：在事件循环中使用 Call 会阻塞本模块对其他消息的处理；
// 若对端模块同时向本模块发起 Call，则形成循环等待（死锁），生产环境应优先使用 AsyncCall。
func (c *Client) Call(s *Server, request any, opts ...CallOption) *RetInfo {
	id, err := c.check(s, request)
	if err != nil {
		slog.Warn("chanrpc sync call failed", "id", id, "err", err)
		return &RetInfo{Err: err}
	}
	o := c.applyOpts(opts...)

	chanRet := newSyncRet()
	err = c.call(s.chanCall, &CallInfo{
		id:       id,
		Request:  request,
		chanRet:  chanRet,
		metadata: o.metadata,
	})
	if err != nil {
		slog.Warn("chanrpc sync call failed", "id", id, "err", err)
		return &RetInfo{Err: err}
	}

	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		select {
		case ri := <-chanRet:
			return ri
		case <-tick.C:
			slog.Warn("chanrpc call timeout", "id", id)
		}
	}
}

// AsyncCall 向指定 Server 发起异步 RPC 调用，注册回调后立即返回。
//
// 异步结果写入共享的 chanAsyncRet 队列，由调用方模块的事件循环通过 AsyncCallback 触发回调，
// 保证回调在发起调用的 goroutine 中串行执行，无需为访问模块状态加锁。
// pendingAsyncCall 计数在此加一，在 AsyncCallback 中减一，用于 Close 时的优雅等待。
func (c *Client) AsyncCall(s *Server, request any, callback Callback, opts ...CallOption) error {
	if callback == nil {
		return ErrCallbackNil
	}

	id, err := c.check(s, request)
	if err != nil {
		slog.Warn("chanrpc async call failed", "id", id, "err", err)
		return err
	}
	o := c.applyOpts(opts...)

	err = c.call(s.chanCall, &CallInfo{
		id:       id,
		Request:  request,
		chanRet:  asyncRet{c.chanAsyncRet}, // 共享异步回调队列，回调由事件循环统一消费
		callback: callback,
		metadata: o.metadata,
	})
	if err != nil {
		slog.Warn("chanrpc async call failed", "id", id, "err", err)
		return err
	}

	c.pendingAsyncCall.Add(1)
	return nil
}

// Cast 向指定 Server 单向投递消息，不等待响应，也不关心处理结果。
//
// 适用于日志上报、事件通知、统计埋点等无需确认的场景，开销最低。
// 与 AsyncCall 的本质区别：CallInfo 中 chanRet 和 callback 均为 nil，
// Server 处理后直接丢弃结果，不产生任何回调开销。
// 对 ErrServerNil 不打 warn 日志：允许对端模块尚未就绪时静默丢弃，避免大量误报。
func (c *Client) Cast(s *Server, request any, opts ...CallOption) {
	id, err := c.check(s, request)
	if err != nil {
		if !errors.Is(err, ErrServerNil) {
			slog.Warn("chanrpc cast failed", "id", id, "err", err)
		}
		return
	}
	o := c.applyOpts(opts...)

	err = c.call(s.chanCall, &CallInfo{
		id:       id,
		Request:  request,
		metadata: o.metadata,
		// chanRet 和 callback 均为 nil，Server 端处理后不回包
	})
	if err != nil {
		slog.Warn("chanrpc cast failed", "id", id, "err", err)
	}
}

// execCallback 安全执行单个异步回调，通过 recover 捕获回调内部的 panic。
//
// 将 panic 隔离在单次回调内，防止一个业务回调的异常传播导致整个模块崩溃。
func (c *Client) execCallback(ri *RetInfo) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("chanrpc callback panic", "panic", r, "stack", string(debug.Stack()))
		}
	}()

	if ri.callback != nil {
		ri.callback(ri)
	}
}

// AsyncCallback 处理一条异步调用的响应：递减待处理计数并执行业务回调。
//
// 在调用方模块的事件循环中串行调用，保证回调的执行上下文与业务逻辑在同一 goroutine，
// 从而可以无锁安全地访问模块内部状态。
func (c *Client) AsyncCallback(ri *RetInfo) {
	c.pendingAsyncCall.Add(-1)
	c.execCallback(ri)
}

// Close 关闭客户端，等待所有待处理的异步回调执行完毕后退出。
//
// 内部通过 sync.WaitGroup.Go 启动一个辅助 goroutine 消费 chanAsyncRet，
// 原因是调用方此时已不再运行事件循环，需要专门的 goroutine 来消费剩余的异步结果。
//
// 超时保护（5 秒）：防止因某个回调永久阻塞或计数异常导致 Close 无法返回；
// 超时后强制清零 pendingAsyncCall 并返回，可能丢失部分未执行的回调，会记录警告日志。
//
// 最后无条件关闭 chanAsyncRet：这一步是必须的——chanAsyncRet 内部有一个
// 常驻转发 goroutine，只有显式 Close 才会让它退出，否则即使 Client 本身
// 不再被引用，那个 goroutine 也会一直阻塞、造成泄漏（这是无界队列相比
// 普通 channel 多出来的生命周期管理责任）。
func (c *Client) Close() {
	// CAS 保证 Close 的幂等性，重复调用安全
	if !c.closed.CompareAndSwap(false, true) {
		slog.Warn("chanrpc client already closed")
		return
	}

	pending := c.pendingAsyncCall.Load()
	slog.Info("closing chanrpc client", "pending_calls", pending)

	if pending > 0 {
		var wg sync.WaitGroup
		wg.Go(func() {
			timer := time.NewTimer(5 * time.Second)
			defer timer.Stop()

			for {
				if c.pendingAsyncCall.Load() <= 0 {
					return
				}

				select {
				case ret := <-c.chanAsyncRet.Out():
					c.AsyncCallback(ret)
				case <-timer.C:
					// 超时后强制清零，避免 Close 永久阻塞，但可能丢失部分未处理的回调
					remaining := c.pendingAsyncCall.Load()
					slog.Warn("chanrpc client close timeout", "remaining_calls", remaining)
					c.pendingAsyncCall.Store(0)
					return
				}
			}
		})

		wg.Wait()
		slog.Info("chanrpc client closed successfully")
	}

	c.chanAsyncRet.Close()
}

// Idle 判断客户端是否处于空闲状态（无待处理的异步调用）。
//
// 在 Skeleton.close 中用于轮询判断是否可以安全退出，避免提前关闭时丢失异步回调。
func (c *Client) Idle() bool {
	return c.pendingAsyncCall.Load() == 0
}

// PendingCount 获取当前待处理的异步调用数量，用于监控和问题诊断。
func (c *Client) PendingCount() int64 {
	return c.pendingAsyncCall.Load()
}

// call 将 CallInfo 投递到 Server 的调用队列。
//
// chanCall 是无界队列，In() 按设计永不阻塞、永不因队列满而失败；
// 因此不再区分“阻塞模式”和“非阻塞模式”两条路径。
//
// panic 恢复：唯一的失败模式是向已关闭的队列投递（Server.Close 之后），
// 通过 recover 捕获并转化为 error 返回；若 chanRet 非空，还会尝试向调用方
// 回包错误，确保 Call 调用方不会永久阻塞在等待响应上。回包本身也用内层
// recover 包裹，防止 retSink.send 自身 panic（例如 asyncRet 对应的
// chanAsyncRet 也恰好已被关闭）导致这里发生二次 panic 而无法恢复。
func (c *Client) call(chanCall *chanx.Unbounded[*CallInfo], ci *CallInfo) (err error) {
	if chanCall == nil {
		return ErrCallChannelNil
	}
	if ci == nil {
		return ErrCallInfoNil
	}

	reqType := "unknown"
	if ci.Request != nil {
		reqType = reflect.TypeOf(ci.Request).String()
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, string(debug.Stack()))
			slog.Warn("chanrpc call panic", "req_type", reqType, "err", err)
			if ci.chanRet != nil {
				func() {
					defer func() { _ = recover() }() // 防止 send 自身 panic 导致二次崩溃
					ci.chanRet.send(&RetInfo{Err: err})
				}()
			}
		}
	}()

	chanCall.In() <- ci
	return nil
}
