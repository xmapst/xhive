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

	"github.com/xmapst/xhive/internal/queue"
)

// defaultClientCloseTimeout 是 Close 等待待处理异步回调排空的默认硬
// 上限，见 Client.Close 的说明。可通过 WithClientCloseTimeout 按 Client
// 覆盖。
const defaultClientCloseTimeout = 5 * time.Second

// defaultClientChanLen 是异步调用返回队列的容量上限，未显式指定
// WithClientChanLen 时生效。与 Server 侧一样，这个队列有硬上限：满了之后
// 服务端的回包会被丢弃并记录日志，而不是无限堆积直到 OOM；同样地，上限
// 只封顶不预留，实际占用随积压伸缩。
const defaultClientChanLen = 1024

// Client ChanRPC 客户端，向其他模块的 Server 发起 RPC 调用。
//
// 通过 pendingAsyncCall 原子计数器追踪所有未处理完毕的异步调用，
// 在 Close 时等待计数归零，确保模块关闭前所有回调均已执行，防止业务状态不一致。
// closed 标志在 CAS 语义下保证关闭操作的幂等性，防止关闭后再次发起调用。
type Client struct {
	chanAsyncRet     *queue.Queue[*RetInfo] // 异步调用结果队列，容量按积压伸缩但有硬上限；满时回包被丢弃，见 asyncRet.send
	pendingAsyncCall atomic.Int64           // 当前尚未处理完毕的异步调用数量，原子操作保证并发安全
	closed           atomic.Bool            // 关闭标志，防止关闭后继续发起新的调用
	closeTimeout     time.Duration          // Close 排空 pending 异步回调的超时上限，见 WithClientCloseTimeout
}

// clientOptions 保存 NewClient 的可选配置项。
type clientOptions struct {
	chanLen      int
	closeTimeout time.Duration
}

func defaultClientOptions() clientOptions {
	return clientOptions{chanLen: defaultClientChanLen, closeTimeout: defaultClientCloseTimeout}
}

// ClientOption 用于自定义 Client 的可选行为。
type ClientOption func(*clientOptions)

// WithClientChanLen 自定义异步调用返回队列的容量，语义与 Server 的
// WithChanLen 一致：硬性上限，不是容量提示。它应当不小于本模块可能同时
// 在途的异步调用数，否则回包会在服务端被丢弃（见 asyncRet.send）。
// 命名带 Client 前缀是为了跟 ServerOption 的同名选项在包级别不冲突——
// 两者分别只用于 NewClient/NewServer，各自的调用点上语义都是清楚的。
// n <= 0 时该选项不生效，沿用 defaultClientChanLen。
func WithClientChanLen(n int) ClientOption {
	return func(opts *clientOptions) {
		if n > 0 {
			opts.chanLen = n
		}
	}
}

// WithClientCloseTimeout 自定义 Close 等待待处理异步回调排空的超时
// 上限，见 Client.Close 的说明。d <= 0 时该选项不生效，沿用
// defaultClientCloseTimeout（5 秒）。命名带 Client 前缀的原因与
// WithClientChanLen 相同：跟 Server 侧的 WithCloseDrainTimeout
// 是两个不同的 Option 类型，包级别不能同名。
func WithClientCloseTimeout(d time.Duration) ClientOption {
	return func(opts *clientOptions) {
		if d > 0 {
			opts.closeTimeout = d
		}
	}
}

// NewClient 创建 ChanRPC 客户端，所有配置均可选，见各 WithClientXxx 选项。
func NewClient(opts ...ClientOption) *Client {
	cfg := defaultClientOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	c := &Client{
		chanAsyncRet: queue.New[*RetInfo](cfg.chanLen),
		closeTimeout: cfg.closeTimeout,
	}
	return c
}

// NotEmpty 返回「有异步响应待处理」的信号，供调用方事件循环放进 select。
//
// 与 Server.NotEmpty 一样是边沿信号：收到后调用 Pop 取件，并且必须接受
// Pop 返回 false。
func (c *Client) NotEmpty() <-chan struct{} {
	return c.chanAsyncRet.NotEmpty()
}

// Pop 取出一条待处理的异步响应，队列为空时返回 ok=false。
//
// 取出的每一条 RetInfo 都应当交给 AsyncCallback：pendingAsyncCall 计数在
// 发起调用时 +1、在 AsyncCallback 中 -1，只取不回调会让 Close 一直等一个
// 不会到来的回调，直到超时兜底才放弃。
func (c *Client) Pop() (*RetInfo, bool) {
	return c.chanAsyncRet.Pop()
}

// Len 返回异步调用响应队列当前的积压数量，用于监控和告警。
func (c *Client) Len() int64 {
	return int64(c.chanAsyncRet.Len())
}

// Cap 返回异步调用响应队列的容量上限，与 Len 搭配即可算出水位（Len/Cap）。
func (c *Client) Cap() int64 {
	return int64(c.chanAsyncRet.Cap())
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
// 等价于 CallWithContext(context.Background(), s, request, opts...)：
// 不支持通过 ctx 主动取消等待，仅通过周期告警日志诊断长时间未响应的调用。
// 保留该方法是为了兼容既有调用方，新代码建议直接使用 CallWithContext
// 以获得真正的超时/取消能力。
//
// 警告：在事件循环中使用 Call 会阻塞本模块对其他消息的处理；
// 若对端模块同时向本模块发起 Call，则形成循环等待（死锁），生产环境应优先使用 AsyncCall。
func (c *Client) Call(s *Server, request any, opts ...CallOption) *RetInfo {
	return c.CallWithContext(context.Background(), s, request, opts...)
}

// CallWithContext 向指定 Server 发起同步 RPC 调用，阻塞等待处理结果、
// ctx 被取消或超时三者之一发生后返回。
//
// 每次调用创建独立的一次性 syncRet，而非共用 chanAsyncRet，
// 目的是隔离并发 Call 的响应通道，防止多个同时进行的 Call 互相"抢包"。
// syncRet 是普通 channel，无需显式回收，用完由 GC 自动处理。
//
// 相比 Call 固定的"无限等待 + 周期告警"策略，CallWithContext 允许调用方
// 通过 ctx（如 context.WithTimeout）主动放弃等待：ctx 被取消时立即返回
// 携带 ctx.Err() 的 RetInfo，而不必永久阻塞在等待响应上。
// ctx 同样覆盖**入队**这一段：队列满时同步调用是阻塞等待空位的（这正是有界
// 队列提供的背压），ctx 取消可以让调用方从这段等待中脱身，而不是连队列都还
// 没进去就卡死。
//
// 需要注意：ctx 取消只影响调用方的等待，不会取消 Server 端已经入队、
// 正在处理的调用，对端处理完成后仍会尝试回包（届时 syncRet 已无人接收，
// send 会返回 false，转化为 ErrRetDropped，属于预期行为）。
//
// 警告：在事件循环中使用 CallWithContext 会阻塞本模块对其他消息的处理；
// 若对端模块同时向本模块发起 Call，则形成循环等待（死锁），生产环境应优先使用 AsyncCall。
func (c *Client) CallWithContext(ctx context.Context, s *Server, request any, opts ...CallOption) *RetInfo {
	id, err := c.check(s, request)
	if err != nil {
		slog.Warn("chanrpc sync call failed", slog.Any("id", id), slog.Any("error", err))
		return &RetInfo{Err: err}
	}
	o := c.applyOpts(opts...)

	chanRet := newSyncRet()
	err = c.call(ctx, s, &CallInfo{
		id:       id,
		Request:  request,
		chanRet:  chanRet,
		metadata: o.metadata,
	}, true)
	if err != nil {
		slog.Warn("chanrpc sync call failed", slog.Any("id", id), slog.Any("error", err))
		return &RetInfo{Err: err}
	}

	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		select {
		case ri := <-chanRet:
			return ri
		case <-ctx.Done():
			slog.Warn("chanrpc call canceled", slog.Any("id", id), slog.Any("error", ctx.Err()))
			return &RetInfo{Err: ctx.Err()}
		case <-tick.C:
			slog.Warn("chanrpc call timeout", slog.Any("id", id))
		}
	}
}

// AsyncCall 向指定 Server 发起异步 RPC 调用，注册回调后立即返回。
//
// 异步结果写入共享的 chanAsyncRet 队列，由调用方模块的事件循环通过 AsyncCallback 触发回调，
// 保证回调在发起调用的 goroutine 中串行执行，无需为访问模块状态加锁。
// pendingAsyncCall 计数在此加一，在 AsyncCallback 中减一，用于 Close 时的优雅等待。
//
// 投递是非阻塞的：对端队列已满时立即返回 ErrChanFull，不阻塞本模块的事件循环。
// 这一点与同步 Call 相反——同步调用方本来就在等结果，多等一会儿入队是合理的；
// 异步调用方还要继续处理别的消息，为一次投递把整个事件循环停下来得不偿失。
func (c *Client) AsyncCall(s *Server, request any, callback Callback, opts ...CallOption) error {
	if callback == nil {
		return ErrCallbackNil
	}

	id, err := c.check(s, request)
	if err != nil {
		slog.Warn("chanrpc async call failed", slog.Any("id", id), slog.Any("error", err))
		return err
	}
	o := c.applyOpts(opts...)

	// 计数必须在入队**之前**加：入队成功的那一刻，对端就可能已经处理完并回包，
	// 而回包丢弃路径（asyncRet.send）会把计数减回来。先入队后加一存在一个窗口，
	// 会让计数先减到 -1 再加回 0，Close 的排空判断据此提前收工。
	c.pendingAsyncCall.Add(1)
	err = c.call(context.Background(), s, &CallInfo{
		id:       id,
		Request:  request,
		chanRet:  asyncRet{c}, // 共享异步回调队列，回调由事件循环统一消费
		callback: callback,
		metadata: o.metadata,
	}, false)
	if err != nil {
		c.pendingAsyncCall.Add(-1)
		slog.Warn("chanrpc async call failed", slog.Any("id", id), slog.Any("error", err))
		return err
	}

	return nil
}

// Cast 向指定 Server 单向投递消息，不等待响应，也不关心处理结果。
//
// 适用于日志上报、事件通知、统计埋点等无需确认的场景，开销最低。
// 与 AsyncCall 一样是非阻塞投递：对端队列满时本次消息被丢弃并记录警告日志。
// 与 AsyncCall 的本质区别：CallInfo 中 chanRet 和 callback 均为 nil，
// Server 处理后直接丢弃结果，不产生任何回调开销。
// 对 ErrServerNil 不打 warn 日志：允许对端模块尚未就绪时静默丢弃，避免大量误报。
func (c *Client) Cast(s *Server, request any, opts ...CallOption) {
	id, err := c.check(s, request)
	if err != nil {
		if !errors.Is(err, ErrServerNil) {
			slog.Warn("chanrpc cast failed", slog.Any("id", id), slog.Any("error", err))
		}
		return
	}
	o := c.applyOpts(opts...)

	err = c.call(context.Background(), s, &CallInfo{
		id:       id,
		Request:  request,
		metadata: o.metadata,
		// chanRet 和 callback 均为 nil，Server 端处理后不回包
	}, false)
	if err != nil {
		slog.Warn("chanrpc cast failed", slog.Any("id", id), slog.Any("error", err))
	}
}

// execCallback 安全执行单个异步回调，通过 recover 捕获回调内部的 panic。
//
// 将 panic 隔离在单次回调内，防止一个业务回调的异常传播导致整个模块崩溃。
func (c *Client) execCallback(ri *RetInfo) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("chanrpc callback panic", slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
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
// 超时保护（closeTimeout，默认 5 秒，见 WithClientCloseTimeout）：防止
// 因某个回调永久阻塞或计数异常导致 Close 无法返回；超时后强制清零
// pendingAsyncCall 并返回，可能丢失部分未执行的回调，会记录警告日志。
//
// chanAsyncRet 这里**不**关闭：它没有需要唤醒的常驻 goroutine，Client 不再被
// 引用后自然被 GC 回收；而关闭会让此刻仍在处理中、稍后才回包的服务端拿到
// ErrClosed，把一次本可以正常投递的回包变成丢包。
func (c *Client) Close() {
	// CAS 保证 Close 的幂等性，重复调用安全
	if !c.closed.CompareAndSwap(false, true) {
		slog.Warn("chanrpc client already closed")
		return
	}

	pending := c.pendingAsyncCall.Load()
	slog.Info("closing chanrpc client", slog.Int64("pending_calls", pending))

	if pending > 0 {
		var wg sync.WaitGroup
		wg.Go(func() {
			timer := time.NewTimer(c.closeTimeout)
			defer timer.Stop()

			for {
				if c.pendingAsyncCall.Load() <= 0 {
					return
				}

				// 与 Server.Close 同理：下面的 select 只在队列空时才走到，
				// 超时判断不能只挂在那里。这里的队列虽然只减不增（Close 已
				// 置位，不会再有新的 AsyncCall 入队），但一个慢回调仍可能把
				// 总耗时拖过 closeTimeout，显式检查才能守住这个上限。
				select {
				case <-timer.C:
					remaining := c.pendingAsyncCall.Load()
					slog.Warn("chanrpc client close timeout", slog.Int64("remaining_calls", remaining))
					c.pendingAsyncCall.Store(0)
					return
				default:
				}

				if ret, ok := c.chanAsyncRet.Pop(); ok {
					c.AsyncCallback(ret)
					continue
				}

				select {
				case <-c.chanAsyncRet.NotEmpty():
				case <-timer.C:
					// 超时后强制清零，避免 Close 永久阻塞，但可能丢失部分未处理的回调
					remaining := c.pendingAsyncCall.Load()
					slog.Warn("chanrpc client close timeout", slog.Int64("remaining_calls", remaining))
					c.pendingAsyncCall.Store(0)
					return
				}
			}
		})

		wg.Wait()
		slog.Info("chanrpc client closed successfully")
	}
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
// 入队前先给 s.pending 加一、失败再回滚：这是 Server.Close 判断排空是否真正
// 见底的唯一依据，见 Server.Close 的说明。之所以传 *Server 而不是直接传
// chanCall，就是为了能在这里摸到 pending 这个计数——两者本该是同一件事的
// 一体两面（“投给这个 Server 的队列”和“这个 Server 记一笔待办”），拆成两个
// 参数反而会把这份配对关系暴露给调用方，让人误以为可以只做其中一半。
//
// chanCall 是**有界**队列，block 决定队列满时的行为：
//
//   - block=true（同步 Call）：阻塞等待空位，把积压变成调用方的等待——调用方
//     本来就在等结果，让它多等一会儿入队，比丢掉这次调用更符合预期。等待期间
//     每 5 秒打一条警告，并可由 ctx 取消。
//   - block=false（AsyncCall / Cast）：立即返回 ErrChanFull。调用方还要继续
//     处理自己的消息，不能为一次投递把整个事件循环停下来。
//
// 无论哪种模式都不会无声无息地堆积：这正是有界队列相对无界队列的意义——
// 过载在发生的那一刻就变成一个可观测的错误或一次可观测的等待，而不是内存
// 曲线一路上扬直到进程被 OOM 杀掉。
//
// 向已关闭的队列投递（Server.Close 之后）返回 ErrServerClosed。这里保留
// recover 只是防御性的兜底（例如 Request 是个反射会炸的怪类型），而不像
// 过去那样把"向已关闭 channel 发送"这条正常路径也交给 panic 去表达。
//
// 失败时**不**再往 ci.chanRet 补一个错误响应：三个入口（Call/AsyncCall/Cast）
// 都直接消费 call 的返回值，那个补包是多余的；对异步调用它还有害——补进去的
// RetInfo 会被事件循环当成一次正常回包消费掉，AsyncCallback 减一次
// pendingAsyncCall，AsyncCall 自己在失败分支上又减一次，计数就被减穿了。
func (c *Client) call(ctx context.Context, s *Server, ci *CallInfo, block bool) (err error) {
	if s == nil {
		return ErrServerNil
	}
	if s.chanCall == nil {
		return ErrCallChannelNil
	}
	if ci == nil {
		return ErrCallInfoNil
	}

	reqType := "unknown"
	if ci.Request != nil {
		reqType = reflect.TypeOf(ci.Request).String()
	}

	enqueued := false
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, string(debug.Stack()))
			slog.Warn("chanrpc call panic", slog.String("req_type", reqType), slog.Any("error", err))
		}
		if !enqueued {
			s.pending.Add(-1) // 没进队列，把上面预加的那一笔撤销
		}
	}()

	// 先加后投：入队成功的那一刻 Server 就可能已经取走并 Exec 完成（pending 减一），
	// 若此时这边还没来得及加一，计数会先掉到 -1，Close 的排空循环据此提前收工。
	s.pending.Add(1)

	// 队列有空位时两种模式的行为完全一致，先走一次非阻塞尝试把这条共同的
	// 快路径走完，免得为一个用不上的告警 ticker 付出分配与调度开销。
	err = s.chanCall.Push(ci)
	switch {
	case err == nil:
		enqueued = true
		return nil
	case errors.Is(err, queue.ErrClosed):
		return ErrServerClosed
	case !errors.Is(err, queue.ErrFull):
		return err
	}

	if !block {
		return fmt.Errorf("%w: id=%d type=%s len=%d cap=%d",
			ErrChanFull, ci.id, reqType, s.chanCall.Len(), s.chanCall.Cap())
	}

	// 阻塞模式下仍然周期性告警：队列打满是个需要被看见的事故，
	// 不能只表现为“某个模块莫名其妙卡住了”。这里不用队列自带的 PushWait，
	// 就是为了把这条告警塞进同一个 select——多等一个 ticker 而已，
	// 换来的是过载期间有日志可查。
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-s.chanCall.NotFull():
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			slog.Warn("chanrpc call blocked on full server channel",
				slog.String("req_type", reqType), slog.Int("len", s.chanCall.Len()), slog.Int("cap", s.chanCall.Cap()))
			continue
		}

		// NotFull 是边沿信号，醒来不代表一定有位置（可能被别的生产者抢先），
		// 因此必须重试 Push 并接受它仍然返回 ErrFull。
		err = s.chanCall.Push(ci)
		switch {
		case err == nil:
			enqueued = true
			return nil
		case errors.Is(err, queue.ErrClosed):
			return ErrServerClosed
		case !errors.Is(err, queue.ErrFull):
			return err
		}
	}
}
