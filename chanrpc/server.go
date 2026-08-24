package chanrpc

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"runtime/debug"
	"sync/atomic"
	"time"
)

// defaultCloseDrainTimeout 是 Close 排空自投递链条的默认硬上限：防止
// 某个 handler 的自投递逻辑没有收敛条件（bug，或者设计上的死循环）
// 导致停机永远卡住。超时后放弃继续等待，直接完成关闭——代价是还没
// 处理到的那部分调用会被永久丢弃，这是"用一次可能的数据丢失换取停机
// 不再无限期挂起"的权衡，与 Client.Close 的超时兜底（见
// WithClientCloseTimeout）是同一个思路。可通过 WithCloseDrainTimeout
// 按 Server 覆盖。
const defaultCloseDrainTimeout = 30 * time.Second

// defaultChanLen 是 RPC 调用队列的默认容量，未显式指定 WithChanLen 时生效。
//
// 队列是**有界**的，这一点是刻意的：无界队列在消费端跟不上时只会一路把积压
// 堆进内存，最终以 OOM 的形式在离现场很远的地方崩掉，且中途没有任何信号。
// 有界队列把同一个问题在发生的那一刻就暴露出来——异步投递立刻失败（带消息
// 类型和水位），同步调用阻塞等待空位形成背压。
const defaultChanLen = 1024

// serverOptions 保存 NewServer 的可选配置项。
type serverOptions struct {
	chanLen           int
	closeDrainTimeout time.Duration
}

func defaultServerOptions() serverOptions {
	return serverOptions{chanLen: defaultChanLen, closeDrainTimeout: defaultCloseDrainTimeout}
}

// ServerOption 用于自定义 Server 的可选行为。
type ServerOption func(*serverOptions)

// WithChanLen 自定义 RPC 调用队列的容量，它是**硬性上限**而非容量提示：
// 队列满之后 Cast/AsyncCall 立即返回 ErrChanFull，同步 Call 阻塞等待空位。
//
// 取值上的权衡：调得太小会让业务的正常突发被误判成过载，调得太大则推迟了
// 过载的暴露时机、也占更多内存。建议按模块的稳态 QPS × 可容忍的排队时长
// 估算，并配合 Len()/Cap() 做水位告警。n <= 0 时该选项不生效，沿用
// defaultChanLen。
func WithChanLen(n int) ServerOption {
	return func(opts *serverOptions) {
		if n > 0 {
			opts.chanLen = n
		}
	}
}

// WithCloseDrainTimeout 自定义 Close 排空自投递链条的超时上限，
// 见 Server.Close 的说明。d <= 0 时该选项不生效，沿用 defaultCloseDrainTimeout。
func WithCloseDrainTimeout(d time.Duration) ServerOption {
	return func(opts *serverOptions) {
		if d > 0 {
			opts.closeDrainTimeout = d
		}
	}
}

// Server ChanRPC 服务端，接收并处理来自 Client 的 RPC 调用。
//
// 每个模块持有一个 Server 实例，所有外部 RPC 调用通过有界队列排队，
// 在模块的事件循环（Skeleton.Serve）中通过 Server.Event() 串行出队处理，从而保证模块内部状态访问无并发竞争。
//
// 架构优势：消息路由通过 functions 哈希表实现 O(1) 查找，
// 相比传统的 switch-case 分发，新增消息类型只需调用 Register 注册一次，扩展成本极低。
type Server struct {
	functions         map[uint32]Handler // 消息名 → 处理函数的路由表，初始化后只读，无需加锁
	chanCall          chan *CallInfo     // RPC 调用队列，有界；满时异步投递失败、同步调用阻塞等待
	closing           atomic.Bool        // Close 是否已经开始，只用于保证 Close 本身的幂等性
	closed            atomic.Bool        // 是否已经完全关闭（排空彻底完成），client.check 据此拒绝新调用
	pending           atomic.Int64       // 已成功入队但还未 Exec 完成的调用数，Close 排空时的终止条件，见 Close 的说明
	closeDrainTimeout time.Duration      // Close 排空的超时上限，见 WithCloseDrainTimeout
}

// NewServer 创建 ChanRPC 服务端，所有配置均可选，见各 WithXxx 选项。
func NewServer(opts ...ServerOption) *Server {
	cfg := defaultServerOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	s := new(Server)
	s.functions = map[uint32]Handler{}
	s.chanCall = make(chan *CallInfo, cfg.chanLen)
	s.closeDrainTimeout = cfg.closeDrainTimeout
	return s
}

// Event 返回 RPC 调用队列的只读接收端，供模块事件循环消费。
//
// 从这里取出的每一条 CallInfo 最终都必须传给 Exec（Skeleton.Serve 与本包
// 测试的每一处直接消费都紧跟一次 Exec）：pending 计数在成功入队时 +1、
// 在 Exec 完成时 -1，是 Close 排空循环判断"真的没有更多工作了"的唯一
// 依据；只取不 Exec 会让计数永久多出一次，使 Close 失去终止条件而永久
// 阻塞在排空循环里。
func (s *Server) Event() <-chan *CallInfo {
	return s.chanCall
}

// Len 返回 RPC 调用队列当前的积压数量，用于监控和告警。
func (s *Server) Len() int64 {
	return int64(len(s.chanCall))
}

// Cap 返回 RPC 调用队列的容量上限，与 Len 搭配即可算出水位（Len/Cap），
// 用于在真正打满、开始丢消息之前就发出告警。
func (s *Server) Cap() int64 {
	return int64(cap(s.chanCall))
}

// Register 注册消息处理函数，通过传入 message 实例的类型自动推导消息名。
//
// 每种消息类型只允许注册一个处理函数（防止意外覆盖）。
// 通常在模块的 OnInit 阶段完成注册，此后路由表只读，访问无需加锁。
func (s *Server) Register(message any, f Handler) error {
	if message == nil {
		slog.Error("message is nil")
		return ErrRegisterMsgNil
	}
	if f == nil {
		slog.Error("message handler is nil")
		return ErrRegisterHandlerNil
	}
	id := ID(message)
	if id == 0 {
		return fmt.Errorf("chanrpc register: invalid message type %v", reflect.TypeOf(message))
	}

	if _, ok := s.functions[id]; ok {
		slog.Error("duplicate message", slog.Any("id", id), slog.Any("type", reflect.TypeOf(message)))
		return fmt.Errorf("chanrpc register: id=%d type=%v already registered", id, reflect.TypeOf(message))
	}
	slog.Info("chanrpc register", slog.Any("id", id))
	s.functions[id] = f
	return nil
}

// exec 执行单次 RPC 调用的核心逻辑：路由到处理函数、执行并回包。
//
// 防御性设计：通过 defer + recover 捕获处理函数内部抛出的 panic，
// 并在 panic 恢复后自动向调用方回包错误，防止业务逻辑异常导致调用方的 Call 永久阻塞。
//
// 响应策略三选一：
//
//   - handler 返回非 nil *RetInfo → 框架代为回包；
//   - handler 调用了 ci.Hold() 并返回 nil → 框架**不**回包，由业务稍后用 Hold 返回的 Replier 回包
//     （延迟响应，用于把阻塞 IO 挪出事件循环）；
//   - handler 直接返回 nil 且未 Hold → 框架补一个空包。
//
// 第三条是必须的兜底：返回 nil 天然有「无需响应」与「稍后再回」两种语义，
// 框架无法从返回值区分，若一律不回包，所有只想表达前者的 handler 都会让
// 同步 Call 的调用方永久挂死。Hold 就是让业务把后一种意图显式讲出来。
//
// panic 路径**无视 Hold 一律回包**：handler 已经崩了，再守着"稍后会回"的承诺
// 只会让调用方一直等下去。
//
// 无论哪条路径，hasRet 的 CAS 保证最多只发送一次响应。
func (s *Server) exec(ci *CallInfo) (err error) {
	defer func() {
		panicked := false
		if r := recover(); r != nil {
			panicked = true
			if e, ok := r.(error); ok {
				err = e
			} else {
				err = fmt.Errorf("panic: %v", r)
			}
			slog.Error("chanrpc exec panic", slog.Any("id", ci.ID()), slog.Any("error", err), slog.String("stack", string(debug.Stack())))
		}
		if (panicked || !ci.held.Load()) && !ci.hasRet.Load() {
			_ = ci.ret(&RetInfo{Err: err})
		}
	}()

	handler, ok := s.functions[ci.ID()]
	if !ok {
		err = fmt.Errorf("chanrpc %d not registered", ci.ID())
		return
	}

	ret := handler(ci)
	if ret != nil && !ci.hasRet.Load() {
		_ = ci.ret(ret)
	}
	return nil
}

// Exec 公开的消息执行入口，在模块的 Serve 事件循环中逐一调用。
//
// 执行前把 hasRet / held 一并重置，保证同一个 CallInfo 若被重复投递也从干净状态开始。
// 延迟响应（handler 先 Hold、稍后由别的 goroutine 调 Ret）的语义见 CallInfo.Hold。
//
// defer 里的 pending.Add(-1) 与入队时的 +1（见 client.go 的 call）配对，
// 是 Close 排空循环判断真正见底的唯一依据，见 Close 的说明。
func (s *Server) Exec(ci *CallInfo) {
	if ci == nil {
		slog.Warn("chanrpc exec callInfo is nil")
		return
	}
	defer s.pending.Add(-1)
	ci.hasRet.Store(false)
	ci.held.Store(false)
	if err := s.exec(ci); err != nil {
		slog.Warn("chanrpc exec failed", slog.Any("error", err))
	}
}

// IsClosed 检查服务端是否已关闭。
func (s *Server) IsClosed() bool {
	return s.closed.Load()
}

// Close 关闭服务端：拒绝新调用写入，并把队列里已经排队的调用真正执行完
// （而不是回一个"服务已关闭"的错误）。
//
// 调用前提：Close 必须在这个 Server 所属模块自己的事件循环 goroutine 上
// 调用（xhive 里唯一的调用点是 Skeleton.Serve，由 Serve 在返回前同步调用），
// 这样 Exec 里对业务状态的访问仍然只发生在这一个 goroutine 上，
// 没有引入新的并发访问。
//
// 排空必须一直持续到 pending 真正归零，而不是只处理调用这一刻的存量：
// 不少 handler 是"每次处理一批，剩余部分自己 Cast 给本模块继续"的分批
// 写法（例如大批量任务的分帧处理），Close 期间执行到这类 handler 时，它
// 产生的自投递也必须被继续处理，否则任务的尾部会在关闭那一刻被腰斩、
// 且没有任何补救机会。因此 s.closed 在整个排空过程中保持 false，直到
// pending 归零才置位——这段时间里 client.check 不会拒绝任何调用（含
// 自投递），但按 xhive 的 LIFO 停机顺序，此刻不会再有别的模块向本模块
// 投递新请求，所以这不会让排空永远停不下来。
//
// pending 由每次入队前 +1（client.go 的 call，投递失败再回滚）、每次 Exec
// 完成 -1 配对维护：Exec 里触发的自投递在 handler 返回前必定已经完成计数，
// 所以"pending 归零"就是确凿的"再也不会有新工作"，而不是对队列瞬时快照的
// 轮询猜测。
//
// 排空不是无条件等下去：closeDrainTimeout 兜底防止某个 handler 的自
// 投递没有收敛条件而永远排不空，超时后放弃剩余部分、直接完成关闭。
//
// 使用 CompareAndSwap（closing 字段）保证 Close 的幂等性（重复调用安全，不会 panic）。
func (s *Server) Close() {
	if !s.closing.CompareAndSwap(false, true) {
		slog.Warn("chanrpc server already closed")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.closeDrainTimeout)
	defer cancel()

	timedOut := false
drain:
	for s.pending.Load() > 0 {
		select {
		case ci, ok := <-s.chanCall:
			if !ok {
				break drain
			}
			s.Exec(ci)
		case <-ctx.Done():
			timedOut = true
			break drain
		}
	}

	s.closed.Store(true)
	// 关闭队列而不是只置 closed 标志：同步 Call 在队列满时是**阻塞**等待空位的，
	// 若此刻恰好有调用方卡在那次发送上，只置标志它永远醒不过来；关闭会让那次
	// 发送 panic，由 client.call 的 recover 转成错误返回给调用方。
	close(s.chanCall)

	if timedOut {
		slog.Error("chanrpc server close drain timeout, dropping remaining self-cast chain",
			slog.Int64("remaining_pending", s.pending.Load()))
	}
}
