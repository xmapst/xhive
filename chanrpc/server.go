package chanrpc

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"runtime/debug"
	"sync/atomic"

	"github.com/xmapst/xhive/chanx"
)

// Server ChanRPC 服务端，接收并处理来自 Client 的 RPC 调用。
//
// 每个模块持有一个 Server 实例，所有外部 RPC 调用通过无界队列排队，
// 在模块的事件循环（Skeleton.OnRun）中通过 Server.Event() 串行出队处理，从而保证模块内部状态访问无并发竞争。
//
// 架构优势：消息路由通过 functions 哈希表实现 O(1) 查找，
// 相比传统的 switch-case 分发，新增消息类型只需调用 Register 注册一次，扩展成本极低。
type Server struct {
	functions map[uint32]Handler          // 消息名 → 处理函数的路由表，初始化后只读，无需加锁
	chanCall  *chanx.Unbounded[*CallInfo] // RPC 调用队列，发送方永不阻塞、永不失败
	closed    atomic.Bool                 // 关闭标志，采用原子操作保证多 goroutine 并发访问时的可见性
}

// NewServer 创建 ChanRPC 服务端。
//
// initCap 是内部环形缓冲区的初始容量提示，用于减少高频场景下的反复扩容，
// 不再是硬性上限：队列会随积压自动增长，也会在消费跟上后自动收缩。
// 如需对积压做主动告警或限流，请基于 Server.Len() 自行判断，
// 不要依赖“发送失败”这个信号——它已经不存在了。
func NewServer(initCap int) *Server {
	s := new(Server)
	s.functions = map[uint32]Handler{}
	s.chanCall = chanx.NewUnbounded[*CallInfo](context.Background(),
		chanx.WithInitialCapacity(initCap))
	return s
}

func (s *Server) Event() <-chan *CallInfo {
	return s.chanCall.Out()
}

func (s *Server) Len() int64 {
	return s.chanCall.Len()
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
		slog.Error("duplicate message", "id", id)
		return fmt.Errorf("%d: already registered", id)
	}
	slog.Info("chanrpc register", "id", id)
	s.functions[id] = f
	return nil
}

// exec 执行单次 RPC 调用的核心逻辑：路由到处理函数、执行并回包。
//
// 防御性设计：通过 defer + recover 捕获处理函数内部抛出的 panic，
// 并在 panic 恢复后自动向调用方回包错误，防止业务逻辑异常导致调用方的 Call 永久阻塞。
// hasRet 的 CAS 检查确保 panic 恢复路径与正常执行路径互斥，不会产生重复响应。
//
// 注意：去重职责唯一地由 CallInfo.ret 内部的 CAS 承担。此处 defer 不能抢先 CAS，
// 否则会把 hasRet 置为 true 导致随后 ret 内部的 CAS 失败而不发回包，
// 使「handler panic」和「消息未注册」两条错误路径下的同步调用方永久阻塞（死锁）。
// 因此这里仅用无副作用的 Load 判断 handler 是否已回包：未回包时再调用 ret 兜底，
// ret 内部的 CAS 仍保证最终只发送一次响应。
func (s *Server) exec(ci *CallInfo) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
			} else {
				err = fmt.Errorf("panic: %v", r)
			}
			slog.Error("chanrpc exec panic", "id", ci.ID(), "err", err, "stack", string(debug.Stack()))
		}
		// 确保无论 panic 恢复还是未注册等错误路径，调用方都能收到响应，避免死锁。
		// 成功路径下 handler 已通过 ci.ret 回包（hasRet 已为 true），此处 Load 为 true 直接跳过，
		// 既不重复发送也不产生 "can not ret twice" 噪声日志。
		if !ci.hasRet.Load() {
			_ = ci.ret(&RetInfo{Err: err})
		}
	}()

	// 根据消息名在路由表中 O(1) 查找处理函数
	handler, ok := s.functions[ci.ID()]
	if !ok {
		err = fmt.Errorf("chanrpc %d not registered", ci.ID())
		return
	}

	ret := handler(ci)
	return ci.ret(ret)
}

// Exec 公开的消息执行入口，在模块的 OnRun 事件循环中逐一调用。
//
// 执行前将 hasRet 重置为 false，允许处理函数通过 CallInfo.ret 延迟响应
// （如异步等待数据库返回后再回包），而不强制在 handler 返回时立即响应。
func (s *Server) Exec(ci *CallInfo) {
	if ci == nil {
		slog.Warn("chanrpc exec callInfo is nil")
		return
	}
	ci.hasRet.Store(false)
	if err := s.exec(ci); err != nil {
		slog.Warn("error", "err", err)
	}
}

// IsClosed 检查服务端是否已关闭。
func (s *Server) IsClosed() bool {
	return s.closed.Load()
}

// Close 关闭服务端并清空消息队列，向所有积压的调用方回包 ErrServerClosed 错误。
//
// 使用 CompareAndSwap 保证 Close 的幂等性（重复调用安全，不会 panic）。
// 关闭流程：先将 closed 置为 true 阻断新调用写入 → 再关闭内部无界队列的发送端 →
// 最后排空队列中的积压消息并逐一回包，防止调用方因无响应而永久等待。
//
// 注意：内部队列 Close() 后立即遍历 Out() 是安全的，
// 缓冲区读尽后 Out() 会自动关闭，for range 不会阻塞。
func (s *Server) Close() {
	// CAS 保证只有第一次 Close 调用真正执行关闭逻辑，后续调用直接返回
	if !s.closed.CompareAndSwap(false, true) {
		slog.Warn("chanrpc server already closed")
		return
	}

	s.chanCall.Close()

	// 排空队列中尚未处理的调用，向每个调用方返回服务已关闭错误，避免死锁
	for ci := range s.chanCall.Out() {
		_ = ci.ret(&RetInfo{
			Err: ErrServerClosed,
		})
	}
}
