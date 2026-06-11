package xhive

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/xmapst/xhive/chanrpc"
	"github.com/xmapst/xhive/stat"
	"github.com/xmapst/xhive/timer"
)

// IRPC 定义跨模块 RPC 调用的接口，提供三种调用语义覆盖不同并发场景。
//
// 调用模式对比：
//   - Cast：单向投递，无响应，吞吐最高，适合通知/事件
//   - AsyncCall：异步调用，回调在调用方 goroutine 执行，无锁安全，推荐使用
//   - Call：同步阻塞，有死锁风险，仅在确认无循环依赖时使用
type IRPC interface {
	// Cast 单向消息投递，不等待结果，适合日志上报、事件通知等不需要响应的场景。
	Cast(mod string, req any, opts ...chanrpc.CallOption)
	// Call 同步 RPC 调用，阻塞等待对端处理完成并返回结果。
	// 警告：若调用链形成环（A→B→A），将导致死锁，生产环境应优先使用 AsyncCall。
	Call(mod string, req any, opts ...chanrpc.CallOption) *chanrpc.RetInfo
	// AsyncCall 异步 RPC 调用，立即返回，结果通过 cb 回调在调用方 goroutine 处理。
	// 回调在事件循环中串行执行，可安全访问模块内部状态，无需加锁。
	AsyncCall(mod string, req any, cb chanrpc.Callback, opts ...chanrpc.CallOption) error
}

// ITimer 定义定时器管理接口，支持一次性定时器和周期性 Ticker 的完整生命周期管理。
type ITimer interface {
	// RegisterTimer 注册指定类型定时器的处理函数，同 name 仅能注册一个处理器（后注册覆盖前者）。
	RegisterTimer(name string, handler timer.Handler)
	// NewTimer 创建并启动一个定时器，返回定时器 ID。
	NewTimer(name string, duraMs int64, opts ...timer.Option) int64
	// AccTimer 加速指定定时器，提前其触发时间。
	AccTimer(id int64, kind timer.AccKind, value int64) error
	// DelayTimer 延迟指定定时器，推迟其触发时间。
	DelayTimer(id int64, kind timer.AccKind, value int64) (err error)
	// UpdateTimer 直接设置定时器的绝对到期时间戳（毫秒），用于需要精确控制触发时刻的场景
	UpdateTimer(id int64, endTs int64)
	// CancelTimer 取消指定 ID 的定时器，对已触发或已取消的定时器调用是安全的（幂等）。
	CancelTimer(id int64)
}

// Skeleton 模块骨架，将 ChanRPC（服务端/客户端）和定时器管理器整合为统一的事件驱动框架。
//
// 核心设计思想（Actor 模型）：
// 所有事件（RPC 调用、异步回调、定时器）在单一 goroutine（OnStart）中串行处理，
// 彻底消除模块内部的并发竞争，开发者无需为访问模块状态加任何锁，极大降低了复杂度。
//
// 使用方式：业务模块内嵌 Skeleton，重写 OnInit 注册处理函数，重写 OnDestroy 清理资源，
// 无需重写 OnStart 和 ChanRPC（Skeleton 已提供默认实现）。
type Skeleton struct {
	name   string
	timer  *timer.Mgr      // 定时器管理器，负责创建、调度和取消定时任务
	server *chanrpc.Server // ChanRPC 服务端，接收并路由来自其他模块的 RPC 调用
	client *chanrpc.Client // ChanRPC 客户端，向其他模块发起 RPC 调用
	stat   *stat.TPStats   // 消息耗时统计
}

const timerKindDumpStat = "TimerKindDumpStat"

// NewSkeleton 创建模块骨架，初始化 ChanRPC 和定时器组件。
//
// 各组件缓冲区均为 10000，适合高并发游戏服务器场景下的消息吞吐需求。
// 若某模块的消息量远超此值，需根据业务峰值流量调整，过小会导致背压和调用方超时。
func NewSkeleton(name string) *Skeleton {
	s := &Skeleton{
		name:   name,
		timer:  timer.NewMgr(100000),
		server: chanrpc.NewServer(100000),
		client: chanrpc.NewClient(100000),
		stat:   stat.NewTPStats(100000),
	}
	return s
}

// Name 返回模块名称，实现 IModule.Name 接口。
func (s *Skeleton) Name() string {
	return s.name
}

// OnRun 启动模块事件循环，阻塞至 ctx 被取消（即框架调用 cancel）。
//
// 事件循环采用 select 多路复用以下三类事件，保证在单一 goroutine 内串行处理：
//  1. ctx.Done()：接收框架的停止信号，触发模块关闭流程
//  2. ChanAsyncRet：处理本模块发起的异步 RPC 调用的返回结果（执行注册的 Callback）
//  3. ChanCall：处理其他模块发来的 RPC 调用请求（查找并执行已注册的 Handler）
//  4. ChanTimer：处理到期的定时器事件（执行注册的 TimerHandler，并自动续期 Ticker）
//
// 单 goroutine 串行处理是性能与正确性权衡的结果：
// 牺牲了 CPU 并行利用率，换取了零锁开销和极低的编程复杂度。
func (s *Skeleton) OnRun(ctx context.Context) {
	s.timer.Run()
	s.RegisterTimer(timerKindDumpStat, func(_ int64, _ map[string]string) {
		s.dumpStat(true)
		s.scheduleDumpTimer()
	})
	s.scheduleDumpTimer()
	for {
		select {
		case <-ctx.Done():
			s.close()
			slog.Info("skeleton stopped", "name", s.name)
			return
		case t := <-s.timer.Event():
			startUs := time.Now().UnixMicro()
			t.Cb()
			s.recordStat(t.Name(), time.Now().UnixMicro()-startUs)
		case ri := <-s.client.ChanAsyncRet:
			startUs := time.Now().UnixMicro()
			s.client.AsyncCallback(ri)
			s.recordStat(ri.ID(), time.Now().UnixMicro()-startUs)
		case ci := <-s.server.ChanCall:
			startUs := time.Now().UnixMicro()
			s.server.Exec(ci)
			s.recordStat(ci.ID(), time.Now().UnixMicro()-startUs)
		}
	}
}

// close 在模块退出前有序清理资源：停止定时器 → 关闭 RPC 服务端 → 等待异步调用完成。
//
// 轮询等待异步回调（!Idle）：直到所有发出的异步调用都收到响应并执行完回调，
// 防止未处理的回调在模块销毁后被执行时访问已释放的资源。
// 每次调用 client.Close 会处理当前 ChanAsyncRet 中的回调，Idle 检查保证全部处理完毕才退出。
func (s *Skeleton) close() {
	s.dumpStat(false)
	s.timer.Stop()
	s.server.Close()
	// 循环等待，直到客户端所有异步回调都处理完毕（Idle），防止未处理的回调泄漏
	for !s.client.Idle() {
		s.client.Close()
		slog.Info("skeleton client close", "name", s.Name())
	}
}

// scheduleDumpTimer 计算下一个触发时刻并创建一次性定时器，错峰 30s到60s 随机抖动
func (s *Skeleton) scheduleDumpTimer() {
	// 每15分钟整点执行
	nowTs := time.Now().UnixMilli()
	nextTs := s.dayStartTs(nowTs)
	for nextTs <= nowTs {
		nextTs += timer.HourMs
	}
	// 增加一个随机30s到60s之间的随机变量来错峰
	jitter := rand.Int64N(30000) + 30000
	s.NewTimer(timerKindDumpStat, nextTs-nowTs+jitter)
}

func (s *Skeleton) dayStartTs(nowMs int64) int64 {
	now := time.Unix(nowMs/timer.SecMs, (nowMs%timer.SecMs)*int64(time.Millisecond))
	nowDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	return nowDay.UnixMilli()
}

// dumpStat dump 并可选重置统计
func (s *Skeleton) dumpStat(reset bool) {
	slog.Info("dump stat", "name", s.name, "stat", s.stat.Dump(100))
	if reset {
		s.stat.Reset()
	}
}

// recordStat 记录一次事件的耗时（微秒）
func (s *Skeleton) recordStat(name any, costUs int64) {
	if s.stat != nil {
		s.stat.Add(name, costUs)
	}
}

// RegisterTimer 注册指定 kind 类型的定时器处理函数，通常在 OnInit 中调用完成所有注册。
func (s *Skeleton) RegisterTimer(name string, handler timer.Handler) {
	s.timer.Register(name, handler)
}

// NewTimer 创建并启动一个定时器，返回定时器 ID。
func (s *Skeleton) NewTimer(name string, duraMs int64, opts ...timer.Option) int64 {
	return s.timer.New(name, duraMs, opts...)
}

// AccTimer 按指定方式加速定时器，提前其触发时间。
func (s *Skeleton) AccTimer(id int64, kind timer.AccKind, value int64) error {
	return s.timer.Acc(id, kind, value)
}

// DelayTimer 按指定方式延迟定时器，推迟其触发时间。
func (s *Skeleton) DelayTimer(id int64, kind timer.AccKind, value int64) error {
	return s.timer.Delay(id, kind, value)
}

// UpdateTimer 直接设置定时器的绝对到期时间戳（毫秒），用于需要精确控制触发时刻的场景
func (s *Skeleton) UpdateTimer(id int64, endTs int64) {
	s.timer.Update(id, endTs)
}

// CancelTimer 取消指定 ID 的定时器，同时清理业务层元数据，对已触发/已取消的定时器调用安全（幂等）。
func (s *Skeleton) CancelTimer(id int64) {
	s.timer.Cancel(id)
}

// ChanRPC 返回模块的 ChanRPC 服务端，供框架注册到模块映射表，以及外部模块通过 ChanRPC 获取后投递消息。
func (s *Skeleton) ChanRPC() *chanrpc.Server {
	return s.server
}

// RegisterChanRPC 注册 RPC 消息处理函数，通过 msg 的类型自动推导消息 ID 并完成路由注册。
//
// 通常在 OnInit 中批量注册，注册完成后路由表不再变更，访问无需加锁。
func (s *Skeleton) RegisterChanRPC(msg any, f chanrpc.Handler) error {
	return s.server.Register(msg, f)
}

// AsyncCall 向指定模块发起异步 RPC 调用，结果通过 cb 回调在本模块事件循环中执行。
//
// 回调在 OnStart 的 select 循环中消费 ChanAsyncRet 时执行，
// 与模块其他事件处理串行，无并发问题，可安全访问模块内部状态。
func (s *Skeleton) AsyncCall(mod string, req any, cb chanrpc.Callback, opts ...chanrpc.CallOption) error {
	server := defaultApp.ChanRPC(mod)
	return s.client.AsyncCall(server, req, cb, opts...)
}

// Cast 向指定模块投递单向消息，不等待响应，适合日志记录、事件通知等无需确认的场景。
func (s *Skeleton) Cast(mod string, req any, opts ...chanrpc.CallOption) {
	server := defaultApp.ChanRPC(mod)
	s.client.Cast(server, req, opts...)
}

// Call 向指定模块发起同步 RPC 调用，阻塞当前模块的事件处理直到收到响应。
//
// 危险提示：Call 会阻塞本模块对其他消息的处理；
// 若 A 调用 B，同时 B 也在等待 A 的响应，则形成死锁，需通过仔细的调用关系分析来规避。
// 在事件循环中应优先使用 AsyncCall，仅在调用关系明确单向且不存在环路时才使用 Call。
func (s *Skeleton) Call(mod string, req any, opts ...chanrpc.CallOption) *chanrpc.RetInfo {
	server := defaultApp.ChanRPC(mod)
	return s.client.Call(server, req, opts...)
}

// DumpStat 获取前n个处理耗时最长的消息
func (s *Skeleton) DumpStat(n int) string {
	return s.stat.Dump(n)
}
