// Package chanrpc provides channel-based RPC primitives for actor-style modules.
package chanrpc

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/xmapst/xhive/chanx"
)

// BKDRBytesHash 使用 BKDR 哈希算法计算字节序列的哈希值。
//
// BKDR 算法以 131 为种子，通过多项式滚动哈希生成 uint32 值。
// 选用 131 作为种子是因为其在字符串哈希场景中具有良好的雪崩效应和均匀分布特性，
// 实践中碰撞率极低，非常适合用于消息类型 ID 的生成。
func BKDRBytesHash(b []byte) uint32 {
	seed := uint32(131)
	hash := uint32(0)

	for _, v := range b {
		hash = hash*seed + uint32(v)
	}
	return hash
}

// BKDRHashStr 计算字符串的 BKDR 哈希值，内部转换为字节序列后复用 BKDRBytesHash。
func BKDRHashStr(s string) uint32 {
	hashInt := BKDRBytesHash([]byte(s))
	return hashInt
}

// IMessage 允许消息结构体自定义其消息名的接口。
//
// 默认策略通过反射获取类型全限定名，但存在两种场景需要自定义：
//  1. 同一结构体在不同上下文中表示不同语义（复用结构体降低内存分配）
//  2. 需要与外部协议名对齐
type IMessage interface {
	// ID 返回消息结构体自定义的全局唯一消息 ID。
	ID() uint32
}

var (
	// idCache 缓存已通过反射计算过的消息类型 → ID 映射，避免重复的反射调用。
	idCache sync.Map // map[reflect.Type]uint32
)

// ID 根据消息对象的类型返回其全局唯一ID（包含包路径的全限定类型名）。
//
// 计算策略（优先级从高到低）：
//  1. 若消息实现了 IMessage 接口，直接调用其 ID() 方法（跳过缓存）
//  2. 否则，基于 BKDRHashStr(fmt.Sprintf("%s:%s", typ.PkgPath(), typ.String())) 返回全限定ID，
//     结果存入 sync.Map 缓存，后续同类型消息直接命中缓存，无需再次反射
//
// 指针类型自动解引用为元素类型（*T → T），保证 T 和 *T 共享同一个名称。
func ID(m any) uint32 {
	if m == nil {
		return 0
	}

	if message, ok := m.(IMessage); ok {
		return message.ID()
	}

	typ := reflect.TypeOf(m)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}

	if v, ok := idCache.Load(typ); ok {
		return v.(uint32)
	}

	id := BKDRHashStr(fmt.Sprintf("%s:%s", typ.PkgPath(), typ.String()))
	idCache.Store(typ, id)
	return id
}

var (
	// ErrServerClosed 表示 ChanRPC 服务端已关闭。
	ErrServerClosed = errors.New("chanrpc: server closed")
	// ErrClientClosed 表示 ChanRPC 客户端已关闭。
	ErrClientClosed = errors.New("chanrpc: client closed")
	// ErrServerNil 表示调用目标服务端为空。
	ErrServerNil = errors.New("chanrpc: server cannot be nil")
	// ErrCallbackNil 表示异步调用未提供回调函数。
	ErrCallbackNil = errors.New("chanrpc: callback cannot be nil")
	// ErrInvalidMsgType 表示请求消息类型无法生成有效消息 ID。
	ErrInvalidMsgType = errors.New("chanrpc: invalid message type")
	// ErrCallTimeout 为兼容旧调用方保留；当前同步 Call 采用无限等待加周期告警策略，不主动返回该错误。
	ErrCallTimeout = errors.New("chanrpc: call timeout waiting for response")
	// ErrRetDropped 表示响应未能投递给调用方。
	ErrRetDropped = errors.New("chanrpc: ret dropped, caller already gone")
	// ErrRegisterMsgNil 表示注册 RPC 处理器时消息样例为空。
	ErrRegisterMsgNil = errors.New("chanrpc: register message cannot be nil")
	// ErrRegisterHandlerNil 表示注册 RPC 处理器时处理函数为空。
	ErrRegisterHandlerNil = errors.New("chanrpc: register handler cannot be nil")
	// ErrCallChannelNil 表示 RPC 调用队列为空。
	ErrCallChannelNil = errors.New("chanrpc: call channel is nil")
	// ErrCallInfoNil 表示 RPC 调用上下文为空。
	ErrCallInfoNil = errors.New("chanrpc: call CallInfo is nil")
)

// CallOption 配置单次调用的可选参数。
type CallOption func(*callOpts)

type callOpts struct {
	metadata map[string]any
}

func (c *Client) applyOpts(opts ...CallOption) *callOpts {
	o := &callOpts{
		metadata: make(map[string]any),
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// WithMeta 向调用附加一个元数据键值对，可多次调用叠加多个 key。
func WithMeta(key string, value any) CallOption {
	return func(o *callOpts) {
		if o.metadata == nil {
			o.metadata = make(map[string]any)
		}
		o.metadata[key] = value
	}
}

// Handler RPC 消息处理函数类型，接收调用信息并返回结果信息。
//
// 处理函数在 Server 所在模块的 goroutine 中串行执行，天然保证对模块内部状态的无锁访问。
// 若处理逻辑无需向调用方回包（如 Cast），可返回 nil。
type Handler func(ci *CallInfo) (ri *RetInfo)

// Callback 异步调用的回调函数类型，在 Client 所在模块的 goroutine 中执行。
//
// 回调与业务逻辑运行在同一 goroutine，无需为访问模块状态加锁，简化了并发编程模型。
type Callback func(ri *RetInfo)

// retSink 是 CallInfo 投递响应结果的目的地，屏蔽同步调用与异步调用在
// 底层通道上的差异，使 CallInfo.ret 无需关心自己面对的是哪一种。
//
// send 返回 false 表示响应未能投递（例如同步 Call 的一次性响应槽已满，
// 或调用方不再有能力消费该响应），调用方应将其视为“已丢弃”而非“需要重试”。
type retSink interface {
	send(ri *RetInfo) bool
}

// syncRet 是同步 Call 使用的一次性响应槽：容量为 1，写一次读一次即弃，
// 无需 goroutine，天然被 GC 回收。
//
// send 非阻塞：若响应槽已满，直接返回 false，不重试、不阻塞，
// 防止服务端回包路径被调用方状态反向拖住。
type syncRet chan *RetInfo

func newSyncRet() syncRet { return make(syncRet, 1) }

func (r syncRet) send(ri *RetInfo) bool {
	select {
	case r <- ri:
		return true
	default:
		return false
	}
}

// asyncRet 包装 Client 的异步返回队列，用于异步调用（AsyncCall）的响应投递。
//
// send 直接写入 chanx.Unbounded.In()：该操作按设计永不阻塞，唯一的失败
// 模式是目标队列已被 Client.Close 关闭，此时会 panic，交由调用方
// （CallInfo.ret）的 recover 统一处理，因此这里总是返回 true。
type asyncRet struct {
	u *chanx.Unbounded[*RetInfo]
}

func (r asyncRet) send(ri *RetInfo) bool {
	r.u.In() <- ri
	return true
}

// CallInfo 封装一次 RPC 调用的完整上下文信息。
//
// chanRet 和 callback 配合使用：同步调用时 chanRet 为 syncRet（一次性响应槽），
// callback 为 nil；异步调用时 chanRet 包装 Client 的异步返回队列，callback 为
// 调用方注册的回调；Cast 时两者均为 nil，Server 处理后不做任何响应。
//
// hasRet 通过 atomic.Bool 的 CAS 语义实现防重复响应：
// 正常路径和 panic 恢复路径都会尝试响应，CAS 保证只有第一次成功。
type CallInfo struct {
	Request  any            `json:"request"` // 请求数据，业务 handler 的输入
	chanRet  retSink        // 响应投递目的地：同步为 syncRet，异步为 asyncRet，Cast 时为 nil
	callback Callback       // 异步调用的回调函数，同步调用时为 nil
	metadata map[string]any // 元数据
	id       uint32         // 消息类型全限定ID，用于路由到对应的 Handler
	hasRet   atomic.Bool    // 防重复响应标志，通过 CAS 操作保证并发安全
}

// ret 向调用方发送响应结果，通过 hasRet CAS 防止同一次调用被重复响应。
//
// 投递本身不会阻塞（retSink 的两种实现都不阻塞）：
//   - syncRet 满/无人接收时返回 false，转化为 ErrRetDropped；
//   - asyncRet 只有在目标队列已被 Client.Close 关闭时才会失败，
//     表现为 panic，由下面的 recover 捕获并转化为 error。
//
// 若 chanRet 为 nil（Cast 调用），直接返回 nil，不做任何操作。
func (ci *CallInfo) ret(ri *RetInfo) (err error) {
	if ci.chanRet == nil {
		return nil
	}

	// CompareAndSwap(false → true) 保证只有第一次 ret 调用成功，后续调用均被忽略
	if !ci.hasRet.CompareAndSwap(false, true) {
		slog.Warn("chanrpc can not ret twice", "id", ci.ID(), "stack", string(debug.Stack()))
		return
	}

	// 捕获向已关闭队列发送时触发的 panic
	// （Server.Close 或 Client.Close 已回收对应队列后，仍有调用在处理中）
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
			slog.Error("chanrpc ret error", "id", ci.ID(), "err", err, "stack", string(debug.Stack()))
		}
	}()

	if ri == nil {
		ri = new(RetInfo)
	}
	if ci.metadata == nil {
		ci.metadata = make(map[string]any)
	}
	if ri.Metadata == nil {
		ri.Metadata = make(map[string]any)
	}

	// 将回调函数附加到响应对象，由 Client.AsyncCallback 在调用方 goroutine 中执行
	ri.callback = ci.callback
	// 拷贝元数据
	maps.Copy(ri.Metadata, ci.metadata)

	if !ci.chanRet.send(ri) {
		return fmt.Errorf("%w: id=%d", ErrRetDropped, ci.ID())
	}
	return nil
}

// ID 返回本次调用的消息类型全限定ID。
func (ci *CallInfo) ID() uint32 {
	return ci.id
}

// RetInfo 封装 RPC 调用的响应数据，同时作为异步回调的上下文载体。
type RetInfo struct {
	Metadata map[string]any `json:"Metadata"` // 元数据
	Ack      any            `json:"Ack"`      // 响应业务数据，作为 Callback 的输入参数
	Err      error          `json:"Err"`      // 调用或处理过程中发生的错误
	callback Callback       // 异步回调函数引用，由 Client.AsyncCallback 触发执行
}

// ID 返回响应数据（Ack）的类型全限定ID，用于异步回调场景下的统计。
//
// 当 Ack 为 nil 或调用本身存在错误时，返回空字符串。
func (ri *RetInfo) ID() uint32 {
	if ri.Err != nil || ri.Ack == nil {
		return 0
	}
	return ID(ri.Ack)
}
