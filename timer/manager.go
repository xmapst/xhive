package timer

import (
	"fmt"
	"log/slog"
	"time"
)

// AccKind 定时器加速/延迟的计算方式枚举，决定 AccTimer/DelayTimer 的参数解释方式。
type AccKind int32

const (
	// AccAbs 按绝对毫秒值调整定时器，value 表示提前/推迟的毫秒数，必须大于 0。
	AccAbs AccKind = iota
	// AccPct 按万分比调整定时器，value 范围 [1, 10000]，
	// 表示对剩余时间乘以 (10000 ± value) / 10000 的比例系数。
	AccPct
)

const (
	// PctBase AccPct 模式的基数（万分之一），将百分比参数换算为实际比例。
	// 使用 10000 而非 100 是为了支持精度到 0.01% 的调整，满足游戏中精细化的时间控制需求。
	PctBase = 10000
)

// Handler 定时器触发回调函数类型。
//
// timerID 为触发的定时器 ID，metadata 为创建定时器时附加的业务元数据，
// 回调在调用方模块的事件循环 goroutine 中执行，可安全访问模块内部状态。
type Handler func(timerID int64, metadata map[string]string)

// Timer 业务层定时器的完整元数据。
//
// 设计上将业务语义（name、metadata）与调度数据（startTs、endTs、isTicker）分开存储，
// 业务层通过 TimerMgr 的 API 操作定时器，底层 Dispatcher 只负责时间轮调度，
// 两层通过 timerID 解耦，使业务逻辑与调度算法互不依赖。
type Timer struct {
	id       int64             // 定时器唯一 ID，与 Dispatcher 层共享同一 ID
	name     string            // 定时器业务类型，用于路由到对应的 TimerHandler
	startTs  int64             // 当前周期的起始毫秒时间戳，Ticker 续期时更新为上次触发时间
	endTs    int64             // 当前周期的期望触发毫秒时间戳
	isTicker bool              // true 表示周期性 Ticker，false 表示一次性 Timer
	metadata map[string]string // 业务元数据，创建时传入，每次回调时透传给 TimerHandler
}

// ID 返回定时器 ID。
func (t *Timer) ID() int64 {
	return t.id
}

// Name 返回定时器业务类型标识。
func (t *Timer) Name() string {
	return t.name
}

// StartTs 返回定时器当前周期的起始时间戳（毫秒）。
// 对 Ticker 而言，每次续期后此值更新为上次触发时间。
func (t *Timer) StartTs() int64 {
	return t.startTs
}

// EndTs 返回定时器期望触发的时间戳（毫秒）。
func (t *Timer) EndTs() int64 {
	return t.endTs
}

// IsTicker 返回该定时器是否为周期性 Ticker。
func (t *Timer) IsTicker() bool {
	return t.isTicker
}

// RangeMetadata 遍历定时器所有元数据键值对，回调返回 false 时提前终止遍历。
func (t *Timer) RangeMetadata(f func(string, string) bool) {
	for k, v := range t.metadata {
		if !f(k, v) {
			break
		}
	}
}

// Mgr 业务层定时器管理器，封装底层时间轮分发器，提供类型化的定时器 API。
//
// 设计特点：
//   - timers map 存储所有活跃定时器的业务元数据，handlers map 按 name 存储回调函数
//   - 所有方法均在调用方的单一 goroutine 中执行（Skeleton 事件循环），无并发竞争，无需加锁
//   - Ticker 的自动续期逻辑封装在 timerCommonCb 内，对业务代码完全透明
//   - 取消操作同步清理 timers map，防止元数据内存无限累积
type Mgr struct {
	timers     map[int64]*Timer   // timerID → 业务层定时器元数据
	handlers   map[string]Handler // name → 触发回调函数
	dispatcher *dispatcher        // 底层多级时间轮分发器
}

// NewMgr 创建定时器管理器，参数 l 为底层分发器的通道容量。
func NewMgr(l int) *Mgr {
	return &Mgr{
		timers:     make(map[int64]*Timer),
		handlers:   make(map[string]Handler),
		dispatcher: newDispatcher(l),
	}
}

// Register 注册指定 name 类型的定时器处理函数，同 name 只能注册一个处理器（后注册覆盖前者）。
func (tm *Mgr) Register(name string, handler Handler) {
	tm.handlers[name] = handler
}

// Run 启动底层时间轮分发器的后台 goroutine，必须在创建定时器之前调用。
func (tm *Mgr) Run() {
	tm.dispatcher.Run()
}

// Stop 停止底层时间轮分发器，发送停止信号后分发器主循环退出。
func (tm *Mgr) Stop() {
	tm.dispatcher.Stop()
}

// Event 返回定时器触发通知通道，供模块事件循环（Skeleton.OnRun）通过 select 监听。
func (tm *Mgr) Event() <-chan Event {
	return tm.dispatcher.chanFired
}

// Find 通过 ID 查询定时器业务层元数据，不存在时返回 nil。
func (tm *Mgr) Find(timerID int64) *Timer {
	return tm.timers[timerID]
}

// FindByName 通过 name 查找第一个匹配的定时器，适用于业务上每种 name 只有单一实例的场景。
func (tm *Mgr) FindByName(name string) *Timer {
	for _, timer := range tm.timers {
		if timer.name == name {
			return timer
		}
	}
	return nil
}

// commonCb 所有定时器的统一回调入口：查找对应的 TimerHandler 并执行，Ticker 在执行后自动续期。
//
// Ticker 续期算法：以上次触发时间（oldEndTs）作为新周期的起始点，
// 计算与当前周期相同的时间间隔（endTs - startTs）作为下次触发的延迟，
// 而非以"当前时间 + 间隔"计算，这样可以消除因回调处理耗时导致的周期漂移，
// 保证长期运行时 Ticker 的触发频率稳定。
func (tm *Mgr) commonCb(timerID int64) {
	t := tm.timers[timerID]
	if t == nil {
		slog.Warn("delay timer not found", "timer_id", timerID)
		return
	}
	if time.Now().UnixMilli() < t.endTs {
		slog.Error("delay timer end_ts bigger than now")
	}
	f, ok := tm.handlers[t.name]
	if !ok {
		slog.Error("delay timer handler not found", "name", t.name)
		return
	}
	defer func() {
		if t.isTicker {
			// 以上次触发时间为基准续期，消除累积漂移：interval = endTs - startTs
			oldEndTs := t.endTs
			t.endTs += t.endTs - t.startTs // 等价于 t.endTs = oldEndTs + (oldEndTs - t.startTs)
			t.startTs = oldEndTs           // 更新起始时间为上次触发时间，下次续期时继续使用稳定间隔
			tm.dispatcher.New(t.name, t.id, t.endTs, tm.commonCb)
		} else {
			// 一次性定时器触发后自动清理，防止元数据泄漏
			tm.Cancel(timerID)
		}
	}()
	f(timerID, t.metadata)
}

// timerOptions NewTimer 可选参数集合。
type timerOptions struct {
	id       int64
	metadata map[string]string
	isTicker bool
}

// Option NewTimer 的可选参数函数。
type Option func(*timerOptions)

// WithID 指定定时器 ID，不设置时由 dispatcher 自动分配。
func WithID(id int64) Option {
	return func(o *timerOptions) { o.id = id }
}

// WithMetadata 附加业务元数据，每次触发时透传给 Handler。
func WithMetadata(metadata map[string]string) Option {
	return func(o *timerOptions) { o.metadata = metadata }
}

// WithTicker 将定时器设置为周期性 Ticker，触发后自动续期直到被取消。
func WithTicker() Option {
	return func(o *timerOptions) { o.isTicker = true }
}

// New 创建并启动一个定时器，返回定时器 ID。
func (tm *Mgr) New(name string, duraMs int64, opts ...Option) int64 {
	o := &timerOptions{}
	for _, opt := range opts {
		opt(o)
	}

	_, ok := tm.handlers[name]
	if !ok {
		slog.Error("new timer handler not found", "name", name)
		return 0
	}
	if o.id != 0 && tm.timers[o.id] != nil {
		slog.Error("new timer id already exists", "timer_id", o.id)
		return 0
	}
	startTs := time.Now().UnixMilli()
	endTs := startTs + duraMs
	id := tm.dispatcher.New(name, o.id, endTs, tm.commonCb)
	tm.timers[id] = &Timer{
		id:       id,
		name:     name,
		startTs:  startTs,
		endTs:    endTs,
		metadata: o.metadata,
		isTicker: o.isTicker,
	}
	return id
}

// calcNewRemain 计算加速/延迟后的新剩余时间。acc=true 为加速（缩短），acc=false 为延迟（延长）。
func (tm *Mgr) calcNewRemain(remain int64, kind AccKind, value int64, acc bool) (int64, error) {
	if value <= 0 {
		return 0, fmt.Errorf("invalid value %d", value)
	}
	switch kind {
	case AccAbs:
		if acc {
			return max(0, remain-value), nil
		}
		return remain + value, nil
	case AccPct:
		if value > PctBase {
			return 0, fmt.Errorf("invalid pct value %d", value)
		}
		if acc {
			return remain * (PctBase - value) / PctBase, nil
		}
		return remain * (PctBase + value) / PctBase, nil
	default:
		return 0, fmt.Errorf("unknown AccKind %d", kind)
	}
}

// Acc 加速定时器，使其提前于原定时间触发。
//
// AccAbs 模式：新剩余时间 = max(0, 原剩余时间 - value)，加速后不允许触发时间早于当前时刻。
// AccPct 模式：新剩余时间 = 原剩余时间 × (10000 - value) / 10000，value 必须在 (0, 10000] 范围内。
func (tm *Mgr) Acc(id int64, kind AccKind, value int64) error {
	nowTs := time.Now().UnixMilli()
	t := tm.timers[id]
	if t == nil {
		return fmt.Errorf("acc timer failed, timer %v not found", id)
	}
	newRemain, err := tm.calcNewRemain(t.endTs-nowTs, kind, value, true)
	if err != nil {
		return fmt.Errorf("acc timer failed, %w", err)
	}
	newEndTs := nowTs + newRemain
	t.endTs = newEndTs
	tm.dispatcher.Update(t.name, id, newEndTs)
	return nil
}

// Delay 延迟定时器，将其触发时间推迟。
//
// AccAbs 模式：新剩余时间 = 原剩余时间 + value（毫秒），value 必须大于 0。
// AccPct 模式：新剩余时间 = 原剩余时间 × (10000 + value) / 10000，value 必须在 (0, 10000] 范围内。
func (tm *Mgr) Delay(id int64, kind AccKind, value int64) error {
	nowTs := time.Now().UnixMilli()
	t := tm.timers[id]
	if t == nil {
		return fmt.Errorf("delay timer failed, timer %v not found", id)
	}
	newRemain, err := tm.calcNewRemain(t.endTs-nowTs, kind, value, false)
	if err != nil {
		return fmt.Errorf("delay timer failed, %w", err)
	}
	newEndTs := nowTs + newRemain
	t.endTs = newEndTs
	tm.dispatcher.Update(t.name, id, newEndTs)
	return nil
}

// Update 直接设置定时器的绝对到期时间戳（毫秒），用于需要精确控制触发时刻的场景。
func (tm *Mgr) Update(id int64, endTs int64) {
	t := tm.timers[id]
	if t == nil {
		return
	}
	tm.dispatcher.Update(t.name, id, endTs)
}

// Cancel 取消定时器并同步清理业务层元数据。
//
// ID 为 0 的取消操作视为异常并记录错误日志后返回，
// 因为 ID=0 是 Dispatcher 的内置停止信号，业务层不应使用该 ID。
// 先调用 dispatcher.CancelTimer（双重取消：立即标记 + 异步删除），
// 再清理 timers map，保证内存不因无效的定时器元数据而持续增长。
func (tm *Mgr) Cancel(id int64) {
	if id == 0 {
		slog.Error("cancel timer id is zero")
		return
	}
	t := tm.timers[id]
	if t == nil {
		return
	}
	tm.dispatcher.Cancel(t.name, id)
	delete(tm.timers, id) // 同步清理业务层元数据，防止 map 内存泄漏
}
