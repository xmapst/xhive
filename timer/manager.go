package timer

import (
	"fmt"
	"log/slog"
	"time"
)

const (
	// PctBase 百分比调整模式的基数（万分之一），将百分比参数换算为实际比例。
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
// 设计上将业务语义（name、metadata）与调度数据（startAt、deadline、isTicker）分开存储，
// 业务层通过 Manager 的 API 操作定时器，底层 dispatcher 只负责最小堆调度，
// 两层通过 timerID 解耦，使业务逻辑与调度算法互不依赖。
type Timer struct {
	startAt  time.Time         // 当前周期的起始时刻，Ticker 续期时更新为上次触发时刻
	deadline time.Time         // 当前周期的期望触发时刻
	metadata map[string]string // 业务元数据，创建时传入，每次回调时透传给 Handler
	name     string            // 定时器业务类型，用于路由到对应的 Handler
	id       int64             // 定时器唯一 ID，与 dispatcher 层共享同一 ID
	isTicker bool              // true 表示周期性 Ticker，false 表示一次性 Timer
}

// ID 返回定时器 ID。
func (t *Timer) ID() int64 {
	return t.id
}

// Name 返回定时器业务类型标识。
func (t *Timer) Name() string {
	return t.name
}

// StartAt 返回定时器当前周期的起始时刻。
// 对 Ticker 而言，每次续期后此值更新为上次触发时刻。
func (t *Timer) StartAt() time.Time {
	return t.startAt
}

// Deadline 返回定时器期望触发的时刻。
func (t *Timer) Deadline() time.Time {
	return t.deadline
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

// Manager 业务层定时器管理器，封装底层最小堆分发器，提供类型化的定时器 API。
//
// 设计特点：
//   - timers map 存储所有活跃定时器的业务元数据，handlers map 按 name 存储回调函数
//   - 所有方法均在调用方的单一 goroutine 中执行（Skeleton 事件循环），无并发竞争，无需加锁
//   - Ticker 的自动续期逻辑封装在 commonCallback 内，对业务代码完全透明
//   - 取消操作同步清理 timers map，防止元数据内存无限累积
type Manager struct {
	timers     map[int64]*Timer   // timerID → 业务层定时器元数据
	handlers   map[string]Handler // name → 触发回调函数
	dispatcher *dispatcher        // 底层最小堆分发器
}

// NewManager 创建定时器管理器，参数 l 为底层分发器的通道容量。
func NewManager(l int) *Manager {
	return &Manager{
		timers:     make(map[int64]*Timer),
		handlers:   make(map[string]Handler),
		dispatcher: newDispatcher(l),
	}
}

// Register 注册指定 name 类型的定时器处理函数，同 name 只能注册一个处理器（后注册覆盖前者）。
func (tm *Manager) Register(name string, handler Handler) {
	tm.handlers[name] = handler
}

// Run 启动底层分发器的后台 goroutine，必须在创建定时器之前调用。
func (tm *Manager) Run() {
	tm.dispatcher.Run()
}

// Stop 停止底层分发器，发送停止信号后分发器主循环退出。
func (tm *Manager) Stop() {
	tm.dispatcher.Stop()
}

// Event 返回定时器触发通知通道，供模块事件循环（Skeleton.OnRun）通过 select 监听。
func (tm *Manager) Event() <-chan Event {
	return tm.dispatcher.chanFired
}

// Find 通过 ID 查询定时器业务层元数据，不存在时返回 nil。
func (tm *Manager) Find(timerID int64) *Timer {
	return tm.timers[timerID]
}

// FindByName 通过 name 查找第一个匹配的定时器，适用于业务上每种 name 只有单一实例的场景。
func (tm *Manager) FindByName(name string) *Timer {
	for _, timer := range tm.timers {
		if timer.name == name {
			return timer
		}
	}
	return nil
}

// commonCallback 所有定时器的统一回调入口：查找对应的 Handler 并执行，Ticker 在执行后自动续期。
//
// Ticker 续期算法：以上次触发时刻（oldDeadline）作为新周期的起始点，
// 计算与当前周期相同的时间间隔（deadline - startAt）作为下次触发的延迟，
// 而非以"当前时间 + 间隔"计算，这样可以消除因回调处理耗时导致的周期漂移，
// 保证长期运行时 Ticker 的触发频率稳定。
func (tm *Manager) commonCallback(timerID int64) {
	t := tm.timers[timerID]
	if t == nil {
		slog.Warn("delay timer not found", "timer_id", timerID)
		return
	}
	if time.Now().Before(t.deadline) {
		slog.Error("delay timer deadline bigger than now")
	}
	f, ok := tm.handlers[t.name]
	if !ok {
		slog.Error("delay timer handler not found", "name", t.name)
		return
	}
	defer func() {
		if t.isTicker {
			// 以上次触发时刻为基准续期，消除累积漂移：interval = deadline - startAt
			oldDeadline := t.deadline
			interval := t.deadline.Sub(t.startAt)
			t.startAt = oldDeadline                // 更新起始时刻为上次触发时刻，下次续期时继续使用稳定间隔
			t.deadline = oldDeadline.Add(interval) // 下次触发 = 上次触发 + 稳定间隔
			tm.dispatcher.New(t.name, t.id, t.deadline, tm.commonCallback)
		} else {
			// 一次性定时器触发后自动清理，防止元数据泄漏
			tm.Cancel(timerID)
		}
	}()
	f(timerID, t.metadata)
}

// timerOptions New 可选参数集合。
type timerOptions struct {
	metadata map[string]string
	id       int64
	isTicker bool
}

// Option New 的可选参数函数。
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

// New 创建并启动一个定时器，d 为相对当前时刻的延迟时长，返回定时器 ID。
func (tm *Manager) New(name string, d time.Duration, opts ...Option) int64 {
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
	startAt := time.Now()
	deadline := startAt.Add(d)
	id := tm.dispatcher.New(name, o.id, deadline, tm.commonCallback)
	tm.timers[id] = &Timer{
		id:       id,
		name:     name,
		startAt:  startAt,
		deadline: deadline,
		metadata: o.metadata,
		isTicker: o.isTicker,
	}
	return id
}

// adjustAbs 按绝对时长调整剩余时间。acc=true 为加速（缩短，不允许早于当前时刻），acc=false 为延迟（延长）。
func (tm *Manager) adjustAbs(remain, d time.Duration, acc bool) (time.Duration, error) {
	if d <= 0 {
		return 0, fmt.Errorf("invalid duration %v", d)
	}
	if acc {
		return max(0, remain-d), nil
	}
	return remain + d, nil
}

// adjustPct 按万分比调整剩余时间。pct 必须在 (0, PctBase] 范围内。
// acc=true 时新剩余 = remain × (PctBase-pct)/PctBase；acc=false 时 = remain × (PctBase+pct)/PctBase。
func (tm *Manager) adjustPct(remain time.Duration, pct int64, acc bool) (time.Duration, error) {
	if pct <= 0 || pct > PctBase {
		return 0, fmt.Errorf("invalid pct value %d", pct)
	}
	if acc {
		return remain * time.Duration(PctBase-pct) / PctBase, nil
	}
	return remain * time.Duration(PctBase+pct) / PctBase, nil
}

// reschedule 按 newRemain 重设定时器到期时刻，统一供四个加速/延迟方法复用。
//
// 同步平移 startAt：若只改 deadline 不改 startAt，会污染 commonCallback 中
// interval = deadline - startAt 的计算，导致 Ticker 后续每次续期的周期都
// 永久带上这次调整量（而不是仅这一次触发提前/延迟）。将 startAt 平移相同的
// delta，可以让 interval 保持数值不变，只有这一次触发的绝对时刻发生偏移，
// 后续周期恢复原有间隔——这才是"加速/延迟一次"应有的效果。
// 仅对 Ticker 生效：一次性定时器的 startAt 语义是"创建时刻"，不应被此操作
// 改写；startAt 本身也只在 Ticker 的续期计算中被读取，对一次性定时器无影响。
func (tm *Manager) reschedule(id int64, calc func(remain time.Duration) (time.Duration, error), what string) error {
	now := time.Now()
	t := tm.timers[id]
	if t == nil {
		return fmt.Errorf("%s timer failed, timer %v not found", what, id)
	}
	newRemain, err := calc(t.deadline.Sub(now))
	if err != nil {
		return fmt.Errorf("%s timer failed, %w", what, err)
	}
	newDeadline := now.Add(newRemain)
	if t.isTicker {
		delta := newDeadline.Sub(t.deadline)
		t.startAt = t.startAt.Add(delta)
	}
	t.deadline = newDeadline
	tm.dispatcher.Update(id, newDeadline)
	return nil
}

// AccAbs 按绝对时长加速定时器，使其提前触发。新剩余 = max(0, 原剩余 - d)，d 必须大于 0。
func (tm *Manager) AccAbs(id int64, d time.Duration) error {
	return tm.reschedule(id, func(remain time.Duration) (time.Duration, error) {
		return tm.adjustAbs(remain, d, true)
	}, "acc")
}

// DelayAbs 按绝对时长延迟定时器，使其推迟触发。新剩余 = 原剩余 + d，d 必须大于 0。
func (tm *Manager) DelayAbs(id int64, d time.Duration) error {
	return tm.reschedule(id, func(remain time.Duration) (time.Duration, error) {
		return tm.adjustAbs(remain, d, false)
	}, "delay")
}

// AccPct 按万分比加速定时器。新剩余 = 原剩余 × (PctBase - pct) / PctBase，pct 必须在 (0, PctBase] 范围内。
func (tm *Manager) AccPct(id int64, pct int64) error {
	return tm.reschedule(id, func(remain time.Duration) (time.Duration, error) {
		return tm.adjustPct(remain, pct, true)
	}, "acc")
}

// DelayPct 按万分比延迟定时器。新剩余 = 原剩余 × (PctBase + pct) / PctBase，pct 必须在 (0, PctBase] 范围内。
func (tm *Manager) DelayPct(id int64, pct int64) error {
	return tm.reschedule(id, func(remain time.Duration) (time.Duration, error) {
		return tm.adjustPct(remain, pct, false)
	}, "delay")
}

// Update 直接设置定时器的绝对到期时刻，用于需要精确控制触发时刻的场景。
// 对 Ticker 同步平移 startAt，理由与 reschedule 一致：避免污染续期周期。
func (tm *Manager) Update(id int64, deadline time.Time) {
	t := tm.timers[id]
	if t == nil {
		return
	}
	if t.isTicker {
		delta := deadline.Sub(t.deadline)
		t.startAt = t.startAt.Add(delta)
	}
	t.deadline = deadline
	tm.dispatcher.Update(id, deadline)
}

// Cancel 取消定时器并同步清理业务层元数据。
//
// ID 为 0 的取消操作视为异常并记录错误日志后返回，
// 因为 ID=0 是 dispatcher 的内置停止信号，业务层不应使用该 ID。
// 先调用 dispatcher.Cancel（双重取消：立即标记 + 异步删除），
// 再清理 timers map，保证内存不因无效的定时器元数据而持续增长。
func (tm *Manager) Cancel(id int64) {
	if id == 0 {
		slog.Error("cancel timer id is zero")
		return
	}
	t := tm.timers[id]
	if t == nil {
		return
	}
	tm.dispatcher.Cancel(id)
	delete(tm.timers, id) // 同步清理业务层元数据，防止 map 内存泄漏
}
