// Package timer 提供基于「单 goroutine 派发 + 最小堆」的定时器管理能力。
//
// 设计思路：所有定时器条目按到期时刻组织进一个最小堆（container/heap），由唯一的派发
// goroutine 维护。派发 goroutine 阻塞等待「堆顶最近的到期时刻」，醒来后批量投递所有已到期
// 的定时器到 chanFired，真正的业务回调由消费方（Skeleton.OnRun）的单一 goroutine 执行。
//
// 相比方案演进中的两种备选：
//   - 相比多级时间轮：精度无损（不被 tick 粒度钉死），无「最大定时时长」上限，且无 tick 空转；
//   - 相比 time.AfterFunc：到期触发不再为每个定时器 spawn 一个 runtime goroutine——无论
//     1 个还是十万个定时器同时到期，派发始终只用这一个 goroutine，消除了海量定时器扎堆到期
//     时的 goroutine 尖峰。投递本身是纳秒级 channel 发送，串行成本远低于上千 goroutine 的
//     调度与抢锁开销。
//
// 并发模型：
//   - 最小堆 heap 仅由派发 goroutine 访问，无需加锁；
//   - 索引表 entries（id → *entry）同时被外部调用方（New/Update/Cancel）与派发 goroutine
//     访问，由 mu 保护，临界区仅一次 map 读写；
//   - 新建经 chanNew 送入完整 entry，加速/延迟/取消经 chanOp 送入轻量 command 值，
//     二者都串行化到派发 goroutine，保证堆的单线程访问；
//   - chanNew/chanOp/chanFired 均为无界队列：外部调用方（可能就是业务事件循环自身）
//     向 chanNew/chanOp 发送永不阻塞，派发 goroutine 向 chanFired 投递也永不阻塞。
//     这避免了一条真实存在的死锁路径——若某个 RPC handler 内同步调用 New/Update
//     创建或调整定时器，而对应队列恰好写满，事件循环会阻塞在发送上；若此时派发
//     goroutine 也恰好阻塞在向写满的 chanFired 投递（只有事件循环回到 select 顶层
//     才能消费），双方就会互相等待、永久卡死。换成无界队列后，第一步永不阻塞，
//     这条死锁链被从根上切断。
//   - 取消采用「同步标记 + 异步删堆」：Cancel 立即置 entry.canceled，使已投递到 chanFired
//     但尚未消费的到期事件也能在 Callback 中被过滤。
//   - 无界队列的代价：三个队列各自持有一个常驻转发 goroutine，必须显式 Close 才会
//     退出。run() 是这三个队列在读取方向上唯一的所有者，因此由它在退出时统一负责
//     关闭；New/Update/Cancel 在 Stop 之后可能仍被业务代码调用，此时向已关闭队列
//     发送会 panic，均以 recover 兜底，行为退化为「静默丢弃」，不会导致进程崩溃。
package timer

import (
	"container/heap"
	"context"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xmapst/xhive/chanx"
)

// Event 定时器触发事件接口，供 Manager 消费者调用。
type Event interface {
	// Callback 执行定时器回调，内部捕获 panic，执行后释放回调引用。
	Callback()
	// Name 返回定时器业务类型标识，用于统计耗时。
	Name() string
}

// opKind 派发器操作类型，区分发送到 chanOp 的命令语义。
type opKind int8

const (
	opUpdate opKind = iota // 更新到期时刻（加速/延迟）
	opCancel               // 取消定时器
)

// command 加速/延迟/取消操作的轻量载体，按值经 chanOp 送入派发 goroutine。
//
// 仅携带定位与改期所需的最小信息——不再像旧实现那样为一次操作 new 一个完整 entry，
// deadline 仅 opUpdate 使用，opCancel 留零值。
type command struct {
	deadline time.Time // 新到期时刻，仅 opUpdate 使用
	id       int64     // 目标定时器 ID
	op       opKind    // 操作类型，决定 doOp 的处理分支
}

// entry 最小堆中的一条定时器记录，同时复用为投递到 chanFired 的 Event。
type entry struct {
	deadline time.Time   // 到期绝对时刻
	callback func(int64) // 到期回调函数
	name     string      // 做消息统计用
	id       int64       // 定时器唯一 ID
	index    int         // 在最小堆中的下标，-1 表示不在堆中
	canceled atomic.Bool // 取消标记，已投递但尚未消费的事件据此被过滤
}

// Callback 安全执行定时器回调，通过 recover 捕获 panic 防止单个回调异常崩溃整个进程。
//
// 若定时器在事件投递后、消费前被取消，则跳过回调。
// 执行后将 callback 置 nil，主动释放回调函数闭包捕获的资源引用，帮助 GC 及时回收。
func (e *entry) Callback() {
	defer func() {
		e.callback = nil // 主动断开引用，允许 GC 回收回调捕获的外部资源（如模块的大对象）
		if r := recover(); r != nil {
			slog.Error("timer callback panic", "panic", r, "stack", string(debug.Stack()))
		}
	}()
	if e.canceled.Load() {
		return // 事件已在通道中排队但定时器已被取消，过滤掉
	}
	e.callback(e.id)
}

func (e *entry) Name() string {
	return e.name
}

// entryHeap 按 deadline 升序排列的最小堆，实现 container/heap.Interface。
type entryHeap []*entry

func (h entryHeap) Len() int           { return len(h) }
func (h entryHeap) Less(i, j int) bool { return h[i].deadline.Before(h[j].deadline) }
func (h entryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *entryHeap) Push(x any) {
	e := x.(*entry)
	e.index = len(*h)
	*h = append(*h, e)
}

func (h *entryHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil // 避免内存泄漏
	e.index = -1
	*h = old[:n-1]
	return e
}

// dispatcher 基于单 goroutine 派发 + 最小堆的定时器分发器。
type dispatcher struct {
	chanFired *chanx.Unbounded[Event]   // 到期通知队列，由 run() 发送，Manager 消费
	chanNew   *chanx.Unbounded[*entry]  // 新建定时器队列，外部调用方发送，run() 消费
	chanOp    *chanx.Unbounded[command] // 加速/延迟/取消命令队列，外部调用方发送，run() 消费
	done      chan struct{}             // Stop 时关闭，通知派发循环退出
	entries   map[int64]*entry          // timerID → 条目，供外部 Update/Cancel 定位堆中条目
	heap      entryHeap                 // 最小堆，仅派发 goroutine 访问
	atomicID  atomic.Int64              // 定时器唯一 ID 生成器
	stopOnce  sync.Once                 // 保证 done 只关闭一次
	closed    atomic.Bool               // Stop 后置真，New/Update/Cancel 据此提前拒绝新请求
	mu        sync.Mutex                // 保护 entries 索引表的并发访问
}

// newDispatcher 创建并初始化分发器。
//
// 参数 bufSize 是三个内部无界队列的初始容量提示，用于减少高频场景下的反复扩容，
// 不再是硬性上限：New/Update/Cancel 永不因队列满而阻塞或失败。
func newDispatcher(bufSize int) *dispatcher {
	if bufSize <= 0 {
		bufSize = 10000
	}
	ctx := context.Background()
	return &dispatcher{
		chanFired: chanx.NewUnbounded[Event](ctx, chanx.WithInitialCapacity(bufSize)),
		chanNew:   chanx.NewUnbounded[*entry](ctx, chanx.WithInitialCapacity(bufSize)),
		chanOp:    chanx.NewUnbounded[command](ctx, chanx.WithInitialCapacity(bufSize)),
		done:      make(chan struct{}),
		entries:   make(map[int64]*entry),
	}
}

// Run 在独立 goroutine 中启动派发主循环。
func (disp *dispatcher) Run() {
	go disp.run()
}

// run 派发主循环：始终将定时器重置为「堆顶最近的到期时刻」，并通过 select 多路复用
// 增删改命令、堆顶到期、停止信号三类事件，全程仅此一个 goroutine 访问堆。
//
// 退出时统一关闭三个内部队列：run() 是它们在读取方向上唯一的所有者，也是唯一
// 知道「此刻不会再有并发读取」的一方，由它负责关闭可以避免转发 goroutine 泄漏。
func (disp *dispatcher) run() {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("timer dispatcher crashed", "panic", r, "stack", string(debug.Stack()))
		}
		disp.chanFired.Close()
		disp.chanNew.Close()
		disp.chanOp.Close()
	}()

	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()

	for {
		var wait <-chan time.Time
		if len(disp.heap) > 0 {
			d := max(time.Until(disp.heap[0].deadline), 0)
			timer.Reset(d) // Go 1.23+ 的 Reset/Stop 不再需要 drain channel
			wait = timer.C
		}

		select {
		case e := <-disp.chanNew.Out():
			timer.Stop() // 堆将变化，本轮的等待作废，下轮重新计算
			disp.doNew(e)
		case cmd := <-disp.chanOp.Out():
			timer.Stop()
			disp.doOp(cmd)
		case <-wait:
			disp.fireExpired()
		case <-disp.done:
			return
		}
	}
}

// doNew 在派发 goroutine 中将新建的 entry 放入最小堆。
func (disp *dispatcher) doNew(e *entry) {
	if e.canceled.Load() {
		return // 入堆前已被取消，忽略
	}
	heap.Push(&disp.heap, e)
}

// doOp 在派发 goroutine 中执行加速/延迟/取消命令，维护最小堆与 entries 索引表。
//
// opUpdate 无条件先更新 e.deadline，只有已入堆（index >= 0）才额外 heap.Fix：
// e 和 chanNew 里尚未处理的那个 entry 是同一个指针，即便 New 对应的插堆操作
// 还没被 doNew 处理，这里提前写入的 deadline 也会被稍后的插堆操作直接带上，
// 不依赖 chanNew/chanOp 两个 channel 谁先被 select 到，从根上避免"创建后立即
// 调整"的竞态丢更新问题。
func (disp *dispatcher) doOp(cmd command) {
	switch cmd.op {
	case opUpdate:
		disp.mu.Lock()
		e, ok := disp.entries[cmd.id]
		disp.mu.Unlock()
		if !ok {
			slog.Error("update timer not found", "timer_id", cmd.id)
			return
		}
		e.deadline = cmd.deadline
		if e.index >= 0 {
			heap.Fix(&disp.heap, e.index)
		}

	case opCancel:
		disp.mu.Lock()
		e, ok := disp.entries[cmd.id]
		if ok {
			delete(disp.entries, cmd.id)
		}
		disp.mu.Unlock()
		if ok && e.index >= 0 {
			heap.Remove(&disp.heap, e.index)
		}
	}
}

// fireExpired 弹出所有已到期（deadline ≤ now）的定时器并投递到 chanFired。
//
// chanFired 是无界队列，In() 永不阻塞，因此不再需要 select + <-disp.done 兜底——
// 不存在"投递卡住导致派发循环停摆、进而拖死等待 chanNew/chanOp 的业务 goroutine"
// 的情况了。已取消的条目跳过投递。这是单 goroutine 串行投递——无论多少定时器
// 同时到期，都由本 goroutine 顺序处理，不会 spawn 额外 goroutine。
func (disp *dispatcher) fireExpired() {
	now := time.Now()
	for len(disp.heap) > 0 && !disp.heap[0].deadline.After(now) {
		e := heap.Pop(&disp.heap).(*entry)
		disp.mu.Lock()
		delete(disp.entries, e.id)
		disp.mu.Unlock()
		if e.canceled.Load() {
			continue
		}
		disp.chanFired.In() <- e
	}
}

// Event 返回定时器触发通知的只读通道，供 Manager.Event 透传给业务事件循环消费。
func (disp *dispatcher) Event() <-chan Event {
	return disp.chanFired.Out()
}

// Stop 通知派发主循环退出。
//
// 三个内部队列的 Close 由 run() 自身的 defer 负责，不在这里做——避免和仍在
// 运行的 run() goroutine 并发关闭同一队列引发数据竞争。
func (disp *dispatcher) Stop() {
	disp.stopOnce.Do(func() {
		disp.closed.Store(true)
		close(disp.done)
	})
}

// New 创建定时器并放入最小堆，timerID 为 0 时自动生成全局唯一 ID。
//
// 若 dispatcher 已经 Stop，提前返回 0（与「未注册 handler」「ID 已存在」等既有
// 错误场景使用同一套哨兵值语义，调用方无需区分）。
// panic 恢复：Stop 和本次调用之间存在极小的竞态窗口——closed 标记之后、chanNew
// 被 run() 关闭之前——此时仍可能向已关闭队列发送而 panic；recover 后回滚已经
// 写入 entries 的记录，避免残留一条永远不会被处理的孤儿元数据。
func (disp *dispatcher) New(name string, timerID int64, deadline time.Time, callback func(int64)) (result int64) {
	if disp.closed.Load() {
		slog.Warn("dispatcher already stopped, drop new timer", "name", name)
		return 0
	}
	if timerID == 0 {
		timerID = disp.atomicID.Add(1)
	}
	e := &entry{name: name, id: timerID, callback: callback, deadline: deadline, index: -1}

	disp.mu.Lock()
	if old, ok := disp.entries[timerID]; ok {
		old.canceled.Store(true) // 同 ID 重建：标记旧条目，使其触发时被过滤
	}
	disp.entries[timerID] = e
	disp.mu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			slog.Warn("dispatcher new timer after stop", "name", name, "panic", r)
			disp.mu.Lock()
			delete(disp.entries, timerID)
			disp.mu.Unlock()
			result = 0
		}
	}()

	disp.chanNew.In() <- e
	return timerID
}

// Update 更新定时器的到期时刻，用于加速或延迟已存在的定时器。
//
// Stop 之后的迟到调用会因 chanOp 已关闭而 panic，此处 recover 后静默丢弃，
// 效果等价于"调整未生效"，与调用方此时本就不该再操作已停止的定时器管理器一致。
func (disp *dispatcher) Update(timerID int64, deadline time.Time) {
	defer func() { _ = recover() }()
	disp.chanOp.In() <- command{op: opUpdate, id: timerID, deadline: deadline}
}

// Cancel 取消定时器：先同步置取消标记（使已投递到 chanFired 但尚未消费的事件被过滤），
// 再异步从最小堆物理删除，兼顾取消的即时生效与堆状态的最终一致。
//
// 同步标记先于异步命令投递：即使随后向已关闭的 chanOp 发送而被 recover 吞掉，
// canceled 标记也已经生效，不影响"取消"这个语义本身的正确性，只是堆里的物理
// 条目会滞留到进程退出（dispatcher 本身也已经在停止流程中，可接受）。
func (disp *dispatcher) Cancel(timerID int64) {
	disp.mu.Lock()
	e, ok := disp.entries[timerID]
	disp.mu.Unlock()
	if !ok {
		return
	}
	e.canceled.Store(true)
	defer func() { _ = recover() }()
	disp.chanOp.In() <- command{op: opCancel, id: timerID}
}
