// Package timer 提供基于多级时间轮算法的定时器管理能力。
//
// 设计思路：以牺牲少量时间精度（最小粒度 4ms）换取比 time.After 更低的 CPU 开销和 GC 压力。
// time.After 每次调用都会创建新的 channel 和 timer 对象，在海量定时器场景下会产生大量短生命周期对象；
// 时间轮算法通过固定大小的槽位数组重用内存，每次 tick 只检查当前层级，避免全量遍历，
// 在游戏服务器数千个同时活跃的定时器场景下具有显著的性能优势。
//
// 时间表示：对外与对内统一使用 Go 内置时间类型——绝对时刻用 time.Time，时间间隔/粒度用 time.Duration。
// 仅时间轮分级所必需的 tick 计数保留为 int64（位运算分级的算法要求），它表示 timerTick 的整数倍计数，
// 由 now.UnixNano() / int64(timerTick) 推导，本身不承载具体时间单位。
package timer

import (
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// 时间轮配置常量。
const (
	// timerTick 时间轮最小触发粒度，使用 2 的幂次方便于后续位运算计算层级掩码。
	// 4ms 的精度对游戏逻辑（如技能 CD、AI 决策）已足够，远优于内核调度的典型抖动范围。
	timerTick = 4 * time.Millisecond

	// timerLevel 时间轮分级数，决定支持的最大定时时长。
	// 28 级支持的最大时长约为 2^28 × 4ms ≈ 12.4 天，覆盖绝大多数游戏业务场景。
	timerLevel = 28
)

// opKind 分发器操作类型，显式区分发送到 chanOp 的命令语义，
// 取代以往靠 deadline/cb 是否为零值隐式判断操作的写法，避免歧义。
type opKind int8

const (
	opNew    opKind = iota // 新建定时器
	opUpdate               // 更新到期时刻（加速/延迟）
	opCancel               // 取消定时器
	opStop                 // 停止分发器主循环
)

// Event 定时器触发事件接口，供 Mgr 消费者调用。
type Event interface {
	// Cb 执行定时器回调，内部捕获 panic，执行后释放回调引用。
	Cb()
	// Name 返回定时器业务类型标识，用于统计耗时。
	Name() string
}

// dispatcher 多级时间轮定时器分发器。
//
// 核心数据结构：
//   - timerSlots[i] 存储剩余时间落在 [2^(i-1), 2^i) × timerTick 区间的定时器
//   - chanOp 通道将外部的增删改操作串行化到分发器 goroutine，避免外部调用方与时间轮逻辑并发修改内部状态
//   - canceledTimers 采用"双重取消"机制：先在 sync.Map 标记取消，再异步从时间轮物理删除，
//     使取消操作对已投递到 chanFired 的到期事件也能立即生效
type dispatcher struct {
	atomicID       atomic.Int64                           // 定时器唯一 ID 生成器
	timerSlots     [timerLevel]map[int64]*dispatcherTimer // 分级时间轮槽位，每级对应不同的时间区间
	chanOp         chan *dispatcherTimer                  // 操作串行化通道，确保时间轮数据的单线程访问
	chanFired      chan Event                             // 定时器到期通知通道，由调用方（Mgr）消费
	canceledTimers sync.Map                               // 已取消定时器的快速过滤集合，key 为 timerID
}

// dispatcherTimer 时间轮内部使用的定时器节点，同时复用为操作命令的载体。
type dispatcherTimer struct {
	op       opKind      // 操作类型，决定 doOp 的处理分支
	name     string      // 做消息统计用
	id       int64       // 定时器唯一 ID，opStop 信号约定 id=0，禁止业务使用
	deadline time.Time   // 到期绝对时刻
	cb       func(int64) // 到期回调函数
}

// Cb 安全执行定时器回调，通过 recover 捕获 panic 防止单个回调异常崩溃整个进程。
//
// 执行后将 cb 置 nil，主动释放回调函数闭包捕获的资源引用，帮助 GC 及时回收。
func (t *dispatcherTimer) Cb() {
	defer func() {
		t.cb = nil // 主动断开引用，允许 GC 回收回调捕获的外部资源（如模块的大对象）
		if r := recover(); r != nil {
			slog.Error("timer callback panic", "panic", r, "stack", string(debug.Stack()))
		}
	}()
	t.cb(t.id)
}

func (t *dispatcherTimer) Name() string {
	return t.name
}

// newDispatcher 创建并初始化多级时间轮分发器。
//
// 参数 l 指定操作通道和触发通道的缓冲容量，建议与模块的 ChanRPC 容量保持一致，
// 防止大量定时器到期时触发通道满而丢失事件。
func newDispatcher(l int) *dispatcher {
	disp := new(dispatcher)
	for k := range disp.timerSlots {
		disp.timerSlots[k] = make(map[int64]*dispatcherTimer)
	}
	if l <= 0 {
		l = 10000
	}

	disp.chanOp = make(chan *dispatcherTimer, l)
	disp.chanFired = make(chan Event, l)

	return disp
}

// Run 在独立 goroutine 中启动时间轮主循环。
func (disp *dispatcher) Run() {
	go disp.run()
}

// run 时间轮主循环，在独立 goroutine 中运行，通过 select 多路复用两类事件：
//   - chanOp：处理来自业务层的增删改操作命令
//   - tickTimer.C：每隔 timerTick 推进一次时间轮，检查并触发到期定时器
func (disp *dispatcher) run() {
	defer func() {
		if x := recover(); x != nil {
			slog.Error("timer dispatcher crashed", "panic", x, "stack", string(debug.Stack()))
		}
	}()

	lastTick := time.Now().UnixNano() / int64(timerTick)
	tickTimer := time.NewTimer(timerTick)
	for {
		select {
		case t := <-disp.chanOp:
			if !disp.doOp(t) {
				return // 收到 opStop 停止信号，退出主循环
			}
		case <-tickTimer.C:
			tickTimer.Reset(timerTick)
			lastTick = disp.doTick(time.Now(), lastTick)
		}
	}
}

// doOp 根据 op 字段执行定时器操作命令，返回 false 表示需要停止分发器主循环。
func (disp *dispatcher) doOp(t *dispatcherTimer) bool {
	switch t.op {
	case opCancel:
		// 取消操作：先将 ID 写入 canceledTimers 快速过滤集合，
		// 再从时间轮物理删除，确保即使定时器已投递到 chanFired 也不会被触发
		disp.canceledTimers.Store(t.id, struct{}{})
		disp.delete(t.id)
		return true

	case opStop:
		// 约定的停止信号，返回 false 使 run() 退出主循环
		return false

	case opNew:
		// 若定时器在操作执行前已被取消，忽略新建，防止"取消后重建"的竞态
		if _, canceled := disp.canceledTimers.Load(t.id); canceled {
			return true
		}
		// 清除可能残留的取消标记（防止 Cancel 后立即 New 时被误过滤），并放入时间轮
		disp.canceledTimers.Delete(t.id)
		disp.place(t)
		return true

	case opUpdate:
		// 同样受取消标记保护：已取消的定时器不再更新
		if _, canceled := disp.canceledTimers.Load(t.id); canceled {
			return true
		}
		// 从旧槽位取出，更新 deadline，放入新槽位
		oldt := disp.delete(t.id)
		if oldt != nil {
			oldt.deadline = t.deadline
			disp.place(oldt)
		} else {
			slog.Error("delay timer get old timer fail", "timer_id", t.id)
		}
		return true

	default:
		return true
	}
}

// delete 从时间轮各层级中删除指定 ID 的定时器，同时清理 canceledTimers 中的标记防止内存泄漏。
//
// 从高层级向低层级扫描，是因为剩余时间较短的定时器在低层级，
// 但已经被降级的定时器可能在任意层，全量扫描保证不遗漏。
func (disp *dispatcher) delete(timerID int64) *dispatcherTimer {
	for i := timerLevel - 1; i >= 0; i-- {
		if v, ok := disp.timerSlots[i][timerID]; ok {
			delete(disp.timerSlots[i], timerID)
			disp.canceledTimers.Delete(timerID) // 物理删除成功后清理取消标记
			return v
		}
	}
	disp.canceledTimers.Delete(timerID) // 定时器已不在轮中，也清理标记，防止 sync.Map 无限累积
	return nil
}

// place 将定时器放入时间轮的合适层级。
//
// 层级选择算法：计算剩余时间 diff，找到满足 diff ≤ 2^i × timerTick 的最小层级 i。
// 已到期（diff ≤ 0）的定时器直接投递到触发通道，使用非阻塞 select 防止分发器主循环被阻塞。
// 对剩余时间小于 timerTick 的定时器强制设置最小值，防止因精度舍入导致的无限循环触发。
func (disp *dispatcher) place(t *dispatcherTimer) {
	if _, canceled := disp.canceledTimers.Load(t.id); canceled {
		return
	}

	diff := t.deadline.Sub(time.Now())
	if diff <= 0 {
		// 已到期，非阻塞投递：chanFired 满时跳过，等待下次 tick 重试
		select {
		case disp.chanFired <- t:
		default:
		}
		return
	}
	if diff < timerTick {
		diff = timerTick // 保底最小粒度，防止极短超时导致在最低层级反复检查但不触发
	}
	// 从低层级向高层级查找第一个能容纳 diff 的槽位
	for i := range timerLevel {
		if diff <= (timerTick << uint(i)) {
			disp.timerSlots[i][t.id] = t
			break
		}
	}
}

// doTick 推进时间轮：根据当前时间与 lastTick 的差值，逐步触发各层级的到期定时器。
//
// 防时钟回拨：若 nowTick ≤ lastTick，直接返回，避免定时器因时钟回拨被重复触发。
// 防时钟跳变：若时钟发生跳变（如系统时间被修正），逐步推进而非一次性跳到当前时刻，
// 确保中间层级的定时器降级操作不被跳过，保证大时长定时器能正确触发。
func (disp *dispatcher) doTick(now time.Time, lastTick int64) int64 {
	nowTick := now.UnixNano() / int64(timerTick)
	if nowTick-lastTick < 1 {
		return nowTick
	}

	for {
		lastTick++
		// 多级时间轮的核心调度逻辑：
		// 第 i 层每隔 2^i 个 tick 扫描一次（当 tick 计数的低 i 位全为 0 时触发）
		// 这样高层级定时器以更低频率被检查，减少不必要的扫描开销
		for i := timerLevel - 1; i >= 0; i-- {
			mask := (1 << uint(i)) - 1
			if lastTick&int64(mask) == 0 {
				disp.trigger(now, i)
			}
		}

		if lastTick >= nowTick {
			break
		}
	}
	return nowTick
}

// trigger 扫描指定层级的定时器，将满足条件的定时器下移至精度更高的低层级，
// 或在第 0 层（最精确层）将已到期的定时器投递到触发通道。
//
// 层级降级的时机：当定时器的剩余时间已短于当前层级的时间跨度时，
// 移至更精确的低层级，使其在正确的时刻被检测到。
// 最低层（level=0）中到期的定时器通过非阻塞 select 投递，发送失败则在下次 tick 重试。
func (disp *dispatcher) trigger(now time.Time, level int) {
	slotMap := disp.timerSlots[level]
	for k, v := range slotMap {
		// 快速过滤已取消的定时器，避免触发无效回调
		if _, canceled := disp.canceledTimers.Load(k); canceled {
			delete(slotMap, k)
			disp.canceledTimers.Delete(k)
			continue
		}

		// 位移运算等价于 timerTick × 2^level
		if v.deadline.Sub(now) < (timerTick << uint(level)) {
			if level != 0 {
				// 将定时器降级到更精确的层级，确保在合适的时刻被触发
				disp.timerSlots[level-1][k] = v
				delete(slotMap, k)
			} else if !now.Before(v.deadline) {
				// 最低层已到期，非阻塞投递；发送失败（通道满）则保留在槽位，等待下次 tick
				select {
				case disp.chanFired <- v:
					delete(slotMap, k)
				default:
				}
			}
		}
	}
}

// Stop 向分发器发送停止信号，通知主循环退出。
func (disp *dispatcher) Stop() {
	disp.chanOp <- &dispatcherTimer{op: opStop, name: "stop", id: 0}
}

// Update 更新定时器的到期时刻，用于加速或延迟已存在的定时器。
//
// 通过 chanOp 将更新操作异步发送到分发器 goroutine 处理，
// 保证时间轮数据的单线程访问，无需外部加锁。
func (disp *dispatcher) Update(name string, timerID int64, deadline time.Time) {
	disp.chanOp <- &dispatcherTimer{op: opUpdate, name: name, id: timerID, deadline: deadline}
}

// New 创建定时器并放入时间轮，timerID 为 0 时自动生成全局唯一 ID。
//
// 通过 chanOp 异步发送创建命令，由分发器 goroutine 执行实际的槽位分配操作。
func (disp *dispatcher) New(name string, timerID int64, deadline time.Time, cb func(int64)) int64 {
	if timerID == 0 {
		timerID = disp.atomicID.Add(1)
	}
	disp.chanOp <- &dispatcherTimer{op: opNew, name: name, id: timerID, deadline: deadline, cb: cb}
	return timerID
}

// Cancel 取消定时器，采用"先标记后删除"的双重机制确保取消的即时生效性。
//
// 先立即将 timerID 写入 canceledTimers（快速过滤集合），
// 使已投递到 chanFired 但尚未被消费的到期事件也能被过滤掉，
// 再通过 chanOp 异步发送物理删除命令，从时间轮槽位中移除定时器节点，
// 两者结合保证取消操作在逻辑层面的即时性和内存层面的最终一致性。
func (disp *dispatcher) Cancel(name string, timerID int64) {
	disp.canceledTimers.Store(timerID, struct{}{}) // 立即生效：即使定时器已到期且在通道中排队，也会被过滤
	disp.chanOp <- &dispatcherTimer{op: opCancel, name: name, id: timerID}
}
