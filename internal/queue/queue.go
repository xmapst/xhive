// Package queue 提供「按积压动态扩缩容 + 硬性容量上限」的 FIFO 队列。
//
// 它存在的理由是原生 channel 做不到这两件事的组合：hchan 的缓冲区在 make
// 时一次性分配、此后永不增长，所以只要数据本身在 channel 里排队，容量就
// 必然是预分配的固定值——一个 make(chan *T, 4096) 的模块，哪怕一条消息都
// 没有，也要先付出 32KB 常驻内存，且元素含指针时这段缓冲区全程参与 GC 扫描。
// 模块一多，这笔固定开销就很可观。
//
// 反过来把队列做成无界的（本仓库早期的 chanx）也不行：消费端一旦跟不上，
// 积压只表现为内存一路上涨，最终以 OOM 的形式在离现场很远的地方崩掉，
// 中途没有任何可告警的信号。
//
// 破局点在于：select 需要的只是一个**可等待的 channel**，它并不要求数据
// 本身流经这个 channel。于是这里把两者拆开——
//
//   - 数据放在自己管理的 ring buffer 里，从 initCap 起按积压翻倍增长、
//     按空闲减半收缩，空闲时的常驻内存与上限无关；
//   - 容量上限由 Push 显式检查，达到 max 即拒绝，语义与原生有界 channel
//     的「满」完全一致，OOM 的下限保护不变；
//   - 另配两个 cap=1 的 chan struct{} 作为「非空」「非满」的边沿信号，
//     使队列仍然能作为 select 的一路参与多路复用。
//
// 边沿信号是这套设计里最容易写错的地方：多次 Push 的信号会被合并成一个，
// 因此消费方**不能**假设「一个信号对应一个元素」。Pop 通过在队列仍非空时
// 把信号补回去来维持不变式，见 Pop 的说明。
package queue

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrFull 表示队列已达容量上限，本次投递被拒绝。
	ErrFull = errors.New("queue: full")
	// ErrClosed 表示队列已关闭，不再接受新元素。
	ErrClosed = errors.New("queue: closed")
)

// defaultInitCap 是 ring buffer 的初始容量，也是收缩的下限。
//
// 取 16 是在两头之间折中：太小会让每个模块启动后的头几十条消息触发多次
// realloc，太大又把「空闲时几乎不占内存」这个收益本身抵消掉。
const defaultInitCap = 16

// shrinkThreshold 是触发容量减半所需的连续低水位次数。
//
// 要求「连续」而不是「单次」，是为了不让一次正常的流量低谷立刻把刚扩上去的
// 容量打回原形，避免在突发流量下 grow/shrink 反复抖动。
const shrinkThreshold = 8

// Queue 是有硬性上限的动态 FIFO 队列，可安全并发使用。
//
// 零值不可用，必须通过 New 创建。
type Queue[T any] struct {
	mu   sync.Mutex
	ring []T // 环形缓冲，长度即当前分配的容量，随积压增长、随空闲收缩
	head int // 队首在 ring 中的下标
	n    int // 当前元素个数

	initCap int  // 初始容量，同时是收缩下限
	max     int  // 硬性容量上限，等价于原生 make(chan T, max) 的 max
	lowCnt  int  // 连续低水位计数，见 shrinkThreshold
	closed  bool // Close 之后为 true，拒绝新元素但仍可 Pop 出残留

	// notEmpty 是 cap=1 的**边沿**信号，不承载数据；它的唯一作用是让 Queue
	// 能出现在 select 的一路上。信号会合并，消费方靠 Pop 的接力维持
	// 「队列非空 ⟹ 有信号」这个不变式。
	notEmpty chan struct{}

	// notFull 走的是**广播**而不是边沿信号：队列变满时换上一个新的未关闭
	// 通道，从满变回不满时把它 close，所有阻塞的生产者一次全醒。
	//
	// 不用 cap=1 的信号，是因为它一次只能放行一个等待者。那样就必须让每个
	// 成功入队的生产者接力再发一个信号，而接力一旦在某条路径上被漏掉（比如
	// 调用方自己用 NotFull() 组织 select 而不是走 PushWait），剩下的生产者
	// 就会永久卡住——队列明明空着，所有人却都在等。广播没有这个陷阱。
	//
	// 它同时也更快：稳态下队列既不满也不空，这两个字段一次都不用碰。
	notFull       chan struct{}
	notFullClosed bool
}

// New 创建容量上限为 max 的队列，初始分配 min(defaultInitCap, max) 个槽位。
//
// max <= 0 时按 1 处理：调用方本就不该建一个容量为零的队列，
// 与其 panic 不如退化成「只能放一个」，让问题以水位告警的形式暴露。
func New[T any](max int) *Queue[T] {
	if max <= 0 {
		max = 1
	}
	initCap := min(defaultInitCap, max)
	return &Queue[T]{
		ring:     make([]T, initCap),
		initCap:  initCap,
		max:      max,
		notEmpty: make(chan struct{}, 1),
		// 新队列是空的，也就是不满，所以 notFull 从「已关闭」状态起步：
		// 此刻等它的人本就不该等。
		notFull:       closedChan,
		notFullClosed: true,
	}
}

// closedChan 是一个永远处于已关闭状态的通道，作为「当前不满」的共享表示。
// 所有空闲队列共用它，省掉每个队列一次无谓的 channel 分配。
var closedChan = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// NotEmpty 返回「队列可能有数据」的信号通道，供调用方放进 select。
//
// 收到信号后必须调用 Pop，且**必须接受 Pop 返回 false**：信号是边沿的，
// 存在虚假唤醒（例如 Push 的信号在 Pop 取走该元素之后才送达）。
// 反过来不会丢唤醒——只要队列非空，信号最终一定存在，见 Pop。
func (q *Queue[T]) NotEmpty() <-chan struct{} { return q.notEmpty }

// NotFull 返回「队列当前不满」的广播通道，供需要在等待入队的同时做别的事
// （例如周期性告警）的调用方自行组织 select，见 chanrpc.Client.call。
//
// 只想单纯阻塞入队的调用方用 PushWait 即可，不必碰这个。
//
// 队列不满时返回的通道已经处于关闭状态，等它会立刻返回；队列满时返回的是
// 一个未关闭的通道，会在下一次「从满变不满」时被 close。两种情况下都可能
// 虚假唤醒（拿到通道后队列又被别人填满），所以醒来必须重试 Push 并接受它
// 仍然返回 ErrFull。
func (q *Queue[T]) NotFull() <-chan struct{} {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.notFull
}

// signal 非阻塞地投递一个边沿信号：通道里已有信号就直接跳过，
// 因为「有数据」这件事不需要计数，一个待处理的信号已经表达了全部含义。
//
// 调用方持有 q.mu。放在锁内是刻意的：signal 是非阻塞发送，不会阻塞也不会
// 触及其他锁，代价极小；而放在锁外则允许 Push 的解锁与发信号之间被 Pop 插入，
// 多消费者场景下推理「队列非空必有信号」这个不变式会变得非常微妙。
func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// Push 非阻塞入队。
//
// 队列已达上限时返回 ErrFull，已关闭时返回 ErrClosed——前者对应原生有界
// channel 的「满」，后者对应「向已关闭 channel 发送」，区别是这里返回错误
// 而不是 panic，调用方无需再包一层 recover。
func (q *Queue[T]) Push(v T) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrClosed
	}
	if q.n >= q.max {
		return ErrFull
	}
	if q.n == len(q.ring) {
		q.grow()
	}
	q.ring[(q.head+q.n)%len(q.ring)] = v
	q.n++
	signal(q.notEmpty)
	if q.n == q.max {
		// 刚好填满：换上一个未关闭的通道，后来的生产者据此阻塞，
		// 直到 Pop 把它 close。没填满则一个字段都不碰。
		q.notFull = make(chan struct{})
		q.notFullClosed = false
	}
	return nil
}

// PushWait 阻塞入队，等待期间可被 ctx 取消，也可被 Close 唤醒。
//
// 对应同步调用的背压语义：调用方本来就在等结果，让它多等一会儿入队，
// 比直接丢掉这次调用更符合预期。notFull 信号使这段等待不需要轮询，
// 而 ctx 那一路保证它不会变成一次无法脱身的挂起。
//
// 它只负责等待本身，不打日志：队列打满是需要被看见的事故，但「多久算异常」
// 只有上层知道，因此周期告警留给调用方在自己的 select 里用 ticker 实现，
// 见 chanrpc.Client.call。
func (q *Queue[T]) PushWait(ctx context.Context, v T) error {
	for {
		err := q.Push(v)
		if !errors.Is(err, ErrFull) {
			return err // nil、ErrClosed，或 ctx 之外的任何终态
		}
		select {
		case <-q.NotFull():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Pop 取出队首元素，队列为空时返回 ok=false。
//
// 取完一个后若队列仍非空，会把 notEmpty 信号补回去，然后立即返回。
// 这是刻意不做「醒来后循环 drain 到空」的原因：drain 循环里若混入一个慢
// handler，事件循环就会长时间回不到 select，ctx.Done() 的响应性和多路之间
// 的公平性都被破坏。补信号让调用方保持「一次 select 处理一个事件」的语义，
// 上层的 select 结构因此可以原样保留。
//
// 补信号同时维持了「队列非空 ⟹ notEmpty 中有信号」这个不变式，
// 它是不丢唤醒的保证。
func (q *Queue[T]) Pop() (v T, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.n == 0 {
		return v, false
	}

	v = q.ring[q.head]
	var zero T
	// 主动清零槽位：ring 的长度是容量而非元素数，不清会让已出队元素
	// 继续被数组引用，拖住本该被 GC 回收的 CallInfo / RetInfo。
	q.ring[q.head] = zero
	q.head = (q.head + 1) % len(q.ring)
	q.n--

	if q.n > 0 {
		signal(q.notEmpty)
	}
	if q.n == q.max-1 {
		// 刚从满变回不满：广播放行所有等待的生产者。判等而不是判「小于」，
		// 是为了让稳态下这一路一次都不触发。
		q.releaseNotFull()
	}
	q.maybeShrink()
	return v, true
}

// releaseNotFull 关闭当前的 notFull 通道，一次放行全部等待的生产者。
// 幂等：已经是关闭状态时什么也不做。调用方持有 q.mu。
func (q *Queue[T]) releaseNotFull() {
	if q.notFullClosed {
		return
	}
	close(q.notFull)
	q.notFullClosed = true
}

// grow 将容量翻倍，不超过 max。调用方持有 q.mu 且已确认 n < max。
func (q *Queue[T]) grow() {
	c := min(len(q.ring)*2, q.max)
	q.realloc(c)
	q.lowCnt = 0
}

// maybeShrink 在连续 shrinkThreshold 次占用率不高于 25% 后将容量减半，
// 下限为 initCap。调用方持有 q.mu。
//
// 25% 而不是 50%：减半后占用率会翻倍，从 50% 缩容会立刻逼近满载，
// 下一次突发马上又要 grow 回去。
func (q *Queue[T]) maybeShrink() {
	if len(q.ring) <= q.initCap {
		return
	}
	if q.n*4 > len(q.ring) {
		q.lowCnt = 0
		return
	}
	q.lowCnt++
	if q.lowCnt < shrinkThreshold {
		return
	}
	q.realloc(max(len(q.ring)/2, q.initCap))
	q.lowCnt = 0
}

// realloc 把现有元素按 FIFO 顺序搬到容量为 c 的新 ring，并把队首归零。
// 调用方持有 q.mu，且必须保证 c >= q.n。
func (q *Queue[T]) realloc(c int) {
	nr := make([]T, c)
	for i := range q.n {
		nr[i] = q.ring[(q.head+i)%len(q.ring)]
	}
	q.ring = nr
	q.head = 0
}

// Len 返回当前积压的元素个数。
func (q *Queue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.n
}

// Cap 返回硬性容量上限，与 Len 搭配即可算出水位（Len/Cap）。
//
// 它是常量，不随 ring 的实际分配变化——水位告警关心的是「离拒绝还有多远」，
// 而不是「当前分配了多少」。后者见 Alloc。
func (q *Queue[T]) Cap() int { return q.max }

// Alloc 返回 ring buffer 当前实际分配的槽位数，用于观测内存曲线。
func (q *Queue[T]) Alloc() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.ring)
}

// Close 关闭队列：此后 Push/PushWait 一律返回 ErrClosed，
// 阻塞在 PushWait 上的调用方被立即唤醒。
//
// 已经入队的元素**不会**被丢弃，仍可通过 Pop 取出——这对应
// chanrpc.Server.Close「拒绝新调用，但把已排队的调用真正处理完」的语义。
//
// 幂等，重复调用安全。
func (q *Queue[T]) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	// 放行所有阻塞在 PushWait 上的生产者，它们随后的 Push 会拿到 ErrClosed
	// 并退出循环。队列此刻若不满，notFull 本就是关闭状态，这里是空操作。
	q.releaseNotFull()
	// notEmpty 不关闭：消费方还要把已入队的元素取完（Server.Close 的排空
	// 语义），关掉会让它陷入忙转。发一个信号叫醒它即可，后续由 Pop 接力。
	signal(q.notEmpty)
}

// IsClosed 报告队列是否已关闭。
func (q *Queue[T]) IsClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}
