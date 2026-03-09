// Package chanx 提供 channel 扩展能力，例如无界 channel。
package chanx

import (
	"context"
	"sync/atomic"
)

// Unbounded 是一个无界的 FIFO channel：向 In 发送永不阻塞，值在内部
// 环形缓冲区中暂存，从 Out 接收时保持发送顺序。
//
// 适用于生产者不能被慢消费者阻塞的场景，例如事件总线、日志队列。
//
//	u := chanx.NewUnbounded[int](context.Background())
//	u.In() <- 1
//	v := <-u.Out()
//	u.Close()
//
// 若传给 NewUnbounded 的 ctx 被取消，转发 goroutine 会立即退出并关闭
// Out，即便缓冲区中仍有未消费的值。这个机制限定了该 goroutine 的
// 生命周期，避免消费者停止读取后它一直阻塞、造成泄漏。
type Unbounded[T any] struct {
	in     chan T
	out    chan T
	length int64 // 内部环形缓冲区中当前持有的值的数量
}

// config 保存 NewUnbounded 的可选配置项。
type config struct {
	initBufCap int
	initInCap  int
	initOutCap int
}

// Option 用于在创建时配置 Unbounded。
type Option func(*config)

// WithInitialCapacity 设置内部环形缓冲区的初始容量。当预期的积压规模
// 已知时，可用它减少扩容次数。默认值为 16，n <= 0 时该选项不生效。
func WithInitialCapacity(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.initBufCap = n
		}
	}
}

// WithChanCapacity 设置 In 和 Out 这两个 Go channel 自身的缓冲大小，
// 与内部环形缓冲区是两回事。调大它可以在高频小对象场景下减少 goroutine
// 调度切换的开销，代价是提前占用更多内存。两者默认均为 0（无缓冲）。
// 负值不生效。
func WithChanCapacity(inCap, outCap int) Option {
	return func(c *config) {
		if inCap >= 0 {
			c.initInCap = inCap
		}
		if outCap >= 0 {
			c.initOutCap = outCap
		}
	}
}

// NewUnbounded 创建一个 Unbounded 并启动其转发 goroutine。ctx 用于
// 控制该 goroutine 的生命周期；如不需要取消能力，传入
// context.Background() 即可。
func NewUnbounded[T any](ctx context.Context, opts ...Option) *Unbounded[T] {
	c := config{initBufCap: 16}
	for _, opt := range opts {
		opt(&c)
	}
	u := &Unbounded[T]{
		in:  make(chan T, c.initInCap),
		out: make(chan T, c.initOutCap),
	}
	go u.run(ctx, c.initBufCap)
	return u
}

// In 返回发送端。发送操作永不阻塞。调用 Close 之后不应再向其发送。
func (u *Unbounded[T]) In() chan<- T { return u.in }

// Out 返回接收端。当 In 被关闭（或 ctx 被取消）且缓冲区中的值已全部
// 读出后，Out 会被关闭。
func (u *Unbounded[T]) Out() <-chan T { return u.out }

// Close 关闭发送端。已缓冲的值仍可从 Out 读出，读尽后 Out 才会关闭。
func (u *Unbounded[T]) Close() { close(u.in) }

// BufLen 返回内部环形缓冲区中大致的积压数量，不包含 In、Out 两个
// channel 自身队列中的值。该结果与转发 goroutine 并发读取，应视为
// 某一时刻的近似快照，而非精确计数。
func (u *Unbounded[T]) BufLen() int {
	return int(atomic.LoadInt64(&u.length))
}

// Len 返回 In、Out 与内部缓冲区中积压数量的近似总和，可安全地并发
// 调用，适合用作积压量监控或背压告警的依据，但同样只是近似快照，
// 而非精确计数。
func (u *Unbounded[T]) Len() int {
	return len(u.in) + len(u.out) + u.BufLen()
}

// run 通过内部环形缓冲区将值从 in 转发到 out，直至 in 被关闭且缓冲区
// 读尽，或 ctx 被取消。
func (u *Unbounded[T]) run(ctx context.Context, initialCap int) {
	defer close(u.out)
	r := newRing[T](initialCap)
	for {
		if r.len() == 0 {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-u.in:
				if !ok {
					return
				}
				r.push(v)
				atomic.StoreInt64(&u.length, int64(r.len()))
			}
		}

		select {
		case <-ctx.Done():
			return
		case v, ok := <-u.in:
			if !ok {
				// in 已关闭：尝试把缓冲区中剩余的值送到 out。
				// 若此时 ctx 也被取消（例如消费者已停止读取），
				// 放弃继续投递，避免永久阻塞。
				for r.len() > 0 {
					select {
					case u.out <- r.front():
						r.pop()
						atomic.StoreInt64(&u.length, int64(r.len()))
					case <-ctx.Done():
						return
					}
				}
				return
			}
			r.push(v)
			atomic.StoreInt64(&u.length, int64(r.len()))
		case u.out <- r.front():
			r.pop()
			atomic.StoreInt64(&u.length, int64(r.len()))
		}
	}
}

// ring 是 Unbounded 内部使用的可伸缩环形缓冲区：写满时扩容，占用率
// 过低时收缩，因此一次突发流量不会永久拉高内存占用。ring 本身不是
// 并发安全的，Unbounded 只在其转发 goroutine 中访问它。
type ring[T any] struct {
	buf    []T
	head   int
	count  int
	minCap int // 容量收缩的下限
}

// growThreshold 是切换扩容策略的容量阈值：低于该值时翻倍扩容，
// 达到或超过该值后改为按 1.25 倍扩容，以减少大缓冲区场景下的
// 内存浪费。
const growThreshold = 1024

// newRing 创建一个初始容量为 capHint 的 ring。capHint 同时也是该
// ring 收缩时不会低于的容量下限。
func newRing[T any](capHint int) *ring[T] {
	if capHint < 1 {
		capHint = 1
	}
	return &ring[T]{buf: make([]T, capHint), minCap: capHint}
}

// len 返回当前存储的值的数量。
func (r *ring[T]) len() int { return r.count }

// front 返回最早存入且尚未取出的值，不将其移除。
// 调用方需自行保证 len() > 0。
func (r *ring[T]) front() T { return r.buf[r.head] }

// push 追加一个值；若缓冲区已满，先扩容再写入。
func (r *ring[T]) push(v T) {
	if r.count == len(r.buf) {
		r.grow()
	}
	idx := (r.head + r.count) % len(r.buf)
	r.buf[idx] = v
	r.count++
}

// grow 扩大缓冲区容量：低于 growThreshold 时翻倍，达到或超过时
// 按 25% 增长。
func (r *ring[T]) grow() {
	old := len(r.buf)
	newCap := old * 2
	if old >= growThreshold {
		newCap = old + old/4
	}
	if newCap <= old {
		newCap = old + 1
	}
	r.resize(newCap)
}

// pop 移除并返回最早存入的值。若移除后占用率降至容量的四分之一
// 或更低，则将容量减半，但不会低于 minCap。调用方需自行保证
// len() > 0。
func (r *ring[T]) pop() T {
	var zero T
	v := r.buf[r.head]
	r.buf[r.head] = zero // 释放引用，避免该值因缓冲区仍持有其槽位而无法被 GC 回收
	r.head = (r.head + 1) % len(r.buf)
	r.count--

	if half := len(r.buf) / 2; half >= r.minCap && r.count*4 <= len(r.buf) {
		r.resize(half)
	}
	return v
}

// resize 将缓冲区重新分配为容量 newCap，并保持原有顺序；
// 实际容量会被夹在 [r.count, r.minCap] 所要求的范围之内。
func (r *ring[T]) resize(newCap int) {
	if newCap < r.count {
		newCap = r.count
	}
	if newCap < r.minCap {
		newCap = r.minCap
	}
	nb := make([]T, newCap)
	for i := 0; i < r.count; i++ {
		nb[i] = r.buf[(r.head+i)%len(r.buf)]
	}
	r.buf = nb
	r.head = 0
}
