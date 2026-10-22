// Package async 提供一个轻量的异步任务调度器。
//
// 典型用法：耗时的 f 通过 Scheduler.Go 派发到独立 goroutine 并发执行；执行结束
// （或 panic）后其回调 cb 并不会立即执行，而是被送入内部的回调队列 —— 调用方
// 需要在自己的事件循环（通常是单一 goroutine 的 select 循环）里通过
// NotEmpty/Pop 持续消费并调用 Cb 执行回调，从而保证所有 cb 都串行运行在同一个
// goroutine 上，无需加锁即可安全访问共享状态。回调队列基于 internal/queue.Queue
// 实现，与 timer.Manager、chanrpc.Server/Client 使用的是同一套"边沿信号 + 硬性
// 容量上限动态队列"模式。
package async

import (
	"container/list"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xmapst/xhive/internal/queue"
)

// defaultCloseTimeout 是 Close 等待未完成异步任务收尾的默认超时上限，
// 可通过 WithCloseTimeout 按实例覆盖。
const defaultCloseTimeout = 5 * time.Second

// Scheduler 异步任务调度器：f 在独立 goroutine 中并发执行，cb 统一通过内部回调
// 队列派发，需要调用方在单一消费者 goroutine 中取出并执行，从而保证所有 cb 串行
// 运行。Go/Cb/Close/Idle 内部的并发计数可安全地被多个 goroutine 同时调用。
type Scheduler struct {
	cbQueue      *queue.Queue[func()]
	pending      atomic.Int64
	pendingMax   int64 // 构造后不变，见 New 的说明
	closeTimeout time.Duration
	closed       atomic.Bool
}

// linearTask 是 LinearContext 队列中的一个待执行任务。
type linearTask struct {
	f  func()
	cb func()
}

// LinearContext 线程安全并保证顺序性的异步任务队列：可以从多个 goroutine 并发
// 调用 Go 提交任务，但各任务的 f 始终按提交顺序依次串行执行，不会并发执行。
type LinearContext struct {
	s              *Scheduler
	tasks          *list.List
	mutexTasks     sync.Mutex
	mutexExecution sync.Mutex
}

// options 保存 New 的可选配置项。
type options struct {
	closeTimeout time.Duration
}

func defaultOptions() options {
	return options{closeTimeout: defaultCloseTimeout}
}

// Option 用于自定义 New 创建的 Scheduler 的可选行为。
type Option func(*options)

// WithCloseTimeout 自定义 Close 等待未完成异步任务收尾的超时上限。
// d <= 0 时该选项不生效，沿用 defaultCloseTimeout（5 秒）。
func WithCloseTimeout(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.closeTimeout = d
		}
	}
}

// New 创建一个 Scheduler：max 同时决定回调队列的硬性容量上限和同时允许存在
// 的未完成异步任务数上限 pendingMax——二者被有意绑成同一个值，从而保证
// 「回调队列里未消费的回调数 <= 同时在途的任务数 <= pendingMax == 队列容量」
// 这个不变量恒成立，Push 因此理论上永远不会因为队列满而失败。
//
// pendingMax 从队列的 Cap() 回读而不是直接使用入参 max：queue.New 对 max<=0
// 会 clamp 成 1，若不回读、直接把原始（可能 <=0 的）max 存进 pendingMax，
// 两者就会不相等，上面这条不变量从一开始就不成立。
//
// 不提供运行时调整上限的方法：队列的硬性容量上限在构造时确定，事后调大
// pendingMax 会立刻打破上面的不变量，调小则没有实际收益，索性只在构造时
// 一次定死。
func New(max int, opts ...Option) *Scheduler {
	cfg := defaultOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	q := queue.New[func()](max)
	return &Scheduler{
		cbQueue:      q,
		pendingMax:   int64(q.Cap()),
		closeTimeout: cfg.closeTimeout,
	}
}

// NotEmpty 返回"有回调待执行"的信号，供调用方的事件循环通过 select 监听。
// 边沿信号，会合并；收到后必须调用 Pop，并且必须接受 Pop 返回 false。
func (s *Scheduler) NotEmpty() <-chan struct{} {
	return s.cbQueue.NotEmpty()
}

// Pop 取出一个待执行的回调，队列为空时返回 ok=false。
func (s *Scheduler) Pop() (func(), bool) {
	return s.cbQueue.Pop()
}

// acquire 原子地占用一个并发配额；已关闭或超出 pendingMax 时返回 false。
//
// closed 检查了两次（占用配额前后）：即便如此仍有一个理论上无法消除的竞态
// 窗口——Close 恰好在两次检查之间才判定 pending==0 并直接返回（不启动排空
// 协程），随后这次任务的回调会永久无人 Pop。这与 chanrpc.Client 现有的
// AsyncCall/Close 之间同样的窗口是同一类、已被这个代码库接受的活性问题而非
// 安全问题，不为此引入额外的锁。
func (s *Scheduler) acquire() bool {
	if s.closed.Load() {
		return false
	}
	if s.pending.Add(1) > s.pendingMax {
		s.pending.Add(-1)
		return false
	}
	if s.closed.Load() {
		s.pending.Add(-1)
		return false
	}
	return true
}

// push 把 cb 投递进回调队列；按 New 的说明，这理论上不会失败。失败时按丢弃
// 处理并释放对应的并发配额——这个回调不会再被 Cb 执行，必须在这里补一次
// pending.Add(-1)，否则对应的并发配额会永久少一个。
func (s *Scheduler) push(cb func()) {
	if err := s.cbQueue.Push(cb); err != nil {
		slog.Error("async push callback failed, dropping", slog.Any("error", err))
		s.pending.Add(-1)
	}
}

// Go 启动一个异步任务：f 在新 goroutine 中并发执行，结束（或 panic）后 cb 被
// 送入回调队列等待调用方通过 Cb 执行。可从多个 goroutine 并发调用。
func (s *Scheduler) Go(f, cb func()) {
	if !s.acquire() {
		slog.Error("async scheduler rejected task", slog.Int64("max", s.pendingMax))
		return
	}

	go func() {
		defer func() {
			s.push(cb)
			if r := recover(); r != nil {
				slog.Error("goroutine panic recovered", slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
			}
		}()
		f()
	}()
}

// Cb 执行一个从回调队列取出的回调，并释放其占用的并发配额。
func (s *Scheduler) Cb(cb func()) {
	defer func() {
		s.pending.Add(-1)
		if r := recover(); r != nil {
			slog.Error("goroutine panic recovered", slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
		}
	}()
	if cb != nil {
		cb()
	}
}

// Close 等待所有已派发的异步任务完成并执行完其回调，最多等待 closeTimeout；
// 超时后直接放弃剩余回调（pending 被强制清零），不保证它们一定会被执行。
// 幂等，重复调用安全。
//
// 之所以能在超时后安全放弃、而不必像早期版本那样派生一个永久后台 goroutine
// 继续兜底：New 保证了回调队列容量与 pendingMax 相等，push 因此永远不会
// 阻塞，被放弃的回调只是静静地留在队列里等待随整个 Scheduler 实例一起被 GC，
// 不会有任何 goroutine 卡在向队列投递上。
//
// 调用方需自行保证 Close 期间不再通过 Go 派发新任务。
func (s *Scheduler) Close() {
	if !s.closed.CompareAndSwap(false, true) {
		slog.Warn("async scheduler already closed")
		return
	}
	if s.pending.Load() <= 0 {
		return
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		timer := time.NewTimer(s.closeTimeout)
		defer timer.Stop()

		for {
			if s.pending.Load() <= 0 {
				return
			}
			select {
			case <-timer.C:
				slog.Warn("async scheduler close timeout, dropping remaining callbacks", slog.Int64("remaining", s.pending.Load()))
				s.pending.Store(0)
				return
			default:
			}

			if cb, ok := s.cbQueue.Pop(); ok {
				s.Cb(cb)
				continue
			}

			select {
			case <-s.cbQueue.NotEmpty():
			case <-timer.C:
				slog.Warn("async scheduler close timeout, dropping remaining callbacks", slog.Int64("remaining", s.pending.Load()))
				s.pending.Store(0)
				return
			}
		}
	})
	wg.Wait()
}

// Idle 是否没有正在进行中的异步任务。
func (s *Scheduler) Idle() bool {
	return s.pending.Load() == 0
}

// NewLinearContext 基于当前 Scheduler 创建一个保证顺序执行的任务队列，与其他
// 通过该 Scheduler 提交的任务共享同一份并发配额（pendingMax）。
func (s *Scheduler) NewLinearContext() *LinearContext {
	return &LinearContext{
		s:     s,
		tasks: list.New(),
	}
}

// Go 提交一个任务到队列，f 保证按提交顺序依次串行执行（不会并发执行）；cb 与
// 顶层 Scheduler.Go 一样通过回调队列派发。可从多个 goroutine 并发调用。
func (c *LinearContext) Go(f, cb func()) {
	if !c.s.acquire() {
		slog.Error("async scheduler rejected task", slog.Int64("max", c.s.pendingMax))
		return
	}

	c.mutexTasks.Lock()
	c.tasks.PushBack(&linearTask{f: f, cb: cb})
	c.mutexTasks.Unlock()

	go func() {
		// 出队必须发生在持有 mutexExecution 期间：谁先抢到这把锁，谁就去取
		// 当前队首并执行，从而让"出队"与"串行执行"严格绑定，保证多个
		// goroutine 即便乱序抢锁，最终处理顺序也始终等于入队顺序。
		var task *linearTask
		func() {
			c.mutexExecution.Lock()
			c.mutexTasks.Lock()
			task = c.tasks.Remove(c.tasks.Front()).(*linearTask)
			c.mutexTasks.Unlock()

			defer func() {
				c.mutexExecution.Unlock()
				if r := recover(); r != nil {
					slog.Error("goroutine panic recovered", slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
				}
			}()
			task.f()
		}()

		c.s.push(task.cb)
	}()
}
