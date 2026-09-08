package queue

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestPushPopFIFO 验证基本的先进先出语义。
func TestPushPopFIFO(t *testing.T) {
	q := New[int](128)
	for i := range 100 {
		if err := q.Push(i); err != nil {
			t.Fatalf("Push(%d) = %v", i, err)
		}
	}
	if got := q.Len(); got != 100 {
		t.Fatalf("Len() = %d, want 100", got)
	}
	for i := range 100 {
		v, ok := q.Pop()
		if !ok || v != i {
			t.Fatalf("Pop() = (%d, %v), want (%d, true)", v, ok, i)
		}
	}
	if _, ok := q.Pop(); ok {
		t.Fatal("Pop() on empty queue should return false")
	}
}

// TestGrowAndShrink 验证容量随积压增长、随空闲收缩，且始终不越过上限。
func TestGrowAndShrink(t *testing.T) {
	const max = 4096
	q := New[*int](max)

	if got := q.Alloc(); got != defaultInitCap {
		t.Fatalf("初始 Alloc() = %d, want %d", got, defaultInitCap)
	}
	if got := q.Cap(); got != max {
		t.Fatalf("Cap() = %d, want %d", got, max)
	}

	for i := range max {
		if err := q.Push(&i); err != nil {
			t.Fatalf("Push #%d = %v", i, err)
		}
		if a := q.Alloc(); a > max {
			t.Fatalf("Alloc() = %d 越过上限 %d", a, max)
		}
	}
	if got := q.Alloc(); got != max {
		t.Fatalf("打满后 Alloc() = %d, want %d", got, max)
	}

	for range max {
		if _, ok := q.Pop(); !ok {
			t.Fatal("Pop() 提前返回 false")
		}
	}
	if got := q.Alloc(); got > defaultInitCap*4 {
		t.Fatalf("排空后 Alloc() = %d, 未有效收缩", got)
	}
	t.Logf("4096 打满再排空后 Alloc() = %d（上限仍为 %d）", q.Alloc(), q.Cap())
}

// TestFullRejects 验证达到上限即拒绝——这是替代无界队列的核心保证。
func TestFullRejects(t *testing.T) {
	q := New[int](4)
	for i := range 4 {
		if err := q.Push(i); err != nil {
			t.Fatalf("Push(%d) = %v", i, err)
		}
	}
	if err := q.Push(99); !errors.Is(err, ErrFull) {
		t.Fatalf("满队列 Push = %v, want ErrFull", err)
	}
	// 取走一个之后应当重新接受
	if _, ok := q.Pop(); !ok {
		t.Fatal("Pop() = false")
	}
	if err := q.Push(99); err != nil {
		t.Fatalf("腾出空位后 Push = %v", err)
	}
}

// TestMinCapNeverExceedsMax 保证 max 小于默认初始容量时不会超额分配。
func TestMinCapNeverExceedsMax(t *testing.T) {
	q := New[int](2)
	if got := q.Alloc(); got != 2 {
		t.Fatalf("Alloc() = %d, want 2", got)
	}
	if got := New[int](0).Cap(); got != 1 {
		t.Fatalf("New(0).Cap() = %d, want 1", got)
	}
}

// TestNoLostWakeup 是这套设计最关键的一条：边沿信号会合并，
// 必须保证「队列非空 ⟹ 消费方最终一定会被唤醒」，一条都不能滞留。
func TestNoLostWakeup(t *testing.T) {
	const N = 200_000
	q := New[int](64) // 故意设小，让生产者频繁撞满、走 PushWait 的等待路径

	var wg sync.WaitGroup
	wg.Go(func() {
		want := 0
		for want < N {
			<-q.NotEmpty()
			v, ok := q.Pop()
			if !ok {
				continue // 虚假唤醒是允许的
			}
			if v != want {
				t.Errorf("顺序错乱：want %d, got %d", want, v)
				return
			}
			want++
		}
	})

	for i := range N {
		if err := q.PushWait(t.Context(), i); err != nil {
			t.Fatalf("PushWait(%d) = %v", i, err)
		}
	}
	wg.Wait()
}

// TestPushWaitCanceled 验证阻塞入队可被 ctx 取消，不会变成无法脱身的挂起。
func TestPushWaitCanceled(t *testing.T) {
	q := New[int](1)
	if err := q.Push(1); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := q.PushWait(ctx, 2)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("PushWait = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("取消耗时 %v，过久", elapsed)
	}
}

// TestPushWaitWokenByPop 验证 Pop 腾出空位后阻塞的生产者能被 notFull 唤醒。
func TestPushWaitWokenByPop(t *testing.T) {
	q := New[int](1)
	if err := q.Push(1); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- q.PushWait(t.Context(), 2) }()

	// 给上面的 goroutine 一点时间真正阻塞进去
	time.Sleep(20 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("PushWait 不该在队列仍满时返回：%v", err)
	default:
	}

	if _, ok := q.Pop(); !ok {
		t.Fatal("Pop() = false")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("PushWait = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Pop 腾出空位后 PushWait 未被唤醒")
	}
}

// TestCloseRejectsAndDrains 验证 Close 的两半语义：
// 拒绝新元素，但已入队的仍可取出（对应 Server.Close 的排空）。
func TestCloseRejectsAndDrains(t *testing.T) {
	q := New[int](8)
	for i := range 3 {
		if err := q.Push(i); err != nil {
			t.Fatal(err)
		}
	}
	q.Close()
	if !q.IsClosed() {
		t.Fatal("IsClosed() = false")
	}
	if err := q.Push(9); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后 Push = %v, want ErrClosed", err)
	}
	for i := range 3 {
		v, ok := q.Pop()
		if !ok || v != i {
			t.Fatalf("关闭后 Pop() = (%d, %v), want (%d, true)", v, ok, i)
		}
	}
	q.Close() // 幂等
}

// TestCloseWakesBlockedPush 验证 Close 能唤醒阻塞在满队列上的生产者，
// 这对应原来 close(chan) 让阻塞的发送方醒来（当时靠 panic + recover）。
func TestCloseWakesBlockedPush(t *testing.T) {
	q := New[int](1)
	if err := q.Push(1); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- q.PushWait(t.Context(), 2) }()
	time.Sleep(20 * time.Millisecond)

	q.Close()
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("PushWait = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close 未唤醒阻塞的 PushWait")
	}
}

// TestPopReleasesReference 验证出队后槽位被清零，不会拖住 GC。
func TestPopReleasesReference(t *testing.T) {
	q := New[*[1 << 16]byte](8)
	collected := make(chan struct{})
	func() {
		big := new([1 << 16]byte)
		runtime.AddCleanup(big, func(struct{}) { close(collected) }, struct{}{})
		if err := q.Push(big); err != nil {
			t.Fatal(err)
		}
		if _, ok := q.Pop(); !ok {
			t.Fatal("Pop() = false")
		}
	}()

	for range 10 {
		runtime.GC()
		select {
		case <-collected:
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatal("出队元素仍被 ring 引用，未被 GC 回收")
}

// TestConcurrentPushPop 在 -race 下验证多生产者多消费者的并发安全，
// 并确认没有元素在信号合并下滞留。
func TestConcurrentPushPop(t *testing.T) {
	const producers, perProducer, consumers = 8, 4000, 4
	const total = producers * perProducer
	q := New[int](256) // 远小于总量，逼着生产者反复走 PushWait 的等待路径

	var consumed atomic.Int64
	var prod, cons sync.WaitGroup

	for range producers {
		prod.Go(func() {
			for i := range perProducer {
				if err := q.PushWait(t.Context(), i); err != nil {
					t.Errorf("PushWait = %v", err)
					return
				}
			}
		})
	}

	for range consumers {
		cons.Go(func() {
			for consumed.Load() < total {
				select {
				case <-q.NotEmpty():
					if _, ok := q.Pop(); ok {
						consumed.Add(1)
					}
				case <-time.After(5 * time.Second):
					t.Error("消费超时，疑似丢唤醒")
					return
				}
			}
			// 收尾时补一个信号，避免其余消费者卡在 NotEmpty 上等不到人叫醒。
			signal(q.notEmpty) // 收尾时补一个信号
		})
	}

	prod.Wait()
	cons.Wait()

	if got := consumed.Load(); got != total {
		t.Fatalf("消费总数 = %d, want %d", got, total)
	}
	if n := q.Len(); n != 0 {
		t.Fatalf("队列残留 %d 个元素", n)
	}
}
