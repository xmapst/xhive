package async

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// drain 持续消费回调队列并执行回调，直到 stop 被关闭。
func drain(t *testing.T, s *Scheduler, stop <-chan struct{}) {
	t.Helper()
	go func() {
		for {
			select {
			case <-s.NotEmpty():
				if cb, ok := s.Pop(); ok {
					s.Cb(cb)
				}
			case <-stop:
				return
			}
		}
	}()
}

func TestSchedulerConcurrentSubmission(t *testing.T) {
	const n = 200
	s := New(n)
	stop := make(chan struct{})
	drain(t, s, stop)

	var done atomic.Int64
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			s.Go(func() {}, func() { done.Add(1) })
		})
	}
	wg.Wait()

	deadline := time.Now().Add(2 * time.Second)
	for done.Load() != n && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(stop)

	if got := done.Load(); got != n {
		t.Fatalf("expected %d callbacks executed, got %d", n, got)
	}
	if !s.Idle() {
		t.Fatalf("expected Scheduler to be idle after all callbacks ran")
	}
}

func TestSchedulerMaxRejectsOverflow(t *testing.T) {
	s := New(4)

	release := make(chan struct{})
	var accepted atomic.Int64
	for range 8 {
		s.Go(func() {
			<-release
		}, func() { accepted.Add(1) })
	}

	if got := s.pending.Load(); got > 4 {
		t.Fatalf("pending exceeded max: got %d, want <= 4", got)
	}
	close(release)

	stop := make(chan struct{})
	drain(t, s, stop)
	deadline := time.Now().Add(2 * time.Second)
	for accepted.Load() != s.pending.Load() && !s.Idle() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(stop)
}

func TestLinearContextPreservesOrder(t *testing.T) {
	const n = 100
	s := New(n)
	stop := make(chan struct{})
	drain(t, s, stop)

	lc := s.NewLinearContext()
	var order []int
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		lc.Go(func() {
			order = append(order, i)
		}, func() {
			wg.Done()
		})
	}
	wg.Wait()
	close(stop)

	if len(order) != n {
		t.Fatalf("expected %d executions, got %d", n, len(order))
	}
	for i, v := range order {
		if v != i {
			t.Fatalf("order broken at index %d: got %d, want %d", i, v, i)
		}
	}
}

func TestLinearContextConcurrentSubmitters(t *testing.T) {
	const submitters = 16
	const perSubmitter = 20
	const total = submitters * perSubmitter

	s := New(total)
	stop := make(chan struct{})
	drain(t, s, stop)

	lc := s.NewLinearContext()
	var executed atomic.Int64
	var wg sync.WaitGroup
	wg.Add(total)
	for range submitters {
		go func() {
			for range perSubmitter {
				lc.Go(func() {
					executed.Add(1)
				}, func() {
					wg.Done()
				})
			}
		}()
	}
	wg.Wait()
	close(stop)

	if got := executed.Load(); got != total {
		t.Fatalf("expected %d tasks executed, got %d", total, got)
	}
}

func TestCloseDrainsPendingCallbacks(t *testing.T) {
	const n = 50
	s := New(n)

	var done atomic.Int64
	for range n {
		s.Go(func() {}, func() { done.Add(1) })
	}

	s.Close()

	if got := done.Load(); got != n {
		t.Fatalf("expected Close to drain %d callbacks, got %d", n, got)
	}
	if !s.Idle() {
		t.Fatalf("expected Scheduler to be idle after Close")
	}
}

func TestCloseTimesOutOnStuckTask(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow close-timeout test in short mode")
	}
	s := New(1)
	block := make(chan struct{})
	s.Go(func() {
		<-block
	}, func() {})

	start := time.Now()
	s.Close()
	elapsed := time.Since(start)

	if elapsed > s.closeTimeout+time.Second {
		t.Fatalf("Close took too long to time out: %v", elapsed)
	}
	if !s.Idle() {
		t.Fatalf("expected Close to force pending to 0 after timeout (abandon semantics)")
	}

	close(block) // 让卡住的任务退出，避免测试结束时残留 goroutine
}

func TestWithCloseTimeoutOverride(t *testing.T) {
	s := New(1, WithCloseTimeout(200*time.Millisecond))
	block := make(chan struct{})
	defer close(block)
	s.Go(func() { <-block }, func() {})

	start := time.Now()
	s.Close()
	elapsed := time.Since(start)

	if elapsed > time.Second {
		t.Fatalf("Close with custom timeout took too long: %v", elapsed)
	}
	if !s.Idle() {
		t.Fatalf("expected Close to force pending to 0 after custom timeout")
	}
}

// TestPushNeverBlocksWithoutConsumer 验证 New 保证的核心不变量：回调队列容量
// 与 pendingMax 相等，因此即使完全没有消费者在 Pop，已经在途的任务数不超过
// max 时，Go/LinearContext.Go 派发的 goroutine 也不会阻塞在向队列投递回调
// 上——所有 f 都能正常跑完。
func TestPushNeverBlocksWithoutConsumer(t *testing.T) {
	const n = 4
	s := New(n) // 容量为 n，且不启动任何消费者

	var done atomic.Int64
	for range n {
		s.Go(func() { done.Add(1) }, func() {})
	}

	deadline := time.Now().Add(2 * time.Second)
	for done.Load() != n && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := done.Load(); got != n {
		t.Fatalf("expected all %d tasks' f to run without a consumer, got %d", n, got)
	}
	if got := s.cbQueue.Len(); got != n {
		t.Fatalf("expected %d undelivered callbacks sitting in cbQueue, got %d", n, got)
	}
}

// TestLinearContextTasksRunWithoutConsumer 是上面这条不变量在 LinearContext
// 上的对应版本：没有消费者时，多个任务的 f 仍然都能正常执行完毕，而不会像
// 旧的原生 channel 实现那样，第一个任务卡在发送上时级联卡死后续任务。
func TestLinearContextTasksRunWithoutConsumer(t *testing.T) {
	s := New(2) // 容量为 2，且不启动任何消费者
	lc := s.NewLinearContext()

	var firstDone, secondDone atomic.Bool
	lc.Go(func() { firstDone.Store(true) }, func() {})
	lc.Go(func() { secondDone.Store(true) }, func() {})

	deadline := time.Now().Add(2 * time.Second)
	for (!firstDone.Load() || !secondDone.Load()) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !firstDone.Load() || !secondDone.Load() {
		t.Fatalf("expected both tasks to run even with no consumer: first=%v second=%v", firstDone.Load(), secondDone.Load())
	}
	if got := s.cbQueue.Len(); got != 2 {
		t.Fatalf("expected both callbacks to sit in cbQueue undelivered, got len=%d", got)
	}
}

func TestSchedulerPanicRecovered(t *testing.T) {
	s := New(1)
	stop := make(chan struct{})
	drain(t, s, stop)

	var called atomic.Bool
	s.Go(func() {
		panic("boom")
	}, func() {
		called.Store(true)
	})

	deadline := time.Now().Add(2 * time.Second)
	for !called.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(stop)

	if !called.Load() {
		t.Fatalf("expected callback to run even after f panicked")
	}
}
