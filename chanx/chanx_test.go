package chanx

import (
	"context"
	"testing"
	"time"
)

func recvValue[T comparable](t *testing.T, ch <-chan T, want T, timeout time.Duration) {
	t.Helper()
	select {
	case got, ok := <-ch:
		if !ok {
			t.Fatalf("channel closed, want %v", want)
		}
		if got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
	case <-time.After(timeout):
		t.Fatalf("timeout waiting value %v", want)
	}
}

func assertClosed[T any](t *testing.T, ch <-chan T, timeout time.Duration) {
	t.Helper()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("channel still open")
		}
	case <-time.After(timeout):
		t.Fatal("timeout waiting channel close")
	}
}

// TestUnboundedFIFOAndCloseDrains 验证无界队列在突发写入超过初始容量时仍保持 FIFO，
// Close 后不会丢弃已缓冲数据，而是在全部读尽后关闭 Out。
func TestUnboundedFIFOAndCloseDrains(t *testing.T) {
	u := NewUnbounded[int](context.Background(), WithInitialCapacity(2))
	for i := 0; i < 128; i++ {
		u.In() <- i
	}
	u.Close()

	for i := 0; i < 128; i++ {
		recvValue(t, u.Out(), i, time.Second)
	}
	assertClosed(t, u.Out(), time.Second)
	if got := u.Len(); got != 0 {
		t.Fatalf("Len after drain = %d, want 0", got)
	}
}

func TestUnboundedContextCancelClosesOut(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	u := NewUnbounded[int](ctx, WithInitialCapacity(4))
	u.In() <- 1
	cancel()
	assertClosed(t, u.Out(), time.Second)
}

func TestUnboundedBufferedChannelsAndApproxLen(t *testing.T) {
	u := NewUnbounded[int](context.Background(), WithInitialCapacity(1), WithChanCapacity(4, 4))
	defer u.Close()

	for i := 0; i < 10; i++ {
		u.In() <- i
	}
	if got := u.Len(); got < 0 {
		t.Fatalf("Len should never be negative: %d", got)
	}
	for i := 0; i < 10; i++ {
		recvValue(t, u.Out(), i, time.Second)
	}
}

// TestRingGrowShrinkAndOrder 直接验证内部 ring：扩容/收缩过程中必须保持元素顺序，
// 且收缩不能低于创建时的最小容量，避免频繁抖动。
func TestRingGrowShrinkAndOrder(t *testing.T) {
	r := newRing[int](2)
	for i := 0; i < 100; i++ {
		r.push(i)
	}
	if r.len() != 100 {
		t.Fatalf("ring len = %d, want 100", r.len())
	}
	if len(r.buf) <= 2 {
		t.Fatalf("ring did not grow, cap=%d", len(r.buf))
	}

	for i := 0; i < 100; i++ {
		if got := r.front(); got != i {
			t.Fatalf("front at %d = %d", i, got)
		}
		if got := r.pop(); got != i {
			t.Fatalf("pop at %d = %d", i, got)
		}
	}
	if r.len() != 0 {
		t.Fatalf("ring len after pop = %d, want 0", r.len())
	}
	if len(r.buf) < r.minCap {
		t.Fatalf("ring shrunk below minCap: cap=%d min=%d", len(r.buf), r.minCap)
	}
}

// TestRingShrinkHysteresis 验证 ring 的收缩迟滞策略：刚进入满足收缩
// 条件的区间时不会立即收缩；只有连续 shrinkStreakThreshold 次 pop
// 都满足收缩条件才会真正收缩容量。
func TestRingShrinkHysteresis(t *testing.T) {
	r := newRing[int](2)
	for i := 0; i < 40; i++ {
		r.push(i)
	}
	grownCap := len(r.buf) // count=40, cap=64
	if grownCap <= 2 {
		t.Fatalf("ring did not grow, cap=%d", grownCap)
	}

	// 从 40 降到 16（cap 的 25%）需要 pop 24 次；从这次 pop 起才开始
	// 满足收缩条件，计数器从 0 累加到 1。
	for i := 0; i < 24; i++ {
		r.pop()
	}
	if len(r.buf) != grownCap {
		t.Fatalf("ring shrunk prematurely: cap=%d, want %d", len(r.buf), grownCap)
	}
	if r.shrinkStreak != 1 {
		t.Fatalf("shrinkStreak = %d, want 1 after first qualifying pop", r.shrinkStreak)
	}

	// 继续 pop 到累计计数为 shrinkStreakThreshold-1（还差最后一次未收缩）。
	// count 从 16 降到 16-(shrinkStreakThreshold-2)，占用率进一步降低，
	// 始终满足收缩条件。
	for i := 0; i < shrinkStreakThreshold-2; i++ {
		r.pop()
	}
	if r.shrinkStreak != shrinkStreakThreshold-1 {
		t.Fatalf("shrinkStreak = %d, want %d", r.shrinkStreak, shrinkStreakThreshold-1)
	}
	if len(r.buf) != grownCap {
		t.Fatalf("ring shrunk prematurely at streak=%d: cap=%d, want %d",
			r.shrinkStreak, len(r.buf), grownCap)
	}

	// 第 shrinkStreakThreshold 次满足条件的 pop：应真正触发收缩并清零计数。
	r.pop()
	if len(r.buf) >= grownCap {
		t.Fatalf("ring did not shrink after reaching streak threshold: cap=%d", len(r.buf))
	}
	if r.shrinkStreak != 0 {
		t.Fatalf("shrinkStreak should reset after shrink, got %d", r.shrinkStreak)
	}
}

func TestRingMinimumCapacityClamp(t *testing.T) {
	r := newRing[int](0)
	if len(r.buf) != 1 || r.minCap != 1 {
		t.Fatalf("newRing(0) cap=%d minCap=%d, want 1/1", len(r.buf), r.minCap)
	}
	r.push(42)
	if got := r.pop(); got != 42 {
		t.Fatalf("pop = %d, want 42", got)
	}
}
