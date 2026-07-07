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
