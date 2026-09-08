package queue

import (
	"runtime"
	"sync"
	"testing"
)

// 这组用例记录用 Queue 换掉原生有界 channel 的代价与收益，供日后调整实现时
// 对照：收益在内存（TestIdleMemoryFootprint），代价在跨 goroutine 的单次收发
// （BenchmarkQueueHandoff 对 BenchmarkChanHandoff）。

type benchMsg struct{ a, b int64 }

const benchCap = 4096

// stopMsg 是收尾哨兵。两个 Handoff 用它而不是各自的关闭机制（chan 用
// close、Queue 用 Close），是为了让两边的消费循环结构完全对称——一边多一路
// select 或多一次加锁判断，比出来的差值就不再只属于数据结构本身。
//
// Queue 那边还有个更实际的原因：Close 只发一个 notEmpty 信号，消费者取走
// 最后一件后队列已空、不再补信号，那个信号又早被消耗掉，于是永久阻塞。
var stopMsg = &benchMsg{}

// BenchmarkChanHandoff / BenchmarkQueueHandoff 测**队列常空**时的单次交接：
// 消费者每收到一个信号只取一件，取完就又空了，于是每条消息都要真正唤醒一次
// goroutine。这是 Queue 相对原生 channel 最吃亏的场景——原生 channel 在这里
// 走 runtime 的直接交接快路径，而 Queue 要多付一次 Push 的锁和一次 Pop 的锁。
//
// 与之对照的是下面的 Serial：队列里有积压时，Pop 会把信号补回去，消费者无需
// 阻塞/唤醒就能连续取件，Queue 反而更快。真实模块的开销落在两者之间，负载越
// 高越靠近 Serial。
func BenchmarkChanHandoff(b *testing.B) {
	ch := make(chan *benchMsg, benchCap)
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			if <-ch == stopMsg {
				return
			}
		}
	})

	m := &benchMsg{}
	for b.Loop() {
		ch <- m
	}
	ch <- stopMsg
	wg.Wait()
}

func BenchmarkQueueHandoff(b *testing.B) {
	q := New[*benchMsg](benchCap)
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			<-q.NotEmpty()
			v, ok := q.Pop()
			if !ok {
				continue // 边沿信号存在虚假唤醒
			}
			if v == stopMsg {
				return
			}
		}
	})

	ctx := b.Context()
	m := &benchMsg{}
	for b.Loop() {
		_ = q.PushWait(ctx, m)
	}
	_ = q.PushWait(ctx, stopMsg)
	wg.Wait()
}

// 单线程收发，剥离调度噪声，只看数据结构本身的开销；也代表高负载下队列有
// 积压、消费者无需阻塞唤醒时的成本。
func BenchmarkChanSerial(b *testing.B) {
	ch := make(chan *benchMsg, benchCap)
	m := &benchMsg{}
	for b.Loop() {
		ch <- m
		<-ch
	}
}

func BenchmarkQueueSerial(b *testing.B) {
	q := New[*benchMsg](benchCap)
	m := &benchMsg{}
	for b.Loop() {
		_ = q.Push(m)
		q.Pop()
	}
}

// TestIdleMemoryFootprint 报告一条空闲队列的常驻内存——这次替换的全部理由。
// 原生 channel 的缓冲区在 make 时一次性分配，容量多大就占多少；
// Queue 只按实际积压分配，上限只封顶。
//
// 写成 Test 而不是 Benchmark：它测的是一次性的分配量，不是每次操作的耗时，
// 交给 -benchtime 去乘 b.N 只会把 1000 个队列的分配放大成十亿次。
func TestIdleMemoryFootprint(t *testing.T) {
	const n = 1000

	measure := func(alloc func() any) uint64 {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		keep := make([]any, n)
		for i := range n {
			keep[i] = alloc()
		}
		runtime.GC()
		runtime.ReadMemStats(&after)
		runtime.KeepAlive(keep)
		return (after.HeapAlloc - before.HeapAlloc) / n
	}

	native := measure(func() any { return make(chan *benchMsg, benchCap) })
	dynamic := measure(func() any { return New[*benchMsg](benchCap) })

	t.Logf("上限 %d 的空闲队列：原生 chan %d B，Queue %d B（省 %.1f 倍）",
		benchCap, native, dynamic, float64(native)/float64(dynamic))

	// 断言只守住数量级，不锁死具体数字：实现细节（initCap、结构体字段）
	// 变化时不该无谓地失败，但如果哪天又变回按上限预分配，这里必须炸。
	if dynamic*8 > native {
		t.Fatalf("空闲占用 %d B，与原生 chan 的 %d B 处于同一量级——队列疑似又在按上限预分配", dynamic, native)
	}
}
