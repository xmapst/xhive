package stat

import (
	"encoding/json"
	"sync"
	"testing"
)

func decodeDump(t *testing.T, raw string) TpDumpStats {
	t.Helper()
	var out TpDumpStats
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("Dump returned invalid json: %v, raw=%s", err, raw)
	}
	return out
}

func TestTPStatsAddDumpPercentilesAndTopN(t *testing.T) {
	s := NewTPStats(100)
	for i := int64(1); i <= 100; i++ {
		s.Add("slow", i)
	}
	for i := int64(1); i <= 10; i++ {
		s.Add("fast", i)
	}

	dump := decodeDump(t, s.Dump(1))
	if dump.Global.ID != "ALL" {
		t.Fatalf("global id = %v, want ALL", dump.Global.ID)
	}
	if dump.Global.Count != 110 {
		t.Fatalf("global count = %d, want 110", dump.Global.Count)
	}
	if dump.Global.Tp100 != 100 {
		t.Fatalf("global tp100 = %d, want 100", dump.Global.Tp100)
	}
	if len(dump.Kinds) != 1 {
		t.Fatalf("topN kinds len = %d, want 1", len(dump.Kinds))
	}
	if dump.Kinds[0].ID != "slow" {
		t.Fatalf("top kind id = %v, want slow", dump.Kinds[0].ID)
	}
	if dump.Kinds[0].Tp25 != 25 || dump.Kinds[0].Tp50 != 50 || dump.Kinds[0].Tp99 != 99 || dump.Kinds[0].Tp100 != 100 {
		t.Fatalf("unexpected percentiles: %+v", dump.Kinds[0])
	}
}

func TestTPStatsIgnoresZeroNames(t *testing.T) {
	s := NewTPStats(10)
	s.Add(nil, 1)
	s.Add("", 2)
	s.Add(uint32(0), 3)
	s.Add("valid", 4)

	dump := decodeDump(t, s.Dump(0))
	if dump.Global.Count != 1 {
		t.Fatalf("global count = %d, want 1", dump.Global.Count)
	}
	if len(dump.Kinds) != 1 || dump.Kinds[0].ID != "valid" {
		t.Fatalf("unexpected kinds: %+v", dump.Kinds)
	}
}

func TestTPStatsMaxCountKeepsCountAndCapsSamples(t *testing.T) {
	s := NewTPStats(3)
	for i := int64(10); i <= 50; i += 10 {
		s.Add("limited", i)
	}
	dump := decodeDump(t, s.Dump(0))
	if dump.Global.Count != 5 {
		t.Fatalf("global count = %d, want 5", dump.Global.Count)
	}
	if dump.Global.TpCount != 3 {
		t.Fatalf("global tp_count = %d, want 3", dump.Global.TpCount)
	}
	if len(dump.Kinds) != 1 || dump.Kinds[0].Count != 5 || dump.Kinds[0].TpCount != 3 {
		t.Fatalf("unexpected limited stat: %+v", dump.Kinds)
	}
	if dump.Global.Avg != 30 {
		t.Fatalf("global avg = %d, want 30", dump.Global.Avg)
	}
}

func TestTPStatsReset(t *testing.T) {
	s := NewTPStats(10)
	s.Add("a", 1)
	s.Reset()
	dump := decodeDump(t, s.Dump(0))
	if dump.Global.Count != 0 || dump.Global.TpCount != 0 {
		t.Fatalf("global after reset = %+v", dump.Global)
	}
	if len(dump.Kinds) != 0 {
		t.Fatalf("kinds after reset = %+v, want empty", dump.Kinds)
	}
}

// TestTPStatsConcurrentAddAndDump 覆盖 Add 与 Dump 并发执行：重点防止同一新 key 首次 Add 时多个 tpStat
// 互相覆盖导致样本丢失，同时验证 Dump 快照逻辑不会与 Add 产生数据竞争。
func TestTPStatsConcurrentAddAndDump(t *testing.T) {
	s := NewTPStats(1000)
	var wg sync.WaitGroup
	for g := range 8 {
		name := g + 1
		wg.Go(func() {
			for i := range 200 {
				s.Add(name, int64(i+1))
				_ = s.Dump(5)
			}
		})
	}
	wg.Wait()

	dump := decodeDump(t, s.Dump(0))
	if dump.Global.Count != 1600 {
		t.Fatalf("global count = %d, want 1600", dump.Global.Count)
	}
	if len(dump.Kinds) != 8 {
		t.Fatalf("kinds len = %d, want 8", len(dump.Kinds))
	}
}
