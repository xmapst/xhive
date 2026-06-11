package stat

import (
	"encoding/json"
	"testing"
)

func TestTPStatsAddDumpAndReset(t *testing.T) {
	s := NewTPStats(10)
	s.Add("rpcA", 10)
	s.Add("rpcA", 20)
	s.Add("rpcA", 30)
	s.Add("rpcB", 100)
	s.Add("", 999)

	if got := s.globalStat.count; got != 4 {
		t.Fatalf("global count = %d, want 4", got)
	}
	if got := s.globalStat.total; got != 160 {
		t.Fatalf("global total = %d, want 160", got)
	}

	dump := s.Dump(1)
	if dump == "" {
		t.Fatal("Dump() = empty")
	}
	var payload TpDumpStats
	if err := json.Unmarshal([]byte(dump), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if payload.Global == nil || payload.Global.ID != "ALL" {
		t.Fatalf("Global = %#v, want ID=ALL", payload.Global)
	}
	if len(payload.Kinds) != 1 {
		t.Fatalf("len(Kinds) = %d, want 1", len(payload.Kinds))
	}
	if payload.Kinds[0].ID != "rpcB" {
		t.Fatalf("top kind ID = %#v, want rpcB", payload.Kinds[0].ID)
	}

	dumpAll := s.Dump(0)
	var payloadAll TpDumpStats
	if err := json.Unmarshal([]byte(dumpAll), &payloadAll); err != nil {
		t.Fatalf("json.Unmarshal(all) error = %v", err)
	}
	if len(payloadAll.Kinds) != 2 {
		t.Fatalf("len(Kinds all) = %d, want 2", len(payloadAll.Kinds))
	}

	s.Reset()
	if got := len(s.stats); got != 0 {
		t.Fatalf("len(stats) after Reset = %d, want 0", got)
	}
	if got := s.globalStat.count; got != 0 {
		t.Fatalf("global count after Reset = %d, want 0", got)
	}
}

func TestTpStatDumpAndPercentile(t *testing.T) {
	ts := &tpStat{}
	for _, v := range []int64{50, 10, 30, 20, 40} {
		ts.add(v, 10)
	}
	result := ts.dump()
	if result.Count != 5 || result.TpCount != 5 {
		t.Fatalf("dump count = (%d,%d), want (5,5)", result.Count, result.TpCount)
	}
	if result.Tp25 != 20 || result.Tp50 != 30 || result.Tp75 != 40 || result.Tp100 != 50 {
		t.Fatalf("percentiles = %#v", result)
	}
	if result.Avg != 30 {
		t.Fatalf("Avg = %d, want 30", result.Avg)
	}
	if got := ts.percentile([]int64{1, 2, 3, 4}, 50); got != 2 {
		t.Fatalf("percentile() = %d, want 2", got)
	}
}

func TestTPStatsRespectsMaxCount(t *testing.T) {
	s := NewTPStats(2)
	s.Add("rpc", 10)
	s.Add("rpc", 20)
	s.Add("rpc", 30)
	st := s.stats["rpc"]
	if st == nil {
		t.Fatal("stats[rpc] = nil")
	}
	if got := len(st.records); got != 2 {
		t.Fatalf("len(records) = %d, want 2", got)
	}
	if got := st.count; got != 3 {
		t.Fatalf("count = %d, want 3", got)
	}
}

// TestTpStatDumpEmpty 覆盖 dump 在无采样记录时的提前返回分支。
func TestTpStatDumpEmpty(t *testing.T) {
	ts := &tpStat{}
	r := ts.dump()
	if r.Count != 0 || r.TpCount != 0 {
		t.Fatalf("empty dump = %#v, want zero counts", r)
	}
	if r.Tp50 != 0 || r.Tp99 != 0 || r.Avg != 0 {
		t.Fatalf("empty dump percentiles = %#v, want all zero", r)
	}
}

// TestTPStatsDumpWithoutData 覆盖 Dump 在没有任何 key 统计时的输出。
func TestTPStatsDumpWithoutData(t *testing.T) {
	s := NewTPStats(10)
	dump := s.Dump(5)
	var payload TpDumpStats
	if err := json.Unmarshal([]byte(dump), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if payload.Global == nil || payload.Global.ID != "ALL" {
		t.Fatalf("Global = %#v, want ID=ALL", payload.Global)
	}
	if len(payload.Kinds) != 0 {
		t.Fatalf("len(Kinds) = %d, want 0", len(payload.Kinds))
	}
}

// TestTPStatsAddSameKeyTwiceTakesExistingBranch 让同一 key 第二次 Add 走"已存在"分支，
// 与首次创建分支区分，确保两条路径都被覆盖。
func TestTPStatsAddSameKeyTwiceTakesExistingBranch(t *testing.T) {
	s := NewTPStats(10)
	s.Add("dup", 5)
	s.Add("dup", 15)
	st := s.stats["dup"]
	if st == nil {
		t.Fatal("stats[dup] = nil")
	}
	if st.count != 2 {
		t.Fatalf("count = %d, want 2", st.count)
	}
	if st.total != 20 {
		t.Fatalf("total = %d, want 20", st.total)
	}
}
