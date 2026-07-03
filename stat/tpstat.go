package stat

import (
	"cmp"
	"encoding/json"
	"maps"
	"math"
	"slices"
	"sync"
)

// TPStats 统计消息执行时间 Top Percentile，goroutine safe
type TPStats struct {
	globalStat *tpStat
	stats      map[any]*tpStat
	maxCnt     int
	sync.Mutex
}

// NewTPStats 创建实例，maxCnt 控制每个 key 最多保留的采样数
func NewTPStats(maxCnt int) *TPStats {
	return &TPStats{
		maxCnt:     maxCnt,
		stats:      make(map[any]*tpStat),
		globalStat: &tpStat{},
	}
}

type tpStat struct {
	records []int64
	count   int64
	total   int64
	sync.Mutex
}

func (s *tpStat) add(cost int64, maxCnt int) {
	s.count++
	s.total += cost
	if len(s.records) < maxCnt {
		s.records = append(s.records, cost)
	}
}

// Add 增加消息耗时统计（单位：微秒），name 为空时忽略
func (s *TPStats) Add(name any, cost int64) {
	if name == "" {
		return
	}
	s.Lock()

	s.globalStat.Lock()
	s.globalStat.add(cost, s.maxCnt)
	s.globalStat.Unlock()

	v, found := s.stats[name]
	if found {
		s.Unlock()
		v.Lock()
		defer v.Unlock()
		v.add(cost, s.maxCnt)
	} else {
		defer s.Unlock()
		s.stats[name] = &tpStat{
			records: []int64{cost},
			count:   1,
			total:   cost,
		}
	}
}

// Reset 清空所有统计
func (s *TPStats) Reset() {
	s.Lock()
	defer s.Unlock()
	s.globalStat = &tpStat{}
	s.stats = make(map[any]*tpStat)
}

// Dump 将统计信息序列化为 JSON 字符串，返回 Tp99 最高的前 n 条
func (s *TPStats) Dump(n int) string {
	s.Lock()
	snapshot := make(map[any]*tpStat, len(s.stats))
	maps.Copy(snapshot, s.stats)
	globalStat := s.globalStat
	s.Unlock()

	dumpStats := &TpDumpStats{}
	dumpStats.Global = globalStat.dump()
	dumpStats.Global.ID = "ALL"

	stats := make([]*TpDumpStat, 0, len(snapshot))
	for k, v := range snapshot {
		result := v.dump()
		result.ID = k
		stats = append(stats, result)
	}
	slices.SortFunc(stats, func(a, b *TpDumpStat) int {
		return cmp.Compare(b.Tp99, a.Tp99)
	})
	if n <= 0 || n > len(stats) {
		n = len(stats)
	}
	dumpStats.Kinds = stats[:n]

	b, _ := json.Marshal(dumpStats)
	return string(b)
}

func (s *tpStat) dump() *TpDumpStat {
	s.Lock()
	defer s.Unlock()

	result := new(TpDumpStat)
	if len(s.records) == 0 {
		return result
	}
	slices.Sort(s.records)
	result.Count = s.count
	result.TpCount = int64(len(s.records))
	result.Tp25 = s.percentile(s.records, 25)
	result.Tp50 = s.percentile(s.records, 50)
	result.Tp75 = s.percentile(s.records, 75)
	result.Tp90 = s.percentile(s.records, 90)
	result.Tp95 = s.percentile(s.records, 95)
	result.Tp99 = s.percentile(s.records, 99)
	result.Tp100 = s.percentile(s.records, 100)
	if s.count > 0 {
		result.Avg = s.total / s.count
	}
	return result
}

func (s *tpStat) percentile(records []int64, pct int) int64 {
	i := int(math.Ceil(float64(len(records))*float64(pct)/100.0)) - 1
	return records[i]
}

// TpDumpStats Dump 输出结构
type TpDumpStats struct {
	Global *TpDumpStat
	Kinds  []*TpDumpStat
}

type TpDumpStat struct {
	ID      any   `json:"id,omitempty"`
	Count   int64 `json:"count,omitempty"`
	TpCount int64 `json:"tp_count,omitempty"`
	Tp25    int64 `json:"tp_25,omitempty"`
	Tp50    int64 `json:"tp_50,omitempty"`
	Tp75    int64 `json:"tp_75,omitempty"`
	Tp90    int64 `json:"tp_90,omitempty"`
	Tp95    int64 `json:"tp_95,omitempty"`
	Tp99    int64 `json:"tp_99,omitempty"`
	Tp100   int64 `json:"tp_100,omitempty"`
	Avg     int64 `json:"avg,omitempty"`
}
