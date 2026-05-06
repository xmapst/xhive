// Package stat provides lightweight latency percentile statistics helpers.
package stat

import (
	"cmp"
	"encoding/json"
	"maps"
	"math"
	"reflect"
	"slices"
	"sync"
)

// TPStats 统计消息执行时间 Top Percentile，goroutine safe。
//
// 结构分两层锁：外层 TPStats.Mutex 保护 stats map 和 globalStat 指针，内层 tpStat.Mutex 保护单个 key 的样本数组。
// Add 在首次创建某个 key 时必须持有外层锁，防止多个 goroutine 同时创建同一 key 后互相覆盖，造成样本丢失。
type TPStats struct {
	globalStat *tpStat
	stats      map[any]*tpStat
	maxCnt     int
	sync.Mutex
}

// NewTPStats 创建 TPStats 实例，maxCnt 控制每个 key 最多保留的采样数。
func NewTPStats(maxCnt int) *TPStats {
	return &TPStats{
		maxCnt:     maxCnt,
		stats:      make(map[any]*tpStat),
		globalStat: &tpStat{},
	}
}

type tpStat struct {
	// records 只保留前 maxCnt 条样本，用于计算分位数；不会无限增长，避免长时间运行内存失控。
	records []int64
	// count/total 统计所有输入，不受 maxCnt 限制，因此 Avg 和 Count 反映完整流量，TpXX 反映采样窗口。
	count int64
	total int64
	sync.Mutex
}

func (s *tpStat) add(cost int64, maxCnt int) {
	s.count++
	s.total += cost
	if len(s.records) < maxCnt {
		s.records = append(s.records, cost)
	}
}

// Add 增加消息耗时统计（单位：微秒），name 为对应类型的零值（如空字符串、0）时忽略。
//
// name 是 any：调用方可能传字符串（定时器名）或 uint32（RPC 消息 ID，未注册/
// Ack 为 nil 时明确约定返回 0），用 reflect.Value.IsZero 统一判断，避免只
// 过滤字符串空值而漏掉数字类型的零值，导致失败/无意义的调用被计入一个虚假的
// "0" 分组，污染 Top-N 耗时统计。
func (s *TPStats) Add(name any, cost int64) {
	if name == nil {
		return
	}
	if v := reflect.ValueOf(name); v.IsZero() {
		return
	}
	s.Lock()
	defer s.Unlock()

	// 外层锁在整个“获取/创建 key 对应 tpStat”的过程中保持持有，确保并发首次 Add 同一 key 时不会覆盖。
	// globalStat 指针也受外层锁保护，避免与 Reset 并发时读到被替换中的对象。
	s.globalStat.Lock()
	s.globalStat.add(cost, s.maxCnt)
	s.globalStat.Unlock()

	v, found := s.stats[name]
	if !found {
		v = &tpStat{}
		s.stats[name] = v
	}
	v.Lock()
	defer v.Unlock()
	v.add(cost, s.maxCnt)
}

// Reset 清空所有统计。
func (s *TPStats) Reset() {
	s.Lock()
	defer s.Unlock()
	s.globalStat = &tpStat{}
	s.stats = make(map[any]*tpStat)
}

// Dump 将统计信息序列化为 JSON 字符串，返回 Tp99 最高的前 n 条。
//
// Dump 先在外层锁下复制 stats map 的快照，然后释放外层锁，再逐个锁定 tpStat 计算分位数。
// 这样可以避免长时间持有全局锁阻塞 Add；排序依据使用 Tp99，便于优先暴露尾延迟最高的消息类型。
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

// TpDumpStats 表示 Dump 输出的整体统计结构。
type TpDumpStats struct {
	Global *TpDumpStat
	Kinds  []*TpDumpStat
}

// TpDumpStat 表示 Dump 输出的单项统计结构。
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
