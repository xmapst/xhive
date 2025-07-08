# xhive

> 基于 **Actor 模型** 的轻量级模块化应用框架，专为高并发游戏服务器等事件驱动场景设计。

[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

`xhive` 把一个进程拆分为若干**模块（Module）**，每个模块拥有独立的 goroutine、ChanRPC 服务端与定时器管理器。模块之间只通过消息通信，内部状态天然隔离——**开发者无需为访问模块状态写任何一行加锁代码**。

---

## 目录

- [核心特性](#核心特性)
- [设计理念](#设计理念)
- [架构总览](#架构总览)
- [安装](#安装)
- [快速开始](#快速开始)
- [核心概念](#核心概念)
  - [模块（IModule）](#模块imodule)
  - [Skeleton 骨架](#skeleton-骨架)
  - [ChanRPC 跨模块通信](#chanrpc-跨模块通信)
  - [Timer 定时器](#timer-定时器)
  - [信号管理](#信号管理)
  - [动态模块（热加载）](#动态模块热加载)
  - [统计（TPStats）](#统计tpstats)
- [应用生命周期](#应用生命周期)
- [包级 API 速查](#包级-api-速查)
- [目录结构](#目录结构)
- [测试](#测试)
- [常见问题](#常见问题)
- [License](#license)

---

## 核心特性

| 特性 | 说明 |
| --- | --- |
| **Actor 模型** | 每个模块单 goroutine 串行处理所有事件（RPC / 回调 / 定时器），消除内部并发竞争，无需加锁。 |
| **ChanRPC** | 基于 channel 的进程内 RPC，支持 `Cast`（单向）/ `AsyncCall`（异步）/ `Call`（同步）三种语义，消息路由 O(1)。 |
| **最小堆定时器** | 单 goroutine 派发 + 最小堆调度，精度无损、无最大时长上限、无 tick 空转，支持一次性 Timer / 周期 Ticker，可加速 / 延迟 / 取消。 |
| **静态 + 动态模块** | 静态模块随应用启停；动态模块支持运行时热加载 / 热卸载，panic 不影响进程。 |
| **优雅关闭** | 监听 `SIGINT/SIGKILL/SIGTERM`，按逆序（LIFO）停止模块，每个模块独立超时保护。 |
| **可扩展信号管理** | 业务层可注册 `SIGHUP`（配置热重载）等自定义信号处理器，并发执行且 panic 隔离。 |
| **内置性能统计** | 每个模块自动统计消息处理耗时的 TP25~TP100 分位，定期 dump，便于定位积压瓶颈。 |
| **零外部依赖** | 仅依赖 Go 标准库（`log/slog` 等），开箱即用。 |

---

## 设计理念

传统并发服务器需要在共享状态上反复加锁，极易引入死锁与数据竞争。`xhive` 借鉴 [Leaf](https://github.com/name5566/leaf) 等框架的思路，采用 **Actor 模型**：

```
每个模块 = 1 个 goroutine + 1 个事件队列（channel）
所有事件在该 goroutine 内串行处理 → 模块内部状态访问天然无锁
模块之间只通过 ChanRPC 消息通信 → 状态彻底隔离
```

这是一次明确的权衡：**牺牲单模块的 CPU 并行度，换取零锁开销与极低的编程复杂度**。在 IO 密集、逻辑复杂的游戏服务器场景中，这一权衡通常非常划算。

---

## 架构总览

```
                         ┌─────────────────────────────────────┐
                         │              app (框架核心)            │
                         │   状态机: None → Init → Run → Stop      │
                         │   SignalManager  ·  静态/动态模块管理     │
                         └───────────────┬─────────────────────┘
                                         │ 管理生命周期
            ┌────────────────────────────┼────────────────────────────┐
            ▼                            ▼                            ▼
   ┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
   │   Module A       │        │   Module B       │        │   Module C (动态) │
   │  ┌────────────┐  │        │  ┌────────────┐  │        │                  │
   │  │ Skeleton    │  │       │  │ Skeleton    │  │       │   独立 goroutine   │
   │  │  ├ Server    │◄─┼── RPC ─┼─►│  ├ Client    │  │      │   panic 不退进程   │
   │  │  ├ Client    │  │       │  │  ├ Server    │  │      └─────────────────┘
   │  │  ├ Timer Mgr │  │       │  │  ├ Timer Mgr │  │
   │  │  └ TPStats   │  │       │  │  └ TPStats   │  │
   │  └────────────┘  │        │  └────────────┘  │
   │  单 goroutine 事件循环 │     │  单 goroutine 事件循环 │
   └─────────────────┘         └─────────────────┘

   每个模块的事件循环（Skeleton.OnRun）通过 select 串行处理：
     ① ctx.Done()          —— 框架停止信号
     ② timer.Event()       —— 定时器到期
     ③ client.ChanAsyncRet —— 异步 RPC 回调
     ④ server.ChanCall     —— 其他模块发来的 RPC 请求
```

---

## 安装

要求 **Go 1.26+**。

```bash
go get github.com/xmapst/xhive
```

```go
import "github.com/xmapst/xhive"
```

---

## 快速开始

下面实现两个模块：`pinger` 周期性地向 `ponger` 发起异步 RPC，`ponger` 收到后回包。

```go
package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/xmapst/xhive"
	"github.com/xmapst/xhive/chanrpc"
	"github.com/xmapst/xhive/timer"
)

// ---------- 消息定义 ----------
type PingReq struct{ Seq int }
type PongAck struct{ Seq int }

// ---------- Ponger 模块：响应 Ping ----------
type Ponger struct {
	*xhive.Skeleton
}

func NewPonger() *Ponger {
	return &Ponger{Skeleton: xhive.NewSkeleton("ponger")}
}

func (m *Ponger) OnInit() error {
	// 注册 PingReq 的处理函数，返回 PongAck
	return m.RegisterChanRPC(&PingReq{}, func(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
		req := ci.Request.(*PingReq)
		slog.Info("ponger received", "seq", req.Seq)
		return &chanrpc.RetInfo{Ack: &PongAck{Seq: req.Seq}}
	})
}

func (m *Ponger) OnDestroy() {}

// ---------- Pinger 模块：定时发起 Ping ----------
type Pinger struct {
	*xhive.Skeleton
	seq int
}

func NewPinger() *Pinger {
	return &Pinger{Skeleton: xhive.NewSkeleton("pinger")}
}

func (m *Pinger) OnInit() error {
	// 注册一个名为 "tick" 的定时器处理器
	m.RegisterTimer("tick", func(_ int64, _ map[string]string) {
		m.seq++
		// 异步调用 ponger，回调在本模块事件循环中串行执行（无需加锁）
		_ = m.AsyncCall("ponger", &PingReq{Seq: m.seq}, func(ri *chanrpc.RetInfo) {
			if ri.Err != nil {
				slog.Error("ping failed", "err", ri.Err)
				return
			}
			ack := ri.Ack.(*PongAck)
			slog.Info("pinger got pong", "seq", ack.Seq)
		})
		// 周期触发：1 秒后再次创建定时器
		m.NewTimer("tick", time.Second)
	})
	// 启动首个定时器
	m.NewTimer("tick", time.Second)
	return nil
}

func (m *Pinger) OnDestroy() {}

// ---------- 入口 ----------
func main() {
	// 注意注册顺序：被依赖的 ponger 先注册（先初始化），pinger 后注册
	xhive.Run(NewPonger(), NewPinger())
	// Run 会阻塞，直到收到 SIGINT / SIGTERM 后优雅关闭
}

var _ = timer.WithTicker // 也可用 m.NewTimer("tick", time.Second, timer.WithTicker()) 实现自动续期
```

运行后按 `Ctrl+C` 即触发优雅关闭，框架会按逆序停止所有模块。

> 提示：周期任务除手动在回调里重建定时器外，也可直接用 `m.NewTimer("tick", time.Second, timer.WithTicker())` 让框架自动续期。

---

## 核心概念

### 模块（IModule）

每个业务单元都实现 `IModule` 接口，框架据此管理其完整生命周期：

```go
type IModule interface {
	Name() string              // 模块唯一名称，用于日志与 RPC 寻址
	OnInit() error             // 初始化；任一模块失败则终止整个应用启动
	OnRun(ctx context.Context) // 主循环；应监听 ctx.Done() 并在收到取消时退出
	OnDestroy()                // 销毁；模块关闭流程中调用，释放资源
	ChanRPC() *chanrpc.Server  // 返回 RPC 服务端；nil 表示不接受外部 RPC
}
```

绝大多数情况下你**不需要手写这些方法**——内嵌 `Skeleton` 即可获得 `Name / OnRun / ChanRPC` 的默认实现，只需重写 `OnInit`（注册处理器）和 `OnDestroy`（清理资源）。

### Skeleton 骨架

`Skeleton` 把 ChanRPC 服务端 / 客户端、定时器管理器、耗时统计整合为统一的事件驱动骨架，并实现了标准的单 goroutine 事件循环（`OnRun`）。

常用方法：

| 类别 | 方法 |
| --- | --- |
| RPC 注册 | `RegisterChanRPC(msg, handler)` |
| RPC 调用 | `Cast` / `AsyncCall` / `Call` |
| 定时器 | `RegisterTimer` / `NewTimer` / `AccAbsTimer` / `AccPctTimer` / `DelayAbsTimer` / `DelayPctTimer` / `UpdateTimer` / `CancelTimer` |
| 统计 | `DumpStat(n)` |

> 内部各组件缓冲区默认为 **100000**，适合高并发场景。若单模块消息量远超此值，需在 `NewSkeleton` 处自行调整以避免背压。

### ChanRPC 跨模块通信

`chanrpc` 包提供进程内 RPC，三种调用语义按需选择：

| 语义 | 方法 | 是否等待结果 | 适用场景 | 风险 |
| --- | --- | --- | --- | --- |
| **单向投递** | `Cast` | 否 | 日志上报、事件通知、埋点 | 无 |
| **异步调用** | `AsyncCall` | 否（回调） | **推荐**，绝大多数跨模块调用 | 无（回调在本模块串行执行） |
| **同步调用** | `Call` | 是（阻塞） | 调用关系明确单向、无环 | ⚠️ 调用成环会**死锁** |

消息 ID 由类型的全限定名经 BKDR 哈希自动推导（结果带缓存）；也可让消息实现 `IMessage` 接口自定义 ID。可通过 `chanrpc.WithMeta(k, v)` 为单次调用附加元数据。

```go
// 单向
m.Cast("logger", &LogEvent{Msg: "hello"})

// 异步（推荐）
m.AsyncCall("db", &QueryReq{ID: 1}, func(ri *chanrpc.RetInfo) {
	// 回调与业务逻辑同 goroutine，可直接读写模块状态，无需加锁
})

// 同步（谨慎使用）
ri := m.Call("config", &GetReq{Key: "max"})
```

### Timer 定时器

`timer` 包采用**单 goroutine 派发 + 最小堆**（`container/heap`）调度：所有定时器按到期时刻组织进一个最小堆，由唯一的派发 goroutine 维护，醒来后批量投递已到期的定时器到 `chanFired`，回调由消费方（`Skeleton.OnRun`）的单一 goroutine 执行。

相比备选方案：

- 相比多级时间轮：**精度无损**（不被 tick 粒度钉死）、**无最大定时时长上限**、无 tick 空转；
- 相比 `time.AfterFunc`：到期触发不再为每个定时器 spawn 一个 runtime goroutine——无论 1 个还是十万个定时器同时到期，派发始终只用这一个 goroutine，消除海量定时器扎堆到期时的 goroutine 尖峰。

```go
// 一次性定时器：5 秒后触发
id := m.NewTimer("reborn", 5*time.Second, timer.WithMetadata(map[string]string{"uid": "1001"}))

// 周期 Ticker：每 1 秒触发，自动续期（消除累积漂移）
m.NewTimer("heartbeat", time.Second, timer.WithTicker())

// 加速 / 延迟 / 精确设置 / 取消
m.AccAbsTimer(id, time.Second)            // 提前 1 秒
m.DelayPctTimer(id, 2000)                 // 延迟 20%（万分比，PctBase=10000）
m.UpdateTimer(id, time.Now().Add(time.Minute)) // 直接设置绝对到期时刻
m.CancelTimer(id)                         // 取消（幂等）
```

时间相关参数统一使用 Go 内置类型：时长用 `time.Duration`（如 `5*time.Second`），绝对时刻用 `time.Time`。
加速 / 延迟提供两组方法：`AccAbsTimer` / `DelayAbsTimer` 按绝对时长（`time.Duration`）调整，
`AccPctTimer` / `DelayPctTimer` 按万分比（`int64`，`PctBase=10000`）调整。

### 信号管理

框架启动时默认绑定：

- `SIGINT` / `SIGKILL` / `SIGTERM` → **触发优雅关闭**（框架保留，不可覆盖）
- `SIGHUP` → 仅在业务层未注册处理器时，默认记录日志并继续运行（业务注册后默认行为不再追加）

业务层可注册自定义信号处理器（同一信号支持多个处理器，**并发执行且 panic 隔离**）：

```go
xhive.RegisterSignal(func() {
	slog.Info("收到 SIGHUP，重新加载配置")
	reloadConfig()
}, syscall.SIGHUP)
```

> 向 `SIGINT/SIGKILL/SIGTERM` 注册会返回错误——它们由框架独占。

### 动态模块（热加载）

静态模块随应用启停且不可卸载；动态模块支持运行时增删，且 **panic 不会导致进程退出**（仅记录日志）。

```go
// 运行时添加并启动
xhive.AddDynamicModules(NewActivityModule())

// 查询当前动态模块名列表
names := xhive.DynamicModules()

// 同步移除并销毁（cancel → 等待退出 → OnDestroy → 移除）
xhive.RemoveDynamicModule("activity")
```

适合活动玩法、临时服务等需要不停机上下线的场景。

### 统计（TPStats）

每个 `Skeleton` 内置 `stat.TPStats`，自动记录每类消息的处理耗时（微秒），并周期性（每个整点 + 30~60s 随机抖动错峰）dump TP 分位与平均值，dump 后重置。

```go
// 主动获取处理耗时最长的前 n 类消息（JSON）
report := m.DumpStat(20)

// 获取所有模块的 RPC 队列积压情况（监控告警用）
fmt.Println(xhive.Stats())
// 输出示例：
// static: ponger, rpc_queue_length: 0
// static: pinger, rpc_queue_length: N/A   (N/A 表示该模块无 ChanRPC 服务端)
```

---

## 应用生命周期

```
AppStateNone ──Register/Run──► AppStateInit ──全部 OnInit 成功──► AppStateRun
                                    │                                  │
                          任一 OnInit 失败                       收到 SIGINT/SIGKILL/SIGTERM
                                    │                                  │
                                    ▼                                  ▼
                              启动中止(返回)                       AppStateStop
                                                                       │
                                                  ① 关闭全部动态模块
                                                     cancel → 等待退出 → OnDestroy
                                                  ② 静态模块按逆序(LIFO)关闭
                                                     OnDestroy → cancel → 等待退出(超时30min)
                                                                       │
                                                                       ▼
                                                                  AppStateNone
```

关键约束：

- **注册顺序即初始化顺序**：被依赖的模块应先注册，保证启动时依赖已就绪。
- **关闭逆序（LIFO）**：后启动的先关闭，保证销毁时依赖关系正确解除。
- 静态模块 panic → 进程以退出码 255 终止；动态模块 panic → 仅记录日志。
- 单模块关闭超时为 **30 分钟**，超时仅告警不强杀，避免数据损坏。

可用 `xhive.State()` 查询当前状态（`AppStateNone/Init/Run/Stop`）。

---

## 包级 API 速查

`xhive` 包提供一组直接作用于全局默认应用实例的包级函数，`main` 中可直接调用：

| 函数 | 说明 |
| --- | --- |
| `Register(mods ...IModule) error` | 启动前注册静态模块（须在 `Run` 之前） |
| `Run(mods ...IModule)` | 注册并启动，阻塞至收到退出信号后优雅关闭 |
| `State() int32` | 获取应用当前状态 |
| `Stats() string` | 获取所有模块 RPC 队列积压统计 |
| `ChanRPC(name string) *chanrpc.Server` | 按模块名获取其 RPC 服务端 |
| `DynamicModules() []string` | 列出当前所有动态模块名 |
| `AddDynamicModules(mods ...IModule) error` | 运行时热加载动态模块 |
| `RemoveDynamicModule(name string) bool` | 同步卸载并销毁动态模块 |
| `RegisterSignal(trap SignalTrap, sigs ...os.Signal) error` | 注册自定义信号处理器 |

---

## 目录结构

```
xhive/
├── app.go          # 包级 API（全局默认应用实例的薄封装）
├── module.go       # IModule 接口、app 核心结构、模块生命周期管理
├── skeleton.go     # Skeleton 骨架：整合 ChanRPC + Timer + Stat 的事件循环
├── signal.go       # SignalManager：信号注册与并发分发
├── chanrpc/        # 进程内 ChanRPC（Cast / AsyncCall / Call）
│   ├── def.go      #   消息 ID、CallInfo / RetInfo、CallOption
│   ├── server.go   #   RPC 服务端：路由表 + 消息执行
│   └── client.go   #   RPC 客户端：三种调用语义 + 优雅关闭
├── timer/          # 最小堆定时器
│   ├── dispatcher.go #  调度核心（单 goroutine 派发 + container/heap 最小堆）
│   └── manager.go  #   业务层定时器 API（New/AccAbs/AccPct/DelayAbs/DelayPct/Cancel/Ticker）
└── stat/
    └── tpstat.go   # 消息耗时 TP 分位统计
```

---

## 测试

仓库为各核心组件提供了较完整的单元测试：

```bash
# 运行全部测试
go test ./...

# 带竞态检测（推荐，验证并发正确性）
go test -race ./...

# 查看覆盖率
go test -cover ./...
```

测试文件覆盖：`app_test.go`、`module_test.go`、`skeleton_test.go`、`chanrpc/*_test.go`、`timer/timer_test.go`、`stat/tpstat_test.go`。

---

## 常见问题

**Q：为什么模块内不用加锁？**
A：每个模块的所有事件（RPC 请求、异步回调、定时器触发）都在该模块唯一的 goroutine 中由 `select` 串行消费，因此访问模块内部状态不存在并发，无需加锁。

**Q：`Call` 和 `AsyncCall` 该用哪个？**
A：优先 `AsyncCall`。`Call` 会阻塞当前模块的整个事件循环，若两个模块互相 `Call` 会形成循环等待而**死锁**。仅在调用关系明确单向、确认无环时才使用 `Call`。

**Q：周期任务怎么做？**
A：两种方式——① 创建定时器时传 `timer.WithTicker()` 让框架自动续期；② 在定时器回调内重新 `NewTimer`。前者更简洁，且内部以上次触发时间为基准续期，可消除累积漂移。

**Q：动态模块和静态模块的核心区别？**
A：静态模块随应用启停、不可卸载、panic 会退出进程；动态模块支持运行时增删、可单独卸载、panic 仅记录日志不影响进程。

**Q：消息 ID 是怎么生成的？**
A：默认对消息类型的全限定名（包路径 + 类型名）做 BKDR 哈希，并缓存结果。若需自定义，可让消息类型实现 `chanrpc.IMessage` 接口的 `ID() uint32` 方法。

---

## License

本项目采用 [MIT License](LICENSE) 开源，Copyright (c) 2026 xmapst。
