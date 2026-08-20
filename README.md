# xhive

> 基于 Actor 模型的轻量级 Go 应用框架，适用于游戏服务器、实时服务和事件驱动后台系统。

[![Go Version](https://img.shields.io/badge/Go-1.26.3+-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

`xhive` 将一个进程拆分为多个模块。每个模块拥有独立 goroutine，并通过同一个事件循环串行处理 RPC 请求、异步 RPC 回调和定时器事件。模块之间只通过消息通信，模块内部状态天然隔离，通常无需为业务状态加锁。

---

## 目录

- [核心特性](#核心特性)
- [安装](#安装)
- [快速开始](#快速开始)
- [设计模型](#设计模型)
- [核心组件](#核心组件)
  - [模块](#模块)
  - [Skeleton](#skeleton)
  - [ChanRPC](#chanrpc)
  - [Timer](#timer)
  - [chanx](#chanx)
  - [Signal](#signal)
  - [TPStats](#tpstats)
- [应用生命周期](#应用生命周期)
- [动态模块](#动态模块)
- [包级 API](#包级-api)
- [目录结构](#目录结构)
- [测试](#测试)
- [常见问题](#常见问题)
- [License](#license)

---

## 核心特性

| 特性 | 说明 |
| --- | --- |
| Actor 模型 | 每个模块单 goroutine 串行处理事件，降低锁竞争和数据竞争风险。 |
| 模块化生命周期 | 静态模块按 `Priority` 升序初始化（同优先级保留注册顺序），并严格逆序关闭；动态模块支持运行时加载和卸载。 |
| ChanRPC | 进程内 RPC，支持 Cast、AsyncCall、Call、CallWithContext 四种调用语义。 |
| 无界队列 | RPC、异步返回和定时器事件基于 FIFO 无界队列，支持自动扩容、收缩和积压观测。 |
| 多级时间轮定时器 | 最小粒度 64ms，支持 Timer、Ticker、加速、延迟、改期和取消。 |
| 时间跳变保护 | 系统时间回退时内部 tick 基准跟随回退，避免定时器停摆（重扫幂等，不会重复触发）；系统时间跳到未来时逐 tick 推进到当前时间，不跳过中间层级的降级。 |
| 信号管理 | 默认处理 SIGINT/SIGTERM 优雅关闭；业务可注册 SIGHUP 等非保留信号。 |
| 耗时统计 | Skeleton 自动记录 RPC、异步回调和定时器处理耗时，每 15 分钟（附 30s~60s 随机抖动错峰）dump 一次 TP 分位，模块关闭时再 dump 最后一次。 |
| 零外部依赖 | 仅依赖 Go 标准库。 |

---

## 安装

要求 Go 1.26.3 或更高版本。

```bash
go get github.com/xmapst/xhive
```

```go
import "github.com/xmapst/xhive"
```

---

## 快速开始

下面示例创建两个模块：`pinger` 每秒向 `ponger` 发送一次异步 RPC，`ponger` 收到后返回响应。

```go
package main

import (
	"log/slog"
	"time"

	"github.com/xmapst/xhive"
	"github.com/xmapst/xhive/chanrpc"
	"github.com/xmapst/xhive/timer"
)

type PingReq struct{ Seq int }
type PongAck struct{ Seq int }

type Ponger struct{ *xhive.Skeleton }

func NewPonger() *Ponger {
	return &Ponger{Skeleton: xhive.NewSkeleton("ponger")}
}

func (m *Ponger) OnInit() error {
	return m.RegisterChanRPC(&PingReq{}, func(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
		req := ci.Request.(*PingReq)
		slog.Info("ponger received ping", slog.Int("seq", req.Seq))
		return &chanrpc.RetInfo{Ack: &PongAck{Seq: req.Seq}}
	})
}

func (m *Ponger) OnDestroy() {}

type Pinger struct {
	*xhive.Skeleton
	seq int
}

func NewPinger() *Pinger {
	return &Pinger{Skeleton: xhive.NewSkeleton("pinger")}
}

func (m *Pinger) OnInit() error {
	m.RegisterTimer("tick", func(_ int64, _ map[string]string) {
		m.seq++
		seq := m.seq
		_ = m.AsyncCall("ponger", &PingReq{Seq: seq}, func(ri *chanrpc.RetInfo) {
			if ri.Err != nil {
				slog.Error("ping failed", slog.Any("error", ri.Err))
				return
			}
			ack := ri.Ack.(*PongAck)
			slog.Info("pinger received pong", slog.Int("seq", ack.Seq))
		})
	})
	m.NewTimer("tick", time.Second, timer.WithTicker())
	return nil
}

func (m *Pinger) OnDestroy() {}

func main() {
	xhive.Run(NewPonger(), NewPinger())
}
```

运行后按 Ctrl+C 会触发 SIGINT，框架随后执行优雅关闭。

---

## 设计模型

`xhive` 的基本模型是：

```text
一个模块 = 一个 goroutine + 一个事件循环 + 一组框架组件
模块内部事件串行执行
模块之间只通过 ChanRPC 传递消息
```

这种模型牺牲单模块内部并行度，换取更简单的状态管理和更低的并发复杂度。需要更高并行度时，可以按业务域拆分多个模块。

---

## 核心组件

### 模块

业务模块实现以下生命周期接口：

```go
type IModule interface {
	Name() string
	Priority() uint
	OnInit() error
	Serve(ctx context.Context)
	Ready() <-chan struct{}
	OnDestroy()
	ChanRPC() *chanrpc.Server
	Close() error
}
```

通常不需要手写完整接口，推荐内嵌 `*xhive.Skeleton`，只实现 `OnInit` 和 `OnDestroy`。

生命周期约定：

1. 静态模块按 `Priority()` 升序执行 `OnInit`，同优先级时保留注册顺序（稳定排序）。
2. 任一静态模块 `OnInit` 失败，应用启动失败。
3. `Serve` 在模块独立 goroutine 中运行，应响应 `ctx.Done()`；进入事件循环后 `Ready()` 返回的 channel 会被关闭。框架只在两处等待该信号：`start` 等全部静态模块 `Ready` 后才把状态置为 `AppStateRun`，`AddDynamicModules` 等模块 `Ready` 后才把它放进动态模块表（在此之前 `ChanRPC(name)` 查不到它）。静态模块的 goroutine 是并发启动的，框架不会阻止已就绪模块向尚未就绪的模块发起调用；`Priority` 只决定 `OnInit` 的先后，不保证 `Serve` 就绪的先后。
4. `OnDestroy` 用于释放业务资源。静态模块的关闭顺序是 `cancel` → 等待 `Serve` 退出 → `OnDestroy` → `Close`，此时事件循环已停，不能再对本模块自投递（见 `IModule.OnDestroy` 的说明），`Close` 释放出站 client 资源；动态模块由 `RemoveDynamicModule` 先执行 `OnDestroy` 再 `cancel` 并等待 goroutine 退出，此时事件循环仍在运行，且框架不会调用 `Close`。
5. 逃出 `Serve` 主循环的 panic：静态模块记录堆栈后以退出码 `255` 终止进程，动态模块只记录日志；`start`（含静态模块 `OnInit`）期间的 panic 同样以 `255` 退出。`OnDestroy` 期间的 panic 由框架统一 recover，静态、动态模块都只记日志并继续后续关闭流程。

### Skeleton

`Skeleton` 是框架提供的模块骨架，整合：

- ChanRPC 服务端。
- ChanRPC 客户端。
- Timer 管理器。
- TPStats 耗时统计。
- 标准事件循环。

事件循环串行处理：

1. `ctx.Done()`：模块停止信号。
2. `timer.Event()`：定时器到期事件。
3. `client.Event()`：异步 RPC 响应。
4. `server.Event()`：RPC 请求。

常用方法：

| 分类 | 方法 |
| --- | --- |
| RPC 注册 | `RegisterChanRPC(msg, handler)` |
| RPC 调用 | `Cast(mod, req)`、`AsyncCall(mod, req, cb)`、`Call(mod, req)`、`CallWithContext(ctx, mod, req)` |
| 定时器 | `RegisterTimer`、`NewTimer`、`AccAbsTimer`、`AccPctTimer`、`DelayAbsTimer`、`DelayPctTimer`、`UpdateTimer`、`CancelTimer` |
| 统计 | `DumpStat(n)` |
| 模块接口默认实现 | `Name()`、`Priority()`、`Serve()`、`Ready()`、`ChanRPC()`、`Close()`；`Priority()` 返回 0，需要调整启动顺序时重写。 |

`NewSkeleton` 选项：

| 选项 | 默认值 | 说明 |
| --- | --- | --- |
| `WithTimerChanLen(n)` | 1024 | 定时器事件队列初始容量。 |
| `WithServerChanLen(n)` | 4096 | ChanRPC 服务端队列初始容量。 |
| `WithClientChanLen(n)` | 4096 | ChanRPC 客户端异步返回队列初始容量。 |
| `WithStatCap(n)` | 8192 | 每类消息用于分位统计的最大采样数。 |
| `WithCloseDrainTimeout(d)` | 30s | 停机时 ChanRPC 服务端排空自投递链条的超时上限，超时后放弃剩余部分直接完成关闭。 |
| `WithClientCloseTimeout(d)` | 5s | 停机时 ChanRPC 客户端等待未处理异步回调排空的超时上限，超时后放弃剩余回调。 |

前三项是无界队列的初始容量提示，不是硬性上限；`WithStatCap(n)` 是硬上限，单个 key 采样数达到 n 后新样本不再计入分位统计。

### ChanRPC

`chanrpc` 是进程内 RPC 组件。

| 方法 | 等待结果 | 说明 |
| --- | --- | --- |
| `Cast` | 否 | 单向投递，适合通知、日志和埋点。目标不存在（`ErrServerNil`）时静默丢弃，其余失败会打 warn 日志后丢弃。 |
| `AsyncCall` | 否，结果走回调 | 推荐方式。回调在发起方模块事件循环中执行。 |
| `Call` | 是 | 同步阻塞调用。调用链成环会死锁，应谨慎使用。 |
| `CallWithContext` | 是 | 同 `Call`，但可通过 `ctx` 主动超时或取消等待，取消时返回携带 `ctx.Err()` 的 `RetInfo`。 |

Handler 响应语义：

- 返回非 nil `*RetInfo`：框架自动回包，适合同步处理场景。
- 调用 `ci.Hold()` 后返回 `nil`：框架不回包，由 `Hold` 返回的 `Replier` 句柄在稍后回包（延迟响应）。
- 直接返回 `nil` 且未调用 `Hold`：框架补一个空包，避免同步 `Call` 挂死。
- `hasRet` CAS 保证同一调用最多只发送一次响应；重复调用 `Replier.Ret` 会返回 `ErrAlreadyRet`。
- handler panic 时框架仍会回包错误，即便已经调用 `Hold`。

消息 ID 生成规则：

- 消息实现 `chanrpc.IMessage` 时，使用自定义 `ID() uint32`。
- 否则通过反射获取类型全限定名，再用 BKDR 哈希生成 ID。
- 反射路径下指针类型会自动解引用，`T` 和 `*T` 共享同一 ID；但 `IMessage` 判断先于解引用，若 `ID()` 定义在指针接收者上，则只有 `*T` 命中自定义 ID。

元数据示例（`WithMeta` 可多次调用叠加多个 key，元数据会随响应回传到 `RetInfo.Metadata`）：

```go
_ = m.AsyncCall("target", &Req{}, callback, chanrpc.WithMeta("trace_id", "abc"))
```

关闭语义：

- `Server.Close()` 不回包 `ErrServerClosed`，而是把队列中已排队的调用（含 handler 自投递）真正执行完再关闭队列；排空默认最多 30 秒（`WithCloseDrainTimeout` 可调），超时后丢弃剩余自投递链条。排空完成后再发起的调用才返回 `ErrServerClosed`。
- `Client.Close()` 尽量消费未处理异步响应，默认最多等待 5 秒（`WithClientCloseTimeout` 可调）；超时后强制清零待处理计数，可能丢失部分未执行的回调。
- `Call()` 是无限等待加 5 秒周期告警，不主动超时返回；需要超时兜底请改用 `CallWithContext`（`ErrCallTimeout` 仅为兼容保留，当前不会返回）。

### Timer

`timer` 包提供模块内定时器能力。

当前实现是多级时间轮：

- `timerTick` 为 64ms。
- `timerLevel` 为 20，当前槽位索引范围为 0 到 19。
- 第 19 层每 `2^19` 个 tick（约 9.32 小时）扫描一次，这不是可调度时长的上限；更长的定时器同样落在第 19 层，多等几轮扫描后逐层级联下来仍会精确触发。
- dispatcher 在独立 goroutine 中运行。
- 到期事件只投递到事件队列，业务回调由模块事件循环执行。
- 新建、更新、取消都通过 dispatcher 命令队列串行处理。

重要语义：

- `Cancel` 先写入取消标记，再异步从时间轮删除；已投递但未消费的事件在 `Callback` 中仍会检查取消标记。
- Ticker 以上次 deadline 为基准续期，减少 handler 耗时带来的累计漂移。
- Ticker 在 handler 内被取消后不会再次续期。
- 系统时间回退时，dispatcher 的内部基准 `lastTick` 跟着回退；重扫是幂等的（已触发的定时器已从槽位删除），基准若停在未来反而会让全进程定时器停摆。
- 系统时间后移到未来时，dispatcher 会逐 tick 推进，并在到达当前 tick 后停止循环。
- `NewTimer` 在 name 未注册处理器、或 `WithID` 指定的 ID 已存在时返回 `0` 且不创建定时器。

业务示例：

```go
m.RegisterTimer("reborn", func(id int64, metadata map[string]string) {
	uid := metadata["uid"]
	_ = uid
})

id := m.NewTimer("reborn", 5*time.Second, timer.WithMetadata(map[string]string{"uid": "1001"}))

m.AccAbsTimer(id, time.Second)
m.DelayPctTimer(id, 2000)
m.UpdateTimer(id, xtime.Now().Add(time.Minute))
m.CancelTimer(id)

m.NewTimer("heartbeat", time.Second, timer.WithTicker())
```

示例中的 `xtime.Now()` 不是笔误：`UpdateTimer` 收的是绝对业务时刻，而定时器判定到期用的是 `xtime` 这个全进程统一时间源，混用 `time.Now()` 在开启时间平移后会与时间轮跑在两根时间轴上。

百分比调整使用万分比，`timer.PctBase` 为 `10000`：

- `AccPctTimer(id, 2000)` 表示剩余时间缩短 20%。
- `DelayPctTimer(id, 2000)` 表示剩余时间延长 20%。

### chanx

`chanx.Unbounded[T]` 是 FIFO 无界队列。

关键语义：

- `In()` 返回发送端，正常运行期间发送不会因容量不足而阻塞。
- `Out()` 返回接收端，按发送顺序读取。
- 内部 ring buffer 会按积压自动扩容；连续 8 次 `pop` 后占用率仍不高于 25% 才会容量减半，且不会低于创建时的初始容量。
- `Close()` 关闭输入端，已缓冲数据会继续 drain 到输出端。
- context 取消会让转发 goroutine 立即退出并关闭输出端。
- `BufLen()` 只统计内部 ring buffer 的积压，`Len()` 还加上 `In`、`Out` 两个 channel 自身队列中的值；两者都是近似快照，适合监控，不适合严格业务判断。

### Signal

框架默认信号行为：

| 信号 | 行为 |
| --- | --- |
| SIGINT | 触发优雅关闭，框架保留，业务不可注册。 |
| SIGTERM | 触发优雅关闭，框架保留，业务不可注册。 |
| SIGKILL | 操作系统不可捕获；框架仅将其作为保留信号禁止业务注册。 |
| SIGHUP | 默认记录日志并继续运行。仅当业务在 `Run` 之前注册过 SIGHUP 处理器时才不装该默认处理器；`Run` 之后（如模块 `OnInit` 中）注册的处理器是追加，默认日志行为仍会执行。 |

业务可注册非保留信号：

```go
err := xhive.RegisterSignal(func() {
	slog.Info("reload config")
}, syscall.SIGHUP)
```

同一信号支持多个处理器。收到信号后，处理器并发执行并等待全部完成；单个处理器 panic 会被捕获。

### TPStats

`stat.TPStats` 用于统计事件处理耗时。

- `Add(name, costUs)` 记录一次耗时，单位为微秒。
- `Dump(n)` 输出 JSON，包含一个 `id` 为 `"ALL"` 的全局汇总项，以及按 TP99 从高到低排序的前 n 类消息；`n <= 0` 或 n 超过实际类别数时返回全部类别。
- `Reset()` 清空统计。
- 每类消息只保留最先到达的 `maxCnt` 条样本用于分位数计算，超出部分只计入 `Count` 和 `Avg`；`Dump` 输出中的全局汇总项（ID 为 `ALL`）同样受此限制。
- `Count` 和 `Avg` 基于全部输入累计。
- `nil`、空字符串、数字零值等无意义 key 会被忽略。

Skeleton 会自动统计定时器事件、RPC 请求和异步 RPC 回调耗时。

---

## 应用生命周期

```text
AppStateNone
    │ Run
    ▼
AppStateInit
    │ 所有静态模块 OnInit 成功
    ▼
AppStateRun
    │ 收到 SIGINT / SIGTERM 或启动失败
    ▼
AppStateStop
    │ 关闭动态模块，再逆序关闭静态模块
    ▼
AppStateNone
```

关闭流程：

1. 进入 `AppStateStop`。
2. 关闭所有动态模块。
3. 按启动顺序（`Priority` 升序，同优先级保留注册顺序）整体逆序关闭静态模块（LIFO）。
4. 每个静态模块先取消 context 并等待 goroutine 退出，再执行 `OnDestroy`，最后调用 `Close` 释放出站 client。
5. 单个静态模块关闭超时默认为 30 分钟；超时不强杀，仅记录错误日志，并跳过该模块的 `OnDestroy` 与 `Close`。
6. 全部关闭后回到 `AppStateNone`。

---

## 动态模块

动态模块适合运行时启停的功能，例如活动玩法、临时任务或调试模块。

```go
results, err := xhive.AddDynamicModules(NewActivityModule())
if err != nil {
	// err 非 nil 仅表示至少一个模块初始化失败，逐个模块的结果见 results
	for _, r := range results {
		if r.Err != nil {
			slog.Error("dynamic module init failed", slog.String("module", r.Name), slog.Any("error", r.Err))
		}
	}
	return err
}

names := xhive.DynamicModules()
_ = names

removed := xhive.RemoveDynamicModule("activity")
_ = removed
```

动态模块特性：

- `AddDynamicModules` 按 `Priority` 升序（同优先级保留传参顺序）依次执行 `OnInit`，成功后启动 `Serve`，并等待模块 `Ready()` 后才登记到动态模块表。
- `RemoveDynamicModule` 同步执行 `OnDestroy`、取消 context、等待 goroutine 退出，再删除模块记录。
- 动态模块 `Serve` 与 `OnDestroy` 中的 panic 会被捕获，不会退出进程；`OnInit` 在调用方 goroutine 上同步执行，其 panic 不被框架捕获。
- 批量添加中途失败时，已经启动的动态模块不会自动回滚。

---

## 包级 API

| API | 说明 |
| --- | --- |
| `Register(mods ...IModule) error` | 启动前注册静态模块。 |
| `Run(mods ...IModule)` | 注册并启动应用，阻塞至收到退出信号并完成关闭。 |
| `State() int32` | 返回当前应用状态。 |
| `Stats() string` | 返回所有模块 RPC 队列积压统计。 |
| `ChanRPC(name string) *chanrpc.Server` | 按模块名获取 ChanRPC 服务端。 |
| `DynamicModules() []string` | 返回当前动态模块名称列表。 |
| `AddDynamicModules(mods ...IModule) (results []AddDynamicModuleResult, err error)` | 运行时添加并启动动态模块；`results` 逐个给出模块的成功/失败结果，`err` 非 nil 仅表示至少一个模块初始化失败。 |
| `RemoveDynamicModule(name string) bool` | 同步卸载动态模块。 |
| `RegisterSignal(trap SignalTrap, sigs ...os.Signal) error` | 注册非保留信号处理器。 |
| `SafeGo(fn func())` | 在独立 goroutine 中执行 `fn`，自动捕获其中的 panic。 |
| `SafeGoContext(ctx context.Context, fn func(ctx context.Context))` | 语义同 `SafeGo`；启动时若 `ctx` 已取消则跳过执行，执行中不主动中断 `fn`。 |

状态常量：

| 常量 | 说明 |
| --- | --- |
| `AppStateNone` | 应用未启动或已完全停止。 |
| `AppStateInit` | 应用正在初始化。 |
| `AppStateRun` | 应用运行中。 |
| `AppStateStop` | 应用正在关闭。 |

---

## 目录结构

```text
xhive/
├── app.go              # 包级 API
├── module.go           # IModule、app 核心结构、模块生命周期
├── skeleton.go         # Skeleton 事件循环
├── signal.go           # SignalManager
├── safego.go           # SafeGo / SafeGoContext，带 panic 恢复的 goroutine
├── chanrpc/
│   ├── def.go          # 消息 ID、CallInfo、RetInfo、CallOption、Hold/Replier
│   ├── server.go       # ChanRPC 服务端
│   └── client.go       # ChanRPC 客户端
├── chanx/
│   └── chanx.go        # 无界 FIFO 队列
├── timer/
│   ├── dispatcher.go   # 多级时间轮调度器
│   └── manager.go      # 业务层 Timer API
├── stat/
│   └── tpstat.go       # TP 分位耗时统计
└── xtime/
    ├── clock.go        # 统一时间源，支持时间平移
    └── calendar.go     # 自然日/周边界与毫秒时间戳换算
```

---

## 测试

运行全部测试：

```bash
go test ./...
```

运行指定包测试：

```bash
go test ./timer
```

查看覆盖率：

```bash
go test -cover ./...
```

带竞态检测：

```bash
go test -race ./...
```

当前测试覆盖重点：

- app / module：静态与动态模块生命周期、状态流转、启动失败、优先级排序和 LIFO 逆序关闭。
- skeleton：事件循环、RPC 包装、Timer 包装、统计记录和资源关闭。
- signal：保留信号、自定义信号、并发分发和 panic 隔离。
- chanrpc：消息 ID、注册校验、Cast、AsyncCall、Call、metadata、`Hold` 延迟响应、panic 恢复、关闭语义。
- chanx：FIFO、关闭 drain、context cancel、ring 扩容收缩、Len 和 BufLen。
- timer：时间轮放置、tick 推进、时钟前移和后移、取消、更新、同 ID 替换、Manager one-shot 和 ticker。
- stat：分位统计、TopN、Reset、零值 key 忽略、并发 Add 和 Dump。
- xtime：时间偏移开关、`Now` 与 `Wall` 的区别、UTC 保证、自然日/周边界和毫秒换算。

---

## 常见问题

### 为什么模块内通常不需要加锁？

因为模块内部事件都在该模块的 `Serve` goroutine 中串行处理。只要业务状态只在该事件循环内访问，就不会发生并发读写。

### 什么时候用 AsyncCall，什么时候用 Call？

优先使用 `AsyncCall`。它不会阻塞当前模块事件循环，回调也会回到当前模块事件循环中执行。`Call` 会阻塞当前模块，如果调用链形成环，会死锁；而且 `Call` 是无限等待，只会每 5s 打一条告警，不会自行超时返回。确实需要同步语义时，新代码应使用 `CallWithContext` 并传入带超时的 ctx，ctx 取消时立即返回携带 `ctx.Err()` 的 `RetInfo`。

### Cast 失败会怎么样？

`Cast` 没有返回值，任何失败都表现为丢弃：目标模块不存在时完全静默，目标 Server 已关闭、消息类型无效等情况只打一条 warn 日志，目标模块未注册该消息的 handler 时调用方也拿不到反馈。它适合对可靠性要求不高的通知类消息。如果需要知道结果，应使用 `AsyncCall` 或 `Call` / `CallWithContext`——处理错误会通过 `RetInfo.Err` 告知调用方，`AsyncCall` 另有 error 返回值用于报告前置校验失败。

### 定时器回调在哪个 goroutine 执行？

底层 dispatcher 只投递到期事件，不执行业务回调。业务回调在模块的 Skeleton 事件循环中执行，因此可以安全访问模块内部状态。

### Ticker 会不会因为 handler 执行慢而漂移？

Ticker 续期以上次 deadline 为基准，而不是以当前时间为基准，因此可以减少处理耗时带来的累积漂移。如果 handler 执行时间超过周期，后续事件可能追赶触发，业务应避免长时间阻塞事件循环。

### 服务器时间被手动调整会怎样？

时间被手动回退时，dispatcher 的内部基准 `lastTick` 跟着回退到当前 tick。重扫是幂等的：定时器在投递到期事件的同时就已从槽位删除，重扫扫不到已触发的定时器，只多花一点 CPU；反之若基准停在未来，后续每次 tick 都不会扫描任何层级，全进程定时器停摆。时间被手动后移到未来时，dispatcher 会从旧 tick 向当前 tick 逐步推进，到达当前时间后停止循环。

### 无界队列是否意味着可以无限堆积？

不是。无界队列避免生产者被慢消费者卡死，但积压仍然会占用内存。生产环境应通过包级 `Stats()`（汇总各模块 RPC 服务端队列长度）或 `ChanRPC(name).Len()` 观测积压，并在业务层做限流、拆模块或告警；这些值都是近似快照，适合监控，不适合严格业务判断。

### 模块的启动顺序怎么控制？

由 `IModule.Priority() uint` 决定：值越小越早初始化，优先级相同时保留注册顺序（静态模块为 `Register`/`Run` 的传参顺序，动态模块为 `AddDynamicModules` 的传参顺序）。关闭顺序与启动顺序完全相反（LIFO），保证被依赖模块最后销毁。内嵌 `Skeleton` 时 `Priority` 默认返回 0，需要靠后启动的模块自行覆盖该方法。

### 动态模块和静态模块有什么区别？

静态模块随应用启停，不支持运行时卸载，逃出 `Serve` 主循环的 panic 会以退出码 `255` 终止进程。动态模块支持运行时加载和卸载，同样位置的 panic 只记录日志。两者的 ChanRPC handler、异步回调、定时器回调和 `OnDestroy` 都由框架各自 recover，只记录日志（handler panic 还会回包错误），不会退出进程。

### 模块关闭超时了会怎样？

框架等待单个模块 `Serve` 退出的上限是 30 分钟。超时后不强杀，但会记录一条 error 并跳过该模块的 `OnDestroy` 和 client 释放——此时事件循环仍在运行，调 `OnDestroy` 会与它并发读写同一批业务内存，触发 Go 运行时不可 recover 的 fatal。因此 `Serve` 必须能及时响应 `ctx.Done()`，否则依赖 `OnDestroy` 的落地逻辑整段不会执行。

### 延迟响应如何使用？

handler 内调用 `ci.Hold()` 拿到 `Replier` 句柄后返回 nil，框架不会自动回包。业务在其他 goroutine 完成阻塞 IO 后，用该句柄回包：

```go
func onQuery(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
    reply := ci.Hold()
    go func() {
        row, err := db.Query(...)                           // 阻塞 IO，不占事件循环
        _ = reply.Ret(&chanrpc.RetInfo{Ack: row, Err: err}) // 稍后回包
    }()
    return nil
}
```

回包能力由 `Hold` 返出而不是挂在 `CallInfo` 上，是为了让「忘了 Hold 就直接回包」这条错误路径在类型上不存在——拿到 `Replier` 的唯一途径就是先 `Hold`。

`Hold` 后必须保证最终调用 `Replier.Ret`，否则同步 `Call` 会挂死（只会每 5s 打一条告警），需要超时兜底请让调用方改用 `CallWithContext`。handler panic 时框架仍会回包错误。

### 重复回包会怎样？

防重复回包由 `CallInfo` 内部的一个 CAS 统一保证，handler 返回值、panic 兜底回包和 `Replier.Ret` 共用同一个标志：只要本次调用已经回过包，后续的 `Replier.Ret` 就返回 `ErrAlreadyRet`，不会静默丢弃。延迟响应场景下回包发生在 handler 之外的 goroutine，这能帮助业务发现响应未成功投递。对 Cast（本就没有回包通道）调用 `Ret` 是空操作，返回 nil。

---

## License

本项目采用 [MIT License](LICENSE) 开源。