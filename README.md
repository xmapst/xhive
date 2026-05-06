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
| 模块化生命周期 | 静态模块按注册顺序初始化、逆序关闭；动态模块支持运行时加载和卸载。 |
| ChanRPC | 进程内 RPC，支持 Cast、AsyncCall、Call 三种调用语义。 |
| 无界队列 | RPC、异步返回和定时器事件基于 FIFO 无界队列，支持自动扩容、收缩和积压观测。 |
| 多级时间轮定时器 | 最小粒度 64ms，支持 Timer、Ticker、加速、延迟、改期和取消。 |
| 时间跳变保护 | 系统时间前移不回退内部 tick，避免重复触发；系统时间后移到未来时逐 tick 推进到当前时间。 |
| 信号管理 | 默认处理 SIGINT/SIGTERM 优雅关闭；业务可注册 SIGHUP 等非保留信号。 |
| 耗时统计 | Skeleton 自动记录 RPC、异步回调和定时器处理耗时，并周期性 dump TP 分位。 |
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
		slog.Info("ponger received ping", "seq", req.Seq)
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
				slog.Error("ping failed", "err", ri.Err)
				return
			}
			ack := ri.Ack.(*PongAck)
			slog.Info("pinger received pong", "seq", ack.Seq)
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
	OnInit() error
	OnRun(ctx context.Context)
	OnDestroy()
	ChanRPC() *chanrpc.Server
}
```

通常不需要手写完整接口，推荐内嵌 `*xhive.Skeleton`，只实现 `OnInit` 和 `OnDestroy`。

生命周期约定：

1. 静态模块按注册顺序执行 `OnInit`。
2. 任一静态模块 `OnInit` 失败，应用启动失败。
3. `OnRun` 在模块独立 goroutine 中运行，应响应 `ctx.Done()`。
4. `OnDestroy` 用于释放业务资源。
5. 静态模块 panic 会导致进程退出；动态模块 panic 只记录日志。

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
| RPC 调用 | `Cast(mod, req)`、`AsyncCall(mod, req, cb)`、`Call(mod, req)` |
| 定时器 | `RegisterTimer`、`NewTimer`、`AccAbsTimer`、`AccPctTimer`、`DelayAbsTimer`、`DelayPctTimer`、`UpdateTimer`、`CancelTimer` |
| 统计 | `DumpStat(n)` |

`NewSkeleton` 选项：

| 选项 | 默认值 | 说明 |
| --- | --- | --- |
| `WithTimerChanLen(n)` | 1024 | 定时器事件队列初始容量。 |
| `WithServerChanLen(n)` | 4096 | ChanRPC 服务端队列初始容量。 |
| `WithClientChanLen(n)` | 4096 | ChanRPC 客户端异步返回队列初始容量。 |
| `WithStatCap(n)` | 8192 | 每类消息用于分位统计的最大采样数。 |

这些容量是初始容量提示，不是硬性上限。

### ChanRPC

`chanrpc` 是进程内 RPC 组件。

| 方法 | 等待结果 | 说明 |
| --- | --- | --- |
| `Cast` | 否 | 单向投递，适合通知、日志和埋点。目标不存在时静默丢弃。 |
| `AsyncCall` | 否，结果走回调 | 推荐方式。回调在发起方模块事件循环中执行。 |
| `Call` | 是 | 同步阻塞调用。调用链成环会死锁，应谨慎使用。 |

消息 ID 生成规则：

- 消息实现 `chanrpc.IMessage` 时，使用自定义 `ID() uint32`。
- 否则通过反射获取类型全限定名，再用 BKDR 哈希生成 ID。
- 指针类型会自动解引用，`T` 和 `*T` 共享同一 ID。

元数据示例：

```go
_ = m.AsyncCall("target", &Req{}, callback, chanrpc.WithMeta("trace_id", "abc"))
```

关闭语义：

- `Server.Close()` 关闭服务端队列，并对积压请求回包 `ErrServerClosed`。
- `Client.Close()` 尽量消费未处理异步响应，最多等待 5 秒。
- `Call()` 是无限等待加 5 秒周期告警，不主动超时返回。

### Timer

`timer` 包提供模块内定时器能力。

当前实现是多级时间轮：

- `timerTick` 为 64ms。
- `timerLevel` 为 20，当前槽位索引范围为 0 到 19。
- 单个定时器最大可调度时长约为 `2^19 * 64ms`，即约 9.3 小时。
- dispatcher 在独立 goroutine 中运行。
- 到期事件只投递到事件队列，业务回调由模块事件循环执行。
- 新建、更新、取消都通过 dispatcher 命令队列串行处理。

重要语义：

- `Cancel` 先写入取消标记，再异步从时间轮删除；已投递但未消费的事件在 `Callback` 中仍会检查取消标记。
- Ticker 以上次 deadline 为基准续期，减少 handler 耗时带来的累计漂移。
- Ticker 在 handler 内被取消后不会再次续期。
- 系统时间前移时，dispatcher 不回退内部 `lastTick`，避免重复扫描已推进区间。
- 系统时间后移到未来时，dispatcher 会逐 tick 推进，并在到达当前 tick 后停止循环。

业务示例：

```go
m.RegisterTimer("reborn", func(id int64, metadata map[string]string) {
	uid := metadata["uid"]
	_ = uid
})

id := m.NewTimer("reborn", 5*time.Second, timer.WithMetadata(map[string]string{"uid": "1001"}))

m.AccAbsTimer(id, time.Second)
m.DelayPctTimer(id, 2000)
m.UpdateTimer(id, time.Now().Add(time.Minute))
m.CancelTimer(id)

m.NewTimer("heartbeat", time.Second, timer.WithTicker())
```

百分比调整使用万分比，`timer.PctBase` 为 `10000`：

- `AccPctTimer(id, 2000)` 表示剩余时间缩短 20%。
- `DelayPctTimer(id, 2000)` 表示剩余时间延长 20%。

### chanx

`chanx.Unbounded[T]` 是 FIFO 无界队列。

关键语义：

- `In()` 返回发送端，正常运行期间发送不会因容量不足而阻塞。
- `Out()` 返回接收端，按发送顺序读取。
- 内部 ring buffer 会按积压自动扩容，并在消费恢复后收缩。
- `Close()` 关闭输入端，已缓冲数据会继续 drain 到输出端。
- context 取消会让转发 goroutine 立即退出并关闭输出端。
- `Len()` 和 `BufLen()` 是近似快照，适合监控，不适合严格业务判断。

### Signal

框架默认信号行为：

| 信号 | 行为 |
| --- | --- |
| SIGINT | 触发优雅关闭，框架保留，业务不可注册。 |
| SIGTERM | 触发优雅关闭，框架保留，业务不可注册。 |
| SIGKILL | 操作系统不可捕获；框架仅将其作为保留信号禁止业务注册。 |
| SIGHUP | 如果业务未注册处理器，则默认记录日志并继续运行。 |

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
- `Dump(n)` 输出 JSON，按 TP99 从高到低返回前 n 类消息。
- `Reset()` 清空统计。
- 每类消息最多保留 `maxCnt` 条样本用于分位数计算。
- `Count` 和 `Avg` 基于全部输入累计。
- `nil`、空字符串、数字零值等无意义 key 会被忽略。

Skeleton 会自动统计定时器事件、RPC 请求和异步 RPC 回调耗时。

---

## 应用生命周期

```text
AppStateNone
    │ Register / Run
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
3. 按注册顺序的逆序关闭静态模块。
4. 每个静态模块先执行 `OnDestroy`，再取消 context，并等待 goroutine 退出。
5. 单个静态模块关闭超时时间为 30 分钟；超时只记录日志，不强杀。
6. 全部关闭后回到 `AppStateNone`。

---

## 动态模块

动态模块适合运行时启停的功能，例如活动玩法、临时任务或调试模块。

```go
err := xhive.AddDynamicModules(NewActivityModule())
if err != nil {
	return err
}

names := xhive.DynamicModules()
_ = names

removed := xhive.RemoveDynamicModule("activity")
_ = removed
```

动态模块特性：

- `AddDynamicModules` 会立即执行 `OnInit`，成功后启动 `OnRun`。
- `RemoveDynamicModule` 同步执行 `OnDestroy`、取消 context、等待 goroutine 退出，再删除模块记录。
- 动态模块 panic 不会退出进程。
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
| `AddDynamicModules(mods ...IModule) error` | 运行时添加并启动动态模块。 |
| `RemoveDynamicModule(name string) bool` | 同步卸载动态模块。 |
| `RegisterSignal(trap SignalTrap, sigs ...os.Signal) error` | 注册非保留信号处理器。 |

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
├── chanrpc/
│   ├── def.go          # 消息 ID、CallInfo、RetInfo、CallOption
│   ├── server.go       # ChanRPC 服务端
│   └── client.go       # ChanRPC 客户端
├── chanx/
│   └── chanx.go        # 无界 FIFO 队列
├── timer/
│   ├── dispatcher.go   # 多级时间轮调度器
│   └── manager.go      # 业务层 Timer API
└── stat/
    └── tpstat.go       # TP 分位耗时统计
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

- app / module：静态模块生命周期、状态流转、启动失败、关闭顺序。
- skeleton：事件循环、RPC 包装、Timer 包装、统计记录和资源关闭。
- signal：保留信号、自定义信号、并发分发和 panic 隔离。
- chanrpc：消息 ID、注册校验、Cast、AsyncCall、Call、metadata、panic 恢复、关闭语义。
- chanx：FIFO、关闭 drain、context cancel、ring 扩容收缩、Len 和 BufLen。
- timer：时间轮放置、tick 推进、时钟前移和后移、取消、更新、同 ID 替换、Manager one-shot 和 ticker。
- stat：分位统计、TopN、Reset、零值 key 忽略、并发 Add 和 Dump。

---

## 常见问题

### 为什么模块内通常不需要加锁？

因为模块内部事件都在该模块的 `OnRun` goroutine 中串行处理。只要业务状态只在该事件循环内访问，就不会发生并发读写。

### 什么时候用 AsyncCall，什么时候用 Call？

优先使用 `AsyncCall`。它不会阻塞当前模块事件循环，回调也会回到当前模块事件循环中执行。`Call` 会阻塞当前模块，如果调用链形成环，会死锁。

### Cast 失败会怎么样？

如果目标模块不存在，`Cast` 会静默丢弃，适合对可靠性要求不高的通知类消息。如果需要知道结果，应使用 `AsyncCall` 或 `Call`。

### 定时器回调在哪个 goroutine 执行？

底层 dispatcher 只投递到期事件，不执行业务回调。业务回调在模块的 Skeleton 事件循环中执行，因此可以安全访问模块内部状态。

### Ticker 会不会因为 handler 执行慢而漂移？

Ticker 续期以上次 deadline 为基准，而不是以当前时间为基准，因此可以减少处理耗时带来的累积漂移。如果 handler 执行时间超过周期，后续事件可能追赶触发，业务应避免长时间阻塞事件循环。

### 服务器时间被手动调整会怎样？

时间被手动前移时，dispatcher 不回退内部 `lastTick`，避免已经推进过的区间被重复扫描。时间被手动后移到未来时，dispatcher 会从旧 tick 向当前 tick 逐步推进，到达当前时间后停止循环。

### 无界队列是否意味着可以无限堆积？

不是。无界队列避免生产者被慢消费者卡死，但积压仍然会占用内存。生产环境应通过 `Len()` 观测队列长度，并在业务层做限流、拆模块或告警。

### 动态模块和静态模块有什么区别？

静态模块随应用启停，不支持运行时卸载，panic 会退出进程。动态模块支持运行时加载和卸载，panic 只记录日志。

---

## License

本项目采用 [MIT License](LICENSE) 开源。