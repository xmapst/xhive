# xhive

> 基于 Actor 模型的轻量级 Go 应用框架，适用于游戏服务器、实时服务和事件驱动后台系统。

[![Go Version](https://img.shields.io/badge/Go-1.27.1+-00ADD8?logo=go)](https://go.dev/)
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
  - [本地异步任务](#本地异步任务)
  - [Timer](#timer)
  - [Signal](#signal)
  - [TPStats](#tpstats)
- [应用生命周期](#应用生命周期)
  - [通过 context 嵌入运行](#通过-context-嵌入运行)
  - [预停机接口](#预停机接口)
- [动态模块](#动态模块)
- [应用 API](#应用-api)
- [目录结构](#目录结构)
- [测试](#测试)
- [常见问题](#常见问题)
- [License](#license)

---

## 核心特性

| 特性             | 说明                                                                                                                                                                                                                            |
|------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Actor 模型       | 每个模块单 goroutine 串行处理事件，降低锁竞争和数据竞争风险。                                                                                                                                                                   |
| 模块化生命周期   | 静态模块按 `Priority` 升序初始化（同优先级保留注册顺序）；支持预停机排空、动态加载与卸载，清理完成前保留模块名称和依赖。 |
| ChanRPC          | 进程内 RPC，支持 Cast、AsyncCall、Call、CallWithContext 四种调用语义。                                                                                                                                                          |
| 本地异步任务     | `AsyncGo` 把一段耗时/阻塞的本地操作挪到独立 goroutine 执行，回调安全带回事件循环；`NewLinearContext` 提供顺序保证的变体。                                                                                                       |
| 伸缩式有界队列   | RPC、异步返回、定时器事件和本地异步任务回调都走同一套队列：容量按积压动态增长、随空闲收缩，同时保留硬性上限。过载在打满的那一刻就变成可观测的失败或等待，而不是把积压堆进内存直到 OOM；空闲时也不会为一个用不上的上限预留内存。 |
| 多级时间轮定时器 | 最小粒度 64ms，支持 Timer、Ticker、加速、延迟、改期和取消。                                                                                                                                                                     |
| 时间跳变保护     | 系统时间回退时内部 tick 基准跟随回退，避免定时器停摆（重扫幂等，不会重复触发）；系统时间跳到未来时逐 tick 推进到当前时间，不跳过中间层级的降级。                                                                                |
| 应用入口         | `Run` 处理 SIGINT/SIGTERM 并优雅关闭；`RunContext` 由外部 context 控制，适合嵌入已有服务。 |
| 信号管理         | 使用 `Run` 时默认监听进程信号；业务可注册 SIGHUP 等非保留信号。 |
| 耗时统计         | Skeleton 自动记录 RPC、异步回调和定时器处理耗时，每 15 分钟（附 30s~60s 随机抖动错峰）dump 一次 TP 分位，模块关闭时再 dump 最后一次。                                                                                           |
| 零外部依赖       | 仅依赖 Go 标准库。                                                                                                                                                                                                              |

---

## 安装

要求 Go 1.27.1 或更高版本。

```bash
go get github.com/xmapst/xhive
```

```go
import (
	"github.com/xmapst/xhive"
	"github.com/xmapst/xhive/app"
)
```

根包 `xhive` 提供 `Skeleton`、`LinearContext` 和 `SafeGo` 等模块工具；`app` 包负责应用入口、模块生命周期和动态模块管理，模块接口为 `app.IModule`。`chanrpc`、`timer`、`signal` 和 `stat` 分别提供 RPC、定时器、信号管理和耗时统计组件。

---

## 快速开始

下面示例创建两个模块：`pinger` 每秒向 `ponger` 发送一次异步 RPC，`ponger` 收到后返回响应。

```go
package main

import (
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/xmapst/xhive"
	"github.com/xmapst/xhive/app"
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
		req, ok := ci.RequestAs[*PingReq]()
		if !ok {
			return &chanrpc.RetInfo{Err: errors.New("unexpected request type")}
		}
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
		_ = m.AsyncCall(app.ChanRPC("ponger"), &PingReq{Seq: seq}, func(ri *chanrpc.RetInfo) {
			if ri.Err != nil {
				slog.Error("ping failed", slog.Any("error", ri.Err))
				return
			}
			ack, ok := ri.AckAs[*PongAck]()
			if !ok {
				return
			}
			slog.Info("pinger received pong", slog.Int("seq", ack.Seq))
		})
	})
	m.NewTimer("tick", time.Second, timer.WithTicker())
	return nil
}

func (m *Pinger) OnDestroy() {}

func main() {
	if err := app.Run(NewPonger(), NewPinger()); err != nil {
		slog.Error("app exited with error", slog.Any("error", err))
		os.Exit(1)
	}
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

业务模块实现 `app` 包中的 `app.IModule` 生命周期接口：

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

模块名称必须在同一个 `App` 内唯一，静态模块与动态模块共享名称空间，并且名称在模块生命周期内保持不变。`Register` 和 `Run` 在初始化前整批检查名称，冲突时返回错误且不追加本批模块；`AddDynamicModules` 拒绝冲突模块并继续处理其余模块。不同 `App` 可以使用相同的模块名。

生命周期约定：

1. 静态模块按 `Priority()` 升序执行 `OnInit`，同优先级时保留注册顺序（稳定排序）。名称和优先级在登记时固定，不能在初始化中修改。
2. 任一静态模块 `OnInit` 失败，应用启动失败。框架随后只对**已成功 `OnInit`** 的模块执行 `OnDestroy` → `Close`：`OnInit` 返回了 error 的模块（它必须在返回前自行回滚已分配的资源）、以及排在它之后一次都没被 `OnInit` 过的模块，都不会收到 `OnDestroy`/`Close`。
3. `Serve` 在模块独立 goroutine 中运行，应响应 `ctx.Done()`；进入事件循环前关闭模块 `Ready()` 返回的 channel。全部静态模块就绪后，应用进入 `StateRun` 并关闭 `App.Ready()`；动态模块就绪后才对 RPC 寻址、统计和模块列表公开。每次等待模块就绪默认最多 30 秒，可通过 `WithStartupTimeout` 调整；该选项不限制 `OnInit`。静态模块的 goroutine 并发启动，框架不会阻止已就绪模块向尚未就绪的模块发起调用；`Priority` 只决定 `OnInit` 的先后，不保证 `Serve` 就绪的先后。
4. 静态模块与动态模块共用关闭路径：已就绪模块先执行可选的 `PrepareStop`，再取消 context → 等待 `Serve` **实际退出** → `OnDestroy` → `Close`。`OnDestroy` 中事件循环已停，不能再对本模块自投递；`Close` 释放出站 client 等资源。动态卸载开始后立即停止对外寻址，但名称保留到清理完成。详见[应用生命周期](#应用生命周期)和[动态模块](#动态模块)。
5. 默认情况下，应用启动期间或静态模块 `Serve` 中的 panic 会以退出码 `255` 终止进程；使用 `WithPanicExit(false)` 后转为错误，并有序清理。静态模块未经框架取消就提前退出（包括正常返回）也会触发应用关闭并返回错误。动态模块 `Serve` 的 panic 只记录日志；动态 `OnInit` 的 panic 默认向调用方传播，关闭 panic 退出策略后转为添加失败。`OnDestroy` 与 `Close` 的 panic 转为清理错误，不中断其他模块的清理；`Run`、`RunContext` 或等待卸载的调用方会收到相应错误。

### Skeleton

`Skeleton` 是框架提供的模块骨架，整合：

- ChanRPC 服务端。
- ChanRPC 客户端。
- Timer 管理器。
- 本地异步任务调度器（见[本地异步任务](#本地异步任务)）。
- TPStats 耗时统计。
- 标准事件循环。

事件循环串行处理：

1. `ctx.Done()`：模块停止信号。
2. `timer.NotEmpty()`：定时器到期事件。
3. `client.NotEmpty()`：异步 RPC 响应。
4. `server.NotEmpty()`：RPC 请求。
5. `async.NotEmpty()`：本地异步任务（`AsyncGo`/`LinearContext`）的回调。

后四路的数据不流经 channel，channel 只承载一个「可能有事件」的边沿信号，取件靠紧跟其后的 `Pop()`。信号会合并，所以 `Pop()` 允许扑空；反过来只要队列非空，信号一定还在，不会丢唤醒。每次只 `Pop` 一条而不是醒来后循环排空，是为了不让一个慢 handler 长时间占住事件循环、拖垮 `ctx.Done()` 的响应和五路之间的公平性。

常用方法：

| 分类             | 方法                                                                                                                          |
|------------------|-------------------------------------------------------------------------------------------------------------------------------|
| RPC 注册         | `RegisterChanRPC(msg, handler)`                                                                                               |
| RPC 调用         | `Cast(svr, req)`、`AsyncCall(svr, req, cb)`、`Call(svr, req)`、`CallWithContext(ctx, svr, req)`；`svr` 为 `*chanrpc.Server`。 |
| 本地异步任务     | `AsyncGo(name, f, cb)`、`NewLinearContext()`（详见[本地异步任务](#本地异步任务)）                                             |
| 定时器           | `RegisterTimer`、`NewTimer`、`AccAbsTimer`、`AccPctTimer`、`DelayAbsTimer`、`DelayPctTimer`、`UpdateTimer`、`CancelTimer`     |
| 统计             | `DumpStat(n)`                                                                                                                 |
| 模块接口默认实现 | `Name()`、`Priority()`、`Serve()`、`Ready()`、`ChanRPC()`、`Close()`；`Priority()` 返回 0，需要调整启动顺序时重写。           |

RPC 方法直接接收目标服务端，由调用方完成模块寻址。使用默认应用时，可通过 `app.ChanRPC("ponger")` 查找；使用 `app.New()` 创建的应用时，应调用该实例的 `ChanRPC` 方法。也可以直接传入目标模块的 `ChanRPC()` 返回值。

`NewSkeleton` 选项：

| 选项                        | 默认值 | 说明                                                                            |
|-----------------------------|--------|---------------------------------------------------------------------------------|
| `WithTimerChanLen(n)`       | 1024   | 定时器操作队列与到期队列的容量上限。                                            |
| `WithServerChanLen(n)`      | 4096   | ChanRPC 服务端队列容量上限。                                                    |
| `WithClientChanLen(n)`      | 4096   | ChanRPC 客户端异步返回队列容量上限。                                            |
| `WithAsyncChanLen(n)`       | 1024   | 本地异步任务回调队列容量上限，同时也是同时允许存在的未完成任务数上限。          |
| `WithStatCap(n)`            | 8192   | 每类消息用于分位统计的最大采样数。                                              |
| `WithCloseDrainTimeout(d)`  | 30s    | 停机时 ChanRPC 服务端排空自投递链条的超时上限，超时后放弃剩余部分直接完成关闭。 |
| `WithClientCloseTimeout(d)` | 5s     | 停机时 ChanRPC 客户端等待未处理异步回调排空的超时上限，超时后放弃剩余回调。     |
| `WithAsyncCloseTimeout(d)`  | 5s     | 停机时等待未执行的本地异步任务回调排空的超时上限，超时后放弃剩余回调。          |

前四项都是**硬性上限**，但**只封顶、不预留**：队列实际占用的内存随积压增长、随空闲收缩，调大上限不会让空闲模块多占一个字节。队列打满时的行为：

| 队列                 | 打满时                                                                                                                                                                                                                |
|----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 服务端调用队列       | `Cast` 立即记录 warn 并丢弃，`AsyncCall` 立即返回 `ErrChanFull`；`Call` / `CallWithContext` 阻塞等待空位（每 5s 打一条 warn，`CallWithContext` 可由 `ctx` 取消）。                                                    |
| 客户端异步返回队列   | 服务端回包被丢弃并打 error 日志，对应回调不会执行，`pendingAsyncCall` 同步回滚。                                                                                                                                      |
| 定时器到期队列       | 到期定时器留在时间轮第 0 层，后续 tick 持续重试投递，**不丢定时器**，只是通知延迟。                                                                                                                                   |
| 本地异步任务回调队列 | 不会真正打满：队列容量与同时允许存在的未完成任务数上限被强制绑成同一个值，背压发生在更早的提交阶段——`AsyncGo`/`LinearContext.Go` 在途任务数已达上限时直接拒绝新任务并打 error 日志，见[本地异步任务](#本地异步任务)。 |

`WithStatCap(n)` 同样是硬上限，单个 key 采样数达到 n 后新样本不再计入分位统计。

配合 `app.ChanRPC(name).Len()` 与 `Cap()` 做水位告警，在真正打满之前就能发现问题。`Cap()` 返回的是配置的硬上限而非当前分配量——水位关心的是「离拒绝还有多远」。

### ChanRPC

`chanrpc` 是进程内 RPC 组件。

| 方法              | 等待结果       | 说明                                                                                                   |
|-------------------|----------------|--------------------------------------------------------------------------------------------------------|
| `Cast`            | 否             | 单向投递，适合通知、日志和埋点。目标不存在（`ErrServerNil`）时静默丢弃，其余失败会打 warn 日志后丢弃。 |
| `AsyncCall`       | 否，结果走回调 | 推荐方式。回调在发起方模块事件循环中执行。                                                             |
| `Call`            | 是             | 同步阻塞调用。调用链成环会死锁，应谨慎使用。                                                           |
| `CallWithContext` | 是             | 同 `Call`，但可通过 `ctx` 主动超时或取消等待，取消时返回携带 `ctx.Err()` 的 `RetInfo`。                |

Handler 响应语义：

- 返回非 nil `*RetInfo`：框架自动回包，适合同步处理场景。
- 调用 `ci.Hold()` 后返回 `nil`：框架不回包，由 `Hold` 返回的 `Replier` 句柄在稍后回包（延迟响应）。
- 直接返回 `nil` 且未调用 `Hold`：框架补一个空包，避免同步 `Call` 挂死。
- `hasRet` CAS 保证同一调用最多只发送一次响应；重复调用 `Replier.Ret` 会返回 `ErrAlreadyRet`。
- handler panic 时框架仍会回包错误，即便已经调用 `Hold`。

解包请求与响应：

`CallInfo.RequestAs[T]()` 与 `RetInfo.AckAs[T]()` 是带 `ok` 的类型断言（Go 1.27 泛型方法），类型不符或字段本身为 nil 时返回 `T` 的零值和 `false`，不会 panic。回调里尤其该用 `AckAs`——`Err` 非 nil 时 `Ack` 通常就是 nil，裸断言必崩。

```go
req, ok := ci.RequestAs[*PingReq]()
if !ok {
    return &chanrpc.RetInfo{Err: errors.New("unexpected request type")}
}
```

消息 ID 生成规则：

- 消息实现 `chanrpc.IMessage` 时，使用自定义 `ID() uint32` 和日志名称 `Name() string`。
- 否则通过反射获取类型全限定名，再用 BKDR 哈希生成 ID，日志中也使用该类型名。
- 反射路径下指针类型会自动解引用，`T` 和 `*T` 共享同一 ID；但 `IMessage` 判断先于解引用，若 `ID()` 或 `Name()` 定义在指针接收者上，则只有 `*T` 命中自定义实现。

元数据示例（`WithMeta` 可多次调用叠加多个 key，元数据会随响应回传到 `RetInfo.Metadata`）：

```go
_ = m.AsyncCall(app.ChanRPC("target"), &Req{}, callback, chanrpc.WithMeta("trace_id", "abc"))
```

关闭语义：

- `Server.Close()` 不回包 `ErrServerClosed`，而是把队列中已排队的调用（含 handler 自投递）真正执行完再关闭队列；排空默认最多 30 秒（`WithCloseDrainTimeout` 可调），超时后丢弃剩余自投递链条。排空完成后再发起的调用才返回 `ErrServerClosed`。
- `Client.Close()` 尽量消费未处理异步响应，默认最多等待 5 秒（`WithClientCloseTimeout` 可调）；超时后强制清零待处理计数，可能丢失部分未执行的回调。
- `Call()` 是无限等待加 5 秒周期告警，不主动超时返回；需要超时兜底请改用 `CallWithContext`（`ErrCallTimeout` 仅为兼容保留，当前不会返回）。

### 本地异步任务

`internal/async` 提供一个轻量的本地异步任务调度器，`Skeleton` 内置了它——语义上是 `AsyncCall` 的本地版本：`AsyncCall` 调用的是别的模块的 `Server`，`AsyncGo`/`NewLinearContext` 执行的是本模块内部的一段代码，两者都遵循同一个「`f` 并发执行、`cb` 回到事件循环执行」的模式。

| 方法                   | 说明                                                                                                                                                                                                   |
|------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `AsyncGo(name, f, cb)` | `f` 在独立 goroutine 中执行，结束（或 panic）后 `cb` 在本模块事件循环中执行，可安全访问模块状态。`name` 用于 `DumpStat` 按类型统计 `cb` 的耗时。                                                       |
| `NewLinearContext()`   | 返回一个 `*LinearContext`：通过它提交的任务，`f` 始终按提交顺序依次串行执行（不会并发），`cb` 仍然回到事件循环执行。多个本地异步步骤需要保证先后顺序时使用；不需要顺序保证时优先用更简单的 `AsyncGo`。 |

`LinearContext` 与 `Skeleton` 共享同一份并发配额（`WithAsyncChanLen` 设置的上限），其 `Go(f, cb)` 没有 `name` 参数，耗时不计入 `DumpStat`。

```go
m.AsyncGo("load-config", func() {
	// 耗时或阻塞操作，例如读文件、算哈希
}, func() {
	// 回到事件循环，可安全访问模块状态
})

lc := m.NewLinearContext()
lc.Go(step1, onStep1Done)
lc.Go(step2, onStep2Done) // 保证在 step1 之后才执行
```

设计上有一个核心不变量：内部回调队列的硬性容量与「同时允许存在的未完成任务数上限」被强制绑成同一个值，因此队列在正常情况下永远不会因为满而失败或阻塞——真正的背压发生在提交阶段，一旦在途任务数达到上限，`AsyncGo`/`LinearContext.Go` 直接拒绝新任务并打一条 error 日志，而不是让某个 goroutine 卡在向队列投递上。

关闭语义与 `chanrpc.Client` 完全对称：`Skeleton.Close()` 会先排空未执行的本地异步回调，默认最多等待 5 秒（`WithAsyncCloseTimeout` 可调），超时后放弃剩余回调；这一步在 `client.Close()` **之前**执行，因为排空阶段跑的 `cb` 可能还要用 `client` 做最后的跨模块投递。

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
m.UpdateTimer(id, time.Now().Add(time.Minute))
m.CancelTimer(id)

m.NewTimer("heartbeat", time.Second, timer.WithTicker())
```

百分比调整使用万分比，`timer.PctBase` 为 `10000`：

- `AccPctTimer(id, 2000)` 表示剩余时间缩短 20%。
- `DelayPctTimer(id, 2000)` 表示剩余时间延长 20%。

### Signal

`signal` 包提供 `signal.Manager` 和处理函数类型 `signal.Trap`。`app.App` 内部使用 `signal.NewManager()` 创建信号管理器，在 `Run` 中启动监听，关闭应用后停止监听。`RunContext` 不启动信号监听，由调用方通过 context 控制应用生命周期。

框架默认信号行为：

| 信号    | 行为                                                                                                                                                              |
|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| SIGINT  | 触发优雅关闭，框架保留，业务不可注册。                                                                                                                            |
| SIGTERM | 触发优雅关闭，框架保留，业务不可注册。                                                                                                                            |
| SIGKILL | 操作系统不可捕获；框架仅将其作为保留信号禁止业务注册。                                                                                                            |
| SIGHUP  | 默认记录日志并继续运行。仅当业务在 `Run` 之前注册过 SIGHUP 处理器时才不装该默认处理器；`Run` 之后（如模块 `OnInit` 中）注册的处理器是追加，默认日志行为仍会执行。 |

业务可注册非保留信号：

```go
err := app.RegisterSignal(func() {
	slog.Info("reload config")
}, syscall.SIGHUP)
```

同一信号支持多个处理器。收到信号后，处理器并发执行并等待全部完成；单个处理器 panic 会被捕获。

使用独立应用实例时，通过该实例的 `RegisterSignal` 方法注册。需要单独管理进程信号时，也可以直接使用 `signal.NewManager()` 及其 `Register`、`Start`、`Stop` 方法。

### TPStats

`stat.TPStats` 用于统计事件处理耗时。

- `Add(name, costUs)` 记录一次耗时，单位为微秒。
- `Dump(n)` 输出 JSON，包含一个 `id` 为 `"ALL"` 的全局汇总项，以及按 TP99 从高到低排序的前 n 类消息；`n <= 0` 或 n 超过实际类别数时返回全部类别。
- `Reset()` 清空统计。
- 每类消息只保留最先到达的 `maxCnt` 条样本用于分位数计算，超出部分只计入 `Count` 和 `Avg`；`Dump` 输出中的全局汇总项（ID 为 `ALL`）同样受此限制。
- `Count` 和 `Avg` 基于全部输入累计。
- `nil`、空字符串、数字零值等无意义 key 会被忽略。

Skeleton 会自动统计定时器事件、RPC 请求、异步 RPC 回调和本地异步任务（`AsyncGo`）回调的耗时。

---

## 应用生命周期

以下状态常量均定义在 `app` 包中，可通过 `app.State()` 或应用实例的 `State()` 方法读取。

```text
StateNone
    │ Run / RunContext
    ▼
StateInit
    │ 所有静态模块 OnInit 成功且就绪  │ 启动失败
    ▼                                │
StateRun（关闭 App.Ready）            │
    │ 退出信号 / context 取消         │
    │ / 静态模块异常退出              │
    ▼                                ▼
StateStop
    │ 冻结静态控制入口 → 关闭动态模块 → 逆序关闭静态模块
    ▼
StateNone
```

关闭流程：

1. 进入 `StateStop`，拒绝新增动态模块。
2. 对已经就绪且实现 `Quiescer` 的静态模块，按逆启动顺序执行 `Quiesce`，先冻结创建、修改或启停动态模块的控制入口。
3. 完整关闭所有动态模块，包括仍在初始化或后台清理的实例；按优先级和登记序号倒序等待它们清理完成。
4. 按启动顺序（`Priority` 升序，同优先级保留注册顺序）整体逆序关闭静态模块。已启动且就绪的模块若实现 `StopPreparer`，先执行 `PrepareStop`；成功后才取消 context、等待 `Serve` 实际退出，再执行 `OnDestroy` → `Close`。只对 `OnInit` 成功返回过的模块执行 `OnDestroy`/`Close`，未运行的服务端队列则由框架关闭并拒绝积压请求。
5. 单次排空和退出等待默认 30 分钟，可通过 `WithShutdownTimeout` 调整。等待超时后记录告警并继续等待，**不会跳过清理、释放名称或提前关闭依赖**。该时间也用作每次预停机调用的 context 超时；钩子必须主动响应 context，框架不会强杀 goroutine，也不限制 `OnDestroy`、`Close` 自身的执行时间。
6. 启动过程中收到停止请求时，框架先等待启动收敛（默认最多 5 秒），随后进入各模块的清理屏障。仍在执行的 `OnInit` 必须实际返回后才能继续清理，避免初始化与销毁并发；成功后销毁，失败则按 `OnInit` 契约自行回滚。等待期间保留它所依赖的模块。
7. 全部清理完成后回到 `StateNone`，`Run`/`RunContext` 返回运行和清理错误的汇总；正常关闭返回 nil。同一个 `App` 不允许并发或重入运行，**停止后也不支持再次启动**，需要重启时新建应用和模块实例。

### 通过 context 嵌入运行

以下代码使用快速开始中的 `NewPonger`，由宿主控制退出，无需注册进程信号：

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

application := app.New(
	app.WithOSThreadLock(false),
	app.WithPanicExit(false),
	app.WithStartupTimeout(10*time.Second),
	app.WithShutdownTimeout(time.Minute),
)
done := make(chan error, 1)
go func() {
	done <- application.RunContext(ctx, NewPonger())
}()

select {
case <-application.Ready():
	slog.Info("application ready")
case err := <-done:
	// 启动失败时 Ready 可能不会关闭，必须同时等待运行结果。
	return err
}

// 宿主需要停止应用时取消 context，并等待实际清理完成。
cancel()
return <-done
```

这段代码应放在返回 `error` 的函数内，并导入 `context`、`log/slog` 和 `time`。context 在调用前已取消时，`RunContext` 直接返回 `ctx.Err()`；运行期间取消用于发起关闭，正常清理完成后返回 nil。`App.Ready()` 是一次性启动信号，不代表应用此后一直可用，运行期间仍需观察 `done`。

### 预停机接口

模块可按需实现两个可选接口，无需修改 `IModule`：

```go
type Quiescer interface {
	Quiesce(context.Context) error
}

type StopPreparer interface {
	PrepareStop(context.Context) error
}
```

`Quiesce` 用于静态控制模块冻结入口；动态模块仍依赖的共享服务应在 `PrepareStop` 中排空，避免过早关闭。`PrepareStop` 执行期间，框架尚未取消该模块的 `Serve`，可以通过事件循环停止生产者、等待回执和持久化剩余工作。若 `Serve` 已经异常退出，钩子也必须能等待后台任务结束。

钩子由框架的关闭流程调用，不在业务事件循环内执行；涉及模块状态时应通过消息把操作交给状态所有者，不能直接并发读写业务字段。两个钩子都必须响应传入 context，并支持重复调用：返回错误或发生 panic 后，框架会限频记录告警并重试，直到成功；在此之前不会继续销毁该模块及后续依赖。

---

## 动态模块

动态模块适合运行时启停的功能，例如活动玩法、临时任务或调试模块。

```go
results, err := app.AddDynamicModules(NewActivityModule())
if err != nil {
	// err 可能表示模块添加失败，也可能表示应用正在关闭、整批添加被拒绝。
	slog.Error("dynamic modules add failed", slog.Any("error", err))
	for _, r := range results {
		if r.Err != nil {
			slog.Error("dynamic module init failed", slog.String("module", r.Name), slog.Any("error", r.Err))
		}
	}
	return err
}

names := app.DynamicModules()
_ = names

removed := app.RemoveDynamicModule("activity")
_ = removed
```

动态模块特性：

- `AddDynamicModules` 按 `Priority` 升序（同优先级保留传参顺序）依次执行 `OnInit`，成功后启动 `Serve`，并等待模块 `Ready()` 后才对 RPC 寻址、统计和模块列表公开。初始化前先在动态模块表中占住名称，重名模块会被直接拒绝，不会覆盖已有模块；应用已进入关闭流程后调用会整体拒绝，避免登记出无人负责关闭的模块。
- `RemoveDynamicModule` 开始卸载后停止对外寻址，并等待清理完成，等待上限为 `WithShutdownTimeout`。模块不存在、尚未就绪、等待超时或清理出错时返回 `false`。超时只结束调用方等待，后台仍继续清理，名称保留到清理实际结束；此期间同名添加会被拒绝。
- 实例方法 `RemoveDynamicModuleContext(ctx, name)` 允许调用方指定等待期限：已不存在的模块返回 nil，未就绪且尚未进入清理的模块返回错误；等待期间 context 取消则返回 `ctx.Err()`。重复或并发卸载同一实例会等待同一次清理，不会重复销毁。动态启动等待就绪失败后也由后台清理接管，不能因添加失败就提前复用名称。
- 动态模块 `Serve` 中的 panic 会被捕获并记录，不退出进程；`OnDestroy` 与 `Close` 的 panic 转为清理错误。`OnInit` 在调用方 goroutine 上同步执行，默认向调用方传播 panic；`WithPanicExit(false)` 将其转为该模块添加失败。
- 批量添加中途失败时，已经启动的动态模块不会自动回滚；`OnInit` 失败的那个模块不会收到 `OnDestroy`/`Close`（须由 `OnInit` 自行回滚），与静态模块语义一致。

需要区分卸载失败原因或重试等待时，使用同一应用实例：

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := application.RemoveDynamicModuleContext(ctx, "activity"); err != nil {
	slog.Error("dynamic module removal incomplete", slog.Any("error", err))
}
```

---

## 应用 API

`app` 包的 `Register`、`Run`、`State` 等函数操作同一个默认应用实例。需要独立实例或自定义选项时，使用 `app.New(opts ...app.Option) *app.App`，随后调用实例上的方法；包级函数仍操作默认实例。`RunContext`、`Ready` 和 `RemoveDynamicModuleContext` 仅提供实例方法，不能通过包级函数调用；独立应用内的 RPC 寻址也应使用该实例的 `ChanRPC`。

例如，使用快速开始中的 `Ponger` 创建独立应用：

```go
application := app.New(app.WithShutdownTimeout(time.Minute))
if err := application.Run(NewPonger()); err != nil {
	slog.Error("app exited with error", slog.Any("error", err))
}
```

| API                                                                                            | 说明                                                                                           |
|------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| `app.New(opts ...app.Option) *app.App`                                                         | 创建独立应用实例，选项仅影响该实例。 |
| `app.Register(mods ...app.IModule) error`                                                      | 启动前注册静态模块。                                                                           |
| `app.Run(mods ...app.IModule) error`                                                           | 注册并启动应用、监听进程信号，关闭完成后返回；启动、运行或清理失败时返回 error。 |
| `(*app.App).RunContext(ctx context.Context, mods ...app.IModule) error`                         | 由 context 控制运行，不监听进程信号，等待有序清理完成后返回。 |
| `(*app.App).Ready() <-chan struct{}`                                                          | 全部静态模块就绪并进入 `StateRun` 后关闭的一次性信号。 |
| `app.State() int32`                                                                            | 返回当前应用状态。                                                                             |
| `app.Stats() string`                                                                           | 返回所有模块 RPC 队列积压统计。                                                                |
| `app.ChanRPC(name string) *chanrpc.Server`                                                     | 按模块名获取 ChanRPC 服务端。                                                                  |
| `app.DynamicModules() []string`                                                                | 返回已就绪且未进入卸载的动态模块名称列表。 |
| `app.AddDynamicModules(mods ...app.IModule) (results []app.AddDynamicModuleResult, err error)` | 添加并启动动态模块；`results` 给出逐个模块的结果，模块添加失败或关闭期间整批拒绝时返回 error。 |
| `app.RemoveDynamicModule(name string) bool`                                                    | 卸载动态模块，在关闭等待上限内完成且无清理错误时返回 true。 |
| `(*app.App).RemoveDynamicModuleContext(ctx context.Context, name string) error`                | 等待动态模块清理，context 超时后后台清理继续；模块已不存在时返回 nil。 |
| `app.RegisterSignal(trap signal.Trap, sigs ...os.Signal) error`                                | 注册非保留信号处理器。                                                                         |

构造选项：

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `app.WithStartupTimeout(d)` | 30 秒 | 每次等待模块 `Ready` 的上限，不限制 `OnInit`。 |
| `app.WithShutdownTimeout(d)` | 30 分钟 | 预停机调用的 context 期限、退出等待的告警周期，以及 `RemoveDynamicModule` 的等待上限；全应用关闭持续等待实际清理完成。 |
| `app.WithOSThreadLock(enabled)` | true | 是否为每个模块的 `Serve` 绑定专用 OS 线程；纯 Go 服务可关闭。 |
| `app.WithPanicExit(enabled)` | true | 是否在应用启动或静态 `Serve` panic 时终止进程；false 时返回错误并有序清理，动态 `OnInit` panic 转为添加失败。 |

两个超时选项收到 `d <= 0` 时不覆盖已有配置。卸载调用的 context 只限制等待者；`RunContext` 在取消后仍等待清理完成，预停机 context 则需要钩子主动响应。它们都不能强制终止业务代码。

根包的 goroutine 工具：

| API                                                                      | 说明                                                                            |
|--------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| `xhive.SafeGo(fn func())`                                                | 在独立 goroutine 中执行 `fn`，自动捕获其中的 panic。                            |
| `xhive.SafeGoContext(ctx context.Context, fn func(ctx context.Context))` | 语义同 `xhive.SafeGo`；启动时若 `ctx` 已取消则跳过执行，执行中不主动中断 `fn`。 |

状态常量：

| 常量               | 说明                                                     |
|--------------------|----------------------------------------------------------|
| `app.StateNone` | 应用未启动或已完全停止；已停止的应用**不支持再次启动**。 |
| `app.StateInit` | 应用正在初始化。 |
| `app.StateRun`  | 所有静态模块就绪，应用运行中。 |
| `app.StateStop` | 应用正在关闭。 |

---

## 目录结构

```text
xhive/
├── skeleton.go         # Skeleton、RPC/Timer/Async 接口与事件循环
├── safego.go           # SafeGo / SafeGoContext，带 panic 恢复的 goroutine
├── app/
│   ├── def.go          # 默认应用的包级 API、应用状态、Option
│   ├── app.go          # App、New、应用启停与动态模块管理
│   ├── context.go      # context 入口、就绪通知、预停机接口与清理屏障
│   └── module.go       # IModule、模块生命周期状态与运行时包装
├── chanrpc/
│   ├── def.go          # 消息 ID、CallInfo、RetInfo、CallOption、Hold/Replier
│   ├── server.go       # ChanRPC 服务端
│   └── client.go       # ChanRPC 客户端
├── internal/
│   ├── async/          # Scheduler / LinearContext，Skeleton.AsyncGo/NewLinearContext 的底座
│   └── queue/          # 动态扩缩容 + 硬上限的 FIFO 队列，上面各条队列的共同底座
├── signal/
│   └── signal.go       # Manager / Trap，进程信号注册与分发
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

- app：名称唯一性、并发注册、启动失败、context 取消、控制入口冻结与动态模块排空顺序、初始化与关闭交接、卸载和启动超时期间的名称保留、依赖存活、预停机重试、异常退出和 panic 返回。
- chanrpc：消息 ID、注册校验、Cast、AsyncCall、Call、metadata、`Hold` 延迟响应、panic 恢复、关闭语义、队列打满时的失败/阻塞/丢包与计数回滚。
- timer：时间轮放置、tick 推进、时钟前移和后移、取消、更新、同 ID 替换、到期队列打满时的重试、Manager one-shot 和 ticker。
- internal/async：并发提交、`LinearContext` 顺序保证（含并发提交方）、`Close` 正常排空与超时放弃、无消费者时回调队列不阻塞、panic 恢复。
- internal/queue：FIFO 顺序、扩容与收缩、打满即拒绝、阻塞入队的取消与唤醒、关闭后的拒绝与排空、出队后解除引用、边沿信号不丢唤醒、多生产者多消费者并发。
- stat：分位统计、TopN、Reset、零值 key 忽略、并发 Add 和 Dump。

根包 `xhive` 和 `signal` 当前没有测试文件。

---

## 常见问题

### 为什么模块内通常不需要加锁？

因为模块内部事件都在该模块的 `Serve` goroutine 中串行处理。只要业务状态只在该事件循环内访问，就不会发生并发读写。

### 什么时候用 AsyncCall，什么时候用 Call？

优先使用 `AsyncCall`。它不会阻塞当前模块事件循环，回调也会回到当前模块事件循环中执行。`Call` 会阻塞当前模块，如果调用链形成环，会死锁；而且 `Call` 是无限等待，只会每 5s 打一条告警，不会自行超时返回。确实需要同步语义时，新代码应使用 `CallWithContext` 并传入带超时的 ctx，ctx 取消时立即返回携带 `ctx.Err()` 的 `RetInfo`。

### AsyncGo 和 AsyncCall 有什么区别？

`AsyncCall` 调用的是**别的模块**的 `Server`：请求经过对端的 ChanRPC 服务端排队、由对端的事件循环执行 handler，再把结果回传。`AsyncGo` 执行的是**本模块内部**的一段代码：`f` 直接在一个新起的 goroutine 里跑，不经过任何模块间路由。两者的共同点是回调（`cb`/`Callback`）都会回到发起方模块的事件循环里执行，可以安全访问模块状态；区别只在于要执行的代码跑在哪——别的模块用 `AsyncCall`，本模块内部想把一段耗时/阻塞代码挪出事件循环用 `AsyncGo`。需要保证多个本地异步步骤按顺序执行时，用 `NewLinearContext` 而不是多次调用 `AsyncGo`。

### Cast 失败会怎么样？

`Cast` 没有返回值，任何失败都表现为丢弃：目标模块不存在时完全静默，目标 Server 已关闭、消息类型无效等情况只打一条 warn 日志，目标模块未注册该消息的 handler 时调用方也拿不到反馈。它适合对可靠性要求不高的通知类消息。如果需要知道结果，应使用 `AsyncCall` 或 `Call` / `CallWithContext`——处理错误会通过 `RetInfo.Err` 告知调用方，`AsyncCall` 另有 error 返回值用于报告前置校验失败。

### 定时器回调在哪个 goroutine 执行？

底层 dispatcher 只投递到期事件，不执行业务回调。业务回调在模块的 Skeleton 事件循环中执行，因此可以安全访问模块内部状态。

### Ticker 会不会因为 handler 执行慢而漂移？

Ticker 续期以上次 deadline 为基准，而不是以当前时间为基准，因此可以减少处理耗时带来的累积漂移。如果 handler 执行时间超过周期，后续事件可能追赶触发，业务应避免长时间阻塞事件循环。

### 服务器时间被手动调整会怎样？

时间被手动回退时，dispatcher 的内部基准 `lastTick` 跟着回退到当前 tick。重扫是幂等的：定时器在投递到期事件的同时就已从槽位删除，重扫扫不到已触发的定时器，只多花一点 CPU；反之若基准停在未来，后续每次 tick 都不会扫描任何层级，全进程定时器停摆。时间被手动后移到未来时，dispatcher 会从旧 tick 向当前 tick 逐步推进，到达当前时间后停止循环。

### 队列打满了会发生什么？

队列容量全都有硬上限，打满时的行为按调用语义区分（详见 [Skeleton](#skeleton) 一节的表格）：`Cast` 立即记录 warn 并丢弃，`AsyncCall` 立即返回 `ErrChanFull`，同步语义（`Call`、`CallWithContext`）阻塞等待空位形成背压，定时器到期事件则留在时间轮里下个 tick 重试、不丢失。

这是刻意的取舍。早期版本使用无界队列，生产者永不阻塞、永不失败，代价是消费端一旦跟不上，积压只表现为内存一路上涨，最终以 OOM 的形式在离现场很远的地方崩掉，中途没有任何可告警的信号。有上限的队列把同一个问题在发生的那一刻就暴露成一个带消息类型和水位的错误。

生产环境应通过 `app.Stats()`（汇总各模块 RPC 服务端队列长度）或 `app.ChanRPC(name).Len()` / `Cap()` 观测水位；使用独立应用时调用该实例的同名方法。在打满之前就限流、拆模块或告警。

### 有上限的队列，为什么不用原生 channel？

因为原生 channel 只能在「固定容量」和「无界」之间二选一，而这两个都不是我们要的。

`hchan` 的缓冲区在 `make` 时一次性分配、此后永不增长。一个 `make(chan *CallInfo, 4096)` 的模块，哪怕一条消息都没有，也要先付出 32KB 常驻内存；元素含指针时，这段缓冲区还全程参与 GC 扫描。按每模块 server 4096 + client 4096 + timer 1024×2 算，空转就是 88KB，模块一多这笔固定开销很可观。反过来把队列做成无界的（早期的 `chanx`）又回到了上一个问题：消费端跟不上时只会一路吃内存直到 OOM。

破局点在于：`select` 需要的只是一个**可等待的 channel**，它并不要求数据本身流经这个 channel。于是 `internal/queue` 把两者拆开——数据放在自己管理的 ring buffer 里，从 16 个槽位起按积压翻倍、按空闲减半；容量上限由入队时显式检查，达到上限即拒绝，与原生有界 channel 的「满」语义完全一致；另外用两个 `chan struct{}` 承载「非空」「非满」两个信号，让队列仍然能作为 `select` 的一路。

两个信号的机制不同，各自对应一种用法：

- **非空**是 `cap=1` 的边沿信号。信号会合并，所以消费方不能假设「一个信号对应一个元素」；`Pop` 在队列仍非空时把信号补回去，以此保证只要队列非空就一定还有信号在，不丢唤醒。
- **非满**是广播：队列填满时换上一个新的未关闭通道，从满变回不满时 `close` 它，所有阻塞的生产者一次全醒。不用边沿信号，是因为它一次只能放行一个等待者，剩下的必须靠每个成功入队的生产者接力再发一个——接力只要在任何一条路径上被漏掉，其余生产者就会永久卡住，队列明明空着却没人能进。广播没有这个陷阱，而且稳态下（既不满也不空）这两个字段一次都不用碰。

实测数字：

| 指标                     | 原生 channel | Queue      |
|--------------------------|--------------|------------|
| 每个 `Skeleton` 常驻内存 | 93.5 KB      | **3.2 KB** |
| 1000 个模块              | 91.3 MB      | **3.2 MB** |
| 单次收发（队列常空）     | 36 ns        | 79 ns      |
| 单次收发（队列有积压）   | 34 ns        | **29 ns**  |

慢的那一栏是队列常空、每条消息都要真正唤醒一次 goroutine 的场景，原生 channel 在这里走 runtime 的直接交接快路径，而 Queue 要多付一次入队锁和一次出队锁。队列一旦有积压，`Pop` 补回的信号让消费者无需阻塞唤醒就能连续取件，反而比原生更快。模块负载越高越靠近后者。

### 模块的启动顺序怎么控制？

由 `app.IModule.Priority() uint` 决定，名称和优先级在登记时固定：值越小越早初始化，优先级相同时保留注册顺序（静态模块为 `Register`/`Run` 的传参顺序，动态模块为单次 `AddDynamicModules` 的传参顺序）。应用关闭时先冻结静态控制入口，再关闭全部动态模块，最后逆序关闭静态模块；动态模块内部按优先级和登记序号倒序关闭。共享依赖应作为较早启动的静态模块。内嵌 `Skeleton` 时 `Priority` 默认返回 0，需要靠后初始化的模块自行覆盖该方法；它不保证各模块 `Serve` 就绪的先后。

### 动态模块和静态模块有什么区别？

静态模块随应用启停，不支持运行时卸载；`Serve` panic 默认以退出码 `255` 终止进程，`WithPanicExit(false)` 时触发有序关闭并返回错误。动态模块支持运行时加载和卸载，`Serve` panic 只记录日志。两者的 ChanRPC handler、异步回调和定时器回调中的 panic 由组件捕获，handler panic 还会回包错误；`OnDestroy` 和 `Close` panic 转为清理错误，并继续清理其他模块。

### 模块关闭超时了会怎样？

默认每 30 分钟记录退出等待告警，可通过 `app.New(app.WithShutdownTimeout(d))` 调整。全应用关闭会继续等待模块实际退出，清理完成前保留名称和依赖，不会强杀 goroutine，也不会跳过 `OnDestroy`/`Close`。`RemoveDynamicModule` 等待超时返回 false，`RemoveDynamicModuleContext` 在调用方 context 取消时返回 `ctx.Err()`；两者都不会终止后台清理，后续卸载请求可继续等待同一次清理。

`Serve` 必须及时响应 `ctx.Done()`，预停机钩子必须响应传入 context，否则停机可能持续等待。关闭超时也不限制 `OnDestroy`/`Close` 自身执行时间。`OnDestroy` 里跨模块投递请用 `Cast`/`AsyncCall` 或带超时的 `CallWithContext`，不要用无限等待的同步 `Call`：启动中途失败时对端事件循环可能尚未运行。需要本模块事件循环参与的回执和排空，应在 `PrepareStop` 中完成。

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
