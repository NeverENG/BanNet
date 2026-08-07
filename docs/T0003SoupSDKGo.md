# T0003 soup-sdk-go · 逻辑服 SDK 技术规格书（Go）

> 定位符：`T0003M{模块}F{功能}`
> 语言：Go 1.22+ / 纯 Go，**零 cgo** / 上游：`T0002SoupEngine`（Rust 框架）
> 配套：`T0001SoupOut`（本游戏协议，待出）

## Version History

| 版本 | 变更 |
|---|---|
| v0.1 | 首版。定义 Room 接口、tick 循环、抖动缓冲、baseline、确定性与零分配规则 |

---

# M01 定位与硬边界

## T0003M01F01 一句话

> **soup-sdk-go 让 Go 开发者只写一个 `Room` 接口，就得到一个跑在 soup-engine 上的权威对战服务器。**

它承担：与框架的连接管理、房间调度、定频 tick 循环、输入抖动缓冲与去重、快照 baseline 追踪、带宽降级、确定性保障、缓冲池。

它**不**承担：任何游戏规则。那是 `Room` 实现者的事。

## T0003M01F02 【最重要】不得假设游戏状态是「实体列表」

> 绝大多数游戏网络 SDK 会在内核里硬编码 `[]Entity` + 逐实体字段 delta。**本 SDK 禁止这样做。**

SDK **只知道**：某个客户端在某个 tick 需要一段字节，这段字节可以相对某个 baseline tick 做增量。**字节里是什么，SDK 永远不问。**

具体要求：

- 快照编码由使用者实现（`EncodeSnapshot`）。SDK 只提供缓冲区、baseline 追踪、调度、带宽预算
- SDK **不得**提供任何形如 `RegisterEntity` / `EntityID` / `NetworkedField` / `[]Replicated` 的 API
- 使用者的状态可以是实体表、连续区域场、位图、任意结构 —— SDK 一视同仁

**理由**：本项目的核心同步对象不是实体列表，而是一个每 tick 被多方并发改写的连续区域场。**任何把实体表焊进内核的 SDK，这个游戏都跑不起来。这一条优先级高于性能。**

## T0003M01F03 与框架的职责分界

| 归框架（Rust） | 归 SDK / 逻辑服（Go） |
|---|---|
| UDP、可靠层、四通道、分片 | 房间生命周期与调度 |
| 会话、重连、NAT 漂移 | tick 循环、落后补偿 |
| 组播、限流、背压、抗攻击 | 输入抖动缓冲与去重 |
| `SessionStats` / `Overload` 信号 | baseline 追踪、快照调度、降级决策 |
| | 游戏状态、规则、编解码 |

---

# M02 核心接口 【本文最重要的一节】

> 以下签名是**契约**。改签名需同步更新本文。
> module path: `github.com/<org>/soup-sdk-go`

## T0003M02F01 基础类型

```go
type PlayerID uint32
type Tick     uint32
type InputSeq uint16
type MsgID    uint16
type Channel  uint8

const (
    ChUnreliable        Channel = 0 // 快照下行
    ChInput             Channel = 1 // 输入上行（sequenced）
    ChReliableOrdered   Channel = 2 // 房间事件
    ChReliableUnordered Channel = 3 // 一次性通知
)

// Baseline 是值类型，传参不分配。
type Baseline struct {
    Tick  Tick
    Valid bool // false 表示必须输出可独立解码的全量
}

type Outcome uint8
const (
    Continue Outcome = iota
    End
)

type LeaveReason uint8
const (
    LeaveTimeout LeaveReason = iota
    LeaveKicked
    LeaveQuit
)
```

## T0003M02F02 Room 接口

```go
type Room interface {
    OnJoin  (ctx *RoomCtx, p PlayerID)
    OnLeave (ctx *RoomCtx, p PlayerID, why LeaveReason)

    // 断线重连成功。SDK 已自动推过全量状态，此处只需处理业务侧副作用
    // （例如清掉"离线中"标记）。⚠️ 不要当新玩家处理。
    OnResume(ctx *RoomCtx, p PlayerID, gapMS uint32)

    // 一条已通过抖动缓冲、按序去重后交付的输入。
    // ⚠️ payload 是 SDK 的复用缓冲，本调用返回后即失效。
    //    必须解码进你自己预分配的结构，不得持有该切片。
    OnInput(ctx *RoomCtx, p PlayerID, seq InputSeq, payload []byte)

    // 定频推进一步。dtMS 恒等于 1000/tickHz，不是墙钟差值。
    Tick(ctx *RoomCtx, tick Tick, dtMS uint32) Outcome

    // 【零分配契约】直接写进 SDK 提供的缓冲区，禁止返回持有所有权的值。
    // baseline.Valid == false 时必须输出可独立解码的完整状态。
    EncodeSnapshot(target PlayerID, baseline Baseline, out *Buffer)

    // 重连 / 中途加入时的全量状态。
    EncodeFullState(target PlayerID, out *Buffer)

    // 可选：确定性回放校验用。不做校验时返回 0。
    StateHash() uint64
}
```

> ⚠️ `EncodeSnapshot` 无返回值是刻意的。若返回 `[]byte`，则每客户端每 tick 一次堆分配：4 人 × 20Hz × 数百房间 = 每秒数万次分配 + GC 压力，直接违反 `M07`。**房间必须往 SDK 给的 `*Buffer` 里写。**

## T0003M02F03 RoomCtx

房间与外界交互的**唯一**通道。

```go
// ---- 发送：Begin/Commit 而不是闭包回调 ----
// ⚠️ Go 中 func(*Buffer) 闭包一旦捕获外部变量就会逃逸到堆。
//    热路径每 tick 调用数次，必须避开。故采用 Begin/Commit 风格。
func (c *RoomCtx) BeginSend(to PlayerID, ch Channel, msg MsgID) *Buffer
func (c *RoomCtx) BeginMulticast(to []PlayerID, ch Channel, msg MsgID) *Buffer
func (c *RoomCtx) BeginBroadcast(ch Channel, msg MsgID) *Buffer
func (c *RoomCtx) Commit(b *Buffer)   // 交给 SDK，缓冲归池
func (c *RoomCtx) Abort(b *Buffer)    // 放弃发送，缓冲归池

// ---- 查询 ----
func (c *RoomCtx) Players() []PlayerID              // 复用切片，勿持有
func (c *RoomCtx) LastProcessedInput(p PlayerID) InputSeq // 客户端和解用，见 M09
func (c *RoomCtx) RTTms(p PlayerID) uint16
func (c *RoomCtx) LossPermille(p PlayerID) uint16
func (c *RoomCtx) IsConnected(p PlayerID) bool      // 宽限期内为 false，但不触发 OnLeave

// ---- 确定性随机：由建房 seed 播种 ----
func (c *RoomCtx) Rand() *DetRand

// ---- 控制 ----
func (c *RoomCtx) End(result Result)
func (c *RoomCtx) RequestKeyframe(p PlayerID)       // 强制下一帧发全量
func (c *RoomCtx) Kick(p PlayerID, reason uint8)

// ⛔ 故意不提供：time.Now / math/rand / 任何 IO / 任何 goroutine 启动接口
```

## T0003M02F04 Buffer

```go
type Buffer struct{ /* 内部持有从 sync.Pool 取出的 []byte */ }

func (b *Buffer) PutU8(v uint8)
func (b *Buffer) PutU16(v uint16)   // 小端
func (b *Buffer) PutU32(v uint32)
func (b *Buffer) PutI16(v int16)
func (b *Buffer) PutVarint(v int64)
func (b *Buffer) PutFixed16(v float32, scale float32) // 定点量化
func (b *Buffer) PutAngle(rad float32)                // u16 量化角度
func (b *Buffer) PutBytes(p []byte)
func (b *Buffer) Bits() *BitWriter                    // 位打包
func (b *Buffer) Len() int
```

## T0003M02F05 服务器装配

```go
func main() {
    srv := soup.NewServer(soup.Config{
        EngineSocket:       "/run/soup-engine.sock",
        TickHz:             20,
        SnapshotHz:         20,              // 可低于 TickHz
        JitterBufferTicks:  2,
        KeyframeIntervalTicks: 100,
        BaselineRingSize:   32,
        MaxRooms:           1024,
        BudgetKbpsPerClient: 24,
        Gatekeeper:         &MyGatekeeper{},
    })
    log.Fatal(srv.Run(context.Background()))
}
```

## T0003M02F06 Gatekeeper（把匹配挡在 SDK 外面）

```go
type Gatekeeper interface {
    // token 由框架原样透传。返回 nil 表示拒绝。
    Authenticate(token []byte, addr string) *PlayerID

    // 决定这个玩家进哪个房间。
    Route(p PlayerID, hint JoinHint) RoomRoute

    // 建房工厂。seed 用于确定性随机。
    NewRoom(roomID RoomID, cfg any, players []PlayerID, seed uint64) Room
}

type RoomRoute struct {
    Action RouteAction // Join / Create / Reject
    RoomID RoomID
    Config any
}
```

匹配、房间码、组队全部在你的 `Gatekeeper` 实现里。**SDK 不关心。**

---

# M03 房间调度与 Tick 循环

## T0003M03F01 一个房间 = 一个 goroutine

```
帧读取 goroutine ──┬──▶ room#1 chan ──▶ goroutine#1 (ticker 20Hz)
   (来自框架)      ├──▶ room#2 chan ──▶ goroutine#2
                  └──▶ room#N chan ──▶ goroutine#N
                                            │
帧写入 goroutine ◀────── 共享出站 chan ◀──────┘
```

| 规则 | 说明 |
|---|---|
| 房间状态被其 goroutine **独占** | 不加锁，不跨 goroutine 共享 |
| 入站 channel **有界**（默认 256） | 满了丢弃该玩家最旧的输入并计数。**实时游戏丢包优于排队** |
| 500 房间 = 500 goroutine | Go 调度器的舒适区，这正是选 Go 的理由 |

> **Go 的并发红利在房间之间，不在房间内部。** 单个房间的 tick 是有硬截止时间的 CPU 密集循环，goroutine 帮不上忙，只会引入不确定性。见 `M06`。

## T0003M03F02 Tick 循环

```go
// 伪码
step := time.Second / time.Duration(cfg.TickHz)
next := time.Now().Add(step)
for {
    drainInbox()          // 帧 → 抖动缓冲
    deliverReadyInputs()  // 到期输入 → room.OnInput()
    t0 := time.Now()
    out := room.Tick(ctx, tick, dtMS)
    recordTickDuration(time.Since(t0))
    scheduleSnapshots()   // 见 M05
    flushOutbox()
    tick++
    next = next.Add(step)
    if d := time.Until(next); d > 0 { timer.Reset(d); <-timer.C } else { catchUpOrDrop() }
}
```

## T0003M03F03 落后补偿（防死亡螺旋）

- 落后 ≤ 3 tick：连续追帧补上
- 落后 > 3 tick：**丢弃积压，直接跳到当前时间**，记 `tick_skipped` 并告警
- ⛔ 严禁无上限追帧 —— 一次 GC 卡顿会滚成永久 CPU 饱和

## T0003M03F04 Tick 预算

20Hz 下每 tick 预算 50ms。**SDK 自身开销目标 < 1ms**，其余留给房间逻辑。`tick_duration` p99 超过预算 60% 打 WARN。

---

# M04 输入处理

## T0003M04F01 抖动缓冲

- 客户端输入带 `clientTick` 与 `inputSeq`
- SDK 按 `JitterBufferTicks`（默认 2）延迟交付，抹平抖动
- 可根据框架推来的 `SessionStats` 的 RTT 抖动**动态调整深度**（2~5 tick）
- **缓冲为空** → 该玩家本 tick **重复上一帧输入**（比冻结手感好），计 `input_starved`
- 缓冲溢出 → 丢最旧

## T0003M04F02 输入冗余去重

约定客户端每包携带**最近 3 帧输入**。SDK 按 `inputSeq` 去重，只把没见过的交给房间。**1~2 个包的丢失对手感完全无感。**

---

# M05 快照与 Baseline

## T0003M05F01 Baseline 机制

1. SDK 为每个客户端维护**已确认 tick 的环形缓冲**（默认 32）
2. 客户端在每个输入包里回传 `lastRecvSnapshotTick`
3. SDK 据此选 baseline，调用 `room.EncodeSnapshot(target, Baseline{tick, true}, buf)`
4. baseline 超出环形范围（长时间丢包）→ 传 `Baseline{Valid:false}`，房间输出全量

**SDK 不解释快照内容，只负责选 baseline、调度、发送。增量算法完全在房间实现里。**

## T0003M05F02 关键帧

每 `KeyframeIntervalTicks` 强制发一次全量，用于纠偏与快速重连。也可 `ctx.RequestKeyframe(p)` 手动触发。

## T0003M05F03 带宽降级

- 每客户端配置 `BudgetKbpsPerClient`，SDK 滑动窗口统计实际用量
- 超预算或收到框架 `Overload` 时，**降低快照频率**（20Hz → 10Hz → 5Hz）
- **绝不截断快照内容** —— 半截快照会解出错误状态
- 触发降级必须打 WARN 并计 `snapshot_degraded`

> **不许静默降级。** 一个悄悄降到 5Hz 的服务器比一个报警的服务器难查十倍。

## T0003M05F04 重连处理

SDK 收到框架的 `SessionResume` 后：
1. 清空该客户端的 baseline 环形缓冲
2. 调用 `room.EncodeFullState` 推全量
3. 调用 `room.OnResume(ctx, p, gapMS)`
4. 恢复正常增量

**房间不会收到 `OnLeave`。**

---

# M06 确定性规则

为使「录输入 → 重放 → `StateHash()` 一致」成立：

| 规则 | 谁保证 | 说明 |
|---|---|---|
| 房间拿不到墙钟 | SDK | `RoomCtx` 不暴露时钟；`Tick` 的 `dtMS` 是常量 |
| 房间拿不到非种子随机 | SDK | 只提供 `ctx.Rand()`，由建房 seed 播种 |
| 输入交付顺序确定 | SDK | 按 `(tick, PlayerID)` 全序交付，与到达顺序无关 |
| **不得遍历 map** | **房间实现** | Go 的 map 遍历顺序随机。逻辑中一律用 slice 或有序结构。**建议加 lint 规则** |
| **房间内不得起 goroutine** | **房间实现** | 并发 = 不确定。一个房间一个 goroutine，就地算完 |
| **不得调用 `time.Now` / `math/rand`** | **房间实现** | lint 禁止 |
| 浮点 | 房间实现 | **建议关键状态用定点整数**；跨平台重放一致性只在同架构下承诺 |

---

# M07 零分配与 GC 控制

> Go 的 GC 停顿本身不是问题（亚毫秒 vs 50ms 预算）。**问题是分配速率**：分配得越猛，GC 越频繁，`tick_duration` 的长尾越难看。

| 手段 | 说明 |
|---|---|
| 缓冲池 | 所有出站缓冲来自 `sync.Pool`，`Commit`/`Abort` 后归还 |
| **避免闭包逃逸** | 发送 API 用 `BeginSend`/`Commit` 而非 `func(*Buffer)` 回调（见 `M02F03` 的注释） |
| 输入解码 | `OnInput` 给的是复用切片，房间必须解进**预分配的结构**，不得 `append` 到新切片 |
| 预分配 | 房间状态的所有 slice/map 在 `NewRoom` 时用 `make(..., 0, N)` 给足容量 |
| 避免装箱 | 热路径禁止 `any` / `interface{}` 传值、禁止 `fmt.Sprintf` |
| 日志 | 结构化日志惰性求值，热路径不拼字符串 |
| 运行参数 | 设置 `GOMEMLIMIT`；视实测调 `GOGC`（通常调大以降低 GC 频率） |

**硬性验收**：稳态下 `Tick` + `EncodeSnapshot` 的合计分配 = **0 次/tick**，用 `testing.AllocsPerRun` 断言，进 CI。

---

# M08 与框架的对接

SDK 内部处理 `T0002M04` 的全部帧类型，映射如下：

| 框架帧 | SDK 行为 |
|---|---|
| `SessionOpen` | 调 `Gatekeeper.Authenticate` → 拒绝则 `Kick`；通过则 `Route` → `OnJoin` |
| `SessionResume` | 见 `M05F04` |
| `SessionClose` | `room.OnLeave` |
| `Data` (ch=1) | 进抖动缓冲 |
| `Data` (ch=2/3) | 直接交付房间（业务事件） |
| `SessionStats` | 更新 RTT/丢包，供 `ctx.RTTms` 与抖动缓冲动态深度 |
| `Overload` | 触发全局快照降频 |
| `EngineHello` | 框架重启/热重启，重置连接状态 |

**框架掉线时**：SDK 保持房间继续 tick（状态在 Go 侧，不受影响），出站帧丢弃并计数；框架回来后按 `SessionResume` 逐个推全量。

---

# M09 与客户端的责任划分

| 职责 | 归属 |
|---|---|
| 权威模拟、命中判定、状态裁决 | **逻辑服（Room）** |
| 回传「该客户端最后被处理的 inputSeq」 | **SDK**（`ctx.LastProcessedInput`，随快照下发） |
| 提供快照 baseline 机制 | **SDK** |
| 客户端预测（本地立即响应输入） | **客户端（Godot）** |
| 和解 / 回滚重放 | **客户端（Godot）** |
| 远端对象插值与外推 | **客户端（Godot）** |

**SDK 的义务到「回传 lastProcessedInputSeq + 提供 baseline」为止。预测和回滚一律在客户端实现。**

---

# M10 通用性验收：Pong 测试

> **这是判断 API 是否真的与游戏无关的最便宜的检验。**

要求：用 soup-sdk-go 实现一个双人 Pong 逻辑服，**房间实现不超过 80 行**，且**不修改 SDK 任何一行**。

```go
type Pong struct {
    ball [2]int32
    vel  [2]int32
    pad  [2]int32
    score [2]uint8
}

func (g *Pong) OnJoin (ctx *soup.RoomCtx, p soup.PlayerID) {}
func (g *Pong) OnResume(ctx *soup.RoomCtx, p soup.PlayerID, gap uint32) {}
func (g *Pong) OnLeave(ctx *soup.RoomCtx, p soup.PlayerID, _ soup.LeaveReason) {
    ctx.End(soup.Result{Aborted: true})
}
func (g *Pong) OnInput(ctx *soup.RoomCtx, p soup.PlayerID, _ soup.InputSeq, b []byte) {
    g.pad[p] += int32(int8(b[0])) * 4          // 直接读复用切片，不持有
}
func (g *Pong) Tick(ctx *soup.RoomCtx, t soup.Tick, _ uint32) soup.Outcome { /* ... */ }
func (g *Pong) EncodeSnapshot(target soup.PlayerID, _ soup.Baseline, out *soup.Buffer) {
    out.PutI16(int16(g.ball[0])); out.PutI16(int16(g.ball[1]))
    out.PutI16(int16(g.pad[0]));  out.PutI16(int16(g.pad[1]))
}
func (g *Pong) EncodeFullState(t soup.PlayerID, out *soup.Buffer) { g.EncodeSnapshot(t, soup.Baseline{}, out) }
func (g *Pong) StateHash() uint64 { /* ... */ }
```

**如果实现 Pong 需要往 SDK 里加任何东西，说明 SDK 把某个游戏假设焊死了，必须改。** Pong 作为 CI 的一部分长期保留。

---

# M11 性能目标与测试

## T0003M11F01 目标值

> ⚠️ 待压测验证，非已测事实。

| 指标 | 目标 |
|---|---|
| SDK 自身单 tick 开销（不含房间逻辑） | p99 < 1 ms |
| **稳态分配** | **0 次/tick**（`testing.AllocsPerRun` 断言） |
| 单房间内存（SDK 部分，不含房间状态） | < 64 KB |
| 单进程房间数 | ≥ 500 房间 × 4 人 @ 20Hz / 4C8G |
| GC 停顿 p99 | < 1 ms |

## T0003M11F02 确定性回放（关键能力）

- 录制：`(seed, config, [(tick, player, inputBytes)])` 落盘
- 重放：离线跑完，逐 tick 比对 `StateHash()`
- **线上任何一次 panic 或异常裁决，都必须能靠这份录像在本地复现。** 没有这个能力，实时游戏的 bug 基本查不动

## T0003M11F03 测试要求

| 类型 | 要求 |
|---|---|
| 单元 | 抖动缓冲、去重、baseline 环形缓冲、降级策略 |
| 分配断言 | `Tick` + `EncodeSnapshot` 零分配，进 CI |
| 竞态 | `-race` 全量跑，房间状态不得跨 goroutine |
| 集成（配合框架的网络模拟） | 见下 |

| 场景 | 期望 |
|---|---|
| 150ms 延迟 + 5% 丢包 | 对局正常完成，`input_starved` 在可接受范围 |
| 突发 2s 全丢 | 恢复后自动关键帧同步，状态一致 |
| 客户端断连 10s 后重连 | 状态完全恢复，房间未收到 `OnLeave` |
| **框架进程 `kill -9` 后重启** | 房间不中断，玩家自动恢复 |
| 逻辑服自身高负载 | 落后补偿生效，不进入死亡螺旋 |

## T0003M11F04 指标

`rooms_active` · `players_online` · `tick_duration_{p50,p99}` · `tick_skipped` · `input_starved` · `snapshot_degraded` · `bytes_out_per_client` · `allocs_per_tick` · `gc_pause_p99` · `inbox_depth`

---

# M12 里程碑

| 里程碑 | 内容 | 出口标准 |
|---|---|---|
| **S1 骨架** | UDS 帧编解码、连接管理、Gatekeeper、房间调度、`Room` 接口 | 与框架 E1 联调：echo 通 |
| **S2 Tick** | 定频循环、落后补偿、Buffer/池、Begin/Commit 发送 | **Pong 跑通**（`M10`） |
| **S3 同步** | 抖动缓冲、输入去重、baseline 环形、关键帧、降级、重连 | 断连 10s 重连状态一致 |
| **S4 硬化** | 确定性回放、零分配断言、metrics、GC 调优 | `M11F01` 全部回填实测 |

**S2 完成即可开始写游戏逻辑。** S3 可与游戏开发并行补齐。

---

# M13 需要游戏侧提供的东西（`T0001SoupOut` 待交付）

SDK 不需要知道游戏内容，但落地需要以下契约：

1. **MsgID 表**（`u12` 空间内分配）与各自走哪条 channel
2. `Input` 的字节布局
3. 快照 / 增量的字节布局与增量策略
4. `TickHz`、`JitterBufferTicks`、`KeyframeIntervalTicks` 取值
5. 单客户端带宽预算

**这五项冻结之前，S1/S2 完全可以先行开发** —— 它们不依赖任何协议内容。
