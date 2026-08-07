# 🍲 soup-engine

**给实时对战游戏准备的 UDP 服务器。网络的事,你一行都不用写。**

你在做格斗、射击、MOBA、竞速、体育对战?最头疼的往往不是玩法,是网络:

- 用 **TCP** 做对战 → 一丢包全卡住(队头阻塞),延迟抖动直接劝退玩家
- 裸写 **UDP** → 握手、丢包重传、乱序、断线重连、防作弊……每个都是几个月的坑

soup-engine 把这些全部做完。你只写游戏规则(Go),**一个房间一个 tick 循环**,客户端连上就连上了,断了自动续,逻辑服崩了重启都不掉线。

```
┌──────────┐   UDP    ┌───────────────────┐   Unix Socket   ┌───────────────┐
│ 游戏客户端 │ ⇄──────⇄ │ soup-engine (Rust) │ ⇄─────────────⇄ │  逻辑服 (Go)  │
└──────────┘          └───────────────────┘                 └───────────────┘
   卡不卡、断不断           会话 / 可靠 / 重连                你只写房间规则
   全是框架的事             防作弊 / 压测,全是框架的事          其他都是 SDK 的事
```

---

## 什么时候用它

| 你的情况 | soup-engine 给你 |
|---|---|
| 做**实时对战**游戏,延迟就是体验 | 4 通道语义:位置/朝向走直投、关键事件走可靠 —— 丢包时只有重要的重传 |
| 不想从零写 UDP 可靠层 | 连续 ACK、RTO 重传、分片重组、乱序整理,全内置 |
| 要**多游戏复用**一套服务器 | 引擎与 SDK 零游戏依赖,换玩法 = 换一个 `Room` 实现 |
| 客户端会**断线/换网络**(移动场景) | 宽限期 + NAT 漂移续接,断 20 秒内回来不掉线 |
| 逻辑服要**热更新/崩溃自愈** | 引擎自动重连,重连后补发全量状态,玩家无感 |

## 为什么不是裸 UDP / TCP / 商业方案

| 方案 | 问题 |
|---|---|
| 裸 UDP | 握手、防放大、重传、防伪造、断线判定、压测工具 —— 全要自己造,够写半年 |
| TCP | 队头阻塞:一帧丢了,后面的全等它,游戏直接「卡死」 |
| 商业实时后端 | 闭源、按量计费、绑平台;改不了传输细节,也带不走 |
| **soup-engine** | 开源 Rust 引擎 + Go SDK,本地可跑可改可压测,不绑定任何平台 |

---

## 性能(本机 darwin/arm64,`--release`)

| 场景 | 数值 |
|---|---|
| 吞吐(20 客户端 × 200pps) | **2794 msg/s**,零丢包、**零重传** |
| RTT | p50 **1.1 ms**,p99 **2.4 ms** |
| 连接建立(200 并发握手) | 平均 **3.2 ms**,p99 3.8 ms,全部建成 |
| 断线回收(50 会话静默) | **5.9 s**(窗口随 RTT 自适应;无采样时保守 24.8 s) |
| 逻辑服 kill -9 后恢复 | **7 s 内自动重连 + 状态补发**,客户端全程不掉线 |
| 会话状态 | 每个玩家状态零分配、无锁(Go 一房间一 goroutine) |

完整压测与复现:`docs/BENCHMARK.md`。

---

## 30 秒上手

**1. 启动引擎**(自带压测/echo 逻辑服):

```bash
cargo run --release --example engineload -- --clients 20 --pps 200 --duration 4
# 输出:2794 msg/s,RTT p99 2.4ms,0 未确认 0 重传
```

**2. 写一个逻辑服(Go)—— 20 行:**

```go
package main

import "github.com/NeverENG/BanNet/soup-sdk-go"

type room struct{} // 实现 Room 接口,就是你的整个游戏

func (r *room) OnInput(ctx *soup.RoomCtx, p soup.PlayerID, _ soup.InputSeq, payload []byte) {
    b := ctx.BeginSend(p, soup.ChReliableOrdered, 1) // 发回给 p
    b.PutU16(uint16(p))
    b.PutBytes(payload) // 直接读复用缓冲,零拷贝
    ctx.Commit(b)       // tick 末尾统一发
}
func (r *room) OnJoin(ctx *soup.RoomCtx, p soup.PlayerID)                    {}
func (r *room) OnResume(ctx *soup.RoomCtx, p soup.PlayerID, gap uint32)      {}
func (r *room) OnLeave(ctx *soup.RoomCtx, p soup.PlayerID, why soup.LeaveReason) {}
func (r *room) Tick(ctx *soup.RoomCtx, t soup.Tick, dt uint32) soup.Outcome  { return soup.Continue }
func (r *room) EncodeSnapshot(t soup.PlayerID, b soup.Baseline, out *soup.Buffer) {}
func (r *room) EncodeFullState(t soup.PlayerID, out *soup.Buffer)            {}
func (r *room) StateHash() uint64                                            { return 0 }

func main() {
    srv := soup.NewServer(soup.Config{
        EngineSocket: "/tmp/soup.sock", // 引擎监听的 UDS 路径
        TickHz:       20,
        Gatekeeper: soup.GatekeeperFuncs{
            AuthenticateFn: func(token []byte, addr string) *soup.PlayerID { p := soup.PlayerID(1); return &p },
            RouteFn:        func(p soup.PlayerID, h soup.JoinHint) soup.RoomRoute { return soup.RoomRoute{Action: soup.RouteJoin, RoomID: 1} },
            NewRoomFn:      func(id soup.RoomID, cfg any, players []soup.PlayerID, seed uint64) soup.Room { return &room{} },
        },
    })
    srv.Run() // 阻塞;引擎连上来即开始干活
}
```

**3. 客户端连上** —— 一个 UDP 三次握手拿 `conn_id` + `session_secret`,之后
发消息按 `ch` 选语义(0=直投 / 1=有序 / 2=可靠有序),引擎替你把剩下的
(ACK、重传、分片、HMAC)全做了。

---

## 完整案例:Pong

`soup-sdk-go/pong_test.go` 里有一个 **80 行**的双人 Pong 逻辑服:挡板移动、
球弹跳、计分、快照、状态哈希 —— **没有碰网络任何一行**。它同时是:

- **零分配验收**:稳态 `Tick + EncodeSnapshot` 每次 0 次内存分配(CI 断言)
- **确定性回放验收**:录输入 → 离线重放 → `StateHash` 完全一致

```bash
cd soup-sdk-go && go test -race ./...   # Pong / 回放 / 零分配 / echo 全绿
```

---

## 特性一览

**引擎(Rust)**
- 🛡️ 三次握手防放大 · 每包 4 字节 HMAC 防伪造 · 逐会话带宽限流
- 🔄 可靠层:连续 ACK · RTO 重传 · Ch2 分片重组(大消息自动切包)· 心跳按需
- 🧵 SO_REUSEPORT 多 socket 收发(内核负载均衡,收发同池)
- 📶 NAT 漂移续接(换 IP/端口不掉线)· RTT 自适应断线窗口(低延迟快回收)
- ♻️ 逻辑服热重启:指数退避重连 + `SessionResume` 补全量,客户端无感
- 📊 metrics 快照 · fuzz 基线(畸形包不 panic)· engineload/bench 压测工具

**Go SDK**
- 🏠 一房间一 goroutine,状态独占无锁 · 固定步长 tick + 落后补偿(防死亡螺旋)
- 🎛️ `Gatekeeper` 三回调:鉴权 → 路由 → 建房
- 📦 抖动缓冲 + 输入去重 · baseline 环形快照(增量/全量自动选择)· 带宽降级
- 🧪 确定性 PRNG · 池化 Buffer(零分配)· 回放录制(`Replay()` 离线复现)

---

## 文档 / 目录

```
src/            Rust 引擎:protocol(编解码) / transport(UDP·UDS) / session(会话)
                / reliable(可靠层) / engine.rs(装配)
soup-sdk-go/    Go 逻辑服 SDK
examples/       engineload(吞吐压测)· bench(生命周期压测)
docs/           规格书(T0002 引擎 / T0003 SDK)· 实现说明 · Benchmark
```

- 📖 实现与踩坑:[docs/TECHNICAL.md](docs/TECHNICAL.md)
- 📊 压测数值与复现:[docs/BENCHMARK.md](docs/BENCHMARK.md)
- 📐 规格书:[T0002SoupEngine.md](docs/T0002SoupEngine.md) · [T0003SoupSDKGo.md](docs/T0003SoupSDKGo.md)

## 许可

MIT(待定)。当前为个人学习项目,欢迎拿去改、拿去用。
