# soup-engine —— 通用实时对战服务器框架

**用一句话说**:给实时对战游戏(射击、格斗、MOBA、竞速、动作类)准备的
**UDP 网络框架**。游戏规则用 **Go** 写,网络细节(Rust 引擎)你完全不用碰;
换一个游戏,框架原样复用,只重写游戏规则。

```
┌──────────┐   UDP    ┌────────────────────┐   Unix Socket    ┌──────────────┐
│ 游戏客户端 │ ⇄──────⇄ │ soup-engine (Rust) │ ⇄──────────────⇄ │ 逻辑服 (Go)  │
└──────────┘          └────────────────────┘                  └──────────────┘
  连不连得稳             会话/可靠/重连/防作弊/压测               只写房间规则
  都是框架的事           全是框架的事                             其他都是 SDK 的事
```

- **客户端 ⇄ 引擎(UDP)**:引擎替你处理握手、心跳、NAT 重绑定、丢包重传、
  乱序、分片重组、HMAC 防伪造、按会话限流 —— 客户端看到的永远是**有序可靠
  的消息流**(需要时)。
- **引擎 ⇄ 逻辑服(UDS)**:一条 Unix Socket 通道,Go 逻辑服注册几个回调
  (鉴权、路由、建房),剩下的是每房间一个 goroutine 的 tick 循环。
- **换游戏**:引擎与 SDK **零游戏依赖**(CI 检查 `cargo tree -p soup-engine`
  不含任何业务 crate)。新游戏 = 新写 `Room` 接口实现。

---

## 为什么用 UDP

实时对战的核心诉求是**低延迟**。TCP 的丢包重传会阻塞后续所有数据(队头
阻塞),延迟抖动时游戏直接「卡死」。UDP + 应用层可靠(本项目)把可靠性
做成**可选、可分层**:

| 通道 | 语义 | 典型用途 |
|------|------|----------|
| Ch0 | 直投(不保证顺序,丢就丢) | 实时位置、朝向(每秒 20~60 次,丢一帧无所谓) |
| Ch1 | 有序(只留最新) | 记分板、快速状态覆盖 |
| Ch2 | **可靠有序 + 分片重组** | 关键事件:开火、命中、物品拾取、聊天 |
| Ch3 | 可靠无序 | 需要送达但不要求顺序的次要数据 |

丢包时只有 Ch2/3 走重传,Ch0/1 继续飞 —— 这就是「卡顿」与「平滑」的差别。

---

## 特性(按规格书 docs/T0002、docs/T0003 实现)

**引擎(Rust)**
- 三次握手防放大(服务端回包 ≤ 收包;v2 起可换密钥)
- SO_REUSEPORT 多 socket 收发(内核负载均衡,收发合一)
- 会话表:分片哈希、5s 宽限期 / 20s 重连超时、NAT 漂移续接(换 IP/端口不掉线)
- 可靠层:连续 ACK 语义、Jacobson/Karels RTO(夹 [50ms, 1000ms])、
  每通道重传队列、Ch2 分片重组(大消息自动切包)
- 逻辑服热重启:掉线自动重连,重连后补发 `SessionResume`,客户端全程不掉线
- 上行队列背压:丢 Ch0/1 保 Ch2/3,队列满通知 `Overload`
- 每包 4 字节截断 HMAC 防伪造;逐会话带宽限流(`SetBudget`);metrics 快照
- fuzz 基线(任何畸形包不 panic);engineload 压测工具

**Go SDK**
- UDS 帧编解码(累积缓冲分帧,半包/粘包安全)、自动重连
- `Gatekeeper` 三回调:鉴权 → 路由 → 建房
- 一房间一 goroutine + 固定步长 tick + 落后补偿(≤3 tick 追帧,更多跳时间)
- 池化 Buffer 编码器(PutU8/U16/U32/I16/Varint/Bytes)、确定性 PRNG
- 每房间状态独占无锁,入站有界 chan 满丢最旧

---

## 快速开始

```bash
# 1. 引擎 + 测试
cargo test            # 39 单测 + 端到端(echo/lifecycle/reliable/fuzz)

# 2. Go SDK 测试
cd soup-sdk-go && go test -race ./...

# 3. 压测(引擎 + 内置 echo 逻辑服)
cargo run --release --example engineload -- --clients 20 --pps 200 --duration 5
# 示例输出:吞吐 2832 msg/s,RTT p99 2.1ms,零丢包零重传
```

### 写一个逻辑服(Go)

```go
type 我的房间 struct{} // 实现 Room 接口即可

func (r *我的房间) OnInput(ctx *soup.RoomCtx, p soup.PlayerID, seq soup.InputSeq, payload []byte) {
    b := ctx.BeginSend(p, soup.ChReliableOrdered, 1)
    b.PutU16(uint16(p))
    b.PutBytes(payload)
    ctx.Commit(b) // 本 tick 末尾统一发
}

func main() {
    srv := soup.NewServer(soup.Config{SocketPath: "/tmp/soup.sock",
        Gatekeeper: soup.GatekeeperFuncs{
            Authenticate: func(token []byte, addr string) *soup.PlayerID { ... },
            Route:        func(p soup.PlayerID) string { return "房间A" },
            NewRoom:      func(id string, seed uint64) soup.Room { return &我的房间{} },
        }})
    srv.Run() // 阻塞;引擎连上来即开始干活
}
```

---

## 目录结构

```
src/
├── protocol/    # UDP 数据报 + UDS 帧编解码(纯函数,无 IO,fuzz 前提)
├── transport/   # UDP(SO_REUSEPORT 收发合一)、UDS(长度前缀分帧)、网络模拟器
├── session/     # 会话表、握手、生命周期(宽限期/NAT/超时)、逐会话限流
├── reliable/    # RTT 估计、重传队列、Ch2 分片重组
├── engine.rs    # 门面:装配一切 + 逻辑服链路(热重启/背压)
├── buffer.rs    # 池化缓冲
└── stats.rs     # 原子指标
soup-sdk-go/     # Go 逻辑服 SDK(规格书 docs/T0003)
examples/        # engineload 压测工具(内置 echo 逻辑服)
tests/           # echo/lifecycle/reliable/fuzz 端到端
docs/            # 规格书与实现说明
```

详细设计与本轮踩坑记录见 **[docs/TECHNICAL.md](docs/TECHNICAL.md)**。
