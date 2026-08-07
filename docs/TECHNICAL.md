# soup-engine 技术实现说明

> 面向**想要改框架/接新传输/排查网络问题**的工程师。规格书见
> [docs/T0002SoupEngine.md](T0002SoupEngine.md)(引擎)与
> [docs/T0003SoupSDKGo.md](T0003SoupSDKGo.md)(Go SDK)。

## 1. 分层架构

```
┌─────────────────────────────────────────────────────────────┐
│ engine.rs      装配、逻辑服链路(热重启)、背压/Overload          │
├─────────────────────────────────────────────────────────────┤
│ session/       会话表(分片哈希)、三次握手、生命周期状态机、限流   │
├─────────────────────────────────────────────────────────────┤
│ reliable/      RTT 估计 · 重传队列 · Ch2 分片重组               │
├─────────────────────────────────────────────────────────────┤
│ transport/     UDP(SO_REUSEPORT) · UDS(长度前缀) · netem      │
├─────────────────────────────────────────────────────────────┤
│ protocol/      UDP 数据报编解码 · UDS 帧编解码(纯函数、无 IO)    │
└─────────────────────────────────────────────────────────────┘
```

核心原则:**protocol 是纯函数**(输入字节 → 输出结构,任何输入不 panic,
fuzz 的硬性前提);传输层不解析业务;会话层不碰 IO。

## 2. 协议格式

### 2.1 UDP 数据报(T0002M03F01)

```
magic u16(0x5A50) | version u8 | flags u8 | conn_id u32 | seq u16 | ack u16 | ack_bits u32
然后连续 N 个帧:  ch u4|msg_id u12 (u16) | len u16 | body
```

- `flags`:bit0 分片 · bit1 纯 ACK · bit2 握手 · bit3 加密(预留) · bit4 带 HMAC
- 数据包在报尾附 **4 字节截断 HMAC**(密钥 = 握手下发的 `session_secret`)
- Ch2 分片帧 body:`group_id u16 | first_seq u16 | frag_no u8 | total u8 | data`

### 2.2 UDS 帧(逻辑服通道,T0002M04F02)

```
len u32(不含本字段,= body 字节数) | type u8 | body
```

body 是类型相关的(见 frame.go / frame.rs 常量表)。Go SDK 侧与 Rust 侧
**共用同一套定义**,`len` 语义必须一致(见下「踩坑 5」)。

## 3. 可靠层设计(最容易写错的部分)

### 3.1 ACK 用「连续交付语义」而不是「最大收到」

发送端重传队列是 FIFO(上限 64):`front` 被确认才弹出。
接收端 ACK 的 `ack` = **已连续交付的最大 seq**(不是最大收到!),`ack_bits`
补充 ack 之后 32 个的乱序收到情况。确认判定:

```
seq ≤ ack                        → 确认(连续交付隐含)
seq > ack 且 (seq-ack) ∈ [1,32]  → 看位图
否则                             → 等 ack 推进(绝不误删)
```

### 3.2 为什么不能「最大收到」

早期实现把 `ack` 当「最大收到」:丢包跨度超过 32 时,`front` 永远在
位图窗口之外 → 队列永不确认 → 死锁 → 全部走 RTO 重传 → 重传风暴。
**根治办法就是 3.1 的连续语义 + 接收端重传帧去重(已交付 seq 直接忽略)。**

### 3.3 RTO

Jacobson/Karels:SRTT/RTTVAR 指数加权,`RTO = SRTT + 4·RTTVAR`,夹取
[50ms, 1000ms]。样本取「本次 on_ack 确认的最后一个条目」的往返时长。

### 3.4 Ch2 分片重组

- 出站:消息体超阈值(MTU - 头 - 分片头)自动切片,每片是**独立可靠消息**
  (独立 seq、独立 ACK/重传),组元数据 `group_id + first_seq + no/total`。
- 入站:按 `group_id` 收片,`first_seq` 决定该组在按序流里的位置
  (解决乱序组与占位条目的竞争),收齐后插入乱序缓存参与按序交付。
- **重传帧去重**:组完成后记住 `group_id`(上限 128),重传的分片帧
  (组已消费)直接忽略,否则会重复交付(见「踩坑 3」)。

## 4. 韧性

- **宽限期**:5s 无包 → `Grace` 状态(会话不删、不下行);20s 无包 →
  `Close` 并通知逻辑服 `SessionClose`。
- **NAT 漂移**:按 `conn_id` 续接,更新 `peer` 即可,不发新 `SessionOpen`。
- **逻辑服热重启**:连接循环自动重连;重连后补发 `EngineHello` +
  每个存活会话的 `SessionResume(gap_ms)`,逻辑服据此推全量状态。
- **背压**:上行队列满 → 丢 Ch0/1(可丢),Ch2/3 满则触发 `Overload` 通知
  (永远不丢可靠事件,逻辑服据此熔断)。

## 5. 安全与观测

- HMAC:握手下发 8B `session_secret`,数据包附 4B 截断 HMAC;伪造包
  `pkt_bad` 计数并静默丢弃。
- 限流:逻辑服可 `SetBudget(sess_id, kbps)`;超限的下行整包丢弃并计数。
- metrics:原子计数器,`sessions().stats.snapshot()` 取快照。
- fuzz:数据报 / 帧流 / 会话整链路三条确定性基线,任何输入不 panic。

---

## 6. 本轮踩坑记录(含修复)

> 每一条都对应一次真实调试,按「症状 → 根因 → 修复」记录,供后来者避免
> 重走弯路。

### 踩坑 1:ACK 窗口死锁与重传风暴

**症状**:150ms 延迟 + 5% 丢包下 50 条 Ch2 消息 15s 收不齐;引擎
`retransmits` 数千次。

**根因**:ACK 用「最大收到」语义,丢包跨度 > 32 时重传队列 front 永远
确认不了,只能靠 RTO 反复重传;而 RTO 因无 ACK 样本永远停在初值。

**修复**:3.1 的连续交付语义 + 接收端按已交付 seq 去重。
**验证**:8 客户端 × 100pps 压测 3815 条全部确认,`retransmits=0`。

### 踩坑 2:SO_REUSEPORT 发送 socket 吞包(最隐蔽)

**症状**:本机回环压测 echo 全部超时;引擎 `pkt_in` 有握手与上行,但
客户端回包(ACK)一个都到不了;引擎「纯 ACK」计数为 0。

**根因**:发送 socket 用 `SO_REUSEPORT` 绑了引擎端口,但从不 recv。
内核按四元组把入站包**随机分发**给同端口的所有 socket —— 分到发送
socket 的包全部堆积在它的接收缓冲里丢失(实测 ACK 100% 丢)。

**修复**:**收发合一** —— 发送复用接收端同一批 SO_REUSEPORT socket
(源端口 = 监听端口,所有 socket 都参与 recv)。发送轮询选择 socket。
**验证**:修复后 `acks_seen = 发送数`,零重传零丢包。

> 附带教训:第一次「修复」把发送 socket 改绑随机端口,ACK 反而发往随机
> 端口(客户端回包目标是收到包的源地址)——更糟。**发送源端口必须等于
> 监听端口**。

### 踩坑 3:分片帧被「超窗检查」误伤

**症状**:可靠通道在丢包恢复后出现**重复交付**与**永久空洞**。

**根因**:超窗检查(防缓存膨胀)把「seq < 已推进位置且不在乱序缓存」当
重复包丢弃。但**分片帧的 seq 在组装器中消费,永远不在乱序缓存** ——
重传的分片帧被误判为超窗旧包丢弃;而组完成后重传帧再次到达又导致组被
重建、消息重复交付。

**修复**:超窗检查仅对非分片帧生效;分片帧由「已完成组集合」去重。
另:超窗判断用 `seq < delivered_max`(严格小于),否则首帧 seq=0 会被
误杀。

### 踩坑 4:连续 ACK 的 ack 初值

**症状**:引擎 on_ack 只在 ack=0 命中一次,之后全未命中。

**根因**:压测客户端 `ack_pos` 初始化为 0,收到第一个包(seq=0)后检查
`ack_pos+1` 是否为 1 —— 永远停在 0。

**修复**:`ack_pos` 初始化为 -1(u16::MAX),收到 0 才推进到 0。

### 踩坑 5:Go SDK 出站帧 len 多算 1 字节

**症状**:Go 逻辑服 echo 的 payload 带 `\r\x00` 前缀与残留后缀。

**根因**:SDK 写 `len` 字段时把 type 的 1 字节也算进去(`off+len-4`),
与「len = body 字节数(不含 type)」的协议定义不一致,读端把 type 前
1 字节(恰好是 payload 长度)读进 payload。

**修复**:`len = off+len-5`;两端统一「len 不含 type」语义。

### 踩坑 6:UDS 半包分帧会吞数据

**症状**:Go SDK 粘包/半包测试全挂。

**根因**:`io.ReadFull` 在不足一帧时**消费已读字节**,再次 ReadFrame
从半帧中间读起,把数据当帧头解析。

**修复**:改用累积缓冲 + `TryDecodeFrame`:不足一帧返回 `ErrNeedMore`
且**不消费**,凑齐才整帧取出(Rust 侧 BytesMut 同款语义)。

### 踩坑 7:watch channel sender 被提前 drop

**症状**:engineload 引擎启动 22ms 即退出,压测全丢。

**根因**:`let _ = tx` 把 `watch::Sender` 丢弃,`rx.changed()` 立即返回,
`run_with_shutdown` 当收到关闭信号退出。

**修复**:持有 sender 到压测结束,结束时 `tx.send(true)` 显式关停。

---

## 7. 压测基准(本机 darwin/arm64,release)

| 场景 | 吞吐 | RTT p50/p99 | 未确认 | 重传 |
|------|------|-------------|--------|------|
| 8 客户端 × 100pps × 6s | 635 msg/s | 0.9 / 3.8 ms | 0 | 0 |
| 20 客户端 × 200pps × 5s | 2832 msg/s | 1.0 / 2.1 ms | 0 | 0 |

重跑:`cargo run --release --example engineload -- --clients N --pps M --duration T`

---

## 8. 跨语言联调:真实 Rust 引擎 ⇄ Go SDK 逻辑服

**工具**:`examples/interop.rs`(引擎只做网络 + 客户端)+ `soup-sdk-go/cmd/echologic`(Go 逻辑服,echo 房间)。

```bash
cd soup-sdk-go && go run ./cmd/echologic --socket /tmp/soup-interop.sock
cargo run --release --example interop -- --uds /tmp/soup-interop.sock --count 5
# ✓ 引擎已连上 Go 逻辑服 → 握手 → 5 条 ping 经 Go 逻辑服 echo 往返成功
```

联调暴露并修复的 3 个真实缺陷(纯单元/单侧测试发现不了):

### 踩坑 8:Go SDK 是拨号端,与架构(引擎主动重连)矛盾

**症状**:引擎 connect、SDK 也 Dial —— 没有监听端,谁也连不上谁。
**根因**:T0002M04F06 要求引擎热重启后主动重连逻辑服,逻辑服必须是 **UDS 监听端**;
但 `engineConn` 实现成了客户端。
**修复**:`conn.go` 改为 bind+listen+accept(逻辑服为服务端,引擎按退避重连过来);
`echo_test` 的模拟引擎改为 dial。

### 踩坑 9:引擎进程死亡,Go SDK 会话表残留

**症状**:引擎(kill/崩溃)后新引擎的 SessionOpen(同 sess_id)被 SDK 当「重复」忽略,
玩家永不加入。
**根因**:引擎进程死亡不会发 SessionClose,SDK `sessions` 表残留旧会话。
**修复**:`engineConn` 增加 `onDead` 回调 —— 连接断开时 SDK 清理全部会话
并通知房间 OnLeave(`LeaveDisconnect`),引擎重启后新会话正常加入。

### 踩坑 10:SnapshotHz=0 被兜底成每 tick 快照

**症状**:想关快照做对照实验,反而每 tick 发快照(10Hz → 20Hz)。
**根因**:`snapshotHz()` 的 `if hz < 1 { hz = 1 }` 把 0 兜底成 1。
**修复**:`SnapshotHz <= 0` 直接返回 0(禁用),不再兜底。
