# soup-engine Benchmark

> 本机基准(darwin/arm64,`--release`)。重跑:
> `cargo run --release --example bench -- --scenario {connect|recycle|restart}`
> 吞吐基线:`cargo run --release --example engineload -- --clients N --pps M --duration T`

## 场景与指标

| 场景 | 工具 | 指标 |
|------|------|------|
| 吞吐 / RTT | engineload | msg/s、RTT p50/p95/p99、丢包、重传 |
| 连接建立 | bench connect | 并发握手耗时(p50/p99)、连接速率 |
| 断线回收 | bench recycle | 会话静默后全部 SessionClose 的耗时(生命周期窗口) |
| 逻辑服重启 | bench restart | kill 逻辑服后引擎重连成功耗时(指数退避) |

## 数值(2026-08-08,本机)

### 吞吐 / RTT(engineload,echo 全链路)

| 客户端 × pps | 吞吐 | RTT p50 / p99 | 未确认 | 重传 |
|------|------|---------------|--------|------|
| 8 × 100 × 6s | 635 msg/s | 0.9 / 3.8 ms | 0 | 0 |
| 20 × 200 × 5s | 2832 msg/s | 1.0 / 2.1 ms | 0 | 0 |

### 连接建立(bench connect)

200 并发握手:
- 平均建连耗时 1.69 ms,p50 1.55 ms,p99 2.85 ms
- 200 会话全部建立(连接风暴下 UDP 偶发丢包由握手重试吸收)

### 断线回收(bench recycle,50 会话)

**动态超时(默认,有 RTT 采样)**:
- 建连后短交互产生 RTT 采样 → 回收窗口按 RTT 收紧到
  idle 1.5s + reconnect 5s
- **50 会话全部回收耗时 5.89 s**(空闲会话快速回收,资源友好)

**无采样(客户端全程静默)**:
- 引擎无 RTT 样本 → 保守取窗口上限(20s)
- **回收耗时 24.8 s**(合理:未知网络质量不激进回收,防误杀)

> 对比:改造前固定 5s + 20s,无论 RTT 高低一律 25s 回收。
> 动态超时(ENet/QUIC 调研)让**低延迟环境快 4 倍回收**、高延迟环境不误杀。

### 逻辑服重启(bench restart)

kill -9 逻辑服 + 3s 停机后重启:
- 引擎断线感知(读循环 EOF)→ 指数退避重试(1s→2s→4s…封顶 8s)
- **重连成功耗时 7.02 s**(含 3s 停机;退避路径 1+2+4s 验证)
- 会话在引擎侧存活(宽限期),重连后补发 `SessionResume(gap_ms)`,
  客户端全程不掉线

> 对比:改造前固定 1s 重连 —— 逻辑服反复崩溃时会造成重连风暴。
> 指数退避(yojimbo `connecting_after_disconnect` 调研)在崩溃-重启循环中
> 自动拉开间隔,防止引擎空转。

## 复现

```bash
# 生命周期三场景
cargo run --release --example bench -- --scenario connect --clients 200
cargo run --release --example bench -- --scenario recycle --clients 50
cargo run --release --example bench -- --scenario restart
# 吞吐基线
cargo run --release --example engineload -- --clients 20 --pps 200 --duration 5
```
