# BanNet 设计文档

> 一个极致轻量的**异步 TCP 框架**,学习项目。灵感来自 ZINX。
> 本质:**1 个 Server 管理 N 个 Connection**,按 msgID 把消息路由给业务 handler。

## 产品哲学(所有细节决策的裁判)

> 一句话:**替用户吞下复杂度,给用户留下选择权,对世界划清边界。**

1. **用户写业务,不写网络** —— API 决策的最高裁判。用户碰到 `TcpStream` = 失败。
2. **内部越脏,外部越爽** —— 复杂度是框架该吞的,换用户一行简洁调用。
3. **零配置可跑,旋钮渐进** —— `Server::new(addr)` 即可启动;配置可选不必填。
4. **保序 > 均衡** —— 长连接场景,同连接消息保序压倒负载均匀 → connID 取模。
5. **有界 > 无限** —— 连接数/队列/并发都有上限,资源可预测优于跑得快然后崩。
6. **错误显式,绝不静默 panic** —— handler 返回 `Result`,框架接管,不甩锅。
7. **极致轻量 = 敢做减法** —— "不做清单"和"做什么"一样重要。

### 工程纪律:最后责任时刻
细节在"正在写那一层代码"时决定,而非提前拍脑袋。Config 字段、错误类型、buffer
大小……都等到写到那一层、或压测有数据时再定。提前定 = 猜 = 过度设计。

## 技术栈
| 项 | 选择 |
|---|---|
| 运行时 | tokio(async/await) |
| 协议 | 自定义 TLV:`dataLen(u32 LE) + msgID(u32 LE) + data` |
| 错误处理 | 先 `Box<dyn Error>`,后期 `thiserror` |
| 序列化 | v1 不引入,裸字节 |

## 产品形态定义

### 一句话定位
> **一个极致轻量的异步 TCP 应用服务框架**:让你只写"收到某类消息该干什么",
> 把连接、并发、协议、资源全接管掉。用户在写业务 handler,而不是网络程序。
>
> 验收标准:用户全程不碰 `TcpStream`、不写 `spawn`、不想粘包。

### 目标用户 & 场景
长连接服务:IM、游戏服、IoT 网关、推送。共同点 = 海量长连接 + 自定义二进制协议
+ 按消息类型分发 + 同一连接消息需保序(这解释了并发调度为何选 connID 取模)。

### 核心能力(对外承诺的 5 件事)
| # | 能力 | 用户因此不用操心 |
|---|---|---|
| 1 | 消息化:自动 TLV 拆包 → 完整 `Message` | 粘包/半包 |
| 2 | 路由:按 msgID 投递到 handler | 一坨 match/if |
| 3 | 可控并发:Worker 池 + connID 取模 | spawn 失控、消息乱序、无背压 |
| 4 | 连接管理:注册表 + 群发 + 生命周期钩子 | 手动维护连接集合 |
| 5 | 资源保护:连接数上限(信号量)+ Buffer 复用 | 连接风暴、分配抖动 |

### API 决策
- **handler 形态**:闭包/函数式(axum 风),`server.on(id, |req| async {...})`。无样板。
- **Config 形态**:Builder 链式,`Server::builder(addr).workers(8).build()?`。零配置可启动、构造即校验。
- **Request 便捷方法**:`req.id()` / `req.data()` / `req.reply(data)`(回包自动沿用 msgID)/ `req.conn()`(高级场景)。handler 返回 `Result<()>`,框架统一处理错误。

### 明确不做(守住"极致轻量")
❌ TLS/加密　❌ 序列化框架(只给裸 `&[u8]`)　❌ 服务发现/集群/RPC　❌ HTTP/WebSocket

### 配置旋钮
`workers`(并发度)、`max_conns`(连接上限)、`queue_cap`(背压阈值)、`buffer_size`。

## 🌟 北极星:目标产品形态(验收标准)

用户用 BanNet 写一个服务端的**全部代码**:

```rust
use bannet::Server;

#[tokio::main]
async fn main() -> bannet::Result<()> {
    let mut server = Server::builder("127.0.0.1:8999")
        .workers(8)
        .max_conns(10_000)
        .build()?;

    server.on_conn_start(|conn| println!("上线: {}", conn.id()));
    server.on_conn_stop (|conn| println!("下线: {}", conn.id()));

    server.on(1, |req| async move {
        println!("收到 msgID={}, data={:?}", req.id(), req.data());
        req.reply(b"pong").await          // 回包,msgID 自动沿用
    });

    server.run().await
}
```

### 运行时形态(一条消息的一生)
```
 client ─TCP─►  ┌─────────────── BanNet Server ───────────────┐
                │ ① Semaphore 门卫:超 max_conns 则排队/拒绝     │
                │      │ 放行 → 分配 connID、注册进 ConnManager   │
                │      ▼                                        │
                │  Connection#7 Reader task ─┐  ← Buffer 池      │
                │   read → TLV 拆包 → Message │                  │
                │        → 包成 Request        │                  │
                │  ────────┬──────────────────┘                  │
                │          │ 按 connID % N 投递(保序)             │
                │          ▼                                     │
                │  Worker 池(N个,各带队列)  ← 背压在这里          │
                │   worker 取 Request → 跑用户 handler            │
                │  ────────┬───────────────────                  │
                │          │ handler 调 req.reply()/conn.send()   │
                │          ▼                                     │
                │  Connection#7 Writer task ─┐  ← Buffer 池       │
                │   mpsc 收 → TLV 封包 → 写回 │                    │
                │  ───────────────────────────┘                  │
                │  ConnManager:全局连接表 · 群发 · 钩子           │
                └─────────────────────────────────────────────────┘
```

## 分层结构(src/)

```
src/
├── lib.rs              库根:声明各层 + 对外重导出 API
├── protocol/          【协议层】数据结构 + TLV 编解码(不碰网络)
│   ├── mod.rs
│   ├── message.rs      消息本体 { id, data }
│   └── datapack.rs     TLV 封包/拆包(解决粘包)
├── transport/         【传输层】网络 IO + 连接生命周期
│   ├── mod.rs
│   ├── connection.rs   单连接:读/写两条异步流水线
│   └── manager.rs      连接管理器:管理 N 个连接
├── routing/           【路由层】消息 → 业务逻辑的分发
│   ├── mod.rs
│   ├── request.rs      请求上下文 { conn, message }
│   └── router.rs       Router trait + 路由表
└── server.rs          【门面层】把三层组装,对用户暴露

examples/               验证目录(cargo run --example xxx)
├── echo_server.rs      用框架写的服务端(北极星)
└── echo_client.rs      按 TLV 协议手写的测试客户端
```

| 层 | ZINX 对应 |
|---|---|
| `protocol` | Message / DataPack |
| `transport` | Connection / ConnManager |
| `routing` | Router / Request / MsgHandler |
| `server` | Server |

## 学习路线(阶段 = 成就点)
- **阶段 0 — 骨架**:cargo 结构、模块系统、跑通裸字节 echo。✅ 结构已搭好
- **阶段 1 — Message + DataPack**:TLV 拆包,`read_exact` 干掉粘包。
- **阶段 2 — Connection**:mpsc 拆读/写协程(tokio 并发 + 所有权大关)。
- **阶段 3 — 闭包 handler + 路由**:把 `server.on(id, |req| async {...})` 塞进
  HashMap —— `Fn` trait / `Pin<Box<dyn Future>>` / `Send+Sync`(异步深水区)。
- **阶段 4 — Server Builder + Config**:Builder 模式、`Result`/错误类型、构造校验。
- **阶段 5 — Worker 池 + 背压**:N worker + connID 取模分发、bounded mpsc 背压、保序。
- **阶段 6 — ConnManager + Hooks + 限流**:连接注册表、群发、钩子、Semaphore 限连接数。
- **阶段 7(进阶)**:Buffer 池、优雅关停、`thiserror`、性能压测。

## Rust ↔ Go(ZINX)概念对照
| ZINX (Go) | BanNet (Rust) | 学习点 |
|---|---|---|
| goroutine | `tokio::spawn` | task / Future |
| chan | `tokio::sync::mpsc` | channel + 所有权移动 |
| `map+锁` | `Arc<Mutex<HashMap>>` / DashMap | 共享可变状态 |
| interface | `trait` + `dyn`/泛型 | trait object |
| `func` 回调 | 闭包 + `Box<dyn Fn -> Pin<Box<dyn Future>>>` | Fn trait / Pin / async 闭包 |
| worker 池 + `connID%N` | bounded `mpsc` + `Semaphore` | 背压 / 限流 |
