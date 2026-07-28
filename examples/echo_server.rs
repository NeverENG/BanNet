//! echo_server —— 用 BanNet 写的服务端示例(我们的北极星 🌟)
//!
//! 运行方式(框架实现后):
//!   cargo run --example echo_server
//!
//! ── 目标形态(北极星,阶段推进后逐步能写)──
//! ```ignore
//! use bannet::Server;
//!
//! #[tokio::main]
//! async fn main() -> bannet::Result<()> {
//!     let mut server = Server::builder("127.0.0.1:8999")
//!         .workers(8)
//!         .max_conns(10_000)
//!         .build()?;
//!
//!     server.on_conn_start(|conn| println!("上线: {}", conn.id()));
//!
//!     server.on(1, |req| async move {
//!         println!("收到 msgID={}, data={:?}", req.id(), req.data());
//!         req.reply(b"pong").await
//!     });
//!
//!     server.run().await
//! }
//! ```

// 阶段 0:裸字节 echo server。跑起来后,你发什么它原样回什么。
use bannet::Server;

#[tokio::main]
async fn main() {
    // new 现在要 3 个参数:地址、最大连接数、worker 数(后两个暂时没用上)
    let server = Server::new("127.0.0.1:8999".to_string())
        .await
        .expect("Failed to create server");
    _ = server.run().await;
}
