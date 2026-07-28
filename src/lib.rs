//! # BanNet
//!
//! 一个极致轻量的异步 TCP 框架(学习项目),灵感来自 ZINX。
//! 本质:1 个 Server 管理 N 个 Connection,按 msgID 把消息路由给业务 handler。
//!
//! ## 目标产品形态(北极星)
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
//!     server.on(1, |req| async move {
//!         req.reply(b"pong").await   // 回包,msgID 自动沿用
//!     });
//!
//!     server.run().await
//! }
//! ```
//!
//! ## 分层架构
//! - [`protocol`]  协议层:消息本体 + TLV 编解码(不碰网络)
//! - [`transport`] 传输层:连接读/写流水线 + 连接管理
//! - [`routing`]   路由层:Request 上下文 + Router 分发
//! - [`server`]    门面层:把上面三层组装起来,对用户暴露

// ── 子系统(层)声明 ──
// 每个 `mod` 对应 src/ 下的一个目录(protocol/ transport/ routing/),
// 目录里的 mod.rs 是该层入口。server 是单文件模块。
mod error;
mod protocol;
mod routing;
mod server;
mod transport;
// ── crate 根公开 API(重导出)──
// 逐阶段打开。阶段 0:先把 Server 摆上货架,用户 `use bannet::Server;` 即可。
pub use server::Server;

// TODO(后续阶段逐步打开):
// pub use protocol::Message;
// pub use routing::{Request, Router};
//pub use transport::Connection;
