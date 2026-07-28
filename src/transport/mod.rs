//! # 传输层
//!
//! 负责真正的网络 IO 与连接的生命周期:把一条 TcpStream 包成 Connection,
//! 跑读/写两条异步流水线;并用 Manager 管理所有活跃连接(那个 "N")。
//!
//! - [`connection`] 单个连接:读/写流水线
//! - [`handler`]    Handler trait:框架 <-> 用户业务逻辑的契约
//! - [`req`]        Request:交给 handler 的请求上下文(数据 + 回包通道)
//! - [`manager`]    连接管理器:增删查群发

mod connection;
mod handler;
mod manager;
mod req;

// ── 本层对外暴露的 API ──
// 阶段 0:Server 需要用到 Connection,先在 crate 内导出。
pub(crate) use connection::Connection;

// Handler / Request 是用户直接会碰到的类型,由 lib.rs 再往上提一层。
pub use handler::{BoxFuture, EchoHandler, Handler, SharedHandler};
pub use req::Request;

// TODO(阶段 3):Router(按 msgID 分发)本质就是「一个内部持有 HashMap 的
// Handler」,到时候 `impl Handler for Router` 就能直接接进来,
// Connection / Server 一行都不用改。
//
// TODO(阶段 4):
// pub use manager::ConnManager;
