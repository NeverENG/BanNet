//! # 传输层
//!
//! 负责真正的网络 IO 与连接的生命周期:把一条 TcpStream 包成 Connection,
//! 跑读/写两条异步流水线;并用 Manager 管理所有活跃连接(那个 "N")。
//!
//! - [`connection`] 单个连接:读/写流水线
//! - [`manager`]    连接管理器:增删查群发

mod connection;
mod manager;

// ── 本层对外暴露的 API ──
// TODO(阶段 2 / 4):
// pub use connection::Connection;
// pub use manager::ConnManager;
