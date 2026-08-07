//! # soup-engine
//!
//! 与具体游戏无关的实时对战服务器框架(Rust)。
//! Go 逻辑服通过 soup-sdk-go 调用它,只写游戏规则,不碰任何网络细节
//! (规格书 `docs/T0002SoupEngine.md`)。
//!
//! 它吃掉「做一个能扛住移动网络的实时对战服务器」里**所有与游戏内容无关
//! 的部分**:UDP 传输、可靠层、通道语义、会话、断线重连、NAT 漂移、组播、
//! 背压、限流、抗攻击、可观测性。换一个游戏,这一整套原样复用。
//!
//! ## 分层架构
//! - [`protocol`]  协议层:UDP 数据报 / UDS 帧的编解码(纯函数,不碰网络)
//! - [`session`]   会话层:会话表、握手、生命周期、NAT 漂移
//! - [`transport`] 传输层:UDP 多 socket(SO_REUSEPORT)、UDS 帧流
//! - [`buffer`]    缓冲池:分级复用,热路径零堆分配
//! - [`stats`]     原子指标
//! - [`engine`]    门面层:装配一切,对用户暴露
//!
//! ## 使用
//! ```ignore
//! use soup_engine::{Engine, EngineConfig};
//!
//! #[tokio::main]
//! async fn main() -> soup_engine::Result<()> {
//!     let engine = Engine::new(EngineConfig {
//!         bind_addr: "0.0.0.0:8999".parse().unwrap(),
//!         uds_path: "/run/soup-engine.sock".into(),
//!         ..EngineConfig::default()
//!     });
//!     engine.run().await
//! }
//! ```

mod engine;

pub mod buffer;
pub mod error;
pub mod protocol;
pub mod reliable;
pub mod session;
pub mod stats;
pub mod transport;

pub use buffer::BufferPool;
pub use engine::{Engine, EngineConfig};
pub use error::{Error, Result};
pub use session::SessionTable;
pub use stats::Stats;
