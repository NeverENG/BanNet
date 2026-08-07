//! # 会话层
//!
//! 会话生命周期(规格书 T0002M05)与防放大握手(T0002M07)。
//!
//! ```text
//! [*] --> 握手中: 收到首包
//! 握手中 --> 活跃: 分配 conn_id + sess_id → SessionOpen
//! 握手中 --> [*]: 超时 3s / 校验失败
//! 活跃 --> 活跃: 正常收发 · NAT 重绑定自动跟随
//! 活跃 --> 宽限期: 连续 5s 无包
//! 宽限期 --> 活跃: 收到带原 conn_id 的包 → SessionResume
//! 宽限期 --> [*]: 超过 reconnect_grace (默认 20s) → SessionClose
//! 活跃 --> [*]: 收到 Kick → SessionClose
//! ```
//!
//! 关键点:宽限期内不发 `SessionClose` —— 短暂断网在移动网络下是常态。

pub mod table;

pub use table::{hmac4, SessionEvent, SessionTable, SessionTableConfig};
