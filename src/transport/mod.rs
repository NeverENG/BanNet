//! # 传输层
//!
//! 负责真正的网络 IO,与协议编解码分离(继承 BanNet 的分层原则):
//!
//! - [`udp`] 客户端 UDP 传输:SO_REUSEPORT 多 socket 接收 + 共享发送 socket
//! - [`uds`] 逻辑服 UDS 传输:SOCK_STREAM + 长度前缀分帧的帧流原语

pub mod udp;
pub mod uds;

/// 网络模拟器(仅测试与工具使用,不参与生产路径)。
pub mod netem;
