//! # 传输层
//!
//! 负责真正的网络 IO,与协议编解码分离(继承 BanNet 的分层原则):
//!
//! - [`tcp`] 客户端 TCP 传输:虚拟 peer,复用 datagram 协议(握手/会话/可靠层零分叉)
//! - [`udp`] 客户端 UDP 传输:SO_REUSEPORT 多 socket 接收 + 共享发送 socket
//! - [`uds`] 逻辑服 UDS 传输:SOCK_STREAM + 长度前缀分帧的帧流原语
//! - [`ws`] 客户端 WebSocket 传输:浏览器/H5 桥,二进制消息即报文

pub mod tcp;
pub mod udp;
pub mod uds;
pub mod ws;

/// 网络模拟器(仅测试与工具使用,不参与生产路径)。
pub mod netem;
