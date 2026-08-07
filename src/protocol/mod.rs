//! # 协议层
//!
//! 定义「字节流怎么编解码」。这一层**不碰网络 IO**(继承 BanNet 的分层原则),
//! 只关心数据结构和编码规则,是最纯粹、最好测试、最经得起 fuzz 的一层。
//!
//! - [`types`]     协议常量:魔数 / flags / 通道 / 帧类型 / 超时(规格书 M03、M04 的镜像)
//! - [`datagram`]  客户端 UDP 数据报编解码(纯函数)
//! - [`frame`]     UDS 帧编解码(长度前缀分帧 + 各帧类型 body 构造/解析)

pub mod datagram;
pub mod frame;
pub mod types;

pub use datagram::{handshake_header, FrameRef, DatagramHeader, HEADER_LEN};
pub use frame::{Frame, FRAME_HEADER_LEN};
pub use types::*;
