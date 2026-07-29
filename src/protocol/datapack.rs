//! 封包 / 拆包 —— 解决 TCP 粘包问题。
//!
//! TCP 是字节流,没有"消息边界"。我们自定义一个 TLV 协议:
//!
//!   +----------+----------+------------------+
//!   | dataLen  |  msgID   |      data ...     |
//!   |  u32(LE) |  u32(LE) |   dataLen 字节    |
//!   +----------+----------+------------------+

use crate::protocol::message;
use std::io;

const HEADER_SIZE: usize = 8; // 4 bytes for dataLen + 4 bytes for msgID

/// TLV 封包/拆包工具。
pub struct DataPack;

impl DataPack {
    pub fn new() -> Self {
        Self
    }

    pub fn pack(&self, message: &message::Message) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(HEADER_SIZE + message.len() as usize);
        buffer.extend_from_slice(&message.len().to_le_bytes());
        buffer.extend_from_slice(&message.id().to_le_bytes());
        buffer.extend_from_slice(message.data());
        buffer
    }

    pub fn unpack(&self, data: &[u8]) -> Result<Option<(message::Message, usize)>, io::Error> {
        if data.len() < HEADER_SIZE {
            return Ok(None);
        }

        let data_len = u32::from_le_bytes(data[0..4].try_into().map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("header parse failed: {e}"),
            )
        })?);
        let msg_id = u32::from_le_bytes(data[4..8].try_into().map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("header parse failed: {e}"),
            )
        })?);
        let total_len = HEADER_SIZE + data_len as usize;

        if data.len() < total_len {
            return Ok(None);
        }

        let payload = data[8..total_len].to_vec();
        let message = message::Message::new(msg_id, payload);
        Ok(Some((message, total_len)))
    }
}
