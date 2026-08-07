//! 客户端 UDP 数据报编解码(规格书 T0002M03F01)。
//!
//! ```text
//! ┌──────────────────────────────────────────────────────┐
//! │ magic u16 0x5A50 │ version u8 │ flags u8            │
//! │ conn_id u32 │ seq u16 │ ack u16 │ ack_bits u32      │
//! ├──────────────────────────────────────────────────────┤
//! │ 连续 N 个 Message Frame:                              │
//! │   ch u4 | msg_id u12 (u16) │ len u16 │ body [u8]     │
//! └──────────────────────────────────────────────────────┘
//! ```
//!
//! 本模块是**纯函数、无 IO**(借鉴 str0m 的无 IO 设计):输入字节、输出结构,
//! 由调用方决定网络怎么收。**任何输入都不得 panic** —— 所有访问走边界检查,
//! 畸形包返回 `Err`,这是 fuzz 测试的硬性前提。

use crate::error::{Error, Result};
use crate::protocol::types::{FLAG_HANDSHAKE, MAGIC, MTU, VERSION};

/// 数据报固定头长度:2+1+1+4+2+2+4 = 16 字节。
pub const HEADER_LEN: usize = 16;

/// 数据报头。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatagramHeader {
    pub version: u8,
    pub flags: u8,
    pub conn_id: u32,
    /// 本端发送序号。
    pub seq: u16,
    /// 已收到的对端最大序号。
    pub ack: u16,
    /// ack 之前 32 个包的收到位图。
    pub ack_bits: u32,
}

/// 一条消息帧。`body` 借用输入缓冲,零拷贝。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameRef<'a> {
    pub ch: u8,
    pub msg_id: u16,
    pub body: &'a [u8],
}

impl<'a> FrameRef<'a> {
    pub fn new(ch: u8, msg_id: u16, body: &'a [u8]) -> Self {
        Self { ch, msg_id, body }
    }

    /// 是否是握手帧(由数据报 flags 决定,但单帧也带 ch)。
    pub fn is_handshake(&self) -> bool {
        self.ch == crate::protocol::types::CH_UNRELIABLE_UNORDERED
    }
}

/// 默认空头:握手前的包用(conn_id=0)。
pub fn handshake_header() -> DatagramHeader {
    DatagramHeader {
        version: VERSION,
        flags: FLAG_HANDSHAKE,
        conn_id: 0,
        seq: 0,
        ack: 0,
        ack_bits: 0,
    }
}

/// 编码:把 `header` + `frames` 打包进 `out`(追加写)。
///
/// 错误情况:
/// - 单帧超过 MTU(Ch0/1 不区分片;Ch2 分片由上层做)
/// - 总长度超过 MTU
/// - `ch` 超出 0..3 / `msg_id` 超出 12 bit
pub fn encode(header: &DatagramHeader, frames: &[FrameRef], out: &mut Vec<u8>) -> Result<usize> {
    if header.version != VERSION {
        return Err(Error::Protocol(format!(
            "版本不匹配: 期望 {VERSION}, 收到 {}",
            header.version
        )));
    }
    let mut total = HEADER_LEN;
    for f in frames {
        if f.ch > 3 {
            return Err(Error::Protocol(format!("非法通道 ch={}", f.ch)));
        }
        if f.msg_id > 0x0FFF {
            return Err(Error::Protocol(format!("msg_id 超出 12bit: {}", f.msg_id)));
        }
        // 2 字节帧头 + 2 字节长度 + body
        total = total
            .checked_add(4)
            .and_then(|t| t.checked_add(f.body.len()))
            .ok_or_else(|| Error::Protocol("长度溢出".into()))?;
        if total > MTU {
            return Err(Error::Protocol(format!(
                "数据报超过 MTU({MTU}): 聚合后 {total} 字节"
            )));
        }
    }

    let base = out.len();
    out.extend_from_slice(&MAGIC.to_le_bytes());
    out.push(header.version);
    out.push(header.flags);
    out.extend_from_slice(&header.conn_id.to_le_bytes());
    out.extend_from_slice(&header.seq.to_le_bytes());
    out.extend_from_slice(&header.ack.to_le_bytes());
    out.extend_from_slice(&header.ack_bits.to_le_bytes());

    for f in frames {
        // ch u4 | msg_id u12,小端打包
        let head = (f.msg_id << 4) | u16::from(f.ch);
        out.extend_from_slice(&head.to_le_bytes());
        out.extend_from_slice(&(f.body.len() as u16).to_le_bytes());
        out.extend_from_slice(f.body);
    }
    Ok(out.len() - base)
}

/// 解码:解析一个完整数据报。
///
/// 返回 `(header, frames)`;`frames` 借用 `buf`。
/// 所有字段做边界与合法性检查,畸形输入返回 `Err` 而非 panic。
pub fn decode<'a>(buf: &'a [u8]) -> Result<(DatagramHeader, Vec<FrameRef<'a>>)> {
    if buf.len() < HEADER_LEN {
        return Err(Error::Protocol(format!(
            "数据报过短: {} < 头部 {HEADER_LEN}",
            buf.len()
        )));
    }
    if buf.len() > MTU {
        return Err(Error::Protocol(format!(
            "数据报超过 MTU({MTU}): {}",
            buf.len()
        )));
    }

    let magic = u16::from_le_bytes([buf[0], buf[1]]);
    if magic != MAGIC {
        return Err(Error::Protocol(format!(
            "魔数错误: 0x{magic:04X} != 0x{MAGIC:04X}"
        )));
    }
    let version = buf[2];
    if version != VERSION {
        return Err(Error::Protocol(format!(
            "版本不匹配: 期望 {VERSION}, 收到 {version}"
        )));
    }
    let header = DatagramHeader {
        version,
        flags: buf[3],
        conn_id: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
        seq: u16::from_le_bytes(buf[8..10].try_into().unwrap()),
        ack: u16::from_le_bytes(buf[10..12].try_into().unwrap()),
        ack_bits: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
    };

    let mut frames = Vec::new();
    let mut pos = HEADER_LEN;
    while pos < buf.len() {
        if buf.len() - pos < 4 {
            return Err(Error::Protocol(format!(
                "帧头不完整: 剩余 {} 字节 < 4",
                buf.len() - pos
            )));
        }
        let head = u16::from_le_bytes([buf[pos], buf[pos + 1]]);
        let ch = (head & 0x000F) as u8;
        let msg_id = head >> 4;
        let len = u16::from_le_bytes([buf[pos + 2], buf[pos + 3]]) as usize;
        if ch > 3 {
            return Err(Error::Protocol(format!("非法通道 ch={ch}")));
        }
        let body_start = pos + 4;
        let body_end = body_start.checked_add(len).ok_or_else(|| {
            Error::Protocol("帧长度溢出".to_string())
        })?;
        if body_end > buf.len() {
            return Err(Error::Protocol(format!(
                "帧体越界: 声明 {len} 字节, 实际剩余 {}",
                buf.len() - body_start
            )));
        }
        frames.push(FrameRef {
            ch,
            msg_id,
            body: &buf[body_start..body_end],
        });
        pos = body_end;
    }
    Ok((header, frames))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::FLAG_PURE_ACK;

    fn sample_header() -> DatagramHeader {
        DatagramHeader {
            version: VERSION,
            flags: 0,
            conn_id: 0xDEAD_BEEF,
            seq: 42,
            ack: 7,
            ack_bits: 0xFFFF_FF00,
        }
    }

    #[test]
    fn roundtrip_single_frame() {
        let h = sample_header();
        let f = FrameRef::new(2, 100, b"hello soup");
        let mut buf = Vec::new();
        encode(&h, &[f], &mut buf).unwrap();
        let (dh, df) = decode(&buf).unwrap();
        assert_eq!(dh, h);
        assert_eq!(df.len(), 1);
        assert_eq!(df[0].ch, 2);
        assert_eq!(df[0].msg_id, 100);
        assert_eq!(df[0].body, b"hello soup");
    }

    #[test]
    fn roundtrip_multi_frame_aggregate() {
        let h = sample_header();
        let frames = [
            FrameRef::new(0, 1, b"snapshot"),
            FrameRef::new(1, 2, b"input"),
            FrameRef::new(2, 3, b"event"),
            FrameRef::new(3, 4, b""),
        ];
        let mut buf = Vec::new();
        encode(&h, &frames, &mut buf).unwrap();
        let (dh, df) = decode(&buf).unwrap();
        assert_eq!(dh, h);
        assert_eq!(df.len(), 4);
        for (a, b) in df.iter().zip(frames.iter()) {
            assert_eq!(a.ch, b.ch);
            assert_eq!(a.msg_id, b.msg_id);
            assert_eq!(a.body, b.body);
        }
    }

    #[test]
    fn empty_frames_ok() {
        let h = sample_header();
        let mut buf = Vec::new();
        encode(&h, &[], &mut buf).unwrap();
        assert_eq!(buf.len(), HEADER_LEN);
        let (dh, df) = decode(&buf).unwrap();
        assert_eq!(dh, h);
        assert!(df.is_empty());
    }

    #[test]
    fn rejects_truncated() {
        let h = sample_header();
        let f = FrameRef::new(2, 1, b"payload");
        let mut buf = Vec::new();
        encode(&h, &[f], &mut buf).unwrap();
        // 依次切掉尾部,任何前缀都不得 panic,且要么 Err 要么解析出完整前缀。
        for cut in 0..buf.len() {
            let _ = decode(&buf[..cut]);
        }
    }

    #[test]
    fn rejects_garbage_bytes() {
        // 任意字节序列不得 panic —— fuzz 的种子用例。
        let cases: Vec<Vec<u8>> = vec![
            vec![],
            vec![0x50],
            vec![0x50, 0x5A],
            vec![0x50, 0x5A, 0x01],
            vec![0xFF; 32],
            vec![0x50, 0x5A, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF],
            vec![0x50, 0x5A, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF],
        ];
        for c in cases {
            let _ = decode(&c);
        }
    }

    #[test]
    fn rejects_bad_magic_and_version() {
        let h = sample_header();
        let mut buf = Vec::new();
        encode(&h, &[], &mut buf).unwrap();
        buf[0] = 0x00; // 破坏 magic
        assert!(decode(&buf).is_err());
        buf[0] = 0x50;
        buf[1] = 0x5A;
        buf[2] = 99; // 破坏 version
        assert!(decode(&buf).is_err());
    }

    #[test]
    fn rejects_oversize() {
        // 超过 MTU 的输入直接拒绝。
        let mut big = vec![0u8; MTU + 1];
        big[0] = 0x50;
        big[1] = 0x5A;
        big[2] = VERSION;
        assert!(decode(&big).is_err());
    }

    #[test]
    fn rejects_ch_out_of_range_and_bad_len() {
        // ch = 5 非法
        let h = sample_header();
        let mut buf = Vec::new();
        encode(&h, &[], &mut buf).unwrap();
        // 手工构造一个 ch=5 的帧头
        let bad_head = (1u16 << 4) | 5;
        buf.extend_from_slice(&bad_head.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        assert!(decode(&buf).is_err());
        // 声明长度超出剩余
        let mut buf2 = Vec::new();
        encode(&h, &[], &mut buf2).unwrap();
        let head2 = (1u16 << 4) | 2u16;
        buf2.extend_from_slice(&head2.to_le_bytes());
        buf2.extend_from_slice(&200u16.to_le_bytes());
        assert!(decode(&buf2).is_err());
    }

    #[test]
    fn pure_ack_flag_roundtrip() {
        let mut h = sample_header();
        h.flags = FLAG_PURE_ACK;
        let mut buf = Vec::new();
        encode(&h, &[], &mut buf).unwrap();
        let (dh, df) = decode(&buf).unwrap();
        assert_eq!(dh.flags, FLAG_PURE_ACK);
        assert!(df.is_empty());
    }
}
