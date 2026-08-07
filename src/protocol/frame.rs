//! 逻辑服 UDS 帧编解码(规格书 T0002M04F02)。
//!
//! ```text
//! ┌────────────────────────────────────┐
//! │ len   u32   后续字节数(不含本字段)   │
//! │ type  u8    消息类型                │
//! │ body  [u8]                         │
//! └────────────────────────────────────┘
//! ```
//!
//! 字节序统一小端。所有解析做边界检查,畸形帧返回 `Err` 不 panic。
//! 帧类型与 body 布局见 [`crate::protocol::types`]。

use bytes::{Buf, BufMut, BytesMut};

use crate::error::{Error, Result};
use crate::protocol::types::MAX_FRAME_BODY;

/// 帧头长度:len u32 + type u8 = 5 字节。
pub const FRAME_HEADER_LEN: usize = 5;

/// 一条已解码的帧:`body` 拥有数据(从缓冲区切出)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub ty: u8,
    pub body: Vec<u8>,
}

impl Frame {
    pub fn new(ty: u8, body: Vec<u8>) -> Self {
        Self { ty, body }
    }
}

/// 从长度前缀流中尝试拆出一帧。
///
/// - 数据不足一帧:返回 `Ok(None)`(等更多数据)
/// - 非法长度 / 类型:返回 `Err`
/// - 完整:返回 `Ok(Some((frame, consumed)))`,并从 `buf` 中消费
///
/// 这是 `SOCK_STREAM` 上的分帧器,天然处理半包/粘包。
pub fn try_decode(buf: &mut BytesMut) -> Result<Option<Frame>> {
    if buf.len() < FRAME_HEADER_LEN {
        return Ok(None);
    }
    let len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    if len > MAX_FRAME_BODY as usize {
        return Err(Error::Protocol(format!(
            "帧体超限: {len} > {MAX_FRAME_BODY}"
        )));
    }
    let total = FRAME_HEADER_LEN
        .checked_add(len)
        .ok_or_else(|| Error::Protocol("帧长度溢出".into()))?;
    if buf.len() < total {
        return Ok(None);
    }
    let ty = buf[4];
    let body = buf[FRAME_HEADER_LEN..total].to_vec();
    buf.advance(total);
    Ok(Some(Frame { ty, body }))
}

/// 编码一帧,追加写进 `out`。
pub fn encode(ty: u8, body: &[u8], out: &mut BytesMut) -> Result<()> {
    if body.len() > MAX_FRAME_BODY as usize {
        return Err(Error::Protocol(format!(
            "帧体超限: {} > {MAX_FRAME_BODY}",
            body.len()
        )));
    }
    out.put_u32_le(body.len() as u32);
    out.put_u8(ty);
    out.put_slice(body);
    Ok(())
}

// ── 各帧类型的 body 构造/解析 ──
//
// 与 T0002M04F03/F04 的表格一一对应。所有字段小端。
// 解析函数同样:任何输入不 panic。

pub const ADDR_LEN: usize = 18; // 16 字节 IP + 2 字节端口

/// 把 `SocketAddr` 编码为 `[u8;18]`:IPv4 前 4 字节 + 补零;IPv6 16 字节;末尾 2 字节端口。
pub fn encode_addr(addr: &std::net::SocketAddr) -> [u8; ADDR_LEN] {
    let mut out = [0u8; ADDR_LEN];
    match addr {
        std::net::SocketAddr::V4(v4) => {
            out[..4].copy_from_slice(&v4.ip().octets());
            out[16..18].copy_from_slice(&v4.port().to_le_bytes());
        }
        std::net::SocketAddr::V6(v6) => {
            out[..16].copy_from_slice(&v6.ip().octets());
            out[16..18].copy_from_slice(&v6.port().to_le_bytes());
        }
    }
    out
}

/// 从 `[u8;18]` 解析出 `SocketAddr`(尽力而为;解析不了返回 None)。
pub fn decode_addr(bytes: &[u8]) -> Option<std::net::SocketAddr> {
    if bytes.len() < ADDR_LEN {
        return None;
    }
    let port = u16::from_le_bytes([bytes[16], bytes[17]]);
    // 前 12 字节全零 → IPv4
    let v4_tail_zero = bytes[4..16].iter().all(|&b| b == 0);
    if v4_tail_zero {
        let ip = std::net::Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]);
        Some(std::net::SocketAddr::new(std::net::IpAddr::V4(ip), port))
    } else {
        let mut oct = [0u8; 16];
        oct.copy_from_slice(&bytes[..16]);
        let ip = std::net::Ipv6Addr::from(oct);
        Some(std::net::SocketAddr::new(std::net::IpAddr::V6(ip), port))
    }
}

/// 0x30 EngineHello: version u16 · caps u32
pub fn engine_hello(version: u16, caps: u32, out: &mut BytesMut) -> Result<()> {
    let mut body = Vec::with_capacity(6);
    body.extend_from_slice(&version.to_le_bytes());
    body.extend_from_slice(&caps.to_le_bytes());
    encode(crate::protocol::types::FRAME_ENGINE_HELLO, &body, out)
}

/// 0x90 LogicHello: version u16 · caps u32
pub fn logic_hello(version: u16, caps: u32, out: &mut BytesMut) -> Result<()> {
    let mut body = Vec::with_capacity(6);
    body.extend_from_slice(&version.to_le_bytes());
    body.extend_from_slice(&caps.to_le_bytes());
    encode(crate::protocol::types::FRAME_LOGIC_HELLO, &body, out)
}

/// 0x01 SessionOpen: sess_id u64 · addr [u8;18] · token_len u16 · token[]
pub fn session_open(sess_id: u64, addr: &std::net::SocketAddr, token: &[u8]) -> Frame {
    let mut body = Vec::with_capacity(8 + ADDR_LEN + 2 + token.len());
    body.extend_from_slice(&sess_id.to_le_bytes());
    body.extend_from_slice(&encode_addr(addr));
    body.extend_from_slice(&(token.len() as u16).to_le_bytes());
    body.extend_from_slice(token);
    Frame::new(crate::protocol::types::FRAME_SESSION_OPEN, body)
}

/// 解析 SessionOpen body。返回 (sess_id, addr, token)。
pub fn parse_session_open(body: &[u8]) -> Result<(u64, Option<std::net::SocketAddr>, &[u8])> {
    if body.len() < 8 + ADDR_LEN + 2 {
        return Err(Error::Protocol("SessionOpen body 过短".into()));
    }
    let sess_id = u64::from_le_bytes(body[0..8].try_into().unwrap());
    let addr = decode_addr(&body[8..8 + ADDR_LEN]);
    let token_len = u16::from_le_bytes(
        body[8 + ADDR_LEN..8 + ADDR_LEN + 2]
            .try_into()
            .unwrap(),
    ) as usize;
    let start = 8 + ADDR_LEN + 2;
    let end = start.checked_add(token_len).ok_or_else(|| {
        Error::Protocol("token 长度溢出".to_string())
    })?;
    if end > body.len() {
        return Err(Error::Protocol("token 越界".into()));
    }
    Ok((sess_id, addr, &body[start..end]))
}

/// 0x02 SessionClose: sess_id u64 · reason u8
pub fn session_close(sess_id: u64, reason: u8) -> Frame {
    let mut body = Vec::with_capacity(9);
    body.extend_from_slice(&sess_id.to_le_bytes());
    body.push(reason);
    Frame::new(crate::protocol::types::FRAME_SESSION_CLOSE, body)
}

pub fn parse_session_close(body: &[u8]) -> Result<(u64, u8)> {
    if body.len() < 9 {
        return Err(Error::Protocol("SessionClose body 过短".into()));
    }
    let sess_id = u64::from_le_bytes(body[0..8].try_into().unwrap());
    Ok((sess_id, body[8]))
}

/// 0x03 SessionResume: sess_id u64 · gap_ms u32
pub fn session_resume(sess_id: u64, gap_ms: u32) -> Frame {
    let mut body = Vec::with_capacity(12);
    body.extend_from_slice(&sess_id.to_le_bytes());
    body.extend_from_slice(&gap_ms.to_le_bytes());
    Frame::new(crate::protocol::types::FRAME_SESSION_RESUME, body)
}

pub fn parse_session_resume(body: &[u8]) -> Result<(u64, u32)> {
    if body.len() < 12 {
        return Err(Error::Protocol("SessionResume body 过短".into()));
    }
    let sess_id = u64::from_le_bytes(body[0..8].try_into().unwrap());
    let gap_ms = u32::from_le_bytes(body[8..12].try_into().unwrap());
    Ok((sess_id, gap_ms))
}

/// 0x10 Data(框架 → 逻辑服): sess_id u64 · ch u8 · msg_id u16 · payload[]
pub fn data_up(sess_id: u64, ch: u8, msg_id: u16, payload: &[u8]) -> Frame {
    let mut body = Vec::with_capacity(11 + payload.len());
    body.extend_from_slice(&sess_id.to_le_bytes());
    body.push(ch);
    body.extend_from_slice(&msg_id.to_le_bytes());
    body.extend_from_slice(payload);
    Frame::new(crate::protocol::types::FRAME_DATA_UP, body)
}

/// 解析 Data 帧 body,返回 (sess_id, ch, msg_id, payload)。
pub fn parse_data(body: &[u8]) -> Result<(u64, u8, u16, &[u8])> {
    if body.len() < 11 {
        return Err(Error::Protocol("Data body 过短".into()));
    }
    let sess_id = u64::from_le_bytes(body[0..8].try_into().unwrap());
    let ch = body[8];
    let msg_id = u16::from_le_bytes(body[9..11].try_into().unwrap());
    Ok((sess_id, ch, msg_id, &body[11..]))
}

/// 0x20 SessionStats: sess_id u64 · rtt_ms u16 · loss_permille u16 · out_kbps u16
pub fn session_stats(sess_id: u64, rtt_ms: u16, loss_permille: u16, out_kbps: u16) -> Frame {
    let mut body = Vec::with_capacity(14);
    body.extend_from_slice(&sess_id.to_le_bytes());
    body.extend_from_slice(&rtt_ms.to_le_bytes());
    body.extend_from_slice(&loss_permille.to_le_bytes());
    body.extend_from_slice(&out_kbps.to_le_bytes());
    Frame::new(crate::protocol::types::FRAME_SESSION_STATS, body)
}

/// 0x2F Overload: dropped_up u32 · dropped_down u32
pub fn overload(dropped_up: u32, dropped_down: u32) -> Frame {
    let mut body = Vec::with_capacity(8);
    body.extend_from_slice(&dropped_up.to_le_bytes());
    body.extend_from_slice(&dropped_down.to_le_bytes());
    Frame::new(crate::protocol::types::FRAME_OVERLOAD, body)
}

/// 0x81 Send: sess_id u64 · ch u8 · msg_id u16 · payload[]
pub fn send(sess_id: u64, ch: u8, msg_id: u16, payload: &[u8]) -> Frame {
    data_up(sess_id, ch, msg_id, payload)
        .into_send()
}

impl Frame {
    /// 把 Data 帧换成 Send 帧(逻辑服 → 框架方向,body 布局一致)。
    pub fn into_send(self) -> Frame {
        Frame::new(crate::protocol::types::FRAME_SEND, self.body)
    }
}

/// 0x82 Multicast: n u8 · sess_id[n] u64 · ch u8 · msg_id u16 · payload[]
pub fn multicast(ids: &[u64], ch: u8, msg_id: u16, payload: &[u8]) -> Frame {
    let mut body = Vec::with_capacity(1 + ids.len() * 8 + 3 + payload.len());
    body.push(ids.len() as u8);
    for id in ids {
        body.extend_from_slice(&id.to_le_bytes());
    }
    body.push(ch);
    body.extend_from_slice(&msg_id.to_le_bytes());
    body.extend_from_slice(payload);
    Frame::new(crate::protocol::types::FRAME_MULTICAST, body)
}

/// 解析 Multicast body:返回 (ids, ch, msg_id, payload)。
pub fn parse_multicast(body: &[u8]) -> Result<(Vec<u64>, u8, u16, &[u8])> {
    if body.is_empty() {
        return Err(Error::Protocol("Multicast body 为空".into()));
    }
    let n = body[0] as usize;
    let ids_end = 1usize
        .checked_add(n.checked_mul(8).ok_or_else(|| {
            Error::Protocol("Multicast 长度溢出".to_string())
        })?)
        .ok_or_else(|| Error::Protocol("Multicast 长度溢出".to_string()))?;
    if body.len() < ids_end + 3 {
        return Err(Error::Protocol("Multicast body 过短".into()));
    }
    let mut ids = Vec::with_capacity(n);
    for i in 0..n {
        let start = 1 + i * 8;
        ids.push(u64::from_le_bytes(body[start..start + 8].try_into().unwrap()));
    }
    let ch = body[ids_end];
    let msg_id = u16::from_le_bytes(body[ids_end + 1..ids_end + 3].try_into().unwrap());
    Ok((ids, ch, msg_id, &body[ids_end + 3..]))
}

/// 0x83 Kick: sess_id u64 · reason u8
pub fn kick(sess_id: u64, reason: u8) -> Frame {
    session_close(sess_id, reason).into_send()
}

/// 0x84 SetBudget: sess_id u64 · kbps u16
pub fn set_budget(sess_id: u64, kbps: u16) -> Frame {
    let mut body = Vec::with_capacity(10);
    body.extend_from_slice(&sess_id.to_le_bytes());
    body.extend_from_slice(&kbps.to_le_bytes());
    Frame::new(crate::protocol::types::FRAME_SET_BUDGET, body)
}

pub fn parse_set_budget(body: &[u8]) -> Result<(u64, u16)> {
    if body.len() < 10 {
        return Err(Error::Protocol("SetBudget body 过短".into()));
    }
    let sess_id = u64::from_le_bytes(body[0..8].try_into().unwrap());
    let kbps = u16::from_le_bytes(body[8..10].try_into().unwrap());
    Ok((sess_id, kbps))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::*;

    #[test]
    fn frame_roundtrip_stream() {
        let mut stream = BytesMut::new();
        engine_hello(1, 0xDEAD, &mut stream).unwrap();
        let f1 = Frame::new(FRAME_SESSION_OPEN, vec![1, 2, 3]);
        encode(f1.ty, &f1.body, &mut stream).unwrap();
        // 模拟半包读取:每次只喂 3 字节
        let mut feed = BytesMut::new();
        let mut got = Vec::new();
        for chunk in stream.chunks(3) {
            feed.extend_from_slice(chunk);
            while let Some(f) = try_decode(&mut feed).unwrap() {
                got.push(f);
            }
        }
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].ty, FRAME_ENGINE_HELLO);
        assert_eq!(got[1].ty, FRAME_SESSION_OPEN);
        assert_eq!(got[1].body, vec![1, 2, 3]);
    }

    #[test]
    fn rejects_bad_length() {
        let mut stream = BytesMut::new();
        // len 声明 10 字节,但只有 5 字节
        stream.extend_from_slice(&10u32.to_le_bytes());
        stream.extend_from_slice(&[0x10]);
        assert!(try_decode(&mut stream).unwrap().is_none());
        // 长度超限
        let mut s2 = BytesMut::new();
        s2.extend_from_slice(&(MAX_FRAME_BODY + 1).to_le_bytes());
        s2.extend_from_slice(&[0x10]);
        assert!(try_decode(&mut s2).is_err());
    }

    #[test]
    fn session_open_roundtrip() {
        let addr: std::net::SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let f = session_open(42, &addr, b"token-abc");
        assert_eq!(f.ty, FRAME_SESSION_OPEN);
        let (sid, got_addr, token) = parse_session_open(&f.body).unwrap();
        assert_eq!(sid, 42);
        assert_eq!(got_addr, Some(addr));
        assert_eq!(token, b"token-abc");
    }

    #[test]
    fn addr_v6_roundtrip() {
        let addr: std::net::SocketAddr = "[::1]:8080".parse().unwrap();
        let enc = encode_addr(&addr);
        let dec = decode_addr(&enc).unwrap();
        assert_eq!(dec, addr);
    }

    #[test]
    fn data_and_multicast_roundtrip() {
        let f = data_up(7, CH_UNRELIABLE_SEQUENCED, 1234, b"payload");
        let (sid, ch, msg_id, payload) = parse_data(&f.body).unwrap();
        assert_eq!((sid, ch, msg_id, payload), (7, CH_UNRELIABLE_SEQUENCED, 1234, b"payload".as_slice()));

        let m = multicast(&[1, 2, 3], CH_RELIABLE_ORDERED, 99, b"broadcast");
        let (ids, ch, msg_id, payload) = parse_multicast(&m.body).unwrap();
        assert_eq!(ids, vec![1, 2, 3]);
        assert_eq!(ch, CH_RELIABLE_ORDERED);
        assert_eq!(msg_id, 99);
        assert_eq!(payload, b"broadcast");
    }

    #[test]
    fn garbage_never_panics() {
        let mut stream = BytesMut::new();
        for b in 0u8..=255 {
            stream.extend_from_slice(&[b; 8]);
        }
        // 每次尝试解码,任何输入不得 panic
        let mut feed = BytesMut::from(&stream[..]);
        loop {
            match try_decode(&mut feed) {
                Ok(Some(_)) => continue,
                _ => break,
            }
        }
    }
}
