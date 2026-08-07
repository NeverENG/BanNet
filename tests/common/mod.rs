//! 集成测试共享辅助:客户端侧握手 / HMAC / 编解码工具。
//!
//! 模拟真实客户端行为:握手拿 conn_id + session_secret;发数据包带
//! 4 字节截断 HMAC;收包先校验剥离 HMAC 再解析。

use soup_engine::protocol::datagram::{
    decode, encode, handshake_header, DatagramHeader, FrameRef,
};
use soup_engine::protocol::types::*;
use soup_engine::session::hmac4;

/// 握手:返回 (conn_id, secret)。
pub async fn handshake(
    client: &tokio::net::UdpSocket,
    engine_addr: std::net::SocketAddr,
) -> (u32, [u8; 8]) {
    let token: &[u8] = b"test-token";
    let mut buf = Vec::with_capacity(64);
    encode(&handshake_header(), &[FrameRef::new(0, 0, token)], &mut buf).unwrap();
    client.send_to(&buf, engine_addr).await.unwrap();

    let mut rbuf = [0u8; MTU];
    let (n, _) = tokio::time::timeout(std::time::Duration::from_secs(5), client.recv_from(&mut rbuf))
        .await
        .expect("握手超时:challenge")
        .unwrap();
    let (h, frames) = decode(&rbuf[..n]).unwrap();
    assert_ne!(h.flags & FLAG_HANDSHAKE, 0);
    let challenge = frames.first().expect("challenge 帧缺失").body.to_vec();

    let mut body = Vec::with_capacity(8 + token.len());
    body.extend_from_slice(&challenge);
    body.extend_from_slice(token);
    let mut buf2 = Vec::with_capacity(64);
    encode(&handshake_header(), &[FrameRef::new(0, 0, &body)], &mut buf2).unwrap();
    client.send_to(&buf2, engine_addr).await.unwrap();

    let (n, _) = tokio::time::timeout(std::time::Duration::from_secs(5), client.recv_from(&mut rbuf))
        .await
        .expect("握手超时:确认")
        .unwrap();
    let (h2, frames2) = decode(&rbuf[..n]).unwrap();
    assert_ne!(h2.conn_id, 0);
    let secret_bytes = frames2.first().expect("确认包缺 secret 帧").body;
    assert_eq!(secret_bytes.len(), 8);
    let mut secret = [0u8; 8];
    secret.copy_from_slice(secret_bytes);
    (h2.conn_id, secret)
}

/// 校验并剥离数据包的 HMAC。不带 HMAC 的包(握手/纯 ACK)原样返回。
#[allow(dead_code)] // 各测试 crate 独立编译,并非每个都用
pub fn verify_strip<'a>(buf: &'a [u8], secret: &[u8; 8]) -> Option<&'a [u8]> {
    if buf.len() < 16 {
        return Some(buf);
    }
    let flags = buf[3];
    if flags & FLAG_HMAC == 0 {
        return Some(buf); // 无 HMAC:兼容明文
    }
    if buf.len() < 16 + 4 {
        return None; // 带 HMAC 标志但过短:畸形
    }
    let (data, mac) = buf.split_at(buf.len() - 4);
    if hmac4(secret, data) == mac {
        Some(data)
    } else {
        None
    }
}

/// 给数据报追加 HMAC(客户端出站)。
#[allow(dead_code)]
pub fn attach(buf: &mut Vec<u8>, secret: &[u8; 8]) {
    let mac = hmac4(secret, buf);
    buf.extend_from_slice(&mac);
}

/// 构造一个带 HMAC 的数据报并追加。
#[allow(dead_code)]
pub fn build_packet(
    header: &DatagramHeader,
    frames: &[FrameRef],
    secret: &[u8; 8],
) -> Vec<u8> {
    let mut header = *header;
    header.flags |= FLAG_HMAC;
    let mut out = Vec::with_capacity(64);
    encode(&header, frames, &mut out).unwrap();
    attach(&mut out, secret);
    out
}
