//! 确定性 fuzz 基线(规格书 T0002M08F02:**任何字节序列都不得 panic**)。
//!
//! 生产环境用 `cargo-fuzz` 做持续 fuzz(CI 5 分钟 / 夜间 1 小时);
//! 这里提供 xorshift64 伪随机种子 fuzz,作为每次 `cargo test` 都跑的基线。
//! 覆盖三个解析面:数据报解码、UDS 帧流、会话表整链路(含握手/HMAC/四通道)。

use std::net::SocketAddr;
use std::time::Instant;

use bytes::BytesMut;
use soup_engine::protocol::datagram::decode;
use soup_engine::protocol::frame::try_decode;
use soup_engine::protocol::types::MTU;
use soup_engine::session::SessionTable;

/// xorshift64*:确定性伪随机。
fn next_rand(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

fn random_bytes(state: &mut u64, max_len: usize) -> Vec<u8> {
    let len = (next_rand(state) % max_len as u64) as usize;
    (0..len).map(|_| (next_rand(state) & 0xFF) as u8).collect()
}

#[test]
fn fuzz_datagram_decode_never_panics() {
    let mut state = 0x5A50_5A50_5A50_5A50u64;
    for _ in 0..50_000 {
        let buf = random_bytes(&mut state, MTU + 16);
        let _ = decode(&buf);
    }
}

#[test]
fn fuzz_frame_stream_never_panics() {
    let mut state = 0xDEAD_BEEF_CAFE_F00Du64;
    for _ in 0..20_000 {
        let mut buf = BytesMut::from(&random_bytes(&mut state, 4096)[..]);
        loop {
            match try_decode(&mut buf) {
                Ok(Some(_)) => continue,
                _ => break,
            }
        }
    }
}

#[test]
fn fuzz_session_handle_never_panics() {
    let table = SessionTable::new(Default::default());
    let peer: SocketAddr = "127.0.0.1:9999".parse().unwrap();
    let mut state = 0x0123_4567_89AB_CDEFu64;
    let mut send_back = |_: &[u8], _: SocketAddr| {};
    for _ in 0..30_000 {
        let buf = random_bytes(&mut state, MTU + 8);
        let _ = table.handle_datagram(peer, &buf, &mut send_back, Instant::now());
    }
    // 会话表状态应保持健康(不会因畸形输入崩溃或无限增长)。
    assert!(table.active_count() < 10_000);
}
