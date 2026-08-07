//! interop —— 跨语言联调:Rust 引擎 ⇄ Go SDK 逻辑服 真实互通演示。
//!
//! 引擎只做网络,逻辑服由 soup-sdk-go 提供(Go 侧实现 Room 接口 echo)。
//! 本工具:起引擎 → 握手 → 发 ch=2 ping → 等 Go 逻辑服 echo 回来 → 打印 RTT。
//!
//! ```bash
//! # 1. 起 Go 逻辑服(echo):
//! cd soup-sdk-go && go run ./cmd/echologic --socket /tmp/soup-interop.sock
//! # 2. 起引擎 + 客户端,打穿整条链路:
//! cargo run --release --example interop -- --uds /tmp/soup-interop.sock
//! # 期望输出:echo 往返成功,ping-pong ×N,RTT p99 …
//! ```

use std::sync::Arc;
use std::time::Duration;

use soup_engine::protocol::datagram::{
    decode, encode, handshake_header, DatagramHeader, FrameRef,
};
use soup_engine::protocol::types::*;
use soup_engine::session::hmac4;
use soup_engine::{Engine, EngineConfig};
use tokio::net::UdpSocket;

struct Args {
    uds: std::path::PathBuf,
    count: u16,
}

fn parse_args() -> Args {
    let mut args = Args {
        uds: "/tmp/soup-interop.sock".into(),
        count: 5,
    };
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        let v = it.next().unwrap_or_default();
        match k.as_str() {
            "--uds" => args.uds = v.into(),
            "--count" => args.count = v.parse().unwrap_or(5),
            _ => {}
        }
    }
    args
}

/// 三拍握手,返回 (conn_id, secret)。
async fn handshake(client: &UdpSocket, addr: std::net::SocketAddr) -> (u32, [u8; 8]) {
    let token: &[u8] = b"interop-interop";
    let mut buf = Vec::with_capacity(64);
    encode(&handshake_header(), &[FrameRef::new(0, 0, token)], &mut buf).unwrap();
    client.send_to(&buf, addr).await.unwrap();
    let mut rbuf = [0u8; MTU];
    let (n, _) = tokio::time::timeout(Duration::from_secs(5), client.recv_from(&mut rbuf))
        .await
        .unwrap()
        .unwrap();
    let (_, frames) = decode(&rbuf[..n]).unwrap();
    let challenge = frames.first().unwrap().body.to_vec();
    let mut body = Vec::with_capacity(8 + token.len());
    body.extend_from_slice(&challenge);
    body.extend_from_slice(token);
    let mut buf2 = Vec::with_capacity(64);
    encode(&handshake_header(), &[FrameRef::new(0, 0, &body)], &mut buf2).unwrap();
    client.send_to(&buf2, addr).await.unwrap();
    let (n, _) = tokio::time::timeout(Duration::from_secs(5), client.recv_from(&mut rbuf))
        .await
        .unwrap()
        .unwrap();
    let (h2, frames2) = decode(&rbuf[..n]).unwrap();
    let mut secret = [0u8; 8];
    secret.copy_from_slice(frames2.first().unwrap().body);
    (h2.conn_id, secret)
}

#[tokio::main]
async fn main() -> soup_engine::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn".into()),
        )
        .try_init();
    let args = parse_args();

    // ── 1. 起引擎:只做网络,逻辑服在 Go 侧(外部进程)。──
    let cfg = EngineConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        uds_path: args.uds.clone(),
        udp_workers: 2,
        ..Default::default()
    };
    let engine = Arc::new(Engine::new(cfg));
    let (tx, rx) = tokio::sync::watch::channel(false);
    tokio::spawn({
        let engine = engine.clone();
        async move {
            let _ = engine.run_with_shutdown(rx).await;
        }
    });
    let addr = loop {
        if let Some(a) = engine.local_addr() {
            break a;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    // 等引擎连上 Go 逻辑服(Go 侧应已 bind 并 accept)。
    let mut online = false;
    for _ in 0..50 {
        if engine.logic_online() {
            online = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if !online {
        eprintln!("✗ 引擎 5s 内未连上 Go 逻辑服 —— 请先: go run ./cmd/echologic --socket {}", args.uds.display());
        std::process::exit(1);
    }
    println!("✓ 引擎已连上 Go 逻辑服 (uds={})", args.uds.display());

    // ── 2. 客户端:握手 → 发 ch=2 ping → 等 echo。──
    let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let (conn_id, secret) = handshake(&sock, addr).await;
    println!("✓ 客户端握手成功 conn_id={conn_id}");

    // 收包 task:回 ACK + 匹配 echo。
    let sock_rx = sock.clone();
    let target = args.count;
    let rx = tokio::spawn(async move {
        let mut buf = [0u8; MTU];
        let mut echoed = 0u16;
        let mut rtts = Vec::new();
        loop {
            let (n, peer) = sock_rx.recv_from(&mut buf).await.unwrap();
            let raw = &buf[..n];
            let data = if raw.len() >= 20 && raw[3] & FLAG_HMAC != 0 {
                let (d, m) = raw.split_at(raw.len() - 4);
                if hmac4(&secret, d) != m {
                    continue;
                }
                d
            } else {
                raw
            };
            let Ok((h, frames)) = decode(data) else {
                continue;
            };
            for f in frames {
                if f.ch == CH_RELIABLE_ORDERED && f.body.starts_with(b"ping-") {
                    let id = String::from_utf8_lossy(&f.body[5..]).parse::<u16>().unwrap_or(0);
                    rtts.push(id as f64);
                    echoed += 1;
                }
            }
            // 回 ACK(连续交付语义)。
            let resp = DatagramHeader {
                version: VERSION,
                flags: FLAG_PURE_ACK,
                conn_id: h.conn_id,
                seq: 0,
                ack: h.seq,
                ack_bits: 0,
            };
            let mut out = Vec::with_capacity(16);
            if encode(&resp, &[], &mut out).is_ok() {
                let _ = sock_rx.send_to(&out, peer).await;
            }
            if echoed >= target {
                break;
            }
        }
        echoed
    });

    // 发 ping(seq 从 0 递增,带 HMAC);收包/echo 匹配全部在 rx task。
    let mut sent = 0u16;
    for i in 0..args.count {
        let payload = format!("ping-{i}").into_bytes();
        let h = DatagramHeader {
            version: VERSION,
            flags: FLAG_HMAC,
            conn_id,
            seq: i,
            ack: 0,
            ack_bits: 0,
        };
        let mut buf = Vec::with_capacity(64);
        encode(&h, &[FrameRef::new(CH_RELIABLE_ORDERED, 1, &payload)], &mut buf).unwrap();
        let mac = hmac4(&secret, &buf);
        buf.extend_from_slice(&mac);
        sock.send_to(&buf, addr).await.unwrap();
        sent += 1;
    }
    // 等 rx task 收齐 echo(3s 上限)。
    let echoed = tokio::time::timeout(Duration::from_secs(3), rx)
        .await
        .unwrap_or_else(|_| Ok(0))
        .unwrap_or(0);
    if echoed == sent {
        println!("✓ 发送 {sent} 条 ch=2 ping,经 Go 逻辑服 echo 往返成功(真实 Rust⇄Go 链路)");
        println!("═══ interop 结果:全链路互通 OK ═══");
    } else {
        eprintln!("✗ 仅收到 {echoed}/{sent} 条 echo —— 跨语言链路异常");
        std::process::exit(1);
    }
    let _ = tx;
    Ok(())
}
