//! TCP 传输端到端测试:客户端经 TCP(带 4B 长度前缀的 datagram 报文)
//! ⇄ 引擎(虚拟 peer,复用握手/HMAC/会话/可靠层)⇄ 逻辑服 echo。
//!
//! 与 echo_e2e.rs 唯一的差异在「客户端↔引擎」的传输:UDP → TCP,
//! 协议帧与可靠层零改动 —— 验证 TCP 传输不是旁路,而是同一套协议。

mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use soup_engine::protocol::datagram::{decode, encode, handshake_header, DatagramHeader, FrameRef};
use soup_engine::protocol::frame;
use soup_engine::protocol::types::*;
use soup_engine::session::hmac4;
use soup_engine::transport::uds::{UdsReader, UdsWriter};
use soup_engine::{Engine, EngineConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::watch;

/// 扮演逻辑服(与 echo_e2e 同款):Data(ch=2)原样回 Send。
async fn fake_logic_server(path: &std::path::Path) -> tokio::task::JoinHandle<()> {
    let listener = tokio::net::UnixListener::bind(path).unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (rh, wh) = tokio::io::split(stream);
        let mut reader = UdsReader::new(rh);
        let mut writer = UdsWriter::new(wh);
        while let Some(f) = reader.read_frame().await.unwrap() {
            match f.ty {
                FRAME_ENGINE_HELLO => {
                    let mut out = BytesMut::new();
                    frame::logic_hello(1, 0, &mut out).unwrap();
                    writer.write_raw(&out).await.unwrap();
                }
                FRAME_DATA_UP => {
                    let (sid, ch, msg_id, payload) = frame::parse_data(&f.body).unwrap();
                    if ch == CH_RELIABLE_ORDERED && payload == b"tcp-hello" {
                        // 原样回。
                        writer.write_frame(&frame::send(sid, ch, msg_id, payload)).await.unwrap();
                    }
                }
                _ => {}
            }
        }
    })
}

/// TCP 客户端:写报文 = 4B len + datagram 报文;读同理。
struct TcpClient {
    stream: TcpStream,
}

impl TcpClient {
    async fn connect(addr: std::net::SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        Self { stream }
    }

    async fn send_datagram(&mut self, bytes: &[u8]) {
        let mut out = Vec::with_capacity(4 + bytes.len());
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(bytes);
        self.stream.write_all(&out).await.unwrap();
    }

    async fn recv_datagram(&mut self) -> Vec<u8> {
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        self.stream.read_exact(&mut buf).await.unwrap();
        buf
    }
}

/// TCP 三拍握手(与 UDP 版同协议,传输换 TCP)。
async fn handshake_tcp(client: &mut TcpClient) -> (u32, [u8; 8]) {
    let token: &[u8] = b"test-token";
    let mut buf = Vec::with_capacity(64);
    encode(&handshake_header(), &[FrameRef::new(0, 0, token)], &mut buf).unwrap();
    client.send_datagram(&buf).await;

    let r1 = client.recv_datagram().await;
    let (_, frames) = decode(&r1).unwrap();
    let challenge = frames.first().unwrap().body.to_vec();

    let mut body = Vec::with_capacity(8 + token.len());
    body.extend_from_slice(&challenge);
    body.extend_from_slice(token);
    let mut buf2 = Vec::with_capacity(64);
    encode(&handshake_header(), &[FrameRef::new(0, 0, &body)], &mut buf2).unwrap();
    client.send_datagram(&buf2).await;

    let r2 = client.recv_datagram().await;
    let (h2, frames2) = decode(&r2).unwrap();
    let mut secret = [0u8; 8];
    secret.copy_from_slice(frames2.first().unwrap().body);
    (h2.conn_id, secret)
}

#[tokio::test]
async fn tcp_transport_echo() {
    let dir = std::env::temp_dir().join(format!("soup-tcp-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let uds = dir.join("s.sock");
    let _ = std::fs::remove_file(&uds);
    let logic = fake_logic_server(&uds).await;

    // 引擎:UDP 也开(对照组),TCP 启用。
    let cfg = EngineConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        uds_path: uds.clone(),
        udp_workers: 1,
        tcp_bind_addr: Some("127.0.0.1:0".parse().unwrap()),
        ..Default::default()
    };
    let engine = Arc::new(Engine::new(cfg));
    let (tx, rx) = watch::channel(false);
    tokio::spawn({
        let engine = engine.clone();
        async move {
            let _ = engine.run_with_shutdown(rx).await;
        }
    });
    // 等 TCP 与 UDP 都绑定 + 逻辑服连上。
    let tcp_addr = loop {
        if let Some(a) = engine.tcp_addr() {
            break a;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let _udp_addr = engine.local_addr().unwrap();
    for _ in 0..100 {
        if engine.logic_online() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // ── TCP 客户端:握手 → 发 ping → 收 echo。──
    let mut client = TcpClient::connect(tcp_addr).await;
    let (conn_id, secret) = handshake_tcp(&mut client).await;
    assert_eq!(conn_id, 1, "首个会话 conn_id 应为 1");

    let payload = b"tcp-hello";
    let h = DatagramHeader {
        version: VERSION,
        flags: FLAG_HMAC,
        conn_id,
        seq: 0,
        ack: 0,
        ack_bits: 0,
    };
    let mut pkt = Vec::with_capacity(64);
    encode(&h, &[FrameRef::new(CH_RELIABLE_ORDERED, 1, payload)], &mut pkt).unwrap();
    let mac = hmac4(&secret, &pkt);
    pkt.extend_from_slice(&mac);
    client.send_datagram(&pkt).await;

    // 等 echo(带重传窗口)。
    let mut echoed = false;
    for _ in 0..50 {
        let r = tokio::time::timeout(Duration::from_millis(200), client.recv_datagram())
            .await
            .unwrap_or_else(|_| vec![]);
        if r.is_empty() {
            continue;
        }
        // 剥离 HMAC(下行都带)。
        let raw = if r.len() >= 20 && r[3] & FLAG_HMAC != 0 {
            let (d, m) = r.split_at(r.len() - 4);
            assert_eq!(hmac4(&secret, d), m, "下行 HMAC 校验失败");
            d.to_vec()
        } else {
            r
        };
        let (_, frames) = decode(&raw).unwrap();
        for f in frames {
            if f.ch == CH_RELIABLE_ORDERED && f.body == payload {
                echoed = true;
            }
        }
        if echoed {
            break;
        }
    }
    assert!(echoed, "TCP 客户端应收到逻辑服 echo");

    // ── 同协议对照:UDP 客户端同时连通(两传输不冲突)。──
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let (udp_conn_id, _) = common::handshake(&sock, engine.local_addr().unwrap()).await;
    assert_eq!(udp_conn_id, 2, "UDP 会话与 TCP 会话独立编号");
    tx.send(true).unwrap();
    let _ = logic.await;
}
