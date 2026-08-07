//! E1 出口验收:端到端 echo(规格书 T0002M09「E1 完成即可与 Go 侧联调」)。
//!
//! 全链路:模拟客户端(UDP,带 HMAC)⇄ engine ⇄ 扮演逻辑服的 UDS 服务端。
//! 逻辑服把收到的 Data 原样回给该会话 —— 等价于 Go 侧 echo 一条消息。

mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use soup_engine::protocol::datagram::{decode, DatagramHeader, FrameRef};
use soup_engine::protocol::frame::self;
use soup_engine::protocol::types::*;
use soup_engine::transport::uds::{UdsReader, UdsWriter};
use soup_engine::{Engine, EngineConfig};
use tokio::net::UdpSocket;

/// 扮演逻辑服:读帧,把 Data(ch=2)原样回 Send。
async fn fake_logic_server(path: &std::path::Path) -> tokio::task::JoinHandle<()> {
    let listener = tokio::net::UnixListener::bind(path).unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (rh, wh) = tokio::io::split(stream);
        let mut reader = UdsReader::new(rh);
        let mut writer = UdsWriter::new(wh);
        // 读帧循环:EngineHello → (SessionOpen) → Data → 回 Send
        while let Some(f) = reader.read_frame().await.unwrap() {
            match f.ty {
                FRAME_ENGINE_HELLO => {
                    let mut out = BytesMut::new();
                    frame::logic_hello(1, 0, &mut out).unwrap();
                    writer.write_raw(&out).await.unwrap();
                }
                FRAME_SESSION_OPEN => {
                    tracing::info!("逻辑服:SessionOpen");
                }
                FRAME_DATA_UP => {
                    let (sid, ch, msg_id, payload) = frame::parse_data(&f.body).unwrap();
                    if ch == CH_RELIABLE_ORDERED && payload == b"hello soup" {
                        // echo 回 Send
                        writer
                            .write_frame(&frame::send(sid, ch, msg_id, payload))
                            .await
                            .unwrap();
                    }
                }
                _ => {}
            }
        }
    })
}

#[tokio::test]
async fn e2e_echo_roundtrip() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    // ── 逻辑服先就位 ──
    let uds_path =
        std::env::temp_dir().join(format!("soup{}-e2e.sock", std::process::id() % 100000));
    let _ = std::fs::remove_file(&uds_path);
    let logic_handle = fake_logic_server(&uds_path).await;

    // ── 引擎(Arc 持有引用,测试可查询 local_addr)──
    let cfg = EngineConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        uds_path: uds_path.clone(),
        udp_workers: 2,
        ..EngineConfig::default()
    };
    let engine = Arc::new(Engine::new(cfg));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let engine_task = tokio::spawn({
        let engine = engine.clone();
        async move {
            let _ = engine.run_with_shutdown(shutdown_rx).await;
        }
    });

    // 等引擎绑定完成。
    let engine_addr = loop {
        if let Some(a) = engine.local_addr() {
            break a;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    // ── 模拟客户端:握手(拿 conn_id + secret)──
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let (conn_id, secret) = common::handshake(&client, engine_addr).await;

    // ── 发一条业务消息(ch=2 可靠有序,带 HMAC;首帧 seq 必须从 0 开始)──
    let header = DatagramHeader {
        version: VERSION,
        flags: 0,
        conn_id,
        seq: 0,
        ack: 0,
        ack_bits: 0,
    };
    let buf = common::build_packet(
        &header,
        &[FrameRef::new(CH_RELIABLE_ORDERED, 1, b"hello soup")],
        &secret,
    );
    client.send_to(&buf, engine_addr).await.unwrap();

    // ── 收 echo(可能先收到心跳空包,循环过滤)──
    let mut rbuf = [0u8; MTU];
    let mut echoed = false;
    for _ in 0..50 {
        let (n, _) = tokio::time::timeout(Duration::from_secs(5), client.recv_from(&mut rbuf))
            .await
            .expect("收 echo 超时")
            .unwrap();
        // 校验并剥离 HMAC。
        let Some(clean) = common::verify_strip(&rbuf[..n], &secret) else {
            continue;
        };
        if let Ok((h, frames)) = decode(clean) {
            if h.conn_id == conn_id && !frames.is_empty() {
                assert_eq!(frames[0].ch, CH_RELIABLE_ORDERED);
                assert_eq!(frames[0].msg_id, 1);
                assert_eq!(frames[0].body, b"hello soup");
                echoed = true;
                break;
            }
        }
    }
    assert!(echoed, "未收到 echo 回复");

    // ── 收尾 ──
    shutdown_tx.send(true).unwrap();
    engine_task.abort();
    logic_handle.abort();
    let _ = std::fs::remove_file(&uds_path);
}
