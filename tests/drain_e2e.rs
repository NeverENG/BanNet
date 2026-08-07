//! 生命周期 drain 测试(PlayFab 模式):引擎关闭时先广播 SessionClose
//! 给逻辑服,留收尾窗口,再退出 —— 逻辑服能感知每个玩家离开,而不是
//! 连接突然消失。

mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use soup_engine::protocol::datagram::{decode, encode, handshake_header, FrameRef};
use soup_engine::protocol::frame;
use soup_engine::protocol::types::*;
use soup_engine::transport::uds::{UdsReader, UdsWriter};
use soup_engine::{Engine, EngineConfig};
use tokio::net::UdpSocket;
use tokio::sync::watch;

/// 逻辑服:记录 SessionClose 事件(用 channel 通知测试)。
#[tokio::test]
async fn shutdown_drains_sessions() {
    let dir = std::env::temp_dir().join("soup-drain");
    std::fs::create_dir_all(&dir).unwrap();
    let uds = dir.join("s.sock");
    if uds.as_os_str().len() > 100 {
        // UDS 路径受 SUN_LEN 限制,退到短路径。
        let short = std::path::PathBuf::from("/tmp/soup-drain.sock");
        let _ = std::fs::remove_file(&short);
        let uds = short;
    }
    let _ = std::fs::remove_file(&uds);

    let listener = tokio::net::UnixListener::bind(&uds).unwrap();
    let (close_tx, mut close_rx) = tokio::sync::mpsc::channel::<u64>(8);
    let logic = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (rh, wh) = tokio::io::split(stream);
        let mut reader = UdsReader::new(rh);
        let mut writer = UdsWriter::new(wh);
        let mut closes = 0u32;
        while let Some(f) = reader.read_frame().await.unwrap() {
            match f.ty {
                FRAME_ENGINE_HELLO => {
                    let mut out = BytesMut::new();
                    frame::logic_hello(1, 0, &mut out).unwrap();
                    writer.write_raw(&out).await.unwrap();
                }
                FRAME_SESSION_CLOSE => {
                    let (sid, _reason) = frame::parse_session_close(&f.body).unwrap();
                    closes += 1;
                    let _ = close_tx.send(sid).await;
                }
                _ => {}
            }
        }
        closes
    });

    let cfg = EngineConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        uds_path: uds.clone(),
        udp_workers: 1,
        ..Default::default()
    };
    let engine = Arc::new(Engine::new(cfg));
    let (tx, rx) = watch::channel(false);
    let run = tokio::spawn({
        let engine = engine.clone();
        async move {
            let _ = engine.run_with_shutdown(rx).await;
        }
    });
    // 等 UDP 绑定 + 逻辑服连上。
    let addr = loop {
        if let Some(a) = engine.local_addr() {
            break a;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    for _ in 0..100 {
        if engine.logic_online() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // 两个客户端握手建会话。
    let sock1 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let (cid1, _) = common::handshake(&sock1, addr).await;
    let sock2 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let (cid2, _) = common::handshake(&sock2, addr).await;
    assert_eq!(cid1, 1);
    assert_eq!(cid2, 2);

    // 触发引擎关闭 → drain 广播 SessionClose。
    tx.send(true).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), run).await;

    // 逻辑服应收到 2 条 SessionClose(1 和 2,顺序无关)。
    let mut got = Vec::new();
    for _ in 0..2 {
        match tokio::time::timeout(Duration::from_secs(1), close_rx.recv()).await {
            Ok(Some(sid)) => got.push(sid),
            other => {
                panic!("drain 未收到全部 SessionClose: got={got:?} other={other:?}");
            }
        }
    }
    got.sort_unstable();
    assert_eq!(got, vec![1, 2], "drain 应广播两个会话的 SessionClose");
    let _ = logic.await;
}
