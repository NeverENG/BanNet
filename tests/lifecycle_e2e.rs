//! E3 韧性验收(规格书 T0002M08F02):
//!
//! 1. 断连后重连:客户端 6s 不发包(>5s 宽限期),引擎不发 SessionClose;
//!    重连(带原 conn_id)后逻辑服收到 SessionResume。
//! 2. NAT 重绑定:客户端换 socket(新 IP:Port)凭 conn_id 无缝续接,
//!    逻辑服不会收到新的 SessionOpen。
//! 3. 逻辑服热重启:逻辑服掉线后引擎自动重连,补发 EngineHello +
//!    对存活会话的 SessionResume,客户端全程不掉线。

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use soup_engine::protocol::datagram::{DatagramHeader, FrameRef};
use soup_engine::protocol::frame::{self};
use soup_engine::transport::uds::{UdsReader, UdsWriter};
use soup_engine::protocol::types::*;
use soup_engine::{Engine, EngineConfig};
use tokio::net::UdpSocket;

mod common;

/// 记录逻辑服收到的全部事件(用于断言)。
#[derive(Debug, Clone, PartialEq)]
enum LogicEvent {
    Hello,
    Open(u64),
    Close(u64),
    Resume(u64),
    Data(u64, u8),
}

/// 扮演逻辑服:循环 accept(首连 + 热重启各一轮),事件经 mpsc 推给测试。
/// 返回 (事件接收端, 踢连接通知):收到通知即断开当前连接(模拟 kill -9)。
async fn fake_logic(
    path: &std::path::Path,
) -> (
    tokio::sync::mpsc::Receiver<LogicEvent>,
    Arc<tokio::sync::Notify>,
) {
    let listener = tokio::net::UnixListener::bind(path).unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let kick = Arc::new(tokio::sync::Notify::new());
    let kick2 = kick.clone();
    tokio::spawn(async move {
        // 引擎会无限重连;这里 accept 两轮(首连 + 热重启)后退出。
        for _round in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let (rh, wh) = tokio::io::split(stream);
            let mut reader = UdsReader::new(rh);
            let mut writer = UdsWriter::new(wh);
            let tx = tx.clone();
            let task = tokio::spawn(async move {
                while let Some(f) = reader.read_frame().await.unwrap() {
                    match f.ty {
                        FRAME_ENGINE_HELLO => {
                            let _ = tx.send(LogicEvent::Hello).await;
                            let mut out = BytesMut::new();
                            frame::logic_hello(1, 0, &mut out).unwrap();
                            writer.write_raw(&out).await.unwrap();
                        }
                        FRAME_SESSION_OPEN => {
                            let sid = frame::parse_session_open(&f.body).unwrap().0;
                            let _ = tx.send(LogicEvent::Open(sid)).await;
                        }
                        FRAME_SESSION_CLOSE => {
                            let sid = frame::parse_session_close(&f.body).unwrap().0;
                            let _ = tx.send(LogicEvent::Close(sid)).await;
                        }
                        FRAME_SESSION_RESUME => {
                            let (sid, _) = frame::parse_session_resume(&f.body).unwrap();
                            let _ = tx.send(LogicEvent::Resume(sid)).await;
                        }
                        FRAME_DATA_UP => {
                            let (sid, ch, _, _) = frame::parse_data(&f.body).unwrap();
                            let _ = tx.send(LogicEvent::Data(sid, ch)).await;
                        }
                        _ => {}
                    }
                }
            });
            // 读循环单独 task;另一个 task 等待「踢」通知后 abort 它
            // (读半/写半随之 drop → 引擎感知断线,模拟 kill -9)。
            let abort_handle = task.abort_handle();
            let kick2 = kick2.clone();
            let kick_watcher = tokio::spawn(async move {
                kick2.notified().await;
                abort_handle.abort();
            });
            let _ = task.await;
            kick_watcher.abort();
        }
    });
    (rx, kick)
}

/// 等逻辑服事件队列里出现谓词匹配的事件(带超时)。
async fn wait_event(
    rx: &mut tokio::sync::mpsc::Receiver<LogicEvent>,
    pred: impl Fn(&LogicEvent) -> bool,
    what: &str,
) -> LogicEvent {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
            Ok(Some(ev)) if pred(&ev) => return ev,
            Ok(Some(_)) => continue,
            _ => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    panic!("等待事件超时: {what}");
}

/// 断言事件队列在 `window` 内**不出现**某类事件。
async fn assert_no_event(
    rx: &mut tokio::sync::mpsc::Receiver<LogicEvent>,
    pred: impl Fn(&LogicEvent) -> bool,
    window: Duration,
    what: &str,
) {
    let deadline = std::time::Instant::now() + window;
    while std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            Ok(Some(ev)) => {
                assert!(!pred(&ev), "不应出现事件: {what}: {ev:?}");
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_reconnect_nat_hotrestart() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    let uds_path =
        std::env::temp_dir().join(format!("soup{}-life.sock", std::process::id() % 100000));
    let _ = std::fs::remove_file(&uds_path);
    let (mut rx, kick) = fake_logic(&uds_path).await;

    let cfg = EngineConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        uds_path: uds_path.clone(),
        udp_workers: 2,
        session: soup_engine::session::table::SessionTableConfig {
            // 本测试验证状态机语义(5s 进宽限/20s 关闭),用固定窗口。
            dynamic_timeouts: false,
            ..Default::default()
        },
        ..EngineConfig::default()
    };
    let engine = Arc::new(Engine::new(cfg));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn({
        let engine = engine.clone();
        async move {
            let _ = engine.run_with_shutdown(shutdown_rx).await;
        }
    });
    let engine_addr = loop {
        if let Some(a) = engine.local_addr() {
            break a;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    // 等首连 Hello。
    wait_event(&mut rx, |e| matches!(e, LogicEvent::Hello), "首连 EngineHello").await;

    // ── 1. 建立会话 ──
    let sock1 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let (conn_id, secret) = common::handshake(&sock1, engine_addr).await;
    let sess_id = match wait_event(&mut rx, |e| matches!(e, LogicEvent::Open(_)), "SessionOpen").await {
        LogicEvent::Open(sid) => sid,
        _ => unreachable!(),
    };
    assert_ne!(conn_id, 0);

    // ── 2. 断连 6s(>5s 宽限期)→ 引擎进宽限期,但**不**发 SessionClose ──
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_no_event(
        &mut rx,
        |e| matches!(e, LogicEvent::Close(_)),
        Duration::from_secs(2),
        "宽限期内 SessionClose",
    )
    .await;

    // ── 3. NAT 重绑定:新 socket(新端口)凭原 conn_id 续接 ──
    let sock2 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    assert_ne!(
        sock2.local_addr().unwrap(),
        sock1.local_addr().unwrap(),
        "测试前提:两个 socket 端口必须不同"
    );
    let header = DatagramHeader {
        version: VERSION,
        flags: 0,
        conn_id,
        seq: 1,
        ack: 0,
        ack_bits: 0,
    };
    let buf = common::build_packet(
        &header,
        &[FrameRef::new(CH_UNRELIABLE_SEQUENCED, 7, b"resume-me")],
        &secret,
    );
    sock2.send_to(&buf, engine_addr).await.unwrap();

    // 逻辑服应收到 SessionResume(而不是新 SessionOpen)。
    wait_event(&mut rx, |e| matches!(e, LogicEvent::Resume(_)), "SessionResume").await;
    assert_no_event(
        &mut rx,
        |e| matches!(e, LogicEvent::Open(_)),
        Duration::from_secs(1),
        "重连后不应有新 SessionOpen",
    )
    .await;

    // 数据也继续可达(NAT 续接后新地址可用)。
    let hdr2 = DatagramHeader {
        version: VERSION,
        flags: 0,
        conn_id,
        seq: 2,
        ack: 0,
        ack_bits: 0,
    };
    let buf2 = common::build_packet(&hdr2, &[FrameRef::new(CH_UNRELIABLE_SEQUENCED, 8, b"after-nat")], &secret);
    sock2.send_to(&buf2, engine_addr).await.unwrap();
    wait_event(
        &mut rx,
        |e| matches!(e, LogicEvent::Data(sid, CH_UNRELIABLE_SEQUENCED) if *sid == sess_id),
        "NAT 重绑定后的 Data",
    )
    .await;

    // ── 4. 逻辑服热重启:踢掉连接(模拟 kill -9)→ 引擎自动重连 ──
    kick.notify_one();
    // 引擎重连间隔 1s,等待重连后的事件(同一 fake_logic 的第二轮 accept)。
    wait_event(&mut rx, |e| matches!(e, LogicEvent::Hello), "热重启后 EngineHello").await;
    // 补发 SessionResume(客户端全程没断)。
    wait_event(&mut rx, |e| matches!(e, LogicEvent::Resume(_)), "热重启后 SessionResume").await;

    // ── 收尾 ──
    shutdown_tx.send(true).unwrap();
    let _ = std::fs::remove_file(&uds_path);
}
