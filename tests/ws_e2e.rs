//! WebSocket 传输端到端测试:浏览器式客户端(WS 二进制消息 = datagram 报文)
//! ⇄ 引擎(WS 虚拟 peer)⇄ 逻辑服 echo。与 tcp_e2e 同协议,仅传输不同。

mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use soup_engine::protocol::datagram::{decode, encode, handshake_header, DatagramHeader, FrameRef};
use soup_engine::protocol::frame;
use soup_engine::protocol::types::*;
use soup_engine::session::hmac4;
use soup_engine::transport::uds::{UdsReader, UdsWriter};
use soup_engine::{Engine, EngineConfig};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;

/// 扮演逻辑服(与 tcp_e2e 同款)。
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
                    if ch == CH_RELIABLE_ORDERED && payload == b"ws-hello" {
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

/// WS 客户端:二进制消息 = datagram 报文(WS 自带消息边界)。
async fn connect_ws(addr: std::net::SocketAddr) -> tokio_tungstenite::WebSocketStream<TcpStream> {
    let stream = TcpStream::connect(addr).await.unwrap();
    tokio_tungstenite::client_async("ws://localhost/", stream)
        .await
        .unwrap()
        .0
}

/// WS 三拍握手(同协议,传输换 WS)。
async fn handshake_ws(ws: &mut tokio_tungstenite::WebSocketStream<TcpStream>) -> (u32, [u8; 8]) {
    let token: &[u8] = b"test-token";
    let mut buf = Vec::with_capacity(64);
    encode(&handshake_header(), &[FrameRef::new(0, 0, token)], &mut buf).unwrap();
    ws.send(Message::Binary(buf.into())).await.unwrap();

    let r1 = match ws.next().await.unwrap().unwrap() {
        Message::Binary(b) => b.to_vec(),
        _ => panic!("预期二进制消息"),
    };
    let (_, frames) = decode(&r1).unwrap();
    let challenge = frames.first().unwrap().body.to_vec();

    let mut body = Vec::with_capacity(8 + token.len());
    body.extend_from_slice(&challenge);
    body.extend_from_slice(token);
    let mut buf2 = Vec::with_capacity(64);
    encode(&handshake_header(), &[FrameRef::new(0, 0, &body)], &mut buf2).unwrap();
    ws.send(Message::Binary(buf2.into())).await.unwrap();

    let r2 = match ws.next().await.unwrap().unwrap() {
        Message::Binary(b) => b.to_vec(),
        _ => panic!("预期二进制消息"),
    };
    let (h2, frames2) = decode(&r2).unwrap();
    let mut secret = [0u8; 8];
    secret.copy_from_slice(frames2.first().unwrap().body);
    (h2.conn_id, secret)
}

#[tokio::test]
async fn ws_transport_echo() {
    let short = std::path::PathBuf::from("/tmp/soup-ws.sock");
    let _ = std::fs::remove_file(&short);
    let logic = fake_logic_server(&short).await;

    let cfg = EngineConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        uds_path: short.clone(),
        udp_workers: 1,
        tcp_bind_addr: Some("127.0.0.1:0".parse().unwrap()),
        ws_bind_addr: Some("127.0.0.1:0".parse().unwrap()),
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
    let ws_addr = loop {
        if let Some(a) = engine.ws_addr() {
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

    // ── WS 客户端:握手 → ping → echo。──
    let mut ws = connect_ws(ws_addr).await;
    let (conn_id, secret) = handshake_ws(&mut ws).await;
    assert_eq!(conn_id, 1, "WS 首个会话 conn_id 应为 1");

    let payload = b"ws-hello";
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
    ws.send(Message::Binary(pkt.into())).await.unwrap();

    let mut echoed = false;
    for _ in 0..50 {
        let msg = match tokio::time::timeout(Duration::from_millis(200), ws.next()).await {
            Ok(Some(Ok(Message::Binary(b)))) => b.to_vec(),
            _ => continue,
        };
        let raw = if msg.len() >= 20 && msg[3] & FLAG_HMAC != 0 {
            let (d, m) = msg.split_at(msg.len() - 4);
            assert_eq!(hmac4(&secret, d), m, "下行 HMAC 校验失败");
            d.to_vec()
        } else {
            msg
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
    assert!(echoed, "WS 客户端应收到逻辑服 echo");

    tx.send(true).unwrap();
    let _ = logic.await;
}
