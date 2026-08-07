//! engineload —— 压测工具(规格书 T0002M08F03)。
//!
//! 模拟 N 个客户端 × 指定 pps,输出吞吐、RTT 分布与引擎指标。
//!
//! ```bash
//! cargo run --example engineload -- --clients 8 --pps 50 --duration 10
//! ```
//!
//! 默认本地起引擎 + echo 逻辑服;也可以 `--addr 127.0.0.1:PORT` 打远端引擎。

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use soup_engine::protocol::datagram::{
    decode, encode, handshake_header, DatagramHeader, FrameRef, HEADER_LEN,
};
use soup_engine::protocol::frame::{self};
use soup_engine::transport::uds::{UdsReader, UdsWriter};
use soup_engine::protocol::types::*;
use soup_engine::session::hmac4;
use soup_engine::{Engine, EngineConfig};
use tokio::net::UdpSocket;

struct Args {
    clients: usize,
    pps: u64,
    duration_secs: u64,
    addr: Option<String>,
}

fn parse_args() -> Args {
    let mut args = Args {
        clients: 4,
        pps: 20,
        duration_secs: 5,
        addr: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        let v = it.next().unwrap_or_default();
        match k.as_str() {
            "--clients" => args.clients = v.parse().unwrap_or(4),
            "--pps" => args.pps = v.parse().unwrap_or(20),
            "--duration" => args.duration_secs = v.parse().unwrap_or(5),
            "--addr" => args.addr = Some(v),
            _ => {}
        }
    }
    args
}

/// 握手,返回 (conn_id, secret)。
async fn handshake(client: &UdpSocket, engine_addr: std::net::SocketAddr) -> (u32, [u8; 8]) {
    let token: &[u8] = b"engineload";
    let mut buf = Vec::with_capacity(64);
    encode(&handshake_header(), &[FrameRef::new(0, 0, token)], &mut buf).unwrap();
    client.send_to(&buf, engine_addr).await.unwrap();
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
    client.send_to(&buf2, engine_addr).await.unwrap();
    let (n, _) = tokio::time::timeout(Duration::from_secs(5), client.recv_from(&mut rbuf))
        .await
        .unwrap()
        .unwrap();
    let (h2, frames2) = decode(&rbuf[..n]).unwrap();
    let mut secret = [0u8; 8];
    secret.copy_from_slice(frames2.first().unwrap().body);
    (h2.conn_id, secret)
}

/// 内置 echo 逻辑服:收到 Data(ch=2)原样回。
async fn echo_logic(path: &std::path::Path) -> tokio::task::JoinHandle<()> {
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
                    if ch == CH_RELIABLE_ORDERED {
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

/// 单客户端负载:发 ping → 收 echo,统计 RTT。
///
/// 一个 socket、一个收包 task:收到 echo 匹配 pending 记 RTT,其余回 ACK。
/// ⚠️ 不能「主循环 try_recv + 后台 task recv」双收 —— 包会被任一方抢走。
async fn client_worker(
    id: usize,
    engine_addr: std::net::SocketAddr,
    pps: u64,
    duration: Duration,
    rtts: Arc<std::sync::Mutex<Vec<f64>>>,
) -> (u64, u64) {
    let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let (conn_id, secret) = handshake(&sock, engine_addr).await;
    let interval = Duration::from_micros(1_000_000 / pps.max(1));

    // 待确认 ping:seq → 发送时刻(ack_task 与主循环共享)。
    let pending: Arc<std::sync::Mutex<std::collections::HashMap<u16, Instant>>> =
        Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    let prefix = format!("ping-{id}-");

    // 唯一收包 task:回 ACK(连续交付语义)+ 匹配 echo 记 RTT。
    let sock_rx = sock.clone();
    let pending_rx = pending.clone();
    let rtts_rx = rtts.clone();
    let secret_rx = secret;
    let prefix_rx = prefix.clone();
    let rx_task = tokio::spawn(async move {
        let mut buf = [0u8; MTU];
        // ack_pos = 已连续收到的最后一个 seq;从 -1 开始,收到 0 才推进到 0。
        let mut ack_pos = u16::MAX;
        let mut seen: std::collections::VecDeque<u16> = std::collections::VecDeque::new();
        loop {
            let (n, peer) = sock_rx.recv_from(&mut buf).await.unwrap();
            let raw = &buf[..n];
            // 校验并剥离 HMAC。
            let data = if raw.len() >= 20 && raw[3] & FLAG_HMAC != 0 {
                let (d, m) = raw.split_at(raw.len() - 4);
                if hmac4(&secret_rx, d) != m {
                    continue;
                }
                d
            } else {
                raw
            };
            let Ok((h, frames)) = decode(data) else {
                continue;
            };
            // echo 匹配。
            for f in frames {
                if f.ch == CH_RELIABLE_ORDERED {
                    let p = String::from_utf8_lossy(f.body);
                    if let Some(rest) = p.strip_prefix(&prefix_rx) {
                        if let Ok(sq) = rest.parse::<u16>() {
                            if let Some(t0) = pending_rx.lock().unwrap().remove(&sq) {
                                let rtt = t0.elapsed().as_secs_f64() * 1000.0;
                                rtts_rx.lock().unwrap().push(rtt);
                            }
                        }
                    }
                }
            }
            // 记录收到并推进连续位置,回 ACK。
            if !seen.contains(&h.seq) {
                seen.push_back(h.seq);
                while seen.len() > 512 {
                    seen.pop_front();
                }
            }
            while seen.contains(&ack_pos.wrapping_add(1)) {
                ack_pos = ack_pos.wrapping_add(1);
            }
            let mut ack_bits = 0u32;
            for i in 1..=32u32 {
                if seen.contains(&ack_pos.wrapping_add(i as u16)) {
                    ack_bits |= 1 << (i - 1);
                }
            }
            let resp = DatagramHeader {
                version: VERSION,
                flags: FLAG_PURE_ACK,
                conn_id: h.conn_id,
                seq: 0,
                ack: ack_pos,
                ack_bits,
            };
            let mut out = Vec::with_capacity(HEADER_LEN);
            if encode(&resp, &[], &mut out).is_ok() {
                let _ = sock_rx.send_to(&out, peer).await;
            }
        }
    });
    let _ = rx_task;

    // 压测主循环:只发 ping(seq 从 0 递增)。
    let start = Instant::now();
    let mut sent = 0u64;
    let mut seq = 0u16;
    while start.elapsed() < duration {
        let payload = format!("ping-{id}-{seq}").into_bytes();
        let header = DatagramHeader {
            version: VERSION,
            flags: FLAG_HMAC,
            conn_id,
            seq,
            ack: 0,
            ack_bits: 0,
        };
        let mut buf = Vec::with_capacity(HEADER_LEN + 4 + payload.len());
        encode(&header, &[FrameRef::new(CH_RELIABLE_ORDERED, 1, &payload)], &mut buf).unwrap();
        // 附加 4 字节截断 HMAC。
        let mac = hmac4(&secret, &buf);
        buf.extend_from_slice(&mac);
        sock.send_to(&buf, engine_addr).await.unwrap();
        pending.lock().unwrap().insert(seq, Instant::now());
        sent += 1;
        seq = seq.wrapping_add(1);
        tokio::time::sleep(interval).await;
    }
    let lost = pending.lock().unwrap().len() as u64;
    (sent, lost)
}
#[tokio::main]
async fn main() -> soup_engine::Result<()> {
    let args = parse_args();
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init();

    // 引擎地址:--addr 或本地起一套。
    let (engine_addr, engine_opt): (std::net::SocketAddr, Option<(Arc<Engine>, tokio::sync::watch::Sender<bool>)>) =
        if let Some(a) = &args.addr {
            (a.parse().expect("--addr 格式: 127.0.0.1:PORT"), None)
        } else {
        let uds_path =
            std::env::temp_dir().join(format!("engineload-{}.sock", std::process::id()));
        let _ = std::fs::remove_file(&uds_path);
        let _logic = echo_logic(&uds_path).await;
        let engine = Arc::new(Engine::new(EngineConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            uds_path: uds_path.clone(),
            udp_workers: 2,
            ..EngineConfig::default()
        }));
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
        // ⚠️ 必须持有 tx:watch sender 一旦 drop,rx.changed() 立即返回,引擎退出。
        (addr, Some((engine, tx)))
    };

    eprintln!(
        "engineload: {} 客户端 × {} pps × {}s → {engine_addr}",
        args.clients, args.pps, args.duration_secs
    );

    let rtts: Arc<std::sync::Mutex<Vec<f64>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut tasks = Vec::new();
    for id in 0..args.clients {
        let rtts = rtts.clone();
        tasks.push(tokio::spawn(async move {
            client_worker(
                id,
                engine_addr,
                args.pps,
                Duration::from_secs(args.duration_secs),
                rtts,
            )
            .await
        }));
    }

    let start = Instant::now();
    let mut total_sent = 0u64;
    let mut total_lost = 0u64;
    for t in tasks {
        let (sent, lost) = t.await.unwrap();
        total_sent += sent;
        total_lost += lost;
    }
    let elapsed = start.elapsed().as_secs_f64();

    // 统计输出。
    let mut rtts = rtts.lock().unwrap();
    rtts.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = rtts.len();
    let (p50, p99) = if n > 0 {
        (
            rtts[n / 2],
            rtts[((n as f64 * 0.99) as usize).min(n - 1)],
        )
    } else {
        (0.0, 0.0)
    };
    eprintln!("═══ engineload 结果 ═══");
    eprintln!("总发送      : {total_sent} 消息 / {elapsed:.1}s");
    eprintln!("吞吐        : {:.0} msg/s", total_sent as f64 / elapsed);
    eprintln!("echo 完成   : {} (RTT 样本 {n})", n);
    eprintln!("未确认(丢/超时): {total_lost}");
    eprintln!("RTT p50     : {p50:.1} ms");
    eprintln!("RTT p99     : {p99:.1} ms");
    // 引擎指标(仅本地引擎)。
    if let Some((engine, tx)) = engine_opt.as_ref() {
        let st = engine.sessions().stats.snapshot();
        eprintln!("引擎指标    : sessions={} pkt_in={} pkt_out={} pkt_bad={} dropped_up={} retransmits={} acks_seen={}",
            st.sessions_active, st.pkt_in, st.pkt_out, st.pkt_bad, st.dropped_up, st.retransmits, st.acks_seen);
        // 压测结束,关引擎。
        let _ = tx.send(true);
    }
    Ok(())
}
