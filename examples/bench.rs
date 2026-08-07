//! bench —— 生命周期 benchmark(调研落地配套,见 docs/BENCHMARK.md)。
//!
//! 场景:
//!   connect   并发连接建立速率与耗时(握手 p50/p99)
//!   recycle   断线回收:会话静默后按 RTT 自适应窗口回收的耗时(回收=SessionClose)
//!   restart   逻辑服重启恢复:杀逻辑服 → 重连(指数退避)→ SessionResume 补发耗时
//!
//! 用法:
//! ```bash
//! cargo run --release --example bench -- --scenario connect --clients 200
//! cargo run --release --example bench -- --scenario recycle --clients 50
//! cargo run --release --example bench -- --scenario restart
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use soup_engine::protocol::datagram::{decode, encode, handshake_header, DatagramHeader, FrameRef};use soup_engine::protocol::frame::{self};
use soup_engine::protocol::types::*;
use soup_engine::transport::uds::{UdsReader, UdsWriter};
use soup_engine::{Engine, EngineConfig};
use tokio::net::UdpSocket;

struct Args {
    scenario: String,
    clients: usize,
    duration_secs: u64,
    /// recycle 场景的窗口缩放(默认 1.0;0.1 = 窗口×0.1,快速出数值)。
    grace_scale: f64,
}

fn parse_args() -> Args {
    let mut args = Args {
        scenario: "echo".into(),
        clients: 100,
        duration_secs: 5,
        grace_scale: 1.0,
    };
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        let v = it.next().unwrap_or_default();
        match k.as_str() {
            "--scenario" => args.scenario = v,
            "--clients" => args.clients = v.parse().unwrap_or(100),
            "--duration" => args.duration_secs = v.parse().unwrap_or(5),
            "--grace-scale" => args.grace_scale = v.parse().unwrap_or(1.0),
            _ => {}
        }
    }
    args
}

/// 客户端三拍握手(带重试,连接风暴下 UDP 丢包是常态),返回 (conn_id, secret)。
async fn handshake(client: &UdpSocket, engine_addr: std::net::SocketAddr) -> (u32, [u8; 8]) {
    let token: &[u8] = b"bench-bench-bench"; // 足够长:request ≥ challenge(防放大检查)
    for attempt in 0..3 {
        let mut buf = Vec::with_capacity(64);
        encode(&handshake_header(), &[FrameRef::new(0, 0, token)], &mut buf).unwrap();
        client.send_to(&buf, engine_addr).await.unwrap();
        let mut rbuf = [0u8; MTU];
        let Ok((n, _)) = tokio::time::timeout(Duration::from_millis(800), client.recv_from(&mut rbuf))
            .await
            .map_err(|_| ())
            .and_then(|r| r.map_err(|_| ()))
        else {
            // 超时重试
            continue; // 超时重试
        };
        let Ok((_, frames)) = decode(&rbuf[..n]) else {
            continue;
        };
        let Some(challenge) = frames.first() else {
            continue;
        };
        let challenge = challenge.body.to_vec();
        let mut body = Vec::with_capacity(8 + token.len());
        body.extend_from_slice(&challenge);
        body.extend_from_slice(token);
        let mut buf2 = Vec::with_capacity(64);
        encode(&handshake_header(), &[FrameRef::new(0, 0, &body)], &mut buf2).unwrap();
        client.send_to(&buf2, engine_addr).await.unwrap();
        let Ok((n, _)) = tokio::time::timeout(Duration::from_millis(800), client.recv_from(&mut rbuf))
            .await
            .map_err(|_| ())
            .and_then(|r| r.map_err(|_| ()))
        else {
            continue;
        };
        let Ok((h2, frames2)) = decode(&rbuf[..n]) else {
            continue;
        };
        let Some(secret_frame) = frames2.first() else {
            continue;
        };
        let mut secret = [0u8; 8];
        if secret_frame.body.len() == 8 {
            secret.copy_from_slice(secret_frame.body);
        }
        let _ = attempt;
        return (h2.conn_id, secret);
    }
    panic!("握手 3 次重试后仍失败");
}

/// 简易 echo 逻辑服:收 Data 原样回(ch=2)。
async fn echo_logic(path: std::path::PathBuf) {
    let listener = tokio::net::UnixListener::bind(path).unwrap();
    let (stream, _) = listener.accept().await.unwrap();
    let (rh, wh) = tokio::io::split(stream);
    let mut reader = UdsReader::new(rh);
    let mut writer = UdsWriter::new(wh);
    while let Some(f) = reader.read_frame().await.unwrap() {
        match f.ty {
            FRAME_ENGINE_HELLO => {
                let mut hello = BytesMut::new();
                frame::logic_hello(1, 0, &mut hello).unwrap();
                writer.write_raw(&hello).await.unwrap();
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
}

/// 起本地引擎(带 echo 逻辑服),返回 (engine, 地址, uds, 关闭 tx)。
/// ⚠️ tx 必须存活(引擎通过 watch 判断关闭;drop 即退出)。
async fn local_engine(
    grace_scale: f64,
) -> (
    Arc<Engine>,
    std::net::SocketAddr,
    std::path::PathBuf,
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    // ⚠️ UDS 路径受 SUN_LEN(~104B)限制,temp_dir 在 macOS 上过长,改用
    // 当前目录下的短相对路径(进程唯一)。
    let uds = std::path::PathBuf::from(format!(".bench-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&uds);
    let logic_handle = tokio::spawn(echo_logic(uds.clone()));
    let scale = Duration::from_secs_f64(if grace_scale > 0.0 { grace_scale } else { 1.0 });
    let cfg = EngineConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        uds_path: uds.clone(),
        udp_workers: 2,
        session: soup_engine::session::table::SessionTableConfig {
            // 自适应窗口 × scale(默认 1.0 即 1.5s/5s、5s/20s)。
            timeout_min: Duration::from_millis(1500).mul_f64(scale.as_secs_f64().min(1.0))
                .max(Duration::from_millis(100)),
            timeout_max: Duration::from_secs(5).mul_f64(scale.as_secs_f64().min(1.0)),
            reconnect_min: Duration::from_secs(5).mul_f64(scale.as_secs_f64().min(1.0)),
            reconnect_max: Duration::from_secs(20).mul_f64(scale.as_secs_f64().min(1.0)),
            ..Default::default()
        },
        ..EngineConfig::default()
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
    // 等待逻辑服连接就绪(1s 内)。
    tokio::time::sleep(Duration::from_millis(200)).await;
    (engine, addr, uds, tx, logic_handle)
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let i = ((sorted.len() as f64) * p / 100.0).floor() as usize;
    sorted[i.min(sorted.len() - 1)]
}

/// 场景 1:连接建立速率与耗时。
async fn bench_connect(n: usize) {
    let (engine, addr, _uds, _tx, _logic) = local_engine(1.0).await;
    // 
    let mut lat = Vec::with_capacity(n);
    let mut tasks = Vec::new();
    for _ in 0..n {
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = addr;
        tasks.push(tokio::spawn(async move {
            let t0 = Instant::now();
            let (_, _secret) = handshake(&sock, addr).await;
            t0.elapsed().as_secs_f64() * 1000.0
        }));
    }
    for t in tasks {
        lat.push(t.await.unwrap());
    }
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let total = lat.iter().sum::<f64>();
    let conns_per_s = n as f64 / (total / 1000.0 / n.max(1) as f64 * n as f64).max(1e-9);
    println!("═══ connect: {n} 并发建连 ═══");
    println!("平均建连耗时 : {:.2} ms", total / n as f64);
    println!("p50          : {:.2} ms", pct(&lat, 50.0));
    println!("p99          : {:.2} ms", pct(&lat, 99.0));
    println!("连接速率(串行等价): {:.0} conn/s", 1000.0 / (total / n as f64));
    println!("引擎会话     : {}", engine.sessions().stats.snapshot().sessions_active);
    let _ = conns_per_s;
}

/// 场景 2:断线回收 —— N 会话建好后全部静默,测引擎把会话全部回收
/// (SessionClose)的耗时。回收窗口 = idle(自适应) + reconnect(自适应)。
async fn bench_recycle(n: usize, scale: f64) {
    let (engine, addr, _uds, _tx, _logic) = local_engine(scale).await;
    let mut socks = Vec::new();
    for _ in 0..n {
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let (conn_id, secret) = handshake(&sock, addr).await;
        socks.push((sock, conn_id, secret));
    }
    // 建连后短交互产生 RTT 采样(发带 HMAC 的 ping):无采样时引擎保守用
    // 20s 上限,采样后按 RTT 收紧到 ~5s。
    let mut tasks = Vec::new();
    for (sock, conn_id, secret) in socks.drain(..) {
        let addr = addr;
        let sock = std::sync::Arc::new(sock);
        tasks.push(tokio::spawn(async move {
            // 收包 task:回 ACK(引擎靠 ACK 采样 RTT → 动态窗口收紧)。
            let sock_rx = sock.clone();
            let ack_task = tokio::spawn(async move {
                let mut buf = [0u8; MTU];
                loop {
                    let Ok(r) = tokio::time::timeout(
                        Duration::from_secs(1),
                        sock_rx.recv_from(&mut buf),
                    )
                    .await
                    else {
                        break; // 1s 无下行,静默期开始
                    };
                    let Ok((n, peer)) = r else { break };
                    let Ok((h, _)) = decode(&buf[..n]) else { continue };
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
                }
            });
            // 发 3 个 ch=2 ping,触发引擎 RTT 采样。
            for i in 0..3u16 {
                let payload = format!("recycle-{i}").into_bytes();
                let h = DatagramHeader {
                    version: VERSION,
                    flags: FLAG_HMAC,
                    conn_id,
                    seq: i,
                    ack: 0,
                    ack_bits: 0,
                };
                let mut buf = Vec::with_capacity(64);
                let _ = encode(&h, &[FrameRef::new(CH_RELIABLE_ORDERED, 1, &payload)], &mut buf);
                let mac = soup_engine::session::hmac4(&secret, &buf);
                buf.extend_from_slice(&mac);
                let _ = sock.send_to(&buf, addr).await;
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
            tokio::time::sleep(Duration::from_millis(300)).await; // 等 ACK 交互
            let _ = ack_task.abort();
        }));
    }
    for t in tasks {
        let _ = t.await;
    }
    tokio::time::sleep(Duration::from_millis(400)).await; // 等 RTT 采样与 ACK 交互落定
    let active0 = engine.sessions().stats.snapshot().sessions_active;
    println!("═══ recycle: {n} 会话静默(窗口 ×{scale})═══");
    println!("建连后活跃会话: {active0}");
    let t0 = Instant::now();
    loop {
        let active = engine.sessions().stats.snapshot().sessions_active;
        if active == 0 || t0.elapsed() > Duration::from_secs(60) {
            let el = t0.elapsed().as_secs_f64();
            println!("全部回收耗时  : {el:.2} s (idle+reconnect 窗口,本机 RTT≈0.2ms → 1.5s+5s)");
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    drop(socks);
}

/// 场景 3:逻辑服重启恢复 —— 会话存活期间 kill 逻辑服,测引擎重连 +
/// SessionResume 补发耗时(指数退避:首连成功 1s 内,二次 2s 内)。
async fn bench_restart() {
    let (engine, addr, uds, _tx, logic_handle) = local_engine(1.0).await;
    // 建 1 会话。
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let (_conn_id, _secret) = handshake(&sock, addr).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // 杀逻辑服(kill -9 模拟):abort 掉连接 → 引擎读循环 EOF 感知断线。
    // 此时没有新逻辑服,引擎按指数退避重试(1s→2s→…)。
    logic_handle.abort();
    let _ = logic_handle.await;
    let _ = std::fs::remove_file(&uds);
    let t0 = Instant::now();

    // 延迟 3s 再重启逻辑服(跨过首轮 1s 退避,验证指数退避路径)。
    tokio::time::sleep(Duration::from_secs(3)).await;
    let _logic2 = tokio::spawn(echo_logic(uds.clone()));

    // 等待引擎重新连上(逻辑服重连成功标志)。
    let mut recon = false;
    while t0.elapsed() < Duration::from_secs(20) {
        if engine.logic_online() {
            recon = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    println!("═══ restart: 逻辑服 kill -9 模拟 ═══");
    println!(
        "引擎重连成功   : {} ({:.2}s 内,含 3s 停机 + 指数退避)",
        recon,
        t0.elapsed().as_secs_f64()
    );
    let _ = sock;
}

#[tokio::main]
async fn main() -> soup_engine::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .try_init();
    let args = parse_args();
    match args.scenario.as_str() {
        "connect" => bench_connect(args.clients).await,
        "recycle" => bench_recycle(args.clients, args.grace_scale).await,
        "restart" => bench_restart().await,
        other => {
            eprintln!("未知场景: {other}(可选 connect|recycle|restart)");
        }
    }
    Ok(())
}
