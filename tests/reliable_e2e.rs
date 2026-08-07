//! E2 出口验收(规格书 T0002M08F02):150ms 延迟 + 5% 丢包下 Ch2 不丢不乱。
//!
//! 场景:逻辑服经引擎向客户端发 50 条 Ch2 消息(混入大消息触发分片),
//! 下行链路注入 150ms 延迟 + 5% 丢包(客户端侧模拟)。引擎可靠层
//! (ack/重传/乱序缓存/分片重组)必须保证客户端收齐且顺序一致。
//!
//! 客户端模拟器实现接收端状态机(record_seq/ack 位图/Ch2 按序交付),
//! 顺带验证协议双向可实现性(含 HMAC)。

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use rand::rngs::StdRng;
use rand::SeedableRng;
use soup_engine::protocol::datagram::{decode, encode, DatagramHeader, HEADER_LEN};
use soup_engine::protocol::frame::{self};
use soup_engine::transport::uds::{UdsReader, UdsWriter};
use soup_engine::protocol::types::*;
use soup_engine::transport::netem::Netem;
use soup_engine::{Engine, EngineConfig};
use tokio::net::UdpSocket;

const N_MSGS: u16 = 50;

/// 模拟客户端(带接收端可靠状态机)。
struct TestClient {
    sock: Arc<UdpSocket>,
    engine_addr: std::net::SocketAddr,
    conn_id: u32,
    secret: [u8; 8],
    send_seq: u16,
    recv_seq: u16,
    recv_bits: u32,
    /// Ch2 按序交付:seq → (msg_id, payload)
    reorder: BTreeMap<u16, (u16, Vec<u8>)>,
    ch2_next: Option<u16>,
    delivered_max: u16,
    delivered_seqs: std::collections::VecDeque<u16>,
    /// 分片重组缓存:group_id → (total, parts, 组首 seq)。
    frags: std::collections::HashMap<u16, (u8, Vec<Option<Vec<u8>>>, u16)>,
    /// 已完成的分片组(重传帧去重)。
    completed_frags: std::collections::VecDeque<u16>,
    /// 最近收到的全部业务帧 seq(连续 ACK 位图计算用)。
    received: std::collections::VecDeque<u16>,
    /// 已按序交付的消息。
    pub delivered: Vec<(u16, Vec<u8>)>,
}

impl TestClient {
    async fn new(
        sock: Arc<UdpSocket>,
        engine_addr: std::net::SocketAddr,
    ) -> Self {
        let (conn_id, secret) = common::handshake(&sock, engine_addr).await;
        Self {
            sock,
            engine_addr,
            conn_id,
            secret,
            send_seq: 0,
            recv_seq: 0,
            recv_bits: 0,
            reorder: BTreeMap::new(),
            // 引擎 send_seq 初值为 0:Ch2 严格按序从 0 推进。
            ch2_next: Some(0),
            delivered_max: 0,
            delivered_seqs: std::collections::VecDeque::new(),
            frags: std::collections::HashMap::new(),
            completed_frags: std::collections::VecDeque::new(),
            received: std::collections::VecDeque::new(),
            delivered: Vec::new(),
        }
    }

    /// 处理一个到达的包(收包循环调用;带下行 netem 丢包/延迟语义由调用方注入)。
    fn handle_packet(&mut self, buf: &[u8]) {
        // 校验并剥离 HMAC。
        let Some(clean) = common::verify_strip(buf, &self.secret) else {
            return;
        };
        let Ok((header, frames)) = decode(clean) else {
            return;
        };
        if header.conn_id != self.conn_id {
            return;
        }
        if frames.is_empty() {
            return; // 心跳等空包忽略
        }
        // Ch2 帧级去重与超窗丢弃(与框架等价)。分片帧不受超窗检查约束。
        let is_ch2 = frames.iter().any(|f| f.ch == CH_RELIABLE_ORDERED);
        if is_ch2 && header.flags & FLAG_FRAGMENT == 0 {
            if self.delivered_seqs.contains(&header.seq) {
                return; // 已交付的重传帧
            }
            // 超窗:< 已交付最大 seq 且不在缓存 → 丢弃(空洞重传帧必须保留)。
            if seq_lt(header.seq, self.delivered_max) && !self.reorder.contains_key(&header.seq) {
                return;
            }
        }
        record_seq(&mut self.recv_seq, &mut self.recv_bits, header.seq);
        // 记录收到(连续 ACK 位图用)。
        if !self.received.contains(&header.seq) {
            self.received.push_back(header.seq);
            while self.received.len() > 512 {
                self.received.pop_front();
            }
        }
        for f in frames {
            if f.ch == CH_RELIABLE_ORDERED {
                if header.flags & FLAG_FRAGMENT != 0 {
                    // 分片帧:重组(与框架协议一致:body = group u16 | first_seq u16 | no u8 | total u8 | chunk)。
                    let body = f.body;
                    if body.len() < 6 {
                        continue;
                    }
                    let gid = u16::from_le_bytes([body[0], body[1]]);
                    let first_seq = u16::from_le_bytes([body[2], body[3]]);
                    let frag_no = body[4];
                    let total = body[5];
                    if total == 0 || frag_no >= total {
                        continue;
                    }
                    // 已完成组的重传帧:忽略(防重复交付)。
                    if self.completed_frags.contains(&gid) {
                        continue;
                    }
                    let entry = self
                        .frags
                        .entry(gid)
                        .or_insert_with(|| (total, vec![None; total as usize], first_seq));
                    entry.1[frag_no as usize] = Some(body[6..].to_vec());
                    if entry.1.iter().all(|p| p.is_some()) {
                        let first_seq = entry.2;
                        let mut full = Vec::new();
                        for p in entry.1.drain(..) {
                            if let Some(v) = p {
                                full.extend_from_slice(&v);
                            }
                        }
                        self.frags.remove(&gid);
                        self.completed_frags.push_back(gid);
                        while self.completed_frags.len() > 128 {
                            self.completed_frags.pop_front();
                        }
                        // 组内其余 seq 已被分片帧消费,插入占位避免交付卡死。
                        for s in 0..total as u16 {
                            let s = first_seq.wrapping_add(s);
                            if s != first_seq {
                                self.reorder.insert(s, (u16::MAX, Vec::new()));
                            }
                        }
                        self.reorder.insert(first_seq, (f.msg_id, full));
                        self.deliver_in_order();
                    }
                } else {
                    // 已交付过的 seq(重传帧):忽略,防重复交付。
                    if self.delivered_seqs.contains(&header.seq) {
                        continue;
                    }
                    self.reorder.insert(header.seq, (f.msg_id, f.body.to_vec()));
                    self.deliver_in_order();
                }
            }
        }
        // 回纯 ACK(连续交付语义:ack = ch2 已连续交付位置,位图覆盖其后 32 个)。
        let ack = self.ch2_next.unwrap_or(0).wrapping_sub(1);
        let mut ack_bits = 0u32;
        for i in 1..=32u32 {
            let s = ack.wrapping_add(i as u16);
            if self.received.contains(&s) {
                ack_bits |= 1 << (i - 1);
            }
        }
        let resp = DatagramHeader {
            version: VERSION,
            flags: FLAG_PURE_ACK,
            conn_id: self.conn_id,
            seq: self.send_seq,
            ack,
            ack_bits,
        };
        let mut out = Vec::with_capacity(HEADER_LEN);
        if encode(&resp, &[], &mut out).is_ok() {
            let sock = self.sock.clone();
            let peer = self.engine_addr;
            tokio::spawn(async move {
                let _ = sock.send_to(&out, peer).await;
            });
        }
    }

    fn deliver_in_order(&mut self) {
        let Some(mut next) = self.ch2_next else {
            return;
        };
        loop {
            match self.reorder.remove(&next) {
                Some((msg_id, payload)) => {
                    self.ch2_next = Some(next.wrapping_add(1));
                    self.delivered_max = next;
                    self.delivered_seqs.push_back(next);
                    while self.delivered_seqs.len() > 512 {
                        self.delivered_seqs.pop_front();
                    }
                    // 占位条目(分片组内部 seq)跳过。
                    if msg_id != u16::MAX || !payload.is_empty() {
                        self.delivered.push((msg_id, payload));
                    }
                }
                None => break, // 空洞:等待重传。
            }
            next = next.wrapping_add(1);
        }
    }
}

/// 16bit 回绕安全 record_seq(与框架实现等价,独立验证)。
fn record_seq(recv_seq: &mut u16, recv_bits: &mut u32, seq: u16) {
    let diff = seq.wrapping_sub(*recv_seq) as i16;
    if diff <= 0 {
        let back = (-diff) as u32;
        if back >= 1 && back <= 32 {
            *recv_bits |= 1 << (back - 1);
        }
    } else {
        let shift = (diff as u32).min(32);
        *recv_bits = if shift == 32 { 1 } else { (*recv_bits << shift) | 1 };
        *recv_seq = seq;
    }
}

/// 16bit 回绕安全 `a > b`。
fn seq_newer(a: u16, b: u16) -> bool {
    (a.wrapping_sub(b) as i16) > 0
}

/// 16bit 回绕安全 `a < b`。
fn seq_lt(a: u16, b: u16) -> bool {
    (a.wrapping_sub(b) as i16) < 0
}

/// 扮演逻辑服:SessionOpen 后发 N 条 Ch2(混入大消息),记录收到的上行 Data。
async fn fake_logic(path: &std::path::Path) -> tokio::task::JoinHandle<Vec<(u16, Vec<u8>)>> {
    let listener = tokio::net::UnixListener::bind(path).unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (rh, wh) = tokio::io::split(stream);
        let mut reader = UdsReader::new(rh);
        let mut writer = UdsWriter::new(wh);
        let mut got = Vec::new();
        let mut sent = false;

        while let Some(f) = reader.read_frame().await.unwrap() {
            match f.ty {
                FRAME_ENGINE_HELLO => {
                    let mut out = BytesMut::new();
                    frame::logic_hello(1, 0, &mut out).unwrap();
                    writer.write_raw(&out).await.unwrap();
                }
                FRAME_SESSION_OPEN => {
                    let sid = frame::parse_session_open(&f.body).unwrap().0;
                    // 拿到会话后,发 N 条 Ch2(混合大消息触发分片)。
                    // 注意节奏:150ms 延迟下瞬间灌入会让引擎重传队列(上限 64)
                    // 溢出断连;真实逻辑服按 tick 节奏发送,这里每条间隔 15ms。
                    {
                        for i in 0..N_MSGS {
                            let big = i % 3 == 0;
                            let payload = if big {
                                format!("msg-{i:03}-{}", "x".repeat(1200)).into_bytes()
                            } else {
                                format!("msg-{i:03}").into_bytes()
                            };
                            writer
                                .write_frame(&frame::send(sid, CH_RELIABLE_ORDERED, i, &payload))
                                .await
                                .unwrap();
                            tokio::time::sleep(Duration::from_millis(25)).await;
                        }
                        sent = true;
                    }
                }
                FRAME_DATA_UP => {
                    let (_sid, ch, msg_id, payload) = frame::parse_data(&f.body).unwrap();
                    if ch == CH_UNRELIABLE_SEQUENCED {
                        got.push((msg_id, payload.to_vec()));
                    }
                }
                _ => {}
            }
        }
        assert!(sent, "逻辑服未成功发送消息");
        got
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ch2_reliable_under_loss() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    // ── 逻辑服 ──
    let uds_path =
        std::env::temp_dir().join(format!("soup{}-rel.sock", std::process::id() % 100000));
    let _ = std::fs::remove_file(&uds_path);
    let logic_task = fake_logic(&uds_path).await;

    // ── 引擎 ──
    let cfg = EngineConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        uds_path: uds_path.clone(),
        udp_workers: 2,
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

    // ── 客户端 + 下行网络模拟 ──
    let netem = Arc::new(Netem {
        delay: Duration::from_millis(150),
        jitter: Duration::from_millis(40),
        loss: 0.05,
        dup: 0.0,
        ..Netem::new()
    });
    let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let client = TestClient::new(sock.clone(), engine_addr).await;

    // 收包循环:下行 netem(丢包/延迟/乱序)后处理。
    let client_ref = Arc::new(tokio::sync::Mutex::new(client));
    let recv_task = tokio::spawn({
        let client_ref = client_ref.clone();
        let netem = netem.clone();
        let sock = sock.clone();
        async move {
            let mut buf = [0u8; MTU];
            let mut rng = StdRng::from_entropy();
            loop {
                let (n, _) = sock.recv_from(&mut buf).await.unwrap();
                let pkt = buf[..n].to_vec();
                // 下行丢包模拟。
                if netem.is_blackout(std::time::Instant::now()) || netem.should_drop(&mut rng) {
                    continue;
                }
                // 下行延迟(随机化 → 乱序)。
                let lat = netem.latency(&mut rng);
                let client_ref = client_ref.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(lat).await;
                    client_ref.lock().await.handle_packet(&pkt);
                });
            }
        }
    });

    // ── 等待客户端收齐 50 条 ──
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut ok = false;
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let delivered = client_ref.lock().await.delivered.len();
        if delivered >= N_MSGS as usize {
            ok = true;
            break;
        }
    }
    if !ok {
        let c = client_ref.lock().await;
        eprintln!(
            "[诊断] 超时 delivered={} ch2_next={:?} recv_seq={}",
            c.delivered.len(),
            c.ch2_next,
            c.recv_seq
        );
        let st = engine.sessions().stats.snapshot();
        eprintln!(
            "[诊断] 引擎 retransmits={} acks_seen={} pkt_out={} pkt_bad={}",
            st.retransmits, st.acks_seen, st.pkt_out, st.pkt_bad
        );
    }
    assert!(ok, "15s 内未收齐 Ch2 消息");

    // ── 校验:不丢、不乱序、payload 完整 ──
    let delivered = client_ref.lock().await.delivered.clone();
    assert_eq!(delivered.len(), N_MSGS as usize);
    for (idx, (msg_id, payload)) in delivered.iter().enumerate() {
        assert_eq!(*msg_id, idx as u16, "消息乱序: 第 {idx} 条 msg_id={msg_id}");
        let expected = if idx % 3 == 0 {
            format!("msg-{idx:03}-{}", "x".repeat(1200)).into_bytes()
        } else {
            format!("msg-{idx:03}").into_bytes()
        };
        assert_eq!(payload, &expected, "消息 {idx} payload 损坏");
    }

    // ── 收尾 ──
    shutdown_tx.send(true).unwrap();
    recv_task.abort();
    logic_task.abort();
    let _ = std::fs::remove_file(&uds_path);
    let _ = client_ref;
}
