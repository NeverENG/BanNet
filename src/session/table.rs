//! 会话表:按 `conn_id` 分片的哈希表(规格书 T0002M02「无锁」的阶段性实现)。
//!
//! 设计:
//! - 按 `conn_id` 高位分片,每片一把短临界区锁。真正的「每片单线程独占、
//!   无锁」依赖把分片绑定到固定 recv task(见模块注释),E1 先用 Mutex 保证
//!   正确性,TODO 标注无锁化路径。
//! - 会话用 `conn_id` 寻址而非 `IP:Port` —— 手机切 WiFi/蜂窝时对端地址
//!   变了照样续接(NAT 漂移,规格书 T0002M03F03)。
//! - 所有解析路径不 panic;恶意包只会得到 `Err` 或被静默丢弃。
//!
//! E2 起集成可靠层:四通道语义、seq/ack/ackbits、RTO 重传、Ch2 分片重组。

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use rand::RngCore;

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// 每包 4 字节截断 HMAC(规格书 T0002M07:防伪造 conn_id 劫持)。
pub const HMAC_LEN: usize = 4;

use crate::error::{Error, Result};
use crate::protocol::datagram::{self, DatagramHeader, FrameRef, HEADER_LEN};
use crate::protocol::types::*;
use crate::reliable::fragment::{self, FragmentAssembler};
use crate::reliable::retransmit::RetransmitQueue;
use crate::reliable::rtt::RttEstimator;

/// 会话表配置。
#[derive(Debug, Clone)]
pub struct SessionTableConfig {
    /// 分片数(通常 = 核数)。
    pub shards: usize,
    /// 握手超时(默认 3s)。
    pub handshake_timeout: Duration,
    /// 活跃会话无包进入宽限期的时长(默认 5s;dynamic_timeouts 时作下限)。
    pub idle_grace: Duration,
    /// 宽限期长度,超过即 SessionClose(默认 20s;dynamic_timeouts 时作下限)。
    pub reconnect_grace: Duration,
    /// 是否启用每包 HMAC(默认 true;与 v1 明文客户端联调时可关)。
    pub enable_hmac: bool,
    /// 断线/回收超时是否随会话 RTT 自适应(默认 true)。
    ///
    /// 调研(ENet `clamp(limit×2×RTT, min, max)` / QUIC idle timeout):
    /// 低延迟环境应快速判定断线并回收会话(资源友好),高延迟环境放宽防误杀。
    /// 启用时:idle = clamp(30×SRTT, timeout_min, timeout_max);
    /// reconnect = clamp(120×SRTT, reconnect_min, reconnect_max)。
    pub dynamic_timeouts: bool,
    /// 动态 idle 下限(默认 1.5s)。
    pub timeout_min: Duration,
    /// 动态 idle 上限(默认 5s)。
    pub timeout_max: Duration,
    /// 动态 reconnect 下限(默认 5s)。
    pub reconnect_min: Duration,
    /// 动态 reconnect 上限(默认 20s)。
    pub reconnect_max: Duration,
}

impl Default for SessionTableConfig {
    fn default() -> Self {
        Self {
            shards: DEFAULT_SESSION_SHARDS,
            handshake_timeout: HANDSHAKE_TIMEOUT,
            idle_grace: IDLE_GRACE,
            reconnect_grace: RECONNECT_GRACE,
            enable_hmac: true,
            dynamic_timeouts: true,
            timeout_min: TIMEOUT_MIN,
            timeout_max: TIMEOUT_MAX,
            reconnect_min: RECONNECT_MIN,
            reconnect_max: RECONNECT_MAX,
        }
    }
}

/// 会话状态机。
#[derive(Debug, Clone)]
enum SessionState {
    /// 正常收发。连续 `idle_grace` 无包 → 宽限期。
    Active,
    /// 重连宽限期内。收到带原 conn_id 的包 → 回到 Active 并发 Resume。
    Grace { since: Instant },
}

/// 一条会话。
#[derive(Debug, Clone)]
pub struct Session {
    pub conn_id: u32,
    pub sess_id: u64,
    /// 当前对端地址(支持 NAT 重绑定:来包地址不同则更新)。
    pub peer: SocketAddr,
    /// 握手时下发的会话密钥(E4 的 HMAC 用;先分配好)。
    pub secret: u64,
    pub token: Vec<u8>,
    /// 本端已发出的最大 seq(分配后自增)。
    pub send_seq: u16,
    /// 最近一次收包时刻(宽限期判定)。
    pub last_rx: Instant,
    state: SessionState,
    /// 逐会话带宽上限 kbps(E4 限流用)。
    pub budget_kbps: Option<u16>,
    /// 入会话时间(Resume 的 gap_ms 基准)。
    pub established: Instant,

    // ── E2 可靠层状态 ──
    /// 已收到的对端最大 seq(ack 字段用)。
    pub recv_seq: u16,
    /// ack 位图:recv_seq 之前 32 个包的收到标记。
    pub recv_bits: u32,
    /// Ch1(不可靠有序):最近交付的 seq。
    pub ch1_last: Option<u16>,
    /// Ch3(可靠无序):最近见过的 seq(去重,上限 256)。
    pub ch3_seen: VecDeque<u16>,
    /// 重传队列:[Ch2, Ch3],每通道上限 64,溢出断连。
    pub retrans: [RetransmitQueue; 2],
    /// RTT 估计与 RTO。
    pub rtt: RttEstimator,
    /// Ch2 分片重组器。
    pub assembler: FragmentAssembler,
    /// Ch2 乱序缓存:seq → (msg_id, payload);按序交付。
    pub reorder: BTreeMap<u16, (u16, Vec<u8>)>,
    /// Ch2 下一个期望交付的 seq。
    pub ch2_next: Option<u16>,
    /// Ch2 已交付的最大 seq(乱序回退时跳过已交付区)。
    pub delivered_max: u16,
    /// Ch2 最近已交付的 seq(重传帧去重,上限 512)。
    pub delivered_seqs: VecDeque<u16>,
    /// 分片组 id 分配器。
    pub frag_group_id: u16,
    /// 限流滑动窗口:近 1s 的 (时刻, 字节数)。
    pub bytes_window: VecDeque<(Instant, u64)>,
}

impl Session {
    pub(crate) fn is_active(&self) -> bool {
        matches!(self.state, SessionState::Active)
    }

    /// 计算本端 ACK 值(连续交付语义,规格书 M03F01 + yojimbo 实践):
    /// - `ack` = 已连续交付的最大 seq(ch2_next - 1;Ch2 严格按序,占位也计入)
    /// - `ack_bits` = ack 之后 32 个 seq 的收到位图(乱序缓存 / 已交付 / Ch3 已见)
    ///
    /// ⚠️ 不能用「最大收到 seq」做 ack:丢包重传跨度超过 32 时,
    /// 窗口外条目将永远无法被对端确认,重传队列死锁。
    pub fn compute_ack(&self) -> (u16, u32) {
        let ack = self.ch2_next.unwrap_or(0).wrapping_sub(1);
        let mut bits = 0u32;
        for i in 1..=32u32 {
            let seq = ack.wrapping_add(i as u16);
            if self.reorder.contains_key(&seq)
                || self.delivered_seqs.contains(&seq)
                || self.ch3_seen.contains(&seq)
            {
                bits |= 1 << (i - 1);
            }
        }
        (ack, bits)
    }
}

/// 握手挂起记录(防放大三次交换)。
#[derive(Debug)]
struct PendingHandshake {
    challenge: [u8; 8],
    created: Instant,
}

/// 会话表对外产出的事件(engine 把它们转成 UDS 帧发给逻辑服)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionEvent {
    /// 握手完成,新会话(0x01 SessionOpen)。
    Open { sess_id: u64, peer: SocketAddr, token: Vec<u8> },
    /// 会话结束(0x02 SessionClose)。
    Close { sess_id: u64, reason: u8 },
    /// 断线重连成功(0x03 SessionResume)。
    Resume { sess_id: u64, gap_ms: u32 },
    /// 一条已解可靠层、按通道语义交付的业务消息(0x10 Data)。
    Data { sess_id: u64, ch: u8, msg_id: u16, payload: Vec<u8> },
}

/// 会话关闭原因。
pub const CLOSE_GRACE_TIMEOUT: u8 = 1;
pub const CLOSE_KICKED: u8 = 2;

/// 16bit 回绕安全的 `a > b`(a 比 b 新)。
pub fn seq_newer(a: u16, b: u16) -> bool {
    (a.wrapping_sub(b) as i16) > 0
}

/// 16bit 回绕安全的 `a < b`。
pub fn seq_lt(a: u16, b: u16) -> bool {
    (a.wrapping_sub(b) as i16) < 0
}

/// 更新接收窗口:记录收到 seq,维护 recv_seq / recv_bits 位图。
fn record_seq(recv_seq: &mut u16, recv_bits: &mut u32, seq: u16) {
    let diff = seq.wrapping_sub(*recv_seq) as i16;
    if diff <= 0 {
        // 旧包(≤ 当前窗口):若在 32 窗口内,补置位。
        let back = (-diff) as u32;
        if back >= 1 && back <= 32 {
            *recv_bits |= 1 << (back - 1);
        }
    } else {
        // 新包:窗口前移。
        let shift = (diff as u32).min(32);
        *recv_bits = if shift == 32 {
            1
        } else {
            (*recv_bits << shift) | 1
        };
        *recv_seq = seq;
    }
}

/// HMAC 校验结果。
enum HmacResult<'a> {
    /// 校验通过,返回剥离 HMAC 后的数据。
    Ok(&'a [u8]),
    /// 包不带 HMAC(握手包 / 未启用)。
    NoHmac,
    /// 校验失败(伪造包)。
    Bad,
}

/// 会话表。
pub struct SessionTable {
    shards: Vec<Mutex<HashMap<u32, Session>>>,
    pending: Mutex<HashMap<SocketAddr, PendingHandshake>>,
    next_conn_id: AtomicU64,
    next_sess_id: AtomicU64,
    cfg: SessionTableConfig,
    /// 指标计数。
    pub stats: crate::stats::Stats,
}

impl SessionTable {
    pub fn new(cfg: SessionTableConfig) -> Self {
        let shards = (1..=cfg.shards.max(1)).map(|_| Mutex::new(HashMap::new())).collect();
        Self {
            shards,
            pending: Mutex::new(HashMap::new()),
            next_conn_id: AtomicU64::new(1),
            next_sess_id: AtomicU64::new(1),
            cfg,
            stats: crate::stats::Stats::new(),
        }
    }

    fn shard(&self, conn_id: u32) -> usize {
        (conn_id as usize) % self.shards.len()
    }

    fn get(&self, conn_id: u32) -> Option<Session> {
        self.shards[self.shard(conn_id)]
            .lock()
            .unwrap()
            .get(&conn_id)
            .cloned()
    }

    /// 校验包 HMAC(规格书 M07:每包带 4 字节截断 HMAC,防伪造 conn_id 劫持)。
    fn verify_hmac<'a>(&self, buf: &'a [u8], _now: Instant) -> HmacResult<'a> {
        if !self.cfg.enable_hmac || buf.len() < HEADER_LEN {
            return HmacResult::NoHmac;
        }
        let flags = buf[3];
        // 握手包无 HMAC(还没有会话密钥);未带 HMAC 标志的包按明文处理(兼容)。
        if flags & (FLAG_HANDSHAKE | FLAG_HMAC) != FLAG_HMAC {
            return HmacResult::NoHmac;
        }
        if buf.len() < HEADER_LEN + HMAC_LEN {
            return HmacResult::Bad;
        }
        let conn_id = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        let Some(session) = self.get(conn_id) else {
            return HmacResult::Bad;
        };
        let (data, mac) = buf.split_at(buf.len() - HMAC_LEN);
        let expect = hmac4(&session.secret.to_le_bytes(), data);
        if mac == expect {
            HmacResult::Ok(data)
        } else {
            HmacResult::Bad
        }
    }

    /// 活跃会话数(指标用)。
    pub fn active_count(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().unwrap().len())
            .sum()
    }

    // ── 入站数据报处理 ──

    /// 处理一个来自客户端的 UDP 数据报。返回要发给逻辑服的事件列表。
    ///
    /// `send_back`:用于把响应数据报送回客户端的回调(传输层注入,
    /// 握手回包 / 纯 ACK 都走这里,保持本模块纯逻辑、可单测)。
    pub fn handle_datagram(
        &self,
        peer: SocketAddr,
        buf: &[u8],
        send_back: &mut dyn FnMut(&[u8], SocketAddr),
        now: Instant,
    ) -> Vec<SessionEvent> {
        let mut events = Vec::new();

        // ── HMAC 校验(规格书 M07):握手包无 HMAC;数据包带 4 字节截断 HMAC。──
        let buf = match self.verify_hmac(buf, now) {
            HmacResult::Ok(b) => b,
            HmacResult::NoHmac => buf, // 握手包或未启用
            HmacResult::Bad => {
                self.stats.pkt_bad.incr();
                tracing::debug!(%peer, "HMAC 校验失败,丢弃");
                return events;
            }
        };

        // 解析失败:静默丢弃并计数(恶意包),绝不 panic。
        let (header, frames) = match datagram::decode(buf) {
            Ok(v) => v,
            Err(e) => {
                self.stats.pkt_bad.incr();
                tracing::debug!(%peer, error = %e, "丢弃畸形数据报");
                return events;
            }
        };
        self.stats.pkt_in.incr();

        if header.flags & FLAG_HANDSHAKE != 0 {
            self.handle_handshake(peer, &header, &frames, send_back, now, &mut events);
        } else if header.conn_id == 0 {
            // 非握手包却带着 conn_id=0:非法,丢弃。
            self.stats.pkt_bad.incr();
        } else {
            self.handle_session_packet(peer, &header, &frames, send_back, now, &mut events);
        }
        events
    }

    /// 握手三次交换(v1 明文,规格书 T0002M03F04 加密留位):
    /// 1. C→S request(flags=握手, body=token)
    /// 2. S→C challenge(8B 随机;回包 ≤ 收包,防放大)
    /// 3. C→S response(body = challenge + token) → 校验通过则建会话
    fn handle_handshake(
        &self,
        peer: SocketAddr,
        _header: &DatagramHeader,
        frames: &[FrameRef],
        send_back: &mut dyn FnMut(&[u8], SocketAddr),
        now: Instant,
        events: &mut Vec<SessionEvent>,
    ) {
        // 握手数据约定放在第一个 frame 的 body。
        let Some(body) = frames.first().map(|f| f.body) else {
            self.stats.pkt_bad.incr();
            return;
        };

        // 清理过期 pending(顺带做,避免单独维护定时器)。
        self.pending.lock().unwrap().retain(|_, p| {
            now.duration_since(p.created) < self.cfg.handshake_timeout
        });

        let mut pending = self.pending.lock().unwrap();
        if let Some(rec) = pending.get(&peer) {
            // 已有 challenge → 视为 response:body = challenge(8B) + token
            if body.len() < 8 {
                self.stats.pkt_bad.incr();
                return;
            }
            let (chal, token) = body.split_at(8);
            if chal != rec.challenge {
                // challenge 不匹配:伪造或重放,丢弃。
                self.stats.pkt_bad.incr();
                return;
            }
            pending.remove(&peer);

            // 分配 conn_id + sess_id,建立会话。
            let conn_id = self.next_conn_id.fetch_add(1, Ordering::Relaxed) as u32;
            let sess_id = self.next_sess_id.fetch_add(1, Ordering::Relaxed);
            let mut secret = [0u8; 8];
            rand::thread_rng().fill_bytes(&mut secret);
            let session = Session {
                conn_id,
                sess_id,
                peer,
                secret: u64::from_le_bytes(secret),
                token: token.to_vec(),
                send_seq: 0,
                last_rx: now,
                state: SessionState::Active,
                budget_kbps: None,
                established: now,
                recv_seq: 0,
                recv_bits: 0,
                ch1_last: None,
                ch3_seen: VecDeque::new(),
                retrans: [RetransmitQueue::new(), RetransmitQueue::new()],
                rtt: RttEstimator::new(),
                assembler: FragmentAssembler::new(),
                reorder: BTreeMap::new(),
                // Ch2 按序交付从会话首个出站 seq(0)开始:引擎 send_seq 初值为 0。
                ch2_next: Some(0),
                delivered_max: 0,
                delivered_seqs: VecDeque::new(),
                frag_group_id: 0,
                bytes_window: VecDeque::new(),
            };
            self.shards[self.shard(conn_id)]
                .lock()
                .unwrap()
                .insert(conn_id, session);
            self.stats.sessions_active.incr();

            // 回握手确认包(带上 conn_id 与 session_secret,规格书 M07),
            // 此后客户端用 conn_id 通信、用 secret 计算每包 HMAC。
            let resp = DatagramHeader {
                version: VERSION,
                flags: FLAG_HANDSHAKE,
                conn_id,
                seq: 0,
                ack: 0,
                ack_bits: 0,
            };
            let secret_frame = FrameRef::new(0, 0, &secret);
            let mut out = Vec::with_capacity(HEADER_LEN + 4 + 8);
            if datagram::encode(&resp, &[secret_frame], &mut out).is_ok() {
                send_back(&out, peer);
            }

            events.push(SessionEvent::Open {
                sess_id,
                peer,
                token: token.to_vec(),
            });
        } else {
            // 首包 → 下发 challenge。
            let mut challenge = [0u8; 8];
            rand::thread_rng().fill_bytes(&mut challenge);
            pending.insert(
                peer,
                PendingHandshake {
                    challenge,
                    created: now,
                },
            );
            self.stats.sessions_handshaking.incr();

            let resp = DatagramHeader {
                version: VERSION,
                flags: FLAG_HANDSHAKE,
                conn_id: 0,
                seq: 0,
                ack: 0,
                ack_bits: 0,
            };
            let frame = FrameRef::new(0, 0, &challenge);
            let mut out = Vec::with_capacity(HEADER_LEN + 4 + 8);
            if datagram::encode(&resp, &[frame], &mut out).is_ok() {
                // 防放大:回包(16+12=28B)必须 ≤ 收包。token 通常更大,天然满足;
                // 但若请求比 28B 还小(空 token),则丢弃。
                if out.len() <= buf_len_hint(frames) {
                    send_back(&out, peer);
                }
            }
        }
    }

    /// 活跃/宽限期会话的数据包:ack 处理 + 四通道分发(E2)。
    fn handle_session_packet(
        &self,
        peer: SocketAddr,
        header: &DatagramHeader,
        frames: &[FrameRef],
        send_back: &mut dyn FnMut(&[u8], SocketAddr),
        now: Instant,
        events: &mut Vec<SessionEvent>,
    ) {
        let conn_id = header.conn_id;
        let shard = self.shard(conn_id);
        let mut guard = self.shards[shard].lock().unwrap();
        let Some(session) = guard.get_mut(&conn_id) else {
            self.stats.pkt_bad.incr();
            return;
        };

        // NAT 重绑定:地址变了就跟随(凭 conn_id 续接,规格书 M05)。
        if session.peer != peer {
            tracing::debug!(conn_id, %peer, "NAT 重绑定");
            session.peer = peer;
            self.stats.nat_rebind.incr();
        }

        // 宽限期 → 活跃:发 SessionResume,逻辑服应推全量状态。
        if let SessionState::Grace { since } = session.state {
            let gap_ms = since.elapsed().as_millis().min(u32::MAX as u128) as u32;
            session.state = SessionState::Active;
            session.last_rx = now;
            events.push(SessionEvent::Resume {
                sess_id: session.sess_id,
                gap_ms,
            });
            return;
        }

        session.last_rx = now;

        // 1. 处理对端 ack:清重传队列 + RTT 采样。
        let mut rtt_ms = None;
        for qi in 0..2 {
            let (acked, sample) = session.retrans[qi].on_ack(header.ack, header.ack_bits, now);
            if !acked.is_empty() {
                self.stats.acks_seen.incr_by(acked.len() as u64);
            }
            if sample.is_some() {
                rtt_ms = sample;
            }
        }
        if let Some(ms) = rtt_ms {
            session.rtt.update(ms);
        }

        // 2. 按通道分发。记录是否需要回纯 ACK。
        let mut need_ack = false;
        for f in frames {
            record_seq(&mut session.recv_seq, &mut session.recv_bits, header.seq);
            self.stats.pkt_bytes_in.incr_by(f.body.len() as u64);

            match f.ch {
                CH_UNRELIABLE_UNORDERED => {
                    // 快照下行:丢了就丢,下一帧覆盖。直接交付。
                    events.push(SessionEvent::Data {
                        sess_id: session.sess_id,
                        ch: f.ch,
                        msg_id: f.msg_id,
                        payload: f.body.to_vec(),
                    });
                }
                CH_UNRELIABLE_SEQUENCED => {
                    // 输入上行:只保留 seq 更大的,旧包直接丢弃。
                    if session.ch1_last.map_or(true, |last| seq_newer(header.seq, last)) {
                        session.ch1_last = Some(header.seq);
                        events.push(SessionEvent::Data {
                            sess_id: session.sess_id,
                            ch: f.ch,
                            msg_id: f.msg_id,
                            payload: f.body.to_vec(),
                        });
                    }
                }
                CH_RELIABLE_ORDERED => {
                    need_ack = true;
                    Self::handle_ch2(session, header, f, now, events);
                }
                CH_RELIABLE_UNORDERED => {
                    need_ack = true;
                    // 去重:最近见过的 seq 直接丢。
                    if session.ch3_seen.contains(&header.seq) {
                        continue;
                    }
                    session.ch3_seen.push_back(header.seq);
                    while session.ch3_seen.len() > 256 {
                        session.ch3_seen.pop_front();
                    }
                    events.push(SessionEvent::Data {
                        sess_id: session.sess_id,
                        ch: f.ch,
                        msg_id: f.msg_id,
                        payload: f.body.to_vec(),
                    });
                }
                _ => {
                    self.stats.pkt_bad.incr();
                }
            }
        }

        // 3. 收到可靠帧后回纯 ACK(连续交付语义,非阻塞;丢了也没事,重传会再带 ack)。
        if need_ack {
            let (ack, ack_bits) = session.compute_ack();
            let resp = DatagramHeader {
                version: VERSION,
                flags: FLAG_PURE_ACK,
                conn_id: session.conn_id,
                seq: session.send_seq,
                ack,
                ack_bits,
            };
            let mut out = Vec::with_capacity(HEADER_LEN);
            if datagram::encode(&resp, &[], &mut out).is_ok() {
                send_back(&out, peer);
            }
        }
    }

/// Ch2(可靠有序):分片重组 → 乱序缓存 → 按序交付。
fn handle_ch2(
        session: &mut Session,
        header: &DatagramHeader,
        f: &FrameRef,
        now: Instant,
        events: &mut Vec<SessionEvent>,
    ) {
        let seq = header.seq;
        // 去重:已交付过的 seq(重传帧)直接忽略。
        if session.delivered_seqs.contains(&seq) {
            return;
        }
        // 超窗旧包(仅非分片帧):< 已交付最大 seq 且不在乱序缓存 → 丢弃。
        // 分片帧的 seq 在组装中消费,不在 reorder,不能按此判断。
        if header.flags & FLAG_FRAGMENT == 0 {
            if seq_lt(seq, session.delivered_max) && !session.reorder.contains_key(&seq) {
                return;
            }
        }

        if header.flags & FLAG_FRAGMENT != 0 {
            // 分片帧:重组。收齐后按组首 seq 进入乱序缓存,统一按序交付。
            let Ok((gid, first_seq, frag_no, total, chunk)) = fragment::decode_fragment(f.body)
            else {
                return; // 畸形分片:静默丢弃
            };
            if let Some((gseq, full)) = session
                .assembler
                .push(gid, first_seq, frag_no, total, chunk, now)
            {
                // 组内其余 seq 已被本组分片帧消费,插入占位,避免按序交付卡在空洞上。
                for s in 0..total as u16 {
                    let s = gseq.wrapping_add(s);
                    if s != gseq {
                        session.reorder.insert(s, (u16::MAX, Vec::new()));
                    }
                }
                session.reorder.insert(gseq, (f.msg_id, full));
                deliver_ch2_in_order(session, events);
            }
        } else {
            session.reorder.insert(seq, (f.msg_id, f.body.to_vec()));
            deliver_ch2_in_order(session, events);
        }
    }

    // ── 维护 ──

    /// 活跃会话的实际断线判定窗口(RTT 自适应,ENet/QUIC 调研)。
    /// dynamic_timeouts 关闭时退化为固定 `idle_grace`。
    pub fn idle_grace_for(&self, s: &Session) -> Duration {
        if !self.cfg.dynamic_timeouts {
            return self.cfg.idle_grace;
        }
        let srtt = Duration::from_millis(s.rtt.srtt_ms() as u64);
        if srtt.is_zero() {
            return self.cfg.timeout_max; // 未采样:保守取上限
        }
        (srtt.saturating_mul(30)).clamp(self.cfg.timeout_min, self.cfg.timeout_max)
    }

    /// 宽限期的实际回收窗口(RTT 自适应)。
    pub fn reconnect_grace_for(&self, s: &Session) -> Duration {
        if !self.cfg.dynamic_timeouts {
            return self.cfg.reconnect_grace;
        }
        let srtt = Duration::from_millis(s.rtt.srtt_ms() as u64);
        if srtt.is_zero() {
            return self.cfg.reconnect_max;
        }
        (srtt.saturating_mul(120)).clamp(self.cfg.reconnect_min, self.cfg.reconnect_max)
    }

    /// 周期性维护(engine 的 maintenance task 调用):
    /// - 活跃超 `idle_grace_for` → 进宽限期
    /// - 宽限期超 `reconnect_grace_for` → 关闭并发 SessionClose
    pub fn maintain(&self, now: Instant) -> Vec<SessionEvent> {
        let mut events = Vec::new();
        for shard in &self.shards {
            let mut guard = shard.lock().unwrap();
            let mut to_close = Vec::new();
            for (conn_id, s) in guard.iter_mut() {
                match s.state {
                    SessionState::Active => {
                        if now.duration_since(s.last_rx) >= self.idle_grace_for(s) {
                            s.state = SessionState::Grace { since: now };
                            self.stats.sessions_grace.incr();
                            tracing::debug!(conn_id = s.conn_id, "进入重连宽限期");
                        }
                    }
                    SessionState::Grace { since } => {
                        if now.duration_since(since) >= self.reconnect_grace_for(s) {
                            to_close.push(*conn_id);
                        }
                    }
                }
            }
            for conn_id in to_close {
                if let Some(s) = guard.remove(&conn_id) {
                    self.stats.sessions_active.decr();
                    events.push(SessionEvent::Close {
                        sess_id: s.sess_id,
                        reason: CLOSE_GRACE_TIMEOUT,
                    });
                }
            }
        }
        events
    }

    /// 重传维护:对每个会话扫描可靠队列,超 RTO 的条目重发。
    /// 同时清理分片重组缓存。`sender` 由 engine 注入(非阻塞 UDP 发送)。
    pub fn maintain_reliable(&self, now: Instant, sender: &mut dyn FnMut(&[u8], SocketAddr)) {
        for shard in &self.shards {
            let mut guard = shard.lock().unwrap();
            for s in guard.values_mut() {
                let rto = s.rtt.rto();
                for qi in 0..2 {
                    let due = s.retrans[qi].retransmit_due(now, rto);
                    for (_, bytes) in due {
                        sender(&bytes, s.peer);
                        self.stats.retransmits.incr();
                    }
                }
                s.assembler.cleanup(now);
            }
        }
    }

    // ── 出站 ──

    /// 查询会话(engine 处理逻辑服的 Send/Multicast/Kick/SetBudget 用)。
    pub fn lookup(&self, sess_id: u64) -> Option<Session> {
        self.shards
            .iter()
            .filter_map(|s| s.lock().unwrap().get_by_sess_id(sess_id).cloned())
            .next()
    }

    /// 踢掉一个会话(逻辑服 Kick 或鉴权失败)。
    pub fn kick(&self, sess_id: u64, _reason: u8) -> Option<Session> {
        for shard in &self.shards {
            let mut guard = shard.lock().unwrap();
            let conn_id = guard
                .iter()
                .find(|(_, s)| s.sess_id == sess_id)
                .map(|(id, _)| *id);
            if let Some(conn_id) = conn_id {
                let s = guard.remove(&conn_id);
                self.stats.sessions_active.decr();
                return s;
            }
        }
        None
    }

    /// 设置逐会话带宽上限。
    pub fn set_budget(&self, sess_id: u64, kbps: u16) {
        for shard in &self.shards {
            let mut guard = shard.lock().unwrap();
            if let Some(s) = guard.values_mut().find(|s| s.sess_id == sess_id) {
                s.budget_kbps = Some(kbps);
                return;
            }
        }
    }

    /// 逐会话带宽上限检查(规格书 T0002M04F04 SetBudget / T0002M07 限流)。
    /// 超限返回 false(engine 丢弃并计数)。
    pub fn check_budget(&self, sess_id: u64, bytes: u64, now: Instant) -> bool {
        for shard in &self.shards {
            let mut guard = shard.lock().unwrap();
            if let Some(s) = guard.values_mut().find(|s| s.sess_id == sess_id) {
                let Some(kbps) = s.budget_kbps else {
                    return true;
                };
                // 清理 1s 前的记录。
                while let Some(&(t, _)) = s.bytes_window.front() {
                    if now.duration_since(t) >= Duration::from_secs(1) {
                        s.bytes_window.pop_front();
                    } else {
                        break;
                    }
                }
                let used: u64 = s.bytes_window.iter().map(|&(_, b)| b).sum();
                let budget_bytes = kbps as u64 * 125; // kbps → bytes/s
                if used + bytes > budget_bytes {
                    return false;
                }
                s.bytes_window.push_back((now, bytes));
                return true;
            }
        }
        true
    }

    /// 打包出站数据报(逻辑服 Send 帧 → 一个或多个 UDP 包)。
    ///
    /// - 普通帧:单个数据报;可靠通道入重传队列
    /// - Ch2 大消息:分片为多个数据报,每片独立 seq/重传
    /// - 返回 `(bytes, peer)` 列表
    pub fn pack_outbound(
        &self,
        sess_id: u64,
        ch: u8,
        msg_id: u16,
        payload: &[u8],
    ) -> Result<Vec<(Vec<u8>, SocketAddr)>> {
        if ch > 3 {
            return Err(Error::Protocol(format!("非法通道 ch={ch}")));
        }
        for shard in &self.shards {
            let mut guard = shard.lock().unwrap();
            if let Some(s) = guard.values_mut().find(|s| s.sess_id == sess_id) {
                if !s.is_active() {
                    return Err(Error::SessionNotFound(sess_id));
                }
                let now = Instant::now();
                let mut out = Vec::new();
                let (ack, ack_bits) = s.compute_ack();

                let mk_header = |seq: u16| DatagramHeader {
                    version: VERSION,
                    flags: if self.cfg.enable_hmac { FLAG_HMAC } else { 0 },
                    conn_id: s.conn_id,
                    seq,
                    ack,
                    ack_bits,
                };
                // 分片帧必须带 FLAG_FRAGMENT(接收端据此识别分片协议)。
                let mk_frag_header = |seq: u16| DatagramHeader {
                    version: VERSION,
                    flags: FLAG_FRAGMENT | if self.cfg.enable_hmac { FLAG_HMAC } else { 0 },
                    conn_id: s.conn_id,
                    seq,
                    ack,
                    ack_bits,
                };

                // 追加 4 字节截断 HMAC(覆盖 header + frames)。
                let attach_hmac = |bytes: &mut Vec<u8>| {
                    if self.cfg.enable_hmac {
                        let mac = hmac4(&s.secret.to_le_bytes(), bytes);
                        bytes.extend_from_slice(&mac);
                    }
                };

                // 仅 Ch2 支持分片重组;Ch0/1 单包超 MTU 视为编码错误。
                let reliable = ch == CH_RELIABLE_ORDERED || ch == CH_RELIABLE_UNORDERED;
                if ch == CH_RELIABLE_ORDERED && payload.len() > fragment::FRAGMENT_THRESHOLD {
                    let group_id = s.frag_group_id;
                    s.frag_group_id = s.frag_group_id.wrapping_add(1);
                    // 组内首片 seq = 当前 send_seq(占位覆盖组内 seq 范围的前提)。
                    let first_seq = s.send_seq;
                    let parts = fragment::split(payload, group_id, first_seq, fragment::FRAGMENT_THRESHOLD);
                    for part in parts {
                        let seq = s.send_seq;
                        s.send_seq = s.send_seq.wrapping_add(1);
                        let header = mk_frag_header(seq);
                        let frame = FrameRef::new(ch, msg_id, &part);
                        let mut bytes = Vec::with_capacity(HEADER_LEN + 4 + part.len());
                        datagram::encode(&header, &[frame], &mut bytes)?;
                        attach_hmac(&mut bytes);
                        s.retrans[0].push(seq, bytes.clone(), now)?;
                        out.push((bytes, s.peer));
                    }
                } else {
                    let seq = s.send_seq;
                    s.send_seq = s.send_seq.wrapping_add(1);
                    let header = mk_header(seq);
                    let frame = FrameRef::new(ch, msg_id, payload);
                    let mut bytes = Vec::with_capacity(HEADER_LEN + 4 + payload.len());
                    datagram::encode(&header, &[frame], &mut bytes)?;
                    attach_hmac(&mut bytes);
                    if reliable {
                        s.retrans[(ch - 2) as usize].push(seq, bytes.clone(), now)?;
                    }
                    out.push((bytes, s.peer));
                }

                self.stats.pkt_out.incr_by(out.len() as u64);
                let bytes_out: u64 = out.iter().map(|(b, _)| b.len() as u64).sum();
                self.stats.pkt_bytes_out.incr_by(bytes_out);
                return Ok(out);
            }
        }
        Err(Error::SessionNotFound(sess_id))
    }

    /// 会话是否活跃(E1 供 engine 心跳判断)。
    pub fn is_active(&self, sess_id: u64) -> bool {
        self.lookup(sess_id).map(|s| s.is_active()).unwrap_or(false)
    }

    /// 遍历全部会话(engine 心跳 / SessionStats / 热重启补发 Resume 用)。
    ///
    /// 回调在分片锁内执行,必须快速返回;收集数据请拷贝出需要的字段。
    pub fn for_each_session(&self, mut f: impl FnMut(&Session)) {
        for shard in &self.shards {
            let guard = shard.lock().unwrap();
            for s in guard.values() {
                f(s);
            }
        }
    }
}

/// Ch2 按序交付:从 ch2_next 严格推进,遇空洞即停(等待重传)。
/// 分片组完成时已插入占位条目,保证组内 seq 不会成为空洞。
fn deliver_ch2_in_order(session: &mut Session, events: &mut Vec<SessionEvent>) {
    let Some(mut next) = session.ch2_next else {
        return;
    };
    loop {
        match session.reorder.remove(&next) {
            Some((msg_id, payload)) => {
                session.ch2_next = Some(next.wrapping_add(1));
                session.delivered_max = next;
                session.delivered_seqs.push_back(next);
                while session.delivered_seqs.len() > 512 {
                    session.delivered_seqs.pop_front();
                }
                // 占位条目(分片组内部 seq)跳过,不产生事件。
                if msg_id != u16::MAX || !payload.is_empty() {
                    events.push(SessionEvent::Data {
                        sess_id: session.sess_id,
                        ch: CH_RELIABLE_ORDERED,
                        msg_id,
                        payload,
                    });
                }
            }
            None => break, // 空洞:等待重传,不越过。
        }
        next = next.wrapping_add(1);
    }
}

/// 计算 4 字节截断 HMAC-SHA256。
pub fn hmac4(key: &[u8], data: &[u8]) -> [u8; HMAC_LEN] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC key 任意长度");
    mac.update(data);
    let out = mac.finalize().into_bytes();
    [out[0], out[1], out[2], out[3]]
}

/// 给防放大判断用的「收包长度」提示:取第一个帧的 body 长度 + 固定头。
fn buf_len_hint(frames: &[FrameRef]) -> usize {
    HEADER_LEN + frames.iter().map(|f| f.body.len() + 4).sum::<usize>()
}

// ── 辅助:trait 让 HashMap 能按 sess_id 查找 ──

trait SessionMapExt {
    fn get_by_sess_id(&self, sess_id: u64) -> Option<&Session>;
}

impl SessionMapExt for HashMap<u32, Session> {
    fn get_by_sess_id(&self, sess_id: u64) -> Option<&Session> {
        self.values().find(|s| s.sess_id == sess_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session_with_rtt(ms: u16) -> Session {
        let mut s = Session {
            conn_id: 1,
            sess_id: 1,
            peer: "127.0.0.1:1".parse().unwrap(),
            secret: 0,
            token: Vec::new(),
            send_seq: 0,
            last_rx: Instant::now(),
            state: SessionState::Active,
            budget_kbps: None,
            established: Instant::now(),
            recv_seq: 0,
            recv_bits: 0,
            ch1_last: None,
            ch3_seen: VecDeque::new(),
            retrans: [RetransmitQueue::new(), RetransmitQueue::new()],
            rtt: RttEstimator::new(),
            assembler: FragmentAssembler::new(),
            frag_group_id: 0,
            bytes_window: VecDeque::new(),
            reorder: BTreeMap::new(),
            delivered_seqs: VecDeque::new(),
            delivered_max: 0,
            ch2_next: None,
        };
        for _ in 0..5 {
            s.rtt.update(ms as f64);
        }
        s
    }

    #[test]
    fn adaptive_idle_grace_by_rtt() {
        let table = SessionTable::new(SessionTableConfig::default());
        // 低延迟(1ms):clamp(30ms, 1.5s, 5s) = 1.5s —— 快速判定断线。
        let low = table.idle_grace_for(&session_with_rtt(1));
        assert_eq!(low, Duration::from_millis(1500), "低延迟应取 idle 下限");
        // 中延迟(100ms):clamp(3s, 1.5s, 5s) = 3s。
        let mid = table.idle_grace_for(&session_with_rtt(100));
        assert_eq!(mid, Duration::from_secs(3));
        // 高延迟(500ms):clamp(15s, 1.5s, 5s) = 5s —— 放宽防误杀。
        let high = table.idle_grace_for(&session_with_rtt(500));
        assert_eq!(high, Duration::from_secs(5));
    }

    #[test]
    fn adaptive_reconnect_grace_by_rtt() {
        let table = SessionTable::new(SessionTableConfig::default());
        // 低延迟:clamp(120ms, 5s, 20s) = 5s。
        assert_eq!(table.reconnect_grace_for(&session_with_rtt(1)), Duration::from_secs(5));
        // 中延迟(100ms):clamp(12s, 5s, 20s) = 12s。
        assert_eq!(table.reconnect_grace_for(&session_with_rtt(100)), Duration::from_secs(12));
        // 高延迟(500ms):clamp(60s, 5s, 20s) = 20s。
        assert_eq!(table.reconnect_grace_for(&session_with_rtt(500)), Duration::from_secs(20));
    }

    #[test]
    fn adaptive_falls_back_to_fixed() {
        let mut cfg = SessionTableConfig::default();
        cfg.dynamic_timeouts = false;
        let table = SessionTable::new(cfg);
        let s = session_with_rtt(1);
        assert_eq!(table.idle_grace_for(&s), IDLE_GRACE);
        assert_eq!(table.reconnect_grace_for(&s), RECONNECT_GRACE);
    }

    #[test]
    fn unsampled_rtt_uses_conservative_max() {
        let table = SessionTable::new(SessionTableConfig::default());
        let s = session_with_rtt(0); // srtt 未采样(update(0) 被忽略)
        assert_eq!(table.idle_grace_for(&s), Duration::from_secs(5));
        assert_eq!(table.reconnect_grace_for(&s), Duration::from_secs(20));
    }
}
