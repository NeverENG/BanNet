//! # Engine —— 框架门面(用户第一个接触的类型)
//!
//! 装配并驱动全部子系统:
//!
//! ```text
//! 客户端(UDP)                 框架                        逻辑服(UDS)
//! ┌─────────┐   SO_REUSEPORT  ┌──────────────────┐ 帧流  ┌──────────┐
//! │ Godot   │ ◀─────────────▶ │ UDP recv task ×N │ ◀────▶ │ soup-sdk │
//! │ / 模拟器 │   共享发送 socket │ 会话表 · 握手      │       │ (Go)     │
//! └─────────┘                  │ 上行队列(背压)    │       └──────────┘
//!                              │ UDS 读写双 task   │
//!                              │ 心跳 · 维护 · 指标 │
//!                              └──────────────────┘
//! ```
//!
//! 硬约束(规格书 T0002M01F04):
//! - 框架不持有游戏状态、不解释 payload
//! - 逻辑服慢/挂了:按通道丢包降级,**绝不阻塞 recv task**
//! - 逻辑服热重启:客户端不掉线,重连后补发 `SessionResume`

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use tokio::sync::{mpsc, watch};

use crate::buffer::BufferPool;
use crate::error::Result;
use crate::protocol::datagram::DatagramHeader;
use crate::protocol::frame::{self, Frame};
use crate::protocol::types::*;
use crate::session::table::{SessionEvent, SessionTable, SessionTableConfig};
use crate::stats::Stats;
use crate::transport::udp::{UdpReceiver, UdpSender};
use crate::transport::uds::{UdsConnection, UdsReader, UdsWriter};

/// 逻辑服掉线后的重连间隔。
/// 逻辑服重连:指数退避(yojimbo connecting_after_disconnect 调研),
/// 1s → 2s → 4s → 8s 封顶,连接成功即重置,防逻辑服重启风暴。
const LOGIC_RECONNECT_INTERVAL: Duration = Duration::from_secs(1);
const LOGIC_RECONNECT_MAX: Duration = Duration::from_secs(8);
/// Overload 通知的最小间隔(节流,避免刷屏)。
const OVERLOAD_MIN_INTERVAL: Duration = Duration::from_millis(100);

/// 引擎配置。
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// 客户端 UDP 监听地址。
    pub bind_addr: SocketAddr,
    /// 逻辑服 UDS socket 路径。
    pub uds_path: PathBuf,
    /// SO_REUSEPORT socket 数(通常 = 核数)。
    pub udp_workers: usize,
    /// 会话表配置。
    pub session: SessionTableConfig,
    /// EngineHello 的 version / caps。
    pub version: u16,
    pub caps: u32,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:0".parse().unwrap(),
            uds_path: PathBuf::from("/run/soup-engine.sock"),
            udp_workers: num_cpus(),
            session: SessionTableConfig::default(),
            version: VERSION as u16,
            caps: 0,
        }
    }
}

/// 引擎内部共享状态(跨 task)。
struct Shared {
    sessions: Arc<SessionTable>,
    #[allow(dead_code)] // E2 可靠层启用后使用
    pool: Arc<BufferPool>,
    stats: Arc<Stats>,
    /// 逻辑服当前是否在线(重连状态机用)。
    logic_online: AtomicBool,
    /// 上次发 Overload 的时刻(节流)。
    last_overload: Mutex<Instant>,
    /// 上次心跳时刻。
    last_heartbeat: Mutex<Instant>,
    /// 有待发 Overload 通知(由维护循环消费)。
    pending_overload: AtomicBool,
    /// 下行 UDP 发送端(run 时注入)。
    udp_sender: Mutex<Option<UdpSender>>,
    /// 上行帧队列发送端:逻辑服在线时存在;掉线时置 None(上行丢弃并计数)。
    up_tx: RwLock<Option<mpsc::Sender<Frame>>>,
    /// UDP 实际监听地址(绑定后写入,供测试/工具查询)。
    bound_udp: Mutex<Option<SocketAddr>>,
    /// 逻辑服上次在线时刻(热重启补发 SessionResume 的 gap 基准)。
    logic_last_online: Mutex<Instant>,
}

impl Shared {
    fn new(sessions: SessionTable) -> Self {
        Self {
            sessions: Arc::new(sessions),
            pool: Arc::new(BufferPool::new()),
            stats: Arc::new(Stats::new()),
            logic_online: AtomicBool::new(false),
            last_overload: Mutex::new(Instant::now()),
            last_heartbeat: Mutex::new(Instant::now()),
            pending_overload: AtomicBool::new(false),
            udp_sender: Mutex::new(None),
            up_tx: RwLock::new(None),
            bound_udp: Mutex::new(None),
            logic_last_online: Mutex::new(Instant::now()),
        }
    }
}

/// 引擎。`run` 后阻塞;测试可用 `run_with_shutdown`。
pub struct Engine {
    cfg: EngineConfig,
    shared: Arc<Shared>,
}

impl Engine {
    /// 逻辑服是否在线(供监控/工具轮询)。
    pub fn logic_online(&self) -> bool {
        self.shared.logic_online.load(Ordering::Relaxed)
    }

    pub fn new(cfg: EngineConfig) -> Self {
        let sessions = SessionTable::new(cfg.session.clone());
        Self {
            cfg,
            shared: Arc::new(Shared::new(sessions)),
        }
    }

    /// 会话表(测试/工具读取指标用)。
    pub fn sessions(&self) -> Arc<SessionTable> {
        self.shared.sessions.clone()
    }

    /// UDP 实际监听地址(run 启动后可用;返回 None 表示未绑定)。
    pub fn local_addr(&self) -> Option<SocketAddr> {
        *self.shared.bound_udp.lock().unwrap()
    }

    pub async fn run(self) -> Result<()> {
        let (_tx, rx) = watch::channel(false);
        self.run_with_shutdown(rx).await
    }

    /// 运行引擎,直到 `shutdown` 收到 true(或 Ctrl-C)。
    pub async fn run_with_shutdown(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let cfg = &self.cfg;

        // ── UDP 收发 ──
        let receiver = UdpReceiver::bind(cfg.bind_addr, cfg.udp_workers).await?;
        // 收发合一:发送复用同一批 SO_REUSEPORT socket,保证源端口 = 监听端口
        //(客户端回包发往收到的包源地址,源端口不对链路就断)。
        let sender = UdpSender::from_receiver(&receiver);
        let actual_addr = receiver.local_addr()?;
        tracing::info!(%actual_addr, uds = %cfg.uds_path.display(), "soup-engine 启动");

        // 下行发送端注入共享状态。
        *self.shared.udp_sender.lock().unwrap() = Some(sender.clone());
        *self.shared.bound_udp.lock().unwrap() = Some(actual_addr);

        // ── UDP recv handler:同步回调(绝不 await,绝不阻塞)──
        let shared = self.shared.clone();
        let recv_sender = sender.clone();
        let recv_handler: Arc<crate::transport::udp::PacketHandler> = Arc::new(
            move |data: &[u8], peer: SocketAddr| {
                let shared = shared.clone();
                let sender = recv_sender.clone();
                let mut send_back = |bytes: &[u8], peer: SocketAddr| {
                    // 握手回包走非阻塞发送;失败即丢弃(客户端会重试)。
                    let _ = sender.try_send(bytes, peer);
                };
                let events = shared.sessions.handle_datagram(peer, data, &mut send_back, Instant::now());
                for ev in events {
                    push_up(&shared, &ev);
                }
            },
        );

        // ── 拉起所有 task ──
        let recv_handle = tokio::spawn({
            let receiver = receiver.clone();
            async move {
                let _ = receiver.run(recv_handler).await;
            }
        });

        let uds_handle = tokio::spawn({
            let shared = self.shared.clone();
            let cfg = self.cfg.clone();
            async move {
                logic_link_loop(&cfg, &shared).await;
            }
        });

        let maint_handle = tokio::spawn({
            let shared = self.shared.clone();
            let sender = sender.clone();
            async move {
                maintenance_loop(&shared, &sender).await;
            }
        });

        // ── 等待退出信号 ──
        let ctrl_c = tokio::spawn(async {
            let _ = tokio::signal::ctrl_c().await;
            true
        });
        tokio::select! {
            _ = ctrl_c => tracing::info!("收到 Ctrl-C,引擎退出"),
            _ = shutdown.changed() => tracing::info!("收到关闭信号,引擎退出"),
        }
        recv_handle.abort();
        uds_handle.abort();
        maint_handle.abort();
        Ok(())
    }
}

/// 上行帧入队 + 背压(规格书 T0002M04F05):
/// 队列满时丢弃 Ch0/Ch1(不可靠)、保留 Ch2/Ch3,并节流发 Overload。
/// ⛔ 本函数是同步的,绝不阻塞 recv task。
fn push_up(shared: &Arc<Shared>, ev: &SessionEvent) {
    let frame = match ev {
        SessionEvent::Open { sess_id, peer, token } => frame::session_open(*sess_id, peer, token),
        SessionEvent::Close { sess_id, reason } => frame::session_close(*sess_id, *reason),
        SessionEvent::Resume { sess_id, gap_ms } => frame::session_resume(*sess_id, *gap_ms),
        SessionEvent::Data {
            sess_id,
            ch,
            msg_id,
            payload,
        } => frame::data_up(*sess_id, *ch, *msg_id, payload),
    };
    push_frame(shared, frame, is_droppable(ev));
}

/// 不可靠通道(0/1)允许直接丢弃;可靠事件(2/3 + 生命周期帧)也受容量保护,
/// 只是丢弃后必须通知逻辑服降频。
fn is_droppable(ev: &SessionEvent) -> bool {
    matches!(
        ev,
        SessionEvent::Data { ch: CH_UNRELIABLE_UNORDERED | CH_UNRELIABLE_SEQUENCED, .. }
    )
}

/// 通用帧入队:从共享状态取当前上行队列。
/// `droppable` = 队列满时可静默丢弃(不可靠通道);否则告警并触发 Overload。
fn push_frame(shared: &Arc<Shared>, frame: Frame, droppable: bool) {
    let guard = shared.up_tx.read().unwrap();
    let Some(up_tx) = guard.as_ref() else {
        // 逻辑服掉线:丢弃并计数。
        shared.stats.dropped_up.incr();
        return;
    };
    match up_tx.try_send(frame) {
        Ok(_) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            shared.stats.dropped_up.incr();
            if !droppable {
                tracing::warn!("上行队列满,丢弃可靠事件");
            }
            notify_overload(shared);
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            shared.stats.dropped_up.incr();
        }
    }
}

/// 节流标记待发 Overload(维护循环每秒消费发送)。
fn notify_overload(shared: &Arc<Shared>) {
    let mut last = shared.last_overload.lock().unwrap();
    let now = Instant::now();
    if now.duration_since(*last) < OVERLOAD_MIN_INTERVAL {
        return;
    }
    *last = now;
    shared.pending_overload.store(true, Ordering::Relaxed);
}

// ── 逻辑服连接循环(支持热重启,规格书 T0002M04F06)──

async fn logic_link_loop(cfg: &EngineConfig, shared: &Arc<Shared>) {
    // 指数退避:每次失败翻倍,成功重置为初始值(防重启风暴,调研 yojimbo)。
    let mut backoff = LOGIC_RECONNECT_INTERVAL;
    loop {
        match UdsConnection::connect(&cfg.uds_path).await {
            Ok(mut conn) => {
                backoff = LOGIC_RECONNECT_INTERVAL; // 连接成功:重置退避
                shared.logic_online.store(true, Ordering::Relaxed);
                shared.stats.logic_reconnects.incr();
                tracing::info!(path = %cfg.uds_path.display(), "逻辑服已连接");

                // 每次连接重建上行队列:掉线期间的积压帧全部丢弃(计数)。
                let (up_tx, up_rx) = mpsc::channel::<Frame>(UP_QUEUE_CAP);
                *shared.up_tx.write().unwrap() = Some(up_tx.clone());

                // 1. EngineHello
                let mut hello = BytesMut::new();
                if frame::engine_hello(cfg.version, cfg.caps, &mut hello).is_ok() {
                    if let Err(e) = conn.writer.write_raw(&hello).await {
                        tracing::warn!(error = %e, "EngineHello 发送失败");
                        *shared.up_tx.write().unwrap() = None;
                        shared.logic_online.store(false, Ordering::Relaxed);
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(LOGIC_RECONNECT_MAX);
                        continue;
                    }
                }

                // 2. 补发 SessionResume:逻辑服据此推全量状态,而非当新玩家处理。
                //    gap_ms = 逻辑服中断时长(热重启时客户端全程不掉线)。
                let gap_ms = {
                    let last = *shared.logic_last_online.lock().unwrap();
                    last.elapsed().as_millis().min(u32::MAX as u128) as u32
                };
                *shared.logic_last_online.lock().unwrap() = Instant::now();
                let mut resumes = Vec::new();
                shared.sessions.for_each_session(|s| {
                    resumes.push(frame::session_resume(s.sess_id, gap_ms));
                });
                for r in resumes {
                    if let Err(e) = conn.writer.write_frame(&r).await {
                        tracing::warn!(error = %e, "SessionResume 发送失败");
                        break;
                    }
                }

                // 3. 并行读写;任一失败 = 逻辑服掉线,回到循环重连。
                let result = run_logic_link(conn, shared.clone(), up_rx).await;
                // 断线:清空上行队列(丢弃积压)。
                *shared.up_tx.write().unwrap() = None;
                shared.logic_online.store(false, Ordering::Relaxed);
                match result {
                    Err(e) => tracing::warn!(error = %e, "逻辑服连接中断,准备重连"),
                    Ok(()) => tracing::info!("逻辑服连接关闭,准备重连"),
                }
            }
            Err(e) => {
                tracing::debug!(path = %cfg.uds_path.display(), error = %e, "逻辑服未就绪,重试");
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(LOGIC_RECONNECT_MAX);
    }
}

/// 在一条逻辑服连接上并行跑读/写循环,任一结束即返回。
async fn run_logic_link(
    conn: UdsConnection,
    shared: Arc<Shared>,
    up_rx: mpsc::Receiver<Frame>,
) -> Result<()> {
    let UdsConnection { reader, writer } = conn;
    let mut reader = reader;
    let mut writer = writer;
    let mut up_rx = up_rx;

    tokio::select! {
        r = logic_read_loop(&mut reader, &shared) => r,
        w = logic_write_loop(&mut writer, &mut up_rx) => w,
    }
}

/// 读循环:逻辑服 → 框架(下行)。
async fn logic_read_loop(reader: &mut UdsReader, shared: &Arc<Shared>) -> Result<()> {
    loop {
        let Some(frame) = reader.read_frame().await? else {
            return Ok(()); // 对端关闭
        };
        if frame.ty == FRAME_SEND && !frame.body.is_empty() {
        }
        handle_logic_frame(&frame, shared).await?;
    }
}

/// 写循环:上行队列 → 逻辑服。
async fn logic_write_loop(writer: &mut UdsWriter, up_rx: &mut mpsc::Receiver<Frame>) -> Result<()> {
    while let Some(frame) = up_rx.recv().await {
        writer.write_frame(&frame).await?;
    }
    Ok(())
}

/// 处理逻辑服下发的指令(0x81 Send / 0x82 Multicast / 0x83 Kick / 0x84 SetBudget / 0x90 LogicHello)。
async fn handle_logic_frame(frame: &Frame, shared: &Arc<Shared>) -> Result<()> {
    match frame.ty {
        FRAME_LOGIC_HELLO => {
            tracing::info!("LogicHello 收到");
        }
        FRAME_SEND => {
            let (sess_id, ch, msg_id, payload) = frame::parse_data(&frame.body)?;
            send_outbound(shared, sess_id, ch, msg_id, payload).await;
        }
        FRAME_MULTICAST => {
            let (ids, ch, msg_id, payload) = frame::parse_multicast(&frame.body)?;
            // TODO(性能):一份 payload 只编码/拷贝一次 —— E1 先逐会话打包,
            // 压测后优化为共享 payload 的多包合并发送。
            for id in ids {
                send_outbound(shared, id, ch, msg_id, payload).await;
            }
        }
        FRAME_KICK => {
            let (sess_id, reason) = frame::parse_session_close(&frame.body)?;
            if shared.sessions.kick(sess_id, reason).is_some() {
                shared.stats.kicks.incr();
                tracing::info!(sess_id, reason, "逻辑服要求踢出会话");
            }
        }
        FRAME_SET_BUDGET => {
            let (sess_id, kbps) = frame::parse_set_budget(&frame.body)?;
            shared.sessions.set_budget(sess_id, kbps);
        }
        other => {
            tracing::warn!(ty = other, "未知下行帧类型,丢弃");
        }
    }
    Ok(())
}

/// 把一条 Send 帧打包成 UDP 数据报发出(可能分片为多包)。
async fn send_outbound(shared: &Arc<Shared>, sess_id: u64, ch: u8, msg_id: u16, payload: &[u8]) {
    match shared.sessions.pack_outbound(sess_id, ch, msg_id, payload) {
        Ok(packets) => {
            // 逐会话限流(规格书 SetBudget):超限丢弃并计数。
            let total_bytes: u64 = packets.iter().map(|(b, _)| b.len() as u64).sum();
            if !shared
                .sessions
                .check_budget(sess_id, total_bytes, Instant::now())
            {
                shared.stats.dropped_down.incr_by(total_bytes);
                tracing::debug!(sess_id, "逐会话带宽超限,丢弃下行");
                return;
            }
            // 先 clone 出发送端再 await,避免 MutexGuard 跨 await 持有。
            let sender = shared.udp_sender.lock().unwrap().clone();
            if let Some(sender) = sender {
                let mut sent = 0usize;
                for (bytes, peer) in packets {
                    if let Err(e) = sender.send(&bytes, peer).await {
                        shared.stats.dropped_down.incr();
                        tracing::debug!(%peer, error = %e, "UDP 下行发送失败");
                    } else {
                        sent += 1;
                    }
                }
            }
        }
        Err(e) => {
            shared.stats.dropped_down.incr();
            tracing::debug!(sess_id, error = %e, "下行投递失败(会话不存在/非活跃)");
        }
    }
}

// ── 维护循环:宽限期推进 / 心跳 / SessionStats / Overload ──

async fn maintenance_loop(shared: &Arc<Shared>, sender: &UdpSender) {
    let mut maint = tokio::time::interval(Duration::from_millis(100));
    let mut second = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = maint.tick() => {
                let now = Instant::now();
                let events = shared.sessions.maintain(now);
                for ev in events {
                    push_up(shared, &ev);
                }
                // 可靠层重传 + 分片重组清理(E2)。
                let sender2 = sender.clone();
                shared.sessions.maintain_reliable(now, &mut |bytes, peer| {
                    let _ = sender2.try_send(bytes, peer);
                });
                // 心跳(QUIC §10.1.1 PING 探测调研):只对「最近无流量」的活跃
                // 会话发空数据报(保 NAT 映射 + 探活);流量正常的会话不发,
                // 省去固定 1Hz 心跳的空包开销。
                let last_hb = *shared.last_heartbeat.lock().unwrap();
                if now.duration_since(last_hb) >= HEARTBEAT_INTERVAL {
                    *shared.last_heartbeat.lock().unwrap() = now;
                    shared.sessions.for_each_session(|s| {
                        if s.is_active() && now.duration_since(s.last_rx) >= HEARTBEAT_INTERVAL {
                            let (ack, ack_bits) = s.compute_ack();
                            let mut out = Vec::with_capacity(16);
                            let header = DatagramHeader {
                                version: VERSION,
                                flags: 0,
                                conn_id: s.conn_id,
                                seq: s.send_seq,
                                ack,
                                ack_bits,
                            };
                            if crate::protocol::datagram::encode(&header, &[], &mut out).is_ok() {
                                let _ = sender.try_send(&out, s.peer);
                            }
                        }
                    });
                }
            }
            _ = second.tick() => {
                // SessionStats:每秒一次(规格书 T0002M04F03)。
                let mut frames = Vec::new();
                shared.sessions.for_each_session(|s| {
                    frames.push(frame::session_stats(s.sess_id, 0, 0, 0));
                });
                for f in frames {
                    push_frame(shared, f, true);
                }
                // 待发 Overload。
                if shared.pending_overload.swap(false, Ordering::Relaxed) {
                    let f = frame::overload(
                        shared.stats.dropped_up.get() as u32,
                        shared.stats.dropped_down.get() as u32,
                    );
                    push_frame(shared, f, false);
                }
            }
        }
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
