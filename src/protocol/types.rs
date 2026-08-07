//! 协议常量 —— 客户端 UDP 协议与逻辑服 UDS 帧协议的全部魔数/标志/超时。
//!
//! 本文件是 `T0002M03`(客户端协议)与 `T0002M04`(框架 ↔ 逻辑服协议)
//! 的代码镜像:改协议先改这里,再改编解码器。

use std::time::Duration;

// ── 客户端 UDP 数据报(T0002M03F01) ──

/// 数据报魔数 `0x5A50`,小端。
pub const MAGIC: u16 = 0x5A50;
/// 协议版本。
pub const VERSION: u8 = 1;
/// UDP 数据报 MTU 上限(字节)。超过即编码错误。
pub const MTU: usize = 1200;

/// flags 位定义。
/// bit0 = 分片 · bit1 = 纯 ACK · bit2 = 握手 · bit3 = 加密(预留) · bit4 = 带 HMAC
pub const FLAG_FRAGMENT: u8 = 0b0000_0001;
pub const FLAG_PURE_ACK: u8 = 0b0000_0010;
pub const FLAG_HANDSHAKE: u8 = 0b0000_0100;
pub const FLAG_ENCRYPTED: u8 = 0b0000_1000;
pub const FLAG_HMAC: u8 = 0b0001_0000;

/// 四条通道(T0002M03F02)。
/// Ch0 不可靠无序(快照下行) · Ch1 不可靠有序(输入上行) · Ch2 可靠有序 · Ch3 可靠无序。
pub const CH_UNRELIABLE_UNORDERED: u8 = 0;
pub const CH_UNRELIABLE_SEQUENCED: u8 = 1;
pub const CH_RELIABLE_ORDERED: u8 = 2;
pub const CH_RELIABLE_UNORDERED: u8 = 3;

/// 帧头里 `ch u4 | msg_id u12` 的位宽。
pub const MSG_ID_MASK: u16 = 0x0FFF;

// ── 逻辑服 UDS 帧类型(T0002M04F03 / F04) ──
//
// 框架 → 逻辑服(上行):
pub const FRAME_ENGINE_HELLO: u8 = 0x30; // version u16 · caps u32
pub const FRAME_SESSION_OPEN: u8 = 0x01; // sess_id u64 · addr [u8;18] · token_len u16 · token[]
pub const FRAME_SESSION_CLOSE: u8 = 0x02; // sess_id u64 · reason u8
pub const FRAME_SESSION_RESUME: u8 = 0x03; // sess_id u64 · gap_ms u32
pub const FRAME_DATA_UP: u8 = 0x10; // sess_id u64 · ch u8 · msg_id u16 · payload[]
pub const FRAME_SESSION_STATS: u8 = 0x20; // sess_id u64 · rtt_ms u16 · loss_permille u16 · out_kbps u16
pub const FRAME_OVERLOAD: u8 = 0x2F; // dropped_up u32 · dropped_down u32
// 逻辑服 → 框架(下行):
pub const FRAME_LOGIC_HELLO: u8 = 0x90; // version u16 · caps u32
pub const FRAME_SEND: u8 = 0x81; // sess_id u64 · ch u8 · msg_id u16 · payload[]
pub const FRAME_MULTICAST: u8 = 0x82; // n u8 · sess_id[n] u64 · ch u8 · msg_id u16 · payload[]
pub const FRAME_KICK: u8 = 0x83; // sess_id u64 · reason u8
pub const FRAME_SET_BUDGET: u8 = 0x84; // sess_id u64 · kbps u16

// ── 会话生命周期(T0002M05) ──

/// 握手超时:握手中会话 3s 无进展即清理。
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(3);
/// 宽限期:活跃会话连续 5s 无包进入重连宽限期(期间不发 SessionClose)。
pub const IDLE_GRACE: Duration = Duration::from_secs(5);
/// 重连宽限期:超过即 SessionClose,默认 20s。
pub const RECONNECT_GRACE: Duration = Duration::from_secs(20);
/// 心跳间隔 1Hz。
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

// ── RTT 自适应超时(调研:ENet `clamp(limit×2×RTT, min, max)` / QUIC idle)──
// 低延迟环境快速判定断线并回收会话(资源友好),高延迟放宽防误杀。
// 公式:idle = clamp(30×SRTT, TIMEOUT_MIN, TIMEOUT_MAX);
//       reconnect = clamp(120×SRTT, RECONNECT_MIN, RECONNECT_MAX)。
/// idle 下限:低延迟下最快 1.5s 判定断线。
pub const TIMEOUT_MIN: Duration = Duration::from_millis(1500);
/// idle 上限:高延迟下最多容忍 5s 无包。
pub const TIMEOUT_MAX: Duration = Duration::from_secs(5);
/// reconnect 下限:低延迟下宽限期最短 5s(超过即 SessionClose)。
pub const RECONNECT_MIN: Duration = Duration::from_secs(5);
/// reconnect 上限:高延迟下宽限期最长 20s。
pub const RECONNECT_MAX: Duration = Duration::from_secs(20);

// ── 可靠层(T0002M03F03) ──

/// 单通道重传队列上限,溢出即断连。
pub const RETRANSMIT_QUEUE_CAP: usize = 64;
/// RTO 上下限。
pub const RTO_MIN: Duration = Duration::from_millis(50);
pub const RTO_MAX: Duration = Duration::from_secs(1);
/// 心跳 1Hz;连续 5s 无包进入宽限期。
pub const HEARTBEAT_PERIOD: Duration = Duration::from_secs(1);

// ── 会话表分片 ──

/// 会话表默认分片数(按 conn_id 高位分片)。
pub const DEFAULT_SESSION_SHARDS: usize = 8;

/// 逻辑服 → 框架 队列深度(E1 先用固定值,背压策略见 T0002M04F05)。
pub const UP_QUEUE_CAP: usize = 4096;
/// 下行(按会话)出站队列深度。
pub const DOWN_QUEUE_CAP: usize = 1024;

/// 帧体长度上限(防恶意超长帧,单位字节)。
pub const MAX_FRAME_BODY: u32 = 1024 * 1024;
