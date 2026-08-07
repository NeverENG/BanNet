//! # 可靠层(规格书 T0002M03F03)
//!
//! - [`rtt`]         RTT 估计与 RTO(RTTVAR 平滑,夹 [50ms, 1000ms])
//! - [`retransmit`]  发送端重传队列(每可靠通道一个,上限 64)
//! - [`fragment`]     分片/重组(仅 Ch2)

pub mod fragment;
pub mod retransmit;
pub mod rtt;
