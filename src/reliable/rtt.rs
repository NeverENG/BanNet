//! RTT 估计与 RTO 计算(规格书 T0002M03F03)。
//!
//! Jacobson/Karels 算法(与 TCP 同源),夹在 [50ms, 1000ms]:
//!
//! ```text
//! srtt   = 0.875 × srtt + 0.125 × rtt      (首样本: srtt = rtt)
//! rttvar = 0.75 × rttvar + 0.25 × |srtt - rtt|
//! RTO    = srtt + 4 × rttvar
//! ```
//!
//! 调研建议(ENet 系):游戏帧同步场景 RTO 不需要 TCP 全套重传补偿,
//! 固定初始值 + RTT 平滑即可,这里保留 Jacobson/Karels 但夹取上下限。

use std::time::Duration;

use crate::protocol::types::{RTO_MAX, RTO_MIN};

/// 初始 RTO:200ms(一次内网往返的宽松估计)。
const INITIAL_RTO: Duration = Duration::from_millis(200);

#[derive(Debug, Clone)]
pub struct RttEstimator {
    srtt_ms: f64,
    rttvar_ms: f64,
    sample_count: u32,
}

impl Default for RttEstimator {
    fn default() -> Self {
        Self::new()
    }
}

impl RttEstimator {
    pub fn new() -> Self {
        Self {
            srtt_ms: 0.0,
            rttvar_ms: 0.0,
            sample_count: 0,
        }
    }

    /// 输入一个 RTT 样本(毫秒),返回更新后的 RTO。
    pub fn update(&mut self, rtt_ms: f64) -> Duration {
        if rtt_ms <= 0.0 {
            return self.rto();
        }
        self.sample_count += 1;
        if self.sample_count == 1 {
            self.srtt_ms = rtt_ms;
            self.rttvar_ms = rtt_ms / 2.0;
        } else {
            // 经典系数:0.125 / 0.25
            self.srtt_ms = 0.875 * self.srtt_ms + 0.125 * rtt_ms;
            self.rttvar_ms = 0.75 * self.rttvar_ms + 0.25 * (self.srtt_ms - rtt_ms).abs();
        }
        self.rto()
    }

    /// 当前 RTO,夹取 [RTO_MIN, RTO_MAX]。
    pub fn rto(&self) -> Duration {
        if self.sample_count == 0 {
            return INITIAL_RTO;
        }
        let rto_ms = (self.srtt_ms + 4.0 * self.rttvar_ms).clamp(
            RTO_MIN.as_millis() as f64,
            RTO_MAX.as_millis() as f64,
        );
        Duration::from_millis(rto_ms as u64)
    }

    /// 当前 SRTT(毫秒,供 SessionStats 上报)。
    pub fn srtt_ms(&self) -> u16 {
        self.srtt_ms.min(u16::MAX as f64) as u16
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_rto_is_sane() {
        let r = RttEstimator::new();
        assert_eq!(r.rto(), INITIAL_RTO);
    }

    #[test]
    fn converges_to_real_rtt() {
        let mut r = RttEstimator::new();
        // 稳定 100ms 延迟
        for _ in 0..20 {
            r.update(100.0);
        }
        let rto = r.rto();
        assert!(rto >= Duration::from_millis(50));
        assert!(rto <= Duration::from_millis(1000));
        // SRTT 收敛到 ~100ms,RTO 应接近 100~300ms
        assert!(rto.as_millis() >= 100, "RTO 偏小: {rto:?}");
        assert!(rto.as_millis() <= 400, "RTO 偏大: {rto:?}");
    }

    #[test]
    fn clamped_after_spike() {
        let mut r = RttEstimator::new();
        // 先收敛
        for _ in 0..10 {
            r.update(50.0);
        }
        // 巨大尖峰
        r.update(5000.0);
        assert!(r.rto() <= RTO_MAX);
    }

    #[test]
    fn zero_samples_ignored() {
        let mut r = RttEstimator::new();
        r.update(0.0);
        assert_eq!(r.sample_count, 0);
    }
}
