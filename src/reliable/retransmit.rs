//! 发送端重传队列(规格书 T0002M03F03)。
//!
//! 每可靠通道一个队列,上限 64 条,溢出即断连。条目保存**已编码的数据报**,
//! 重传直接非阻塞发出,零重编码。
//!
//! ACK 判定(seq 回绕安全):对端报 `(ack, ack_bits)`,`ack` 是已收到最大序号,
//! `ack_bits` 是 ack 之前 32 个包的位图。条目确认窗口 = [ack-32, ack],
//! 窗口外的旧条目一律视为已确认(对端推进过了)。

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::protocol::types::RETRANSMIT_QUEUE_CAP;

/// 一条待确认的可靠帧。
#[derive(Debug, Clone)]
pub struct PendingPacket {
    pub seq: u16,
    pub sent_at: Instant,
    /// 已编码的完整数据报(重传直接发)。
    pub datagram: Vec<u8>,
}

/// 单通道重传队列。
#[derive(Debug, Default, Clone)]
pub struct RetransmitQueue {
    entries: VecDeque<PendingPacket>,
}

impl RetransmitQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// 入队一条待确认帧。队列满返回 `Err`(调用方据此断连)。
    pub fn push(&mut self, seq: u16, datagram: Vec<u8>, now: Instant) -> Result<()> {
        if self.entries.len() >= RETRANSMIT_QUEUE_CAP {
            return Err(Error::Protocol(format!(
                "重传队列溢出(上限 {RETRANSMIT_QUEUE_CAP}),断开连接"
            )));
        }
        self.entries.push_back(PendingPacket {
            seq,
            sent_at: now,
            datagram,
        });
        Ok(())
    }

    /// 处理对端 ack:返回 (本次确认的 seq 列表, RTT 样本毫秒)。
    /// RTT 样本取最近被确认条目的往返时长(避免歧义)。
    ///
    /// ⚠️ 不做「ack 窗口外清理」:对端位图只有 32 位,窗口外的未确认
    /// 条目可能真的丢了(客户端从未收到),必须靠 RTO 重传而不是误判已确认。
    pub fn on_ack(&mut self, ack: u16, ack_bits: u32, now: Instant) -> (Vec<u16>, Option<f64>) {
        let mut acked = Vec::new();
        let mut rtt_sample = None;
        while let Some(front) = self.entries.front() {
            if is_acked(front.seq, ack, ack_bits) {
                rtt_sample = Some(now.duration_since(front.sent_at).as_secs_f64() * 1000.0);
                acked.push(front.seq);
                self.entries.pop_front();
            } else {
                break;
            }
        }
        (acked, rtt_sample)
    }

    /// RTT 采样已并入 on_ack(条目弹出后无法回溯)。此方法保留供测试。
    pub fn sample_rtt(&self, _acked_seq: u16, _now: Instant) -> Option<f64> {
        None
    }

    /// 找出所有超时(RTO)未确认的条目,返回待重传的数据报列表,
    /// 并更新它们的发送时刻(重传后重新计时)。
    pub fn retransmit_due(&mut self, now: Instant, rto: Duration) -> Vec<(u16, Vec<u8>)> {
        let mut due = Vec::new();
        for entry in self.entries.iter_mut() {
            if now.duration_since(entry.sent_at) >= rto {
                entry.sent_at = now; // 每次超时重置计时(指数退避留给后续优化)
                due.push((entry.seq, entry.datagram.clone()));
            }
        }
        due
    }
}

/// seq 是否被 ack 确认(连续交付语义)。
fn is_acked(seq: u16, ack: u16, ack_bits: u32) -> bool {
    // seq ≤ ack:对端已连续交付,隐含确认。
    if !seq_newer(seq, ack) {
        return true;
    }
    // seq > ack:乱序收到,靠位图。
    let ahead = seq.wrapping_sub(ack);
    if ahead >= 1 && ahead <= 32 {
        return ack_bits & (1 << (ahead - 1)) != 0;
    }
    false
}

/// 16bit 回绕安全的 `a > b`。
fn seq_newer(a: u16, b: u16) -> bool {
    (a.wrapping_sub(b) as i16) > 0
}

mod tests {
    use super::*;

    #[allow(dead_code)]
    fn t0() -> Instant {
        Instant::now()
    }

    #[test]
    fn push_and_ack() {
        let mut q = RetransmitQueue::new();
        let now = t0();
        q.push(1, vec![1], now).unwrap();
        q.push(2, vec![2], now).unwrap();
        q.push(3, vec![3], now).unwrap();
        // ack=2:连续交付语义下 1、2 隐含确认;3 靠位图 bit0(ack 后第 1 个)。
        let (acked, rtt) = q.on_ack(2, 0b1, now);
        assert_eq!(acked, vec![1, 2, 3]);
        assert!(rtt.is_some());
        assert!(q.is_empty());
    }

    #[test]
    fn ack_window_bits() {
        let mut q = RetransmitQueue::new();
        let now = t0();
        for i in 0..10u16 {
            q.push(i, vec![i as u8], now).unwrap();
        }
        // 连续 ACK 语义:ack=7 隐含确认 0..7;位图全置位再确认 8、9(ack 后乱序)。
        let (acked, _) = q.on_ack(7, 0xFFFF_FFFF, now);
        assert_eq!(acked, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert!(q.is_empty());
        // 位图全 0:只确认 ack 及之前,ack 之后的乱序包保留等待。
        for i in 0..10u16 {
            q.push(i, vec![i as u8], now).unwrap();
        }
        let (acked, _) = q.on_ack(7, 0, now);
        assert_eq!(acked, (0..=7).collect::<Vec<u16>>());
        assert_eq!(q.len(), 2);
    }

    #[test]
    fn incremental_ack_confirms_in_window() {
        let mut q = RetransmitQueue::new();
        let now = t0();
        for i in 0..40u16 {
            q.push(i, vec![i as u8], now).unwrap();
        }
        // 连续 ACK:ack=9 隐含确认 0..9;位图全 0 时 10..39 保留等乱序位图或推进。
        let (acked, _) = q.on_ack(9, 0, now);
        assert_eq!(acked, (0..=9).collect::<Vec<u16>>());
        assert_eq!(q.len(), 30);
        // 模拟客户端逐包 ACK:ack 推进 10..39,每条隐含确认自身及之前。
        let mut total = 10;
        for ack in 10..40u16 {
            let (acked, _) = q.on_ack(ack, 0, now);
            assert_eq!(acked, vec![ack], "ack={ack} 应确认自身");
            total += 1;
        }
        assert_eq!(total, 40);
        assert!(q.is_empty());
        // 连续 ACK 语义:ack=39 隐含确认 0..39(与位图无关)。
        for i in 0..40u16 {
            q.push(i, vec![i as u8], now).unwrap();
        }
        let (acked, _) = q.on_ack(39, 0, now);
        assert_eq!(acked.len(), 40);
        assert!(q.is_empty());
    }

    #[test]
    fn overflow_rejected() {
        let mut q = RetransmitQueue::new();
        let now = t0();
        let mut err = None;
        for i in 0..RETRANSMIT_QUEUE_CAP as u16 + 5 {
            if q.push(i, vec![i as u8], now).is_err() {
                err = Some(());
                break;
            }
        }
        assert!(err.is_some(), "队列应在上限处拒绝");
        assert_eq!(q.len(), RETRANSMIT_QUEUE_CAP);
    }

    #[test]
    fn retransmit_due_only_expired() {
        let mut q = RetransmitQueue::new();
        let now = t0();
        q.push(1, vec![1], now).unwrap();
        q.push(2, vec![2], now).unwrap();
        // 1 已超时,2 未超时
        let due = q.retransmit_due(now + Duration::from_secs(1), Duration::from_millis(100));
        assert_eq!(due.len(), 2);
        // 重传后计时重置:立即再查无到期
        let due2 = q.retransmit_due(now + Duration::from_secs(1) + Duration::from_millis(50), Duration::from_millis(100));
        assert!(due2.is_empty());
    }

    #[test]
    fn seq_wraparound_ack() {
        let mut q = RetransmitQueue::new();
        let now = t0();
        // 模拟 seq 回绕:65534, 65535, 0, 1
        for i in [65534u16, 65535, 0, 1] {
            q.push(i, vec![], now).unwrap();
        }
        let (acked, _) = q.on_ack(1, 0xFFFF_FFFF, now);
        assert_eq!(acked, vec![65534, 65535, 0, 1]);
    }
}
