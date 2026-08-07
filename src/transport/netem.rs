//! 网络模拟器(规格书 T0002M08F02)。
//!
//! 集成测试注入用:固定延迟 / 抖动 / 丢包率 / 乱序 / 重复 / 突发全丢。
//! 不参与生产路径 —— 只有测试与 `engineload` 工具引入。

use std::sync::Mutex;
use std::time::{Duration, Instant};

use rand::Rng;

/// 突发全丢窗口。
#[derive(Debug, Clone, Copy)]
pub struct Blackout {
    from: Instant,
    len: Duration,
}

/// 网络条件模拟。
#[derive(Debug)]
pub struct Netem {
    /// 固定延迟。
    pub delay: Duration,
    /// 抖动范围:实际延迟 = delay ± jitter 内随机。
    pub jitter: Duration,
    /// 丢包率(0.0 ~ 1.0)。
    pub loss: f64,
    /// 重复率(0.0 ~ 1.0):命中则多发一次。
    pub dup: f64,
    pub blackout: Mutex<Option<Blackout>>,
}

impl Default for Netem {
    fn default() -> Self {
        Self::new()
    }
}

impl Netem {
    pub fn new() -> Self {
        Self {
            delay: Duration::ZERO,
            jitter: Duration::ZERO,
            loss: 0.0,
            dup: 0.0,
            blackout: Mutex::new(None),
        }
    }

    /// 设置突发全丢窗口:`from` 相对当前时刻,持续 `len`。
    pub fn set_blackout(&self, from: Duration, len: Duration) {
        *self.blackout.lock().unwrap() = Some(Blackout {
            from: Instant::now() + from,
            len,
        });
    }

    /// 当前是否处于突发全丢窗口内。
    pub fn is_blackout(&self, now: Instant) -> bool {
        match self.blackout.lock().unwrap().as_ref() {
            Some(b) => now >= b.from && now <= b.from + b.len,
            None => false,
        }
    }

    /// 这个包是否应该丢。
    pub fn should_drop<R: Rng>(&self, rng: &mut R) -> bool {
        self.loss > 0.0 && rng.gen::<f64>() < self.loss
    }

    /// 这个包是否应该重复发送一次。
    pub fn should_dup<R: Rng>(&self, rng: &mut R) -> bool {
        self.dup > 0.0 && rng.gen::<f64>() < self.dup
    }

    /// 这个包的网络延迟(固定延迟 + 抖动,或 ± 抖动随机)。
    pub fn latency<R: Rng>(&self, rng: &mut R) -> Duration {
        let base = self.delay;
        if self.jitter.is_zero() {
            return base;
        }
        let half = self.jitter.as_millis() as i64 / 2;
        let spread = rng.gen_range(-half..=half);
        let ms = (base.as_millis() as i64 + spread).max(0) as u64;
        Duration::from_millis(ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::thread_rng;

    #[test]
    fn blackout_window() {
        let n = Netem::new();
        let now = Instant::now();
        assert!(!n.is_blackout(now));
        n.set_blackout(Duration::from_millis(10), Duration::from_millis(50));
        assert!(!n.is_blackout(now));
        std::thread::sleep(Duration::from_millis(30));
        assert!(n.is_blackout(Instant::now()));
        std::thread::sleep(Duration::from_millis(60));
        assert!(!n.is_blackout(Instant::now()));
    }

    #[test]
    fn loss_and_dup_bounds() {
        let n = Netem {
            loss: 1.0,
            dup: 1.0,
            ..Netem::new()
        };
        let mut rng = thread_rng();
        assert!(n.should_drop(&mut rng));
        assert!(n.should_dup(&mut rng));
    }

    #[test]
    fn latency_within_bounds() {
        let n = Netem {
            delay: Duration::from_millis(100),
            jitter: Duration::from_millis(20),
            ..Netem::new()
        };
        let mut rng = thread_rng();
        for _ in 0..100 {
            let l = n.latency(&mut rng);
            assert!(l >= Duration::from_millis(90), "延迟过小: {l:?}");
            assert!(l <= Duration::from_millis(110), "延迟过大: {l:?}");
        }
    }
}
