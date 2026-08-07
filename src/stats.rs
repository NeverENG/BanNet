//! 原子指标计数(规格书 T0002M08F01)。
//!
//! 热路径全部走 `fetch_add`(Relaxed),零锁。`Snapshot` 供 metrics 导出。

use std::sync::atomic::{AtomicU64, Ordering};

/// 单计数器。
#[derive(Debug, Default)]
pub struct Counter(AtomicU64);

impl Counter {
    pub fn incr(&self) {
        self.incr_by(1);
    }
    pub fn incr_by(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }
    pub fn decr(&self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// 全部指标。
#[derive(Debug, Default)]
pub struct Stats {
    pub sessions_active: Counter,
    pub sessions_handshaking: Counter,
    pub sessions_grace: Counter,
    pub pkt_in: Counter,
    pub pkt_out: Counter,
    pub pkt_bad: Counter,
    pub pkt_bytes_in: Counter,
    pub pkt_bytes_out: Counter,
    pub dropped_up: Counter,
    pub dropped_down: Counter,
    pub nat_rebind: Counter,
    pub logic_reconnects: Counter,
    pub kicks: Counter,
    pub retransmits: Counter,
    pub acks_seen: Counter,
}

impl Stats {
    pub fn new() -> Self {
        Self::default()
    }
}

/// 一次快照(导出用)。
#[derive(Debug, Clone, Copy)]
pub struct StatsSnapshot {
    pub sessions_active: u64,
    pub sessions_handshaking: u64,
    pub sessions_grace: u64,
    pub pkt_in: u64,
    pub pkt_out: u64,
    pub pkt_bad: u64,
    pub pkt_bytes_in: u64,
    pub pkt_bytes_out: u64,
    pub dropped_up: u64,
    pub dropped_down: u64,
    pub nat_rebind: u64,
    pub logic_reconnects: u64,
    pub kicks: u64,
    pub retransmits: u64,
    pub acks_seen: u64,
}

impl Stats {
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            sessions_active: self.sessions_active.get(),
            sessions_handshaking: self.sessions_handshaking.get(),
            sessions_grace: self.sessions_grace.get(),
            pkt_in: self.pkt_in.get(),
            pkt_out: self.pkt_out.get(),
            pkt_bad: self.pkt_bad.get(),
            pkt_bytes_in: self.pkt_bytes_in.get(),
            pkt_bytes_out: self.pkt_bytes_out.get(),
            dropped_up: self.dropped_up.get(),
            dropped_down: self.dropped_down.get(),
            nat_rebind: self.nat_rebind.get(),
            logic_reconnects: self.logic_reconnects.get(),
            kicks: self.kicks.get(),
            retransmits: self.retransmits.get(),
            acks_seen: self.acks_seen.get(),
        }
    }
}
