//! 全局缓冲池(规格书 T0002M02 / T0002M06F02)。
//!
//! 分级复用,用完归还,热路径零堆分配:
//!
//! | 级 | 容量 | 用途 |
//! |---|---|---|
//! | Small  | 256B   | 小消息 / 帧体 |
//! | Medium | 1500B  | 典型 UDP 数据报 |
//! | Large  | 16KB   | 大帧 / 分片重组 |
//!
//! 每级一个有界空闲列表;超限的归还缓冲直接丢弃(避免缓存无界内存)。

use std::sync::Mutex;

use bytes::BytesMut;

pub const SMALL_CAP: usize = 256;
pub const MEDIUM_CAP: usize = 1500;
pub const LARGE_CAP: usize = 16 * 1024;

/// 每级空闲列表容量上限。
const FREE_PER_LEVEL: usize = 1024;

struct Level {
    cap: usize,
    free: Vec<BytesMut>,
}

/// 全局缓冲池。线程安全(std Mutex 短临界区;池竞争可后续换 sharded 实现)。
pub struct BufferPool {
    levels: [Mutex<Level>; 3],
}

impl BufferPool {
    pub fn new() -> Self {
        Self {
            levels: [
                Mutex::new(Level {
                    cap: SMALL_CAP,
                    free: Vec::with_capacity(64),
                }),
                Mutex::new(Level {
                    cap: MEDIUM_CAP,
                    free: Vec::with_capacity(64),
                }),
                Mutex::new(Level {
                    cap: LARGE_CAP,
                    free: Vec::with_capacity(64),
                }),
            ],
        }
    }

    fn level_for(min: usize) -> usize {
        if min <= SMALL_CAP {
            0
        } else if min <= MEDIUM_CAP {
            1
        } else {
            2
        }
    }

    /// 取一块容量 ≥ `min` 的缓冲。命中池则零分配,miss 则新建。
    pub fn acquire(&self, min: usize) -> BytesMut {
        let level = Self::level_for(min);
        let mut guard = self.levels[level].lock().unwrap();
        if let Some(mut b) = guard.free.pop() {
            b.clear();
            b.reserve(min.saturating_sub(b.capacity()));
            b
        } else {
            BytesMut::with_capacity(guard.cap.max(min))
        }
    }

    /// 归还缓冲。容量不匹配该级或列表已满则丢弃。
    pub fn release(&self, mut buf: BytesMut) {
        let level = Self::level_for(buf.capacity());
        let mut guard = self.levels[level].lock().unwrap();
        if buf.capacity() > guard.cap {
            // 扩容过的缓冲不进池,直接释放。
            return;
        }
        if guard.free.len() >= FREE_PER_LEVEL {
            return;
        }
        buf.clear();
        guard.free.push(buf);
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_release_cycle() {
        let pool = BufferPool::new();
        let b = pool.acquire(100);
        assert!(b.capacity() >= 100);
        pool.release(b);
        // 再次取同档,应命中池(容量一致)。
        let b2 = pool.acquire(100);
        assert!(b2.capacity() >= 100);
    }

    #[test]
    fn oversized_buf_dropped() {
        let pool = BufferPool::new();
        let mut b = BytesMut::with_capacity(100);
        b.reserve(100_000);
        pool.release(b);
        // 池不应被撑爆:再取小块正常。
        let b2 = pool.acquire(50);
        assert!(b2.capacity() >= 50);
    }

    #[test]
    fn small_and_large_differ() {
        let pool = BufferPool::new();
        let small = pool.acquire(10);
        assert!(small.capacity() <= SMALL_CAP);
        let large = pool.acquire(20 * 1024);
        assert!(large.capacity() >= 20 * 1024);
    }
}
