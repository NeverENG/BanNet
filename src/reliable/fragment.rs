//! 分片/重组(规格书 T0002M03F03:**仅 Ch2 支持分片重组**)。
//!
//! Ch0/1 单包超 MTU 视为编码错误(编码方自行切分);Ch3 不区分片。
//!
//! 分片格式(框架私有无业务语义,规格书留白,自定):
//!
//! ```text
//! frame body = [group_id u16][first_seq u16][frag_no u8][frag_total u8][chunk data]
//! ```
//!
//! - `group_id`:发送端自增的分片组标识(区分同 msg_id 的连续大消息)
//! - `first_seq`:组内首分片的发送序号 —— 接收端据此精确覆盖组内 seq 范围
//!   (乱序到达时创建组的片不一定是首片,不能拿它当基准)
//! - 每个分片是**独立可靠帧**(独立 seq / 独立 ack / 独立重传)
//! - 重组端按 `(通道, group_id)` 收集,收齐后拼成原始 payload 交付
//! - 长时间未收齐(默认 10s)丢弃,防内存被恶意分片耗尽

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};

/// 重组缓存超时。
pub const FRAGMENT_TIMEOUT: Duration = Duration::from_secs(10);
/// 分片上限(防止 total 字段被恶意利用导致内存爆炸)。
pub const MAX_FRAGMENTS: u8 = 64;
/// 触发分片的 payload 阈值(保证单帧 + 头部 ≤ MTU)。
pub const FRAGMENT_THRESHOLD: usize = 1100;

/// 分片头长度:group u16 + first_seq u16 + frag_no u8 + total u8。
pub const FRAG_HEADER_LEN: usize = 6;

/// 把一个 payload 切成若干分片(≤`max_chunk` 字节数据每片)。
/// `first_seq` = 组内首片将占用的发送序号。
/// 返回 (分片 body 列表)。
pub fn split(payload: &[u8], group_id: u16, first_seq: u16, max_chunk: usize) -> Vec<Vec<u8>> {
    let total = (payload.len().div_ceil(max_chunk)).max(1) as u8;
    let mut parts = Vec::with_capacity(total as usize);
    for (i, chunk) in payload.chunks(max_chunk).enumerate() {
        parts.push(encode_fragment(group_id, first_seq, i as u8, total, chunk));
    }
    parts
}

/// 编码单个分片 body。
pub fn encode_fragment(
    group_id: u16,
    first_seq: u16,
    frag_no: u8,
    total: u8,
    chunk: &[u8],
) -> Vec<u8> {
    let mut body = Vec::with_capacity(FRAG_HEADER_LEN + chunk.len());
    body.extend_from_slice(&group_id.to_le_bytes());
    body.extend_from_slice(&first_seq.to_le_bytes());
    body.push(frag_no);
    body.push(total);
    body.extend_from_slice(chunk);
    body
}

/// 解析分片 body:返回 (group_id, first_seq, frag_no, total, chunk)。
/// 校验失败返回 Err(畸形分片)。
pub fn decode_fragment(body: &[u8]) -> Result<(u16, u16, u8, u8, &[u8])> {
    if body.len() < FRAG_HEADER_LEN {
        return Err(Error::Protocol("分片 body 过短".into()));
    }
    let group_id = u16::from_le_bytes([body[0], body[1]]);
    let first_seq = u16::from_le_bytes([body[2], body[3]]);
    let frag_no = body[4];
    let total = body[5];
    if total == 0 || total > MAX_FRAGMENTS || frag_no >= total {
        return Err(Error::Protocol(format!(
            "非法分片参数: frag_no={frag_no} total={total}"
        )));
    }
    Ok((group_id, first_seq, frag_no, total, &body[FRAG_HEADER_LEN..]))
}

/// 一组分片的重组状态。
#[derive(Debug, Clone)]
struct FragState {
    total: u8,
    /// 组内首分片的发送序号(占位覆盖组内 seq 范围用)。
    first_seq: u16,
    /// 每片的数据;收齐前为 None。
    parts: Vec<Option<Vec<u8>>>,
    last_update: Instant,
}

/// 分片重组器(每会话一个)。
#[derive(Debug, Default, Clone)]
pub struct FragmentAssembler {
    pending: HashMap<u16, FragState>,
    /// 最近完成的组(重传的分片帧会再次到达,需去重)。
    completed: VecDeque<u16>,
}

/// 已完成组记忆上限。
const COMPLETED_CAP: usize = 128;

impl FragmentAssembler {
    pub fn new() -> Self {
        Self::default()
    }

    /// 推入一片。收齐该组时返回 (组首 seq, 完整 payload),否则 None。
    /// 组已存在但 total 不一致 → 视为新组(丢弃旧的,防伪造冲突)。
    pub fn push(
        &mut self,
        group_id: u16,
        first_seq: u16,
        frag_no: u8,
        total: u8,
        chunk: &[u8],
        now: Instant,
    ) -> Option<(u16, Vec<u8>)> {
        // 已完成组的重传帧:直接忽略(防重复交付)。
        if self.completed.contains(&group_id) {
            return None;
        }
        let entry = self.pending.entry(group_id).or_insert_with(|| FragState {
            total,
            first_seq,
            parts: (0..total).map(|_| None).collect(),
            last_update: now,
        });
        // total / 组首 seq 变化(伪造/冲突):重置整组。
        if entry.total != total || entry.first_seq != first_seq {
            *entry = FragState {
                total,
                first_seq,
                parts: (0..total).map(|_| None).collect(),
                last_update: now,
            };
        }
        entry.last_update = now;
        if entry.parts[frag_no as usize].is_none() {
            entry.parts[frag_no as usize] = Some(chunk.to_vec());
        }
        // 检查是否收齐
        if entry.parts.iter().all(|p| p.is_some()) {
            let gseq = entry.first_seq;
            let mut out = Vec::new();
            for p in entry.parts.drain(..) {
                if let Some(v) = p {
                    out.extend_from_slice(&v);
                }
            }
            self.pending.remove(&group_id);
            // 记住完成组,防重传帧重复交付。
            self.completed.push_back(group_id);
            while self.completed.len() > COMPLETED_CAP {
                self.completed.pop_front();
            }
            return Some((gseq, out));
        }
        None
    }

    /// 清理超时未收齐的组。
    pub fn cleanup(&mut self, now: Instant) {
        self.pending
            .retain(|_, s| now.duration_since(s.last_update) < FRAGMENT_TIMEOUT);
    }

    /// 进行中的分片组数(指标/测试用)。
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_and_reassemble() {
        let payload: Vec<u8> = (0..3000u16).map(|i| (i % 251) as u8).collect();
        let parts = split(&payload, 7, 100, 1000);
        assert_eq!(parts.len(), 3);
        let mut asm = FragmentAssembler::new();
        let now = Instant::now();
        let mut complete = None;
        // 乱序推入
        for idx in [1usize, 2, 0] {
            let (g, fs, n, t, chunk) = decode_fragment(&parts[idx]).unwrap();
            assert_eq!(g, 7);
            assert_eq!(fs, 100);
            if let Some((seq, full)) = asm.push(g, fs, n, t, chunk, now) {
                complete = Some((seq, full));
            }
        }
        assert_eq!(complete.as_ref().map(|(_, p)| p.as_slice()), Some(payload.as_slice()));
        // 组首 seq 恒为 first_seq,与到达顺序无关。
        assert_eq!(complete.map(|(s, _)| s), Some(100));
    }

    #[test]
    fn partial_group_not_complete() {
        let payload = vec![1, 2, 3, 4, 5];
        let parts = split(&payload, 1, 200, 3);
        assert_eq!(parts.len(), 2);
        let mut asm = FragmentAssembler::new();
        let now = Instant::now();
        let (g, fs, n, t, c) = decode_fragment(&parts[0]).unwrap();
        assert!(asm.push(g, fs, n, t, c, now).is_none());
        assert_eq!(asm.pending_count(), 1);
    }

    #[test]
    fn duplicate_fragment_ignored() {
        let payload = vec![1, 2, 3, 4];
        let parts = split(&payload, 2, 300, 2);
        let mut asm = FragmentAssembler::new();
        let now = Instant::now();
        for idx in [0usize, 0, 1] {
            let (g, fs, n, t, c) = decode_fragment(&parts[idx]).unwrap();
            if let Some((_, full)) = asm.push(g, fs, n, t, c, now) {
                assert_eq!(full, payload);
            }
        }
        assert_eq!(asm.pending_count(), 0);
    }

    #[test]
    fn malformed_fragment_rejected() {
        assert!(decode_fragment(&[0, 0]).is_err()); // 太短
        // total=0 非法
        let body = encode_fragment(1, 2, 0, 0, b"x");
        assert!(decode_fragment(&body).is_err());
        // frag_no >= total
        let body = encode_fragment(1, 2, 2, 2, b"x");
        assert!(decode_fragment(&body).is_err());
        // total 超过上限
        let body = encode_fragment(1, 2, 0, 65, b"x");
        assert!(decode_fragment(&body).is_err());
    }

    #[test]
    fn cleanup_expired() {
        let payload = vec![1, 2, 3];
        let parts = split(&payload, 3, 400, 2);
        let mut asm = FragmentAssembler::new();
        let now = Instant::now();
        let (g, fs, n, t, c) = decode_fragment(&parts[0]).unwrap();
        asm.push(g, fs, n, t, c, now);
        // 超时后清理
        asm.cleanup(now + FRAGMENT_TIMEOUT + Duration::from_secs(1));
        assert_eq!(asm.pending_count(), 0);
    }

    #[test]
    fn retransmitted_completed_group_ignored() {
        let payload = vec![1, 2, 3, 4];
        let parts = split(&payload, 5, 500, 2);
        let mut asm = FragmentAssembler::new();
        let now = Instant::now();
        // 完成
        for idx in [0usize, 1] {
            let (g, fs, n, t, c) = decode_fragment(&parts[idx]).unwrap();
            let _ = asm.push(g, fs, n, t, c, now);
        }
        // 重传帧(模拟丢包重传):忽略
        let (g, fs, n, t, c) = decode_fragment(&parts[0]).unwrap();
        assert!(asm.push(g, fs, n, t, c, now).is_none());
    }
}
