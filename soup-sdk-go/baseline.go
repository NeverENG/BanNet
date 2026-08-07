package soup

import "log"

// 本文件实现快照调度与基线机制(规格书 T0003M05)。
//
// - baseline 环形缓冲:每客户端维护已发快照的 tick 环;客户端在输入包里
//   回传 lastRecvSnapshotTick,SDK 据此选 baseline 交给 EncodeSnapshot
// - 关键帧:每 KeyframeIntervalTicks 强制一次全量;ctx.RequestKeyframe 手动
// - 带宽降级:超 BudgetKbpsPerClient 或收到框架 Overload 时降低快照频率
//   (绝不截断快照内容),必须打 WARN 并计 snapshot_degraded

// baselineHas 判断 tick 是否在玩家的已发快照环内(可作为增量基准)。
func (st *pstate) baselineHas(t Tick) bool {
	for _, x := range st.baselineRing {
		if x == t {
			return true
		}
	}
	return false
}

// pushBaseline 记录一次已发快照的 tick(环形,满则丢最旧)。
func (st *pstate) pushBaseline(t Tick) {
	st.baselineRing = append(st.baselineRing, t)
	if len(st.baselineRing) > st.baselineCap {
		copy(st.baselineRing, st.baselineRing[1:])
		st.baselineRing = st.baselineRing[:len(st.baselineRing)-1]
	}
}

// clearBaseline 重连/中途加入时清空基线,强制走全量。
func (st *pstate) clearBaseline() {
	st.baselineRing = st.baselineRing[:0]
}

// scheduleSnapshots 按 SnapshotHz 为每个玩家调度快照:
//   - 每 KeyframeIntervalTicks 强制全量(纠偏/快速重连)
//   - 客户端回传的 lastRecvSnapshotTick 在环内 → 增量(Valid=true)
//   - 不在环内(长时间丢包)或 RequestKeyframe → 全量(Valid=false)
//   - 带宽超预算/Overload → 降频(不截断内容)
func (r *room) scheduleSnapshots() {
	every := r.snapshotEvery()
	if every <= 0 {
		return
	}
	if uint32(r.tick)%uint32(every) != 0 {
		return
	}
	forceKey := r.srv.cfg.KeyframeIntervalTicks > 0 &&
		uint32(r.tick)%uint32(r.srv.cfg.KeyframeIntervalTicks) == 0
	for p, st := range r.players {
		b := r.ctx.BeginSend(p, ChUnreliable, MsgSnapshot)
		// SDK 快照头:snapshotTick + lastProcessedInputSeq(客户端据此回传)。
		b.PutU32(uint32(r.tick))
		b.PutU16(uint16(st.lastSeq))
		var baseline Baseline
		switch {
		case forceKey || st.forceKeyframe:
			baseline = Baseline{Valid: false} // 全量
		case st.baselineHas(st.lastRecvSnap):
			baseline = Baseline{Tick: st.lastRecvSnap, Valid: true}
		default:
			baseline = Baseline{Valid: false} // 无有效基线:全量
		}
		r.impl.EncodeSnapshot(p, baseline, b)
		r.ctx.Commit(b)
		r.srv.metrics.SnapshotsSent.Add(1)
		if !baseline.Valid {
			r.srv.metrics.SnapshotsFull.Add(1)
		}
		st.forceKeyframe = false
		st.pushBaseline(r.tick)
		// 带宽统计:本 tick 该玩家快照字节(帧体不含帧头)。
		st.bytesOut += uint32(b.off + b.len - sendHeadLen)
	}
	// 每秒结算一次降级(20Hz 下每 20 tick 一次)。
	if r.srv.cfg.TickHz > 0 && uint32(r.tick)%uint32(r.srv.cfg.TickHz) == 0 {
		r.settleDegrade()
	}
}

// settleDegrade 每秒统计每客户端带宽,超预算则升一级降频(20→10→5Hz),
// 恢复后逐级回落。必须 WARN + 计数,不许静默降级。
func (r *room) settleDegrade() {
	budget := uint32(r.srv.cfg.BudgetKbpsPerClient)
	for _, st := range r.players {
		kbps := st.bytesOut * 8 / 1024
		st.bytesOut = 0
		if budget > 0 {
			if kbps > budget && st.degrade < 2 {
				st.degrade++
				log.Printf("soup: 房间 %d 玩家带宽 %d kbps 超预算 %d,快照降频到 %dHz", r.id, kbps, budget, r.snapshotHz())
				r.srv.metrics.Degraded.Add(1)
			} else if kbps <= budget/2 && st.degrade > 0 {
				st.degrade--
			}
		}
	}
	// 框架 Overload 信号:全局降频一级(持续直到取消)。
	if r.overload {
		r.srv.metrics.Degraded.Add(1)
	}
}

// snapshotHz 返回当前实际快照频率(受降级影响)。
func (r *room) snapshotHz() int {
	if r.srv.cfg.SnapshotHz <= 0 {
		return 0 // 禁用快照
	}
	hz := r.srv.cfg.SnapshotHz
	if r.overload {
		hz /= 2
	}
	// 玩家级降级取最大降级数。
	maxDeg := 0
	for _, st := range r.players {
		if st.degrade > maxDeg {
			maxDeg = st.degrade
		}
	}
	for i := 0; i < maxDeg; i++ {
		hz /= 2
	}
	if hz < 1 {
		hz = 1
	}
	return hz
}
