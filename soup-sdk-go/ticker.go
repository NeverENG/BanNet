package soup

import (
	"context"
	"time"
)

// maxCatchUpTicks 是落后补偿允许连续追帧的上限(单位:tick)。
const maxCatchUpTicks = 3

// run 是房间 goroutine 的主循环:事件驱动 + 定频 tick。
// 房间状态由本 goroutine 独占,全程不加锁。
func (r *room) run(ctx context.Context) {
	defer r.srv.removeRoom(r)

	step := time.Second / time.Duration(r.srv.cfg.TickHz)
	timer := time.NewTimer(step)
	defer timer.Stop()
	r.next = time.Now().Add(step)

	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-r.inbox:
			r.handleEvent(ev)
			if r.stopReq {
				r.stop()
				return
			}
		case <-timer.C:
			r.advance(step, timer)
			if r.stopReq {
				r.stop()
				return
			}
		}
	}
}

// advance 驱动一个或多个 tick 并做落后补偿:
//   - 未落后:重置定时器等待下一 tick;
//   - 落后 ≤ 3 tick:连续追帧补上(循环内继续 doTick,不 sleep);
//   - 落后 > 3 tick:丢弃积压,直接跳到当前时间,计数 tick_skipped。
func (r *room) advance(step time.Duration, timer *time.Timer) {
	for {
		r.doTick()
		r.next = r.next.Add(step)
		now := time.Now()
		if now.Before(r.next) {
			timer.Reset(r.next.Sub(now))
			return
		}
		if behind := now.Sub(r.next); behind > maxCatchUpTicks*step {
			r.srv.metrics.TickSkipped.Add(int64(behind / step))
			r.next = now.Add(step)
			timer.Reset(step)
			return
		}
		// 落后 ≤ 3 tick:继续追帧
	}
}

// doTick 执行一个 tick:drainInbox → 交付输入(抖动缓冲+全序)→ room.Tick →
// 快照调度 → flush。
func (r *room) doTick() {
	r.drainInbox()
	r.deliverReadyInputs()
	out := r.impl.Tick(r.ctx, r.tick, r.dtMS)
	if out == End {
		r.stopReq = true
	}
	r.scheduleSnapshots()
	r.flushOutbox()
	r.tick++
}

// drainInbox 非阻塞地消化全部积压事件(帧 → 事件处理)。
// ch=1 输入进抖动缓冲(S3),ch=2/3 直接交付。
func (r *room) drainInbox() {
	for {
		select {
		case ev := <-r.inbox:
			r.handleEvent(ev)
		default:
			return
		}
	}
}

// handleEvent 处理一条入站事件。
// ch=2/3 的 inData 事件:读缓冲(raw)在 OnInput 返回后归还读池。
// ⚠️ ch=1 输入**不能**在这里归还:raw 的所有权随 jitterEntry 转移,
// 由 deliverReadyInputs 在交付后归还 —— 提前归还会 double-put 且缓冲
// 可能被池复用覆写,导致交付给 OnInput 的数据损坏。
func (r *room) handleEvent(ev inEvent) {
	switch ev.kind {
	case inOpen:
		if _, dup := r.players[ev.player]; dup {
			return
		}
		st := &pstate{
			srv:  r.srv,
			sess: ev.sess,
			// S3:抖动缓冲与基线环容量、上一帧副本缓冲(预分配,零分配)。
			jcap:         r.srv.cfg.JitterBufferTicks*4 + 4,
			jbuf:         make([]jitterEntry, 0, r.srv.cfg.JitterBufferTicks*4+4),
			jdepth:       r.srv.cfg.JitterBufferTicks,
			lastPayload:  make([]byte, 2048),
			baselineCap:  r.srv.cfg.BaselineRingSize,
			baselineRing: make([]Tick, 0, r.srv.cfg.BaselineRingSize),
		}
		r.players[ev.player] = st
		r.sessOf[ev.sess] = ev.player
		r.srv.metrics.PlayersOnline.Add(1)
		r.impl.OnJoin(r.ctx, ev.player)

	case inClose:
		st, ok := r.players[ev.player]
		if !ok {
			return // 已被 ctx.Kick 移除,框架回执的 SessionClose 直接忽略
		}
		delete(r.players, ev.player)
		delete(r.sessOf, st.sess)
		r.srv.removeSession(st.sess)
		r.srv.metrics.PlayersOnline.Add(-1)
		r.impl.OnLeave(r.ctx, ev.player, mapLeaveReason(ev.reason))
		if len(r.players) == 0 {
			r.stopReq = true // 空房:停 tick 并回收
		}

	case inResume:
		if _, ok := r.players[ev.player]; !ok {
			return
		}
		st := r.players[ev.player]
		// 重连(M05F04):清基线环与抖动缓冲 → 推全量 → OnResume(不触发 OnLeave)。
		st.clearBaseline()
		st.jbuf = st.jbuf[:0]
		st.lastSeq = 0
		st.hasInput = false
		b := r.ctx.BeginSend(ev.player, ChReliableOrdered, MsgFullState)
		r.impl.EncodeFullState(ev.player, b)
		r.ctx.Commit(b)
		r.flushOutbox()
		r.impl.OnResume(r.ctx, ev.player, ev.gapMS)

	case inData:
		if ev.ch == ChInput {
			// 输入:进抖动缓冲,由 deliverReadyInputs 按 (clientTick, player) 全序交付。
			r.handleCh1Input(ev)
			return
		}
		if _, ok := r.players[ev.player]; !ok {
			return
		}
		if ev.raw != nil {
			defer r.srv.readPool.Put(ev.raw)
		}
		// ch=2/3 业务事件:直接交付(读缓冲由上面的 defer 归还)。
		r.impl.OnInput(r.ctx, ev.player, InputSeq(ev.msg), ev.payload)

	case inStats:
		if st, ok := r.players[ev.player]; ok {
			st.rtt = ev.rtt
			st.loss = ev.loss
		}
	case inOverload:
		// 全局降频(见 baseline.go settleDegrade)。
		// 保守策略:收到一次 Overload 即保持降频直到房间结束 —— 框架在持续
		// 拥塞时应周期性重发,停止发送即表示恢复(自动恢复未实现,避免抖动)。
		r.overload = true
	}
}

// setOverload 由框架 Overload 帧触发(跨 goroutine,经 channel 投递,
// 房间 goroutine 消费后生效)。
func (r *room) setOverload(on bool) {
	select {
	case r.inbox <- inEvent{kind: inOverload}:
	default:
	}
}

// recordReplay 录制一条输入交付(S4 回放用)。
func (r *room) recordReplay(d pendingDeliver) {
	if r.replay != nil {
		r.replay.Record(uint32(r.tick), uint32(d.player), uint16(d.seq), d.payload)
		r.srv.metrics.ReplayWritten.Add(1)
	}
}

// snapshotEvery 返回每多少个 tick 调度一次快照(基于动态快照频率)。
func (r *room) snapshotEvery() int {
	hz := r.srv.cfg.TickHz
	shz := r.snapshotHz()
	if shz <= 0 || hz <= 0 {
		return 1
	}
	if shz >= hz {
		return 1
	}
	return (hz + shz - 1) / shz
}

// flushOutbox 把本 tick 内 Commit 的帧投入出站队列(非阻塞)。
// 出站队列满时丢弃并计数 out_drops,缓冲归池。
func (r *room) flushOutbox() {
	s := r.srv
	for _, b := range r.outFrames {
		n := b.off + b.len
		select {
		case s.outbox <- outFrame{data: b.data[:n], buf: b}:
		default:
			s.bufPool.Put(b)
			s.metrics.OutDrops.Add(1)
		}
	}
	r.outFrames = r.outFrames[:0]
}

// stop 在房间结束时调用:对剩余玩家逐一 OnLeave 并发送 Kick 帧,然后由
// run 的 defer 完成 rooms/sessions 清理。
func (r *room) stop() {
	for p := range r.players {
		st := r.players[p]
		r.impl.OnLeave(r.ctx, p, LeaveQuit)
		r.srv.sendKick(st.sess, 0)
		r.srv.metrics.PlayersOnline.Add(-1)
	}
	r.flushOutbox()
	r.replay.Finish(uint32(r.tick)) // 回放录制:回写总 tick 数
	r.replay.Close()                // 关闭回放录制
}

// mapLeaveReason 把框架 SessionClose 的 reason 映射为 SDK 的 LeaveReason。// 1 = 宽限期超时(CLOSE_GRACE_TIMEOUT),2 = 被踢(CLOSE_KICKED)。
func mapLeaveReason(reason uint8) LeaveReason {
	switch reason {
	case 1:
		return LeaveTimeout
	case 2:
		return LeaveKicked
	default:
		return LeaveQuit
	}
}
