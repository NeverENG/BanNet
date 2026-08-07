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

// doTick 执行一个 tick:drainInbox → 交付输入 → room.Tick → 快照调度 → flush。
func (r *room) doTick() {
	r.drainInbox()
	out := r.impl.Tick(r.ctx, r.tick, r.dtMS)
	if out == End {
		r.stopReq = true
	}
	r.scheduleSnapshots()
	r.flushOutbox()
	r.tick++
}

// drainInbox 非阻塞地消化全部积压事件(帧 → 事件处理)。
// 输入(ch=1 的 Data)按到达顺序直接交付 OnInput(抖动缓冲属于 S3)。
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
// inData 事件的读缓冲(raw)在 OnInput 返回后归还读池。
func (r *room) handleEvent(ev inEvent) {
	if ev.raw != nil {
		defer r.srv.readPool.Put(ev.raw)
	}
	switch ev.kind {
	case inOpen:
		if _, dup := r.players[ev.player]; dup {
			return
		}
		st := &pstate{sess: ev.sess}
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
		// 先推全量,再通知房间(不触发 OnLeave)
		b := r.ctx.BeginSend(ev.player, ChReliableOrdered, MsgFullState)
		r.impl.EncodeFullState(ev.player, b)
		r.ctx.Commit(b)
		r.flushOutbox()
		r.impl.OnResume(r.ctx, ev.player, ev.gapMS)

	case inData:
		st, ok := r.players[ev.player]
		if !ok {
			return
		}
		st.lastSeq = InputSeq(ev.msg)
		r.impl.OnInput(r.ctx, ev.player, InputSeq(ev.msg), ev.payload)

	case inStats:
		if st, ok := r.players[ev.player]; ok {
			st.rtt = ev.rtt
			st.loss = ev.loss
		}
	}
}

// scheduleSnapshots 按 SnapshotHz 为每个玩家调度一次快照。
// 当前里程碑基线恒为 {Valid:false}(全量);S3 引入基线环形缓冲后
// 此处改为按客户端回传的 lastRecvSnapshotTick 选择基线。
func (r *room) scheduleSnapshots() {
	every := r.snapshotEvery()
	if every <= 0 || uint32(r.tick)%uint32(every) != 0 {
		return
	}
	for p := range r.players {
		b := r.ctx.BeginSend(p, ChUnreliable, MsgSnapshot)
		r.impl.EncodeSnapshot(p, Baseline{Valid: false}, b)
		r.ctx.Commit(b)
	}
}

// snapshotEvery 返回每多少个 tick 调度一次快照(TickHz/SnapshotHz,向上取整)。
func (r *room) snapshotEvery() int {
	hz := r.srv.cfg.TickHz
	shz := r.srv.cfg.SnapshotHz
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
}

// mapLeaveReason 把框架 SessionClose 的 reason 映射为 SDK 的 LeaveReason。
// 1 = 宽限期超时(CLOSE_GRACE_TIMEOUT),2 = 被踢(CLOSE_KICKED)。
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
