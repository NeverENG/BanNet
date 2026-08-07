package soup

import "encoding/binary"

// 本文件实现输入抖动缓冲与去重(规格书 T0003M04)。
//
// 抖动缓冲:
//   - 客户端输入(ch=1)带 clientTick/inputSeq,SDK 按 JitterBufferTicks
//     延迟交付,抹平网络抖动(默认 2 tick,按 RTT 动态调 2~5)
//   - 缓冲为空 → 该玩家本 tick 重复上一帧输入(比冻结手感好),计 input_starved
//   - 缓冲溢出 → 丢最旧,计 InboxDrops
//
// 去重:输入按 inputSeq 去重(客户端每包冗余携带最近几帧),只把没见过的
// 交给房间;1~2 个包的丢失对手感完全无感。

// jitterEntry 是抖动缓冲里的一条待交付输入。
// payload 引用读池缓冲(raw),交付(OnInput)完成后必须归还。
type jitterEntry struct {
	clientTick uint32
	seq        InputSeq
	payload    []byte
	raw        []byte
	due        Tick // 交付时刻(房间 tick),到达时 = r.tick + 深度
}

// jitterDepth 由静态深度与动态调整合成:2~5 tick,随 RTT 增大加深。
func jitterDepth(base int, rtt uint16) int {
	d := base + int(rtt)/60 // 每 ~60ms RTT 加深 1
	if d < 2 {
		d = 2
	}
	if d > 5 {
		d = 5
	}
	return d
}

// seqNewer 是 InputSeq 的回绕安全比较:a > b。
func seqNewer(a, b InputSeq) bool {
	return int16(a-b) > 0
}

// insertJitter 把一条新输入插入抖动缓冲(按 seq 有序),返回是否接受。
// 去重:seq 已交付或已在缓冲中 → 拒绝。
func (st *pstate) insertJitter(e jitterEntry) bool {
	// 去重(回绕安全):不新于已交付最大 seq 的丢弃。
	if st.hasInput && !seqNewer(e.seq, st.lastSeq) {
		if e.raw != nil {
			st.srv.readPool.Put(e.raw)
		}
		return false
	}
	// 缓冲内去重。
	for i := range st.jbuf {
		if st.jbuf[i].seq == e.seq {
			if e.raw != nil {
				st.srv.readPool.Put(e.raw)
			}
			return false
		}
	}
	// 溢出:丢最旧(seq 最小,即头部)。
	if len(st.jbuf) >= st.jcap {
		old := st.jbuf[0]
		if old.raw != nil {
			st.srv.readPool.Put(old.raw)
		}
		copy(st.jbuf, st.jbuf[1:])
		st.jbuf = st.jbuf[:len(st.jbuf)-1]
		st.srv.metrics.InboxDrops.Add(1)
	}
	// 按 seq 有序插入:找第一个比 e 新的位置。
	pos := len(st.jbuf)
	for i := range st.jbuf {
		if seqNewer(st.jbuf[i].seq, e.seq) {
			pos = i
			break
		}
	}
	st.jbuf = append(st.jbuf, jitterEntry{})
	copy(st.jbuf[pos+1:], st.jbuf[pos:])
	st.jbuf[pos] = e
	return true
}

// pendingDeliver 是本 tick 待交付的输入(交付前按 (clientTick, player) 全序
// 排序,保证确定性,与到达顺序无关 —— 规格书 M06)。
type pendingDeliver struct {
	player     PlayerID
	clientTick uint32
	seq        InputSeq
	payload    []byte
}

// deliverReadyInputs 交付所有到期输入:抖动缓冲 → (clientTick, player) 排序 →
// OnInput。输入按序交付(seq 连续推进);缓冲为空时重复上一帧。
// 返回的房间实现调用在房间 goroutine 内,零分配(复用 st.lastPayload 与
// r.pendingBuf)。
func (r *room) deliverReadyInputs() {
	pend := r.pendingBuf[:0]
	for p, st := range r.players {
		// 交付到期且 seq 连续的输入。
		for len(st.jbuf) > 0 {
			head := &st.jbuf[0]
			if head.due > r.tick {
				break
			}
			// 只有 seq 连续才交付(空洞等待重传补帧;ch1 语义:留下续接)。
			if st.hasInput && head.seq != st.lastSeq+1 {
				break
			}
			pend = append(pend, pendingDeliver{player: p, clientTick: head.clientTick, seq: head.seq, payload: head.payload})
			// 副本留作"缓冲为空时重复上一帧"(记实际拷贝长度,防截断越界)。
			st.lastPayloadLen = copy(st.lastPayload, head.payload)
			st.lastSeq = head.seq
			st.hasInput = true
			if head.raw != nil {
				st.srv.readPool.Put(head.raw)
			}
			st.jbuf[0] = jitterEntry{} // 释放引用
			copy(st.jbuf, st.jbuf[1:])
			st.jbuf = st.jbuf[:len(st.jbuf)-1]
		}
		// 缓冲为空:重复上一帧输入(比冻结手感好),计 input_starved。
		if len(st.jbuf) == 0 && st.lastPayloadLen > 0 {
			pend = append(pend, pendingDeliver{player: p, clientTick: uint32(r.tick), seq: st.lastSeq, payload: st.lastPayload[:st.lastPayloadLen]})
			r.srv.metrics.InputStarved.Add(1)
		}
	}
	// 按 (clientTick, player) 全序排序(插入排序,玩家数少,零分配)。
	for i := 1; i < len(pend); i++ {
		for j := i; j > 0 && pend[j-1].clientTick > pend[j].clientTick ||
			pend[j-1].clientTick == pend[j].clientTick && pend[j-1].player > pend[j].player; j-- {
			pend[j-1], pend[j] = pend[j], pend[j-1]
		}
	}
	for _, d := range pend {
		r.recordReplay(d)
		r.impl.OnInput(r.ctx, d.player, d.seq, d.payload)
	}
	r.pendingBuf = pend[:0]
}

// handleCh1Input 处理一条 ch=1 输入:解析帧头 → 抖动缓冲 → (下一 tick 起交付)。
func (r *room) handleCh1Input(ev inEvent) {
	st, ok := r.players[ev.player]
	if !ok {
		r.srv.readPool.Put(ev.raw)
		return
	}
	if len(ev.payload) < inputHeaderLen {
		r.srv.readPool.Put(ev.raw)
		return
	}
	clientTick := binary.LittleEndian.Uint32(ev.payload[0:4])
	seq := InputSeq(binary.LittleEndian.Uint16(ev.payload[4:6]))
	st.lastRecvSnap = Tick(binary.LittleEndian.Uint16(ev.payload[6:8]))
	user := ev.payload[inputHeaderLen:]

	// 每 tick 动态调整深度(按 RTT 抖动,2~5 tick)。
	st.jdepth = jitterDepth(r.srv.cfg.JitterBufferTicks, st.rtt)

	st.insertJitter(jitterEntry{
		clientTick: clientTick,
		seq:        seq,
		payload:    user,
		raw:        ev.raw,
		due:        r.tick + Tick(st.jdepth),
	})
}
