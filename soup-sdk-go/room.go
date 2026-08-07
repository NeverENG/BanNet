package soup

import (
	"encoding/binary"
	"time"
)

// Room 是逻辑服的核心契约:房间实现只写游戏规则,其余(连接、调度、
// 快照调度、缓冲池)全部由 SDK 承担。
//
// 全部方法都在房间专属 goroutine 上被串行调用,实现不得自行启动
// goroutine、不得使用 time.Now / math/rand、不得遍历 map 参与游戏逻辑
// (map 遍历顺序随机,会破坏确定性)。
type Room interface {
	// OnJoin 在玩家加入房间时调用。
	OnJoin(ctx *RoomCtx, p PlayerID)

	// OnLeave 在玩家离开房间时调用(超时/被踢/退出,见 LeaveReason)。
	OnLeave(ctx *RoomCtx, p PlayerID, why LeaveReason)

	// OnResume 在断线重连成功时调用。SDK 已先推送全量状态,
	// 这里只需处理业务副作用(如清除"离线中"标记),不要当新玩家处理。
	OnResume(ctx *RoomCtx, p PlayerID, gapMS uint32)

	// OnInput 交付一条输入。payload 是 SDK 的复用缓冲,返回后即失效,
	// 必须立即解码进自己预分配的结构,不得持有该切片。
	OnInput(ctx *RoomCtx, p PlayerID, seq InputSeq, payload []byte)

	// Tick 定频推进一步。dtMS 恒等于 1000/TickHz,与墙钟无关。
	Tick(ctx *RoomCtx, tick Tick, dtMS uint32) Outcome

	// EncodeSnapshot 把 target 视角的快照写进 out(零分配,直接写缓冲)。
	// baseline.Valid == false 时必须输出可独立解码的全量。
	EncodeSnapshot(target PlayerID, baseline Baseline, out *Buffer)

	// EncodeFullState 输出重连/中途加入时的全量状态。
	EncodeFullState(target PlayerID, out *Buffer)

	// StateHash 返回状态哈希,用于确定性回放校验;不校验时返回 0。
	StateHash() uint64
}

// RoomCtx 是房间与外界交互的唯一通道。
// 它的方法只在房间 goroutine 上被调用,内部不加锁。
//
// ⛔ 刻意不提供:time.Now / math/rand / 任何 IO / 任何 goroutine 启动接口。
type RoomCtx struct {
	r *room
}

// BeginSend 取一个发往单个玩家的出站缓冲。
// 目标不在线时返回的缓冲会在 Commit 时静默丢弃(恒返回非 nil)。
func (c *RoomCtx) BeginSend(to PlayerID, ch Channel, msg MsgID) *Buffer {
	r := c.r
	b := r.buffer()
	b.reset(sendHeadLen, false)
	st, ok := r.players[to]
	if !ok {
		b.invalid = true
		return b
	}
	b.data[4] = FrameSend
	binary.LittleEndian.PutUint64(b.data[5:13], st.sess)
	b.data[13] = byte(ch)
	binary.LittleEndian.PutUint16(b.data[14:16], uint16(msg))
	return b
}

// BeginMulticast 取一个发往多个玩家的出站缓冲(0x82 Multicast,一份 payload)。
// 不在线的目标会被过滤;全部不在线时 Commit 静默丢弃。
func (c *RoomCtx) BeginMulticast(to []PlayerID, ch Channel, msg MsgID) *Buffer {
	r := c.r
	n := 0
	for _, p := range to {
		if _, ok := r.players[p]; ok {
			n++
		}
	}
	b := r.buffer()
	off := multicastHeadLen(n)
	b.reset(off, true)
	if n == 0 {
		b.invalid = true
		return b
	}
	b.data[4] = FrameMulticast
	b.data[5] = byte(n)
	idx := 6
	for _, p := range to {
		st, ok := r.players[p]
		if !ok {
			continue
		}
		binary.LittleEndian.PutUint64(b.data[idx:idx+8], st.sess)
		idx += 8
	}
	b.data[off-3] = byte(ch)
	binary.LittleEndian.PutUint16(b.data[off-2:off], uint16(msg))
	return b
}

// BeginBroadcast 取一个发给房间内全部玩家的出站缓冲(等价于多播所有人)。
func (c *RoomCtx) BeginBroadcast(ch Channel, msg MsgID) *Buffer {
	r := c.r
	n := len(r.players)
	b := r.buffer()
	off := multicastHeadLen(n)
	b.reset(off, true)
	if n == 0 {
		b.invalid = true
		return b
	}
	b.data[4] = FrameMulticast
	b.data[5] = byte(n)
	idx := 6
	for _, st := range r.players {
		binary.LittleEndian.PutUint64(b.data[idx:idx+8], st.sess)
		idx += 8
	}
	b.data[off-3] = byte(ch)
	binary.LittleEndian.PutUint16(b.data[off-2:off], uint16(msg))
	return b
}

// Commit 把缓冲交给 SDK:编码成帧投入出站队列,缓冲归池。
// 同一缓冲只能 Commit/Abort 一次。
func (c *RoomCtx) Commit(b *Buffer) {
	r := c.r
	if b == nil || b.closed {
		return
	}
	b.closed = true
	if b.invalid {
		r.srv.bufPool.Put(b)
		return
	}
	// 填 len 字段:body 字节数(不含 type;帧格式 = len u32 · type u8 · body)。
	// off 含 len(4)+type(1)+body 头(11),故 body 字节数 = off+len-5。
	binary.LittleEndian.PutUint32(b.data[0:4], uint32(b.off+b.len-5))
	r.outFrames = append(r.outFrames, b)
}

// Abort 放弃缓冲,直接归池。
func (c *RoomCtx) Abort(b *Buffer) {
	if b == nil || b.closed {
		return
	}
	b.closed = true
	c.r.srv.bufPool.Put(b)
}

// Players 返回房间内全部玩家的复用切片,勿持有(下次调用会复用)。
func (c *RoomCtx) Players() []PlayerID {
	r := c.r
	r.playersBuf = r.playersBuf[:0]
	for p := range r.players {
		r.playersBuf = append(r.playersBuf, p)
	}
	return r.playersBuf
}

// LastProcessedInput 返回该玩家最后被处理(交付 OnInput)的输入序号。
// 客户端用它做预测与和解。
func (c *RoomCtx) LastProcessedInput(p PlayerID) InputSeq {
	if st, ok := c.r.players[p]; ok {
		return st.lastSeq
	}
	return 0
}

// RTTms 返回该玩家最近的往返时延(由框架 SessionStats 推送)。
func (c *RoomCtx) RTTms(p PlayerID) uint16 {
	if st, ok := c.r.players[p]; ok {
		return st.rtt
	}
	return 0
}

// LossPermille 返回该玩家最近的丢包率(千分比,由框架 SessionStats 推送)。
func (c *RoomCtx) LossPermille(p PlayerID) uint16 {
	if st, ok := c.r.players[p]; ok {
		return st.loss
	}
	return 0
}

// IsConnected 返回玩家是否在线。宽限期内断线为 false,但不触发 OnLeave。
func (c *RoomCtx) IsConnected(p PlayerID) bool {
	_, ok := c.r.players[p]
	return ok
}

// Rand 返回房间的确定性随机源(由建房 seed 播种)。
func (c *RoomCtx) Rand() *DetRand { return c.r.rand }

// End 结束房间:SDK 停止 tick,对剩余玩家逐一 OnLeave 并回收房间。
func (c *RoomCtx) End(result Result) {
	r := c.r
	r.endResult = result
	r.stopReq = true
}

// RequestKeyframe 请求对某玩家强制下发一次全量快照(下一调度即生效)。
// 用于客户端请求纠偏、首次进入、或长时间丢包后的主动重同步。
func (c *RoomCtx) RequestKeyframe(p PlayerID) {
	if st, ok := c.r.players[p]; ok {
		st.forceKeyframe = true
	}
}

// Kick 立即踢出玩家:发 0x83 Kick 帧、调 OnLeave(LeaveKicked)并从房间移除。
// reason 是透传给框架/客户端的踢出原因码。
func (c *RoomCtx) Kick(p PlayerID, reason uint8) {
	r := c.r
	st, ok := r.players[p]
	if !ok {
		return
	}
	r.srv.sendKick(st.sess, reason)
	delete(r.players, p)
	delete(r.sessOf, st.sess)
	r.srv.removeSession(st.sess)
	r.srv.metrics.PlayersOnline.Add(-1)
	r.impl.OnLeave(c, p, LeaveKicked)
	if len(r.players) == 0 {
		r.stopReq = true
	}
}

// sendHeadLen 是单播 Send 帧的头部长度:5(帧头)+ 8(sess)+ 1(ch)+ 2(msg)。
const sendHeadLen = 16

// multicastHeadLen 是多播帧的头部长度:5 + 1(n)+ 8n + 1(ch)+ 2(msg)。
func multicastHeadLen(n int) int { return 9 + 8*n }

// inKind 是房间入站事件的类型。
type inKind uint8

const (
	inOpen     inKind = iota // SessionOpen:玩家加入
	inClose                  // SessionClose:玩家离开
	inResume                 // SessionResume:断线重连
	inData                   // Data:输入/业务消息
	inStats                  // SessionStats:更新 rtt/丢包
	inOverload               // Overload:全局降频
)

// inEvent 是投递给房间 goroutine 的事件。
// Data 事件的 payload 是读缓冲的子切片,处理完(OnInput 返回)后必须归还 raw。
type inEvent struct {
	kind    inKind
	sess    uint64
	player  PlayerID
	reason  uint8
	gapMS   uint32
	ch      Channel
	msg     MsgID
	rtt     uint16
	loss    uint16
	payload []byte
	raw     []byte // 读池缓冲(仅 inData 携带)
}

// pstate 是房间内一个玩家的会话状态(房间 goroutine 独占)。
// S3 字段:抖动缓冲(jbuf/lastSeq/lastPayload)、基线环、带宽降级。
type pstate struct {
	srv  *Server
	sess uint64
	rtt  uint16
	loss uint16

	lastSeq InputSeq // 已交付最大输入序号(去重基准)

	// 抖动缓冲(M04)。
	jbuf           []jitterEntry
	jcap           int
	jdepth         int
	lastPayload    []byte // 缓冲为空时重复上一帧的副本(预分配,零分配)
	lastPayloadLen int

	// 基线机制(M05)。
	lastRecvSnap  Tick // 客户端最后收到的快照 tick(输入包回传)
	baselineRing  []Tick
	baselineCap   int
	bytesOut      uint32 // 本秒快照字节(降级统计)
	degrade       int    // 降级档位 0..2(20→10→5Hz)
	forceKeyframe bool
}

// room 是 SDK 侧的房间运行时:一个房间一个 goroutine,状态被其独占。
type room struct {
	srv  *Server
	id   RoomID
	impl Room
	rand *DetRand
	ctx  *RoomCtx

	inbox chan inEvent // 有界入站队列,满则丢最旧(见 server 路由)

	players    map[PlayerID]*pstate
	sessOf     map[uint64]PlayerID
	playersBuf []PlayerID // Players() 的复用切片

	tick      Tick
	dtMS      uint32
	next      time.Time // 下一次 tick 的墙钟时刻
	stopReq   bool
	endResult Result

	outFrames []*Buffer // 本 tick 内 Commit 的帧,flush 时统一投出站队列

	// S3/S4。
	pendingBuf []pendingDeliver // 本 tick 待交付输入(复用,零分配)
	overload   bool             // 框架 Overload:全局降频
	replay     *ReplayWriter    // 非 nil 时录制输入
}

// buffer 从池取一个出站缓冲。
func (r *room) buffer() *Buffer {
	return r.srv.bufPool.Get().(*Buffer)
}
