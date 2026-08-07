package soup

import (
	"encoding/binary"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// ── M10 通用性验收:Pong(房间实现 <80 行,不修改 SDK 任何一行)──

// pong 是双人 Pong 逻辑服:球弹跳、挡板移动、计分。
// 确定性:不用 time.Now / math/rand / map 遍历;输入只改挡板位置。
type pong struct {
	ball  [2]int32
	vel   [2]int32
	pad   [2]int32
	score [2]uint8
}

func (g *pong) OnJoin(ctx *RoomCtx, p PlayerID)               {}
func (g *pong) OnResume(ctx *RoomCtx, p PlayerID, gap uint32) {}
func (g *pong) OnLeave(ctx *RoomCtx, p PlayerID, why LeaveReason) {
	if len(ctx.Players()) == 1 {
		ctx.End(Result{Aborted: true})
	}
}
func (g *pong) OnInput(ctx *RoomCtx, p PlayerID, _ InputSeq, b []byte) {
	if len(b) > 0 {
		g.pad[p] += int32(int8(b[0])) * 4
	}
}
func (g *pong) Tick(ctx *RoomCtx, t Tick, dtMS uint32) Outcome {
	g.ball[0] += g.vel[0]
	g.ball[1] += g.vel[1]
	if g.ball[1] < 0 || g.ball[1] > 600 {
		g.vel[1] = -g.vel[1]
	}
	for i := range 2 {
		dx := g.ball[0] - g.pad[i]
		if dx > -20 && dx < 20 && (g.ball[1] < 40 || g.ball[1] > 560) {
			g.vel[0] = -g.vel[0]
		}
	}
	if g.ball[0] < 0 {
		g.score[1]++
		g.ball = [2]int32{300, 300}
		g.vel = [2]int32{6, 4}
	}
	if g.ball[0] > 600 {
		g.score[0]++
		g.ball = [2]int32{300, 300}
		g.vel = [2]int32{-6, 4}
	}
	if g.score[0] >= 5 || g.score[1] >= 5 {
		return End
	}
	return Continue
}
func (g *pong) EncodeSnapshot(target PlayerID, _ Baseline, out *Buffer) {
	out.PutI16(int16(g.ball[0]))
	out.PutI16(int16(g.ball[1]))
	out.PutI16(int16(g.pad[0]))
	out.PutI16(int16(g.pad[1]))
}
func (g *pong) EncodeFullState(target PlayerID, out *Buffer) {
	g.EncodeSnapshot(target, Baseline{}, out)
}
func (g *pong) StateHash() uint64 {
	h := fnv.New64a()
	var tmp [14]byte
	binary.LittleEndian.PutUint32(tmp[0:4], uint32(g.ball[0]))
	binary.LittleEndian.PutUint32(tmp[4:8], uint32(g.ball[1]))
	binary.LittleEndian.PutUint16(tmp[8:10], uint16(g.pad[0]))
	binary.LittleEndian.PutUint16(tmp[10:12], uint16(g.pad[1]))
	tmp[12] = g.score[0]
	tmp[13] = g.score[1]
	_, _ = h.Write(tmp[:])
	return h.Sum64()
}

// pongNew 是 pong 的建房工厂:球带初速,确保 Tick 有状态演化
// (否则重放缺 Tick 推进也测不出 —— review 踩坑)。
func pongNew(seed uint64) Room {
	g := &pong{}
	g.vel = [2]int32{6, 4}
	return g
}

// newTestRoom 构造一个测试房间(带一个已加入的玩家)。
func newTestRoom(t *testing.T, impl Room) *room {
	t.Helper()
	srv := &Server{
		cfg: Config{
			TickHz:                20,
			SnapshotHz:            10,
			JitterBufferTicks:     2,
			KeyframeIntervalTicks: 100,
			BaselineRingSize:      32,
		},
		metrics:  Metrics{},
		bufPool:  sync.Pool{New: func() any { return &Buffer{data: make([]byte, poolBufferCap)} }},
		readPool: sync.Pool{New: func() any { return make([]byte, 4096) }},
	}
	r := &room{
		srv:        srv,
		impl:       impl,
		rand:       NewDetRand(42),
		inbox:      make(chan inEvent, 256),
		players:    make(map[PlayerID]*pstate, 4),
		sessOf:     make(map[uint64]PlayerID, 4),
		dtMS:       50,
		outFrames:  make([]*Buffer, 0, 16),
		pendingBuf: make([]pendingDeliver, 0, 8),
	}
	r.ctx = &RoomCtx{r: r}
	st := &pstate{
		srv:  srv,
		sess: 100,
		jcap: 12, jdepth: 2,
		jbuf:         make([]jitterEntry, 0, 12),
		lastPayload:  make([]byte, 2048),
		baselineCap:  32,
		baselineRing: make([]Tick, 0, 32),
	}
	r.players[1] = st
	r.sessOf[100] = 1
	return r
}

// ── 零分配断言(S4 M07/M11F03):稳态 Tick + EncodeSnapshot 合计 0 分配 ──

func TestPongZeroAlloc(t *testing.T) {
	r := newTestRoom(t, &pong{})
	// 预热池。
	for i := 0; i < 100; i++ {
		r.doTick()
	}
	allocs := testing.AllocsPerRun(200, func() {
		r.doTick()
	})
	if allocs > 0 {
		t.Fatalf("稳态 doTick 应零分配,实际 %.2f allocs/tick", allocs)
	}
}

// ── 确定性回放(S4 M11F02):录输入 → 重放 → StateHash 一致 ──

func TestReplayDeterminism(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pong.replay")

	// 录制:驱动一次真实房间,记录最终 hash 与输入序列。
	rec, err := newReplayWriter(path, 42, 20)
	if err != nil {
		t.Fatal(err)
	}
	room := pongNew(42)
	inputs := []struct {
		tick uint32
		pl   uint32
		seq  uint16
		body []byte
	}{
		{0, 0, 1, []byte{1}}, {0, 1, 1, []byte{255}},
		{1, 0, 2, []byte{1}}, {1, 1, 2, []byte{1}},
		{2, 0, 3, []byte{255}},
		{3, 1, 3, []byte{255}},
		{4, 0, 4, []byte{1}}, {4, 1, 4, []byte{1}},
	}
	// 按 (tick, player) 全序喂(与 SDK 交付顺序一致)。
	for t := uint32(0); t < 30; t++ {
		for _, in := range inputs {
			if in.tick == t {
				rec.Record(in.tick, in.pl, in.seq, in.body)
				room.OnInput(nil, PlayerID(in.pl), InputSeq(in.seq), in.body)
			}
		}
		room.Tick(nil, Tick(t), 50)
	}
	wantHash := room.StateHash()
	rec.Finish(30) // 录制跑了 30 个 tick
	rec.Close()

	// 重放:重建房间,离线跑完,比对 hash。
	gotHash, n, err := Replay(path, pongNew)
	if err != nil {
		t.Fatalf("重放失败: %v", err)
	}
	if n != uint64(len(inputs)) {
		t.Fatalf("重放输入条数 = %d, want %d", n, len(inputs))
	}
	if gotHash != wantHash {
		t.Fatalf("重放 StateHash = %#x, 录制时 = %#x —— 状态不一致", gotHash, wantHash)
	}
	// 幂等:再次重放结果相同。
	hash2, _, err := Replay(path, pongNew)
	if err != nil || hash2 != gotHash {
		t.Fatalf("两次重放不一致: %#x vs %#x (%v)", hash2, gotHash, err)
	}
}

// ── 抖动缓冲与去重(S3 M04)──

func TestJitterBufferDedupAndOrder(t *testing.T) {
	srv := &Server{metrics: Metrics{}, readPool: sync.Pool{New: func() any { return make([]byte, 4096) }}}
	st := &pstate{srv: srv, jcap: 8, jdepth: 2, lastPayload: make([]byte, 2048)}

	// 插入乱序输入:3,1,2 → 按 seq 有序。
	st.insertJitter(jitterEntry{seq: 3, payload: []byte("c"), due: 4})
	st.insertJitter(jitterEntry{seq: 1, payload: []byte("a"), due: 2})
	st.insertJitter(jitterEntry{seq: 2, payload: []byte("b"), due: 3})
	if len(st.jbuf) != 3 {
		t.Fatalf("缓冲长度 = %d, want 3", len(st.jbuf))
	}
	for i := 1; i < len(st.jbuf); i++ {
		if !seqNewer(st.jbuf[i].seq, st.jbuf[i-1].seq) {
			t.Fatalf("乱序: %v", st.jbuf)
		}
	}
	// 去重:重复 seq=2 应被拒绝。
	if st.insertJitter(jitterEntry{seq: 2, payload: []byte("x"), due: 3}) {
		t.Fatal("重复 seq 不应被接受")
	}
	// 溢出:连续插入到超过 jcap → 丢最旧(seq 最小)。
	for s := InputSeq(10); s < 20; s++ {
		st.insertJitter(jitterEntry{seq: s, payload: []byte{byte(s)}, due: 99})
	}
	if len(st.jbuf) > st.jcap {
		t.Fatalf("缓冲超过容量: %d > %d", len(st.jbuf), st.jcap)
	}
	for i := 1; i < len(st.jbuf); i++ {
		if !seqNewer(st.jbuf[i].seq, st.jbuf[i-1].seq) {
			t.Fatalf("溢出后失序: %v", st.jbuf)
		}
	}
	// 头部是剩余最小 seq:初始 1,2,3 + 10..19 共 13 条,容量 8,丢 5 条最旧。
	if st.jbuf[0].seq != 12 {
		t.Fatalf("丢最旧后头部应为 seq=12, got %d (%v)", st.jbuf[0].seq, st.jbuf)
	}
}

func TestJitterDepthDynamic(t *testing.T) {
	if d := jitterDepth(2, 0); d != 2 {
		t.Fatalf("低 RTT 深度 = %d, want 2", d)
	}
	if d := jitterDepth(2, 240); d != 5 {
		t.Fatalf("高 RTT 深度 = %d, want 5", d)
	}
	if d := jitterDepth(5, 5000); d != 5 {
		t.Fatalf("深度上限 = %d, want 5", d)
	}
}

// ── 基线环形缓冲与关键帧(S3 M05)──

func TestBaselineRing(t *testing.T) {
	st := &pstate{baselineCap: 4, baselineRing: make([]Tick, 0, 4)}
	for i := Tick(0); i < 6; i++ {
		st.pushBaseline(i)
	}
	// 环容量 4:0,1 被挤出。
	if st.baselineHas(2) != true || st.baselineHas(5) != true {
		t.Fatalf("环内容错误: %v", st.baselineRing)
	}
	if st.baselineHas(0) || st.baselineHas(1) {
		t.Fatalf("环未按容量淘汰: %v", st.baselineRing)
	}
	st.clearBaseline()
	if len(st.baselineRing) != 0 {
		t.Fatalf("clearBaseline 后应为空")
	}
}

// ── 关键帧与全量统计 ──

func TestKeyframeForcesFull(t *testing.T) {
	r := newTestRoom(t, &pong{})
	r.tick = 99 // KeyframeIntervalTicks=100 → tick 99 非关键帧
	r.scheduleSnapshots()
	if r.srv.metrics.SnapshotsFull.Load() != 0 {
		t.Fatal("tick 99 不应强制全量")
	}
	r.tick = 100
	r.scheduleSnapshots()
	if r.srv.metrics.SnapshotsFull.Load() != 1 {
		t.Fatalf("tick 100 关键帧应强制全量, SnapshotsFull=%d", r.srv.metrics.SnapshotsFull.Load())
	}
	if r.srv.metrics.SnapshotsSent.Load() != 1 {
		t.Fatalf("快照计数错误: %d", r.srv.metrics.SnapshotsSent.Load())
	}
	_ = os.Remove
}
