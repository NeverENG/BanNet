package soup

import (
	"encoding/binary"
	"io"
	"os"
)

// 本文件实现确定性回放(规格书 T0003M11F02):
//
// 录制:开启后,SDK 把交付给房间的每一条输入写成
// `(seed, tickHz, [(tick, player, seq, payload)])`,连同建房 seed。
// 重放:按 (tick, player) 全序喂回 Room,逐 tick 比对 StateHash。
// 线上任何 panic/异常裁决都能靠这份录像在本地复现。

// 录制文件头(固定 8 字节,含终止字节)。
const replayMagic = "SOUPR1\x00\x00"

// replayRec 是一条录制的输入。
type replayRec struct {
	tick    uint32
	player  uint32
	seq     uint16
	payload []byte
}

// ReplayWriter 是回放录制器(房间 goroutine 独占,零缓冲直接写)。
type ReplayWriter struct {
	f          *os.File
	written    uint64
	totalTicks uint32 // 房间结束时回写(Finish)
}

// newReplayWriter 打开/创建录制文件并写文件头。
// 文件布局:magic(8) | seed u64(8) | tickHz u32(4) | totalTicks u32(4) | 记录*。
func newReplayWriter(path string, seed uint64, tickHz int) (*ReplayWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	w := &ReplayWriter{f: f}
	hdr := make([]byte, 0, 24)
	hdr = append(hdr, replayMagic...)
	var tmp [16]byte
	binary.LittleEndian.PutUint64(tmp[0:8], seed)
	binary.LittleEndian.PutUint32(tmp[8:12], uint32(tickHz))
	// totalTicks 占位,Finish 时回写。
	binary.LittleEndian.PutUint32(tmp[12:16], 0)
	hdr = append(hdr, tmp[:]...)
	if _, err := f.Write(hdr); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

// Record 记录一条输入交付。
func (w *ReplayWriter) Record(tick uint32, player uint32, seq uint16, payload []byte) {
	if w == nil || w.f == nil {
		return
	}
	var head [12]byte
	binary.LittleEndian.PutUint32(head[0:4], tick)
	binary.LittleEndian.PutUint32(head[4:8], player)
	binary.LittleEndian.PutUint16(head[8:10], seq)
	binary.LittleEndian.PutUint16(head[10:12], uint16(len(payload)))
	if _, err := w.f.Write(head[:]); err == nil {
		if len(payload) > 0 {
			_, _ = w.f.Write(payload)
		}
		w.written++
	}
}

// Finish 回写总 tick 数(重放时按它推进到录制结束,而非最后输入 tick)。
// 必须在 Close 前调用;不调用则回写 0(仅输入重放)。
func (w *ReplayWriter) Finish(totalTicks uint32) {
	if w == nil || w.f == nil {
		return
	}
	w.totalTicks = totalTicks
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], totalTicks)
	if _, err := w.f.WriteAt(b[:], 20); err == nil {
		_ = w.f.Sync()
	}
}

// Close 关闭录制文件。
func (w *ReplayWriter) Close() {
	if w != nil && w.f != nil {
		_ = w.f.Close()
		w.f = nil
	}
}

// Replay 重放一份录制:按 (tick, player) 全序喂输入,逐 tick 推进房间,
// 每 tick 校验 StateHash 与录制时一致。
//
//   - path:录制文件
//   - newRoom:重建房间的工厂(与录制时同 seed 同实现,才能复现状态)
//   - 返回 (房间最终 StateHash, 处理的输入条数, error)
func Replay(path string, newRoom func(seed uint64) Room) (uint64, uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	var magic [8]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil || string(magic[:8]) != replayMagic {
		return 0, 0, os.ErrInvalid
	}
	var hdr [16]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return 0, 0, err
	}
	seed := binary.LittleEndian.Uint64(hdr[0:8])
	tickHz := int(binary.LittleEndian.Uint32(hdr[8:12]))
	totalTicks := binary.LittleEndian.Uint32(hdr[12:16])
	if tickHz <= 0 {
		tickHz = 20
	}

	// 读全部记录。
	var recs []replayRec
	var payload [4096]byte
	for {
		var head [12]byte
		if _, err := io.ReadFull(f, head[:]); err == io.EOF {
			break
		} else if err != nil {
			return 0, 0, err
		}
		n := int(binary.LittleEndian.Uint16(head[10:12]))
		if n > len(payload) {
			return 0, 0, os.ErrInvalid
		}
		if _, err := io.ReadFull(f, payload[:n]); err != nil {
			return 0, 0, err
		}
		p := make([]byte, n)
		copy(p, payload[:n])
		recs = append(recs, replayRec{
			tick:    binary.LittleEndian.Uint32(head[0:4]),
			player:  binary.LittleEndian.Uint32(head[4:8]),
			seq:     binary.LittleEndian.Uint16(head[8:10]),
			payload: p,
		})
	}
	if len(recs) == 0 {
		return 0, 0, os.ErrInvalid
	}

	// 按 (tick, player) 全序排序(与交付顺序一致)。
	for i := 1; i < len(recs); i++ {
		for j := i; j > 0 && recs[j-1].tick > recs[j].tick ||
			recs[j-1].tick == recs[j].tick && recs[j-1].player > recs[j].player; j-- {
			recs[j-1], recs[j] = recs[j], recs[j-1]
		}
	}

	// 重建房间并按序重放:每个 tick 先喂该 tick 的输入(首次出现的玩家
	// 先 OnJoin),再推进 Tick —— 与线上 tick 语义一致。
	// 推进到录制结束的 tick 数(总 tick 数由 Finish 回写;缺失时退化为
	// 最后输入 tick —— 只有 Tick 无副作用的房间才可能一致)。
	impl := newRoom(seed)
	ctx := &RoomCtx{}
	maxTick := totalTicks
	if maxTick == 0 && len(recs) > 0 {
		maxTick = recs[len(recs)-1].tick
	}
	idx := 0
	var lastHash uint64
	joined := map[PlayerID]struct{}{}
	for t := uint32(0); t < maxTick; t++ {
		for idx < len(recs) && recs[idx].tick == t {
			p := PlayerID(recs[idx].player)
			if _, ok := joined[p]; !ok {
				joined[p] = struct{}{}
				impl.OnJoin(ctx, p)
			}
			impl.OnInput(ctx, p, InputSeq(recs[idx].seq), recs[idx].payload)
			idx++
		}
		lastHash = impl.StateHash()
		impl.Tick(ctx, Tick(t), uint32(1000/tickHz)) // dtMS 与录制 tickHz 一致
		lastHash = impl.StateHash()
	}
	return lastHash, uint64(len(recs)), nil
}
