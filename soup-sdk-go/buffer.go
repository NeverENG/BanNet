package soup

import (
	"encoding/binary"
	"math"
)

// Buffer 是出站消息的写入缓冲:房间通过 RoomCtx.Begin* 取走一个 Buffer,
// 用 Put* 系列写入(全部小端),最后 Commit 交给 SDK 发送或 Abort 放弃。
//
// 所有 Buffer 来自 sync.Pool,Commit/Abort 后即归还,禁止继续使用。
// 内部字节布局(单播 Send,头部 16 字节):
//
//	[0:4]  len u32(不含本字段)  [4] type 0x81
//	[5:13] sess_id u64          [13] ch u8  [14:16] msg_id u16
//	[16:]  payload
//
// 多播 Multicast 头部为 9+8n 字节:
//
//	[0:4] len u32  [4] type 0x82  [5] n u8  [6:6+8n] sess_id[n]
//	[6+8n] ch u8  [7+8n:9+8n] msg_id u16  [9+8n:] payload
type Buffer struct {
	data    []byte // 完整帧字节;头部区在 data[:off],payload 从 off 起
	off     int    // payload 起始偏移,即头部长度
	len     int    // 已写入的 payload 字节数
	multi   bool   // 是否为多播帧
	closed  bool   // 已 Commit/Abort
	invalid bool   // 目标全部无效(无人在线),Commit 时静默丢弃
}

// poolBufferCap 是缓冲池的初始容量;超出时 Buffer 自动扩容并保留大容量复用。
const poolBufferCap = 4096

// Len 返回当前已写入的 payload 字节数。
func (b *Buffer) Len() int { return b.len }

// reset 准备复用:设置头部长度与帧类型信息,清空 payload。
func (b *Buffer) reset(off int, multi bool) {
	b.off = off
	b.len = 0
	b.multi = multi
	b.closed = false
	b.invalid = false
	if cap(b.data) < off {
		b.data = make([]byte, off)
	}
}

// grow 保证 data 至少能容纳 off+len+n 字节,必要时扩容(保留已写内容)。
func (b *Buffer) grow(n int) {
	need := b.off + b.len + n
	if need <= len(b.data) {
		return
	}
	nd := make([]byte, need*2)
	copy(nd, b.data[:b.off+b.len])
	b.data = nd
}

// PutU8 写入一个字节。
func (b *Buffer) PutU8(v uint8) {
	b.grow(1)
	b.data[b.off+b.len] = v
	b.len++
}

// PutU16 以小端序写入 uint16。
func (b *Buffer) PutU16(v uint16) {
	b.grow(2)
	binary.LittleEndian.PutUint16(b.data[b.off+b.len:], v)
	b.len += 2
}

// PutU32 以小端序写入 uint32。
func (b *Buffer) PutU32(v uint32) {
	b.grow(4)
	binary.LittleEndian.PutUint32(b.data[b.off+b.len:], v)
	b.len += 4
}

// PutI16 以小端序写入 int16。
func (b *Buffer) PutI16(v int16) {
	b.PutU16(uint16(v))
}

// PutVarint 以 zigzag + LEB128 变长编码写入 int64。
// 小绝对值占用更少字节;负数的绝对值按 2 倍映射(与 Protobuf varint 兼容)。
func (b *Buffer) PutVarint(v int64) {
	u := uint64(v)<<1 ^ uint64(v>>63) // zigzag:0→0,-1→1,1→2,-2→3...
	for u >= 0x80 {
		b.PutU8(byte(u) | 0x80)
		u >>= 7
	}
	b.PutU8(byte(u))
}

// PutFixed16 写入定点量化:q = int16(f * scale)。
// 调用方负责保证 f*scale 落在 int16 范围内(超出时结果截断,行为确定)。
func (b *Buffer) PutFixed16(f float32, scale float32) {
	b.PutI16(int16(f * scale))
}

// PutAngle 以 u16 全范围量化写入弧度角:[-π, π) → [0, 65535]。
// 边界值会被钳制,避免浮点转整数溢出导致的平台相关行为。
func (b *Buffer) PutAngle(rad float32) {
	const half = 32768.0 / float32(math.Pi)
	s := rad * half
	switch {
	case s >= 32767:
		b.PutU16(0xFFFF)
	case s <= -32768:
		b.PutU16(0)
	default:
		b.PutU16(uint16(int16(s)))
	}
}

// PutBytes 写入长度前缀字节串:u16 长度 + 原始字节。
func (b *Buffer) PutBytes(p []byte) {
	b.PutU16(uint16(len(p)))
	b.grow(len(p))
	copy(b.data[b.off+b.len:], p)
	b.len += len(p)
}
