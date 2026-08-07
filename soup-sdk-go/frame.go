package soup

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
)

// 帧类型常量(与上游 Rust 框架 soup-engine 的协议定义一一对应,字节序全部小端)。
const (
	// 框架 → 逻辑服
	FrameEngineHello   byte = 0x30 // version u16 · caps u32(连接建立首帧)
	FrameSessionOpen   byte = 0x01 // sess_id u64 · addr [u8;18] · token_len u16 · token[]
	FrameSessionClose  byte = 0x02 // sess_id u64 · reason u8
	FrameSessionResume byte = 0x03 // sess_id u64 · gap_ms u32
	FrameDataUp        byte = 0x10 // sess_id u64 · ch u8 · msg_id u16 · payload[]
	FrameSessionStats  byte = 0x20 // sess_id u64 · rtt_ms u16 · loss_permille u16 · out_kbps u16
	FrameOverload      byte = 0x2F // dropped_up u32 · dropped_down u32

	// 逻辑服 → 框架
	FrameLogicHello byte = 0x90 // version u16 · caps u32
	FrameSend       byte = 0x81 // sess_id u64 · ch u8 · msg_id u16 · payload[]
	FrameMulticast  byte = 0x82 // n u8 · sess_id[n] u64 · ch u8 · msg_id u16 · payload[]
	FrameKick       byte = 0x83 // sess_id u64 · reason u8
	FrameSetBudget  byte = 0x84 // sess_id u64 · kbps u16
)

// FrameHeaderLen 是帧头的固定长度:len u32 + type u8。
const FrameHeaderLen = 5

// SDK 与框架握手的协议版本与能力位。
const (
	protocolVersion uint16 = 1
	protocolCaps    uint32 = 0
)

// ErrFrameTooLarge 表示帧长超过接收缓冲容量。
var ErrFrameTooLarge = errors.New("soup: 帧长超过限制")

// ErrNeedMore 表示累积缓冲里的字节不足以构成一帧(半包)。
var ErrNeedMore = errors.New("soup: 帧数据不足")

// TryDecodeFrame 从累积缓冲 acc 尝试解一帧(长度前缀分帧)。
//
//   - 不足一帧:返回 ErrNeedMore,**不消费 acc**(等待更多数据)
//   - 完整:返回 (type, body, nil),body 是 buf 的子切片(零拷贝),
//     调用方保证 buf 在 body 不再使用前不被复用/归还
//   - 帧体超限:返回 ErrFrameTooLarge(畸形流,调用方应丢弃 acc)
//
// 这是 SOCK_STREAM 上的标准分帧器:天然处理半包/粘包。
func TryDecodeFrame(acc *bytes.Buffer, buf []byte) (byte, []byte, error) {
	if acc.Len() < FrameHeaderLen {
		return 0, nil, ErrNeedMore
	}
	hdr := acc.Bytes()[:FrameHeaderLen]
	n := binary.LittleEndian.Uint32(hdr[0:4]) // body 字节数(不含 type)
	typ := hdr[4]
	total := FrameHeaderLen + int(n)
	if uint64(n) > uint64(len(buf)) {
		return 0, nil, ErrFrameTooLarge
	}
	if acc.Len() < total {
		return 0, nil, ErrNeedMore
	}
	acc.Next(FrameHeaderLen) // 消费头(不消费的语义:只有整帧可解时才消费)
	body := buf[:n]
	_, _ = acc.Read(body)
	return typ, body, nil
}

// ReadFrame 从流上读取一帧,返回帧类型与 body。
//
// 通过 io.ReadFull 处理半包(不足一帧时阻塞等待);
// 每次只读一帧,粘包由调用方循环 ReadFrame 消化。
// body 是 buf 的子切片(零拷贝),调用方负责在不再使用后归还/复用 buf。
func ReadFrame(r io.Reader, buf []byte) (typ byte, body []byte, err error) {
	var hdr [FrameHeaderLen]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.LittleEndian.Uint32(hdr[0:4])
	typ = hdr[4]
	if uint64(n) > uint64(len(buf)) {
		return 0, nil, ErrFrameTooLarge
	}
	body = buf[:n]
	if _, err = io.ReadFull(r, body); err != nil {
		return 0, nil, err
	}
	return typ, body, nil
}

// WriteFrame 把一帧写入流(5 字节头 + body)。
func WriteFrame(w io.Writer, typ byte, body []byte) error {
	var hdr [FrameHeaderLen]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(body)))
	hdr[4] = typ
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(body)
	return err
}

// 以下为各帧 body 的解析函数(所有多字节字段均为小端)。

// parseEngineHello 解析 0x30 的 body:version u16 · caps u32。
func parseEngineHello(body []byte) (version uint16, caps uint32, err error) {
	if len(body) < 6 {
		return 0, 0, errShortFrame("EngineHello")
	}
	return binary.LittleEndian.Uint16(body[0:2]),
		binary.LittleEndian.Uint32(body[2:6]), nil
}

// parseSessionOpen 解析 0x01 的 body。
// 返回 (sess_id, addr 字符串, token)。addr 按框架约定解码:
// 前 16 字节为 IP(前 12 字节全零表示 IPv4,取前 4 字节),后 2 字节为端口(小端)。
func parseSessionOpen(body []byte) (sess uint64, addr string, token []byte, err error) {
	const addrLen = 18
	if len(body) < 8+addrLen+2 {
		return 0, "", nil, errShortFrame("SessionOpen")
	}
	sess = binary.LittleEndian.Uint64(body[0:8])
	addr = decodeAddr(body[8 : 8+addrLen])
	tl := int(binary.LittleEndian.Uint16(body[8+addrLen : 10+addrLen]))
	end := 10 + addrLen + tl
	if end > len(body) {
		return 0, "", nil, errShortFrame("SessionOpen token")
	}
	return sess, addr, body[10+addrLen : end], nil
}

// parseSessionClose 解析 0x02 的 body:sess_id u64 · reason u8。
func parseSessionClose(body []byte) (sess uint64, reason uint8, err error) {
	if len(body) < 9 {
		return 0, 0, errShortFrame("SessionClose")
	}
	return binary.LittleEndian.Uint64(body[0:8]), body[8], nil
}

// parseSessionResume 解析 0x03 的 body:sess_id u64 · gap_ms u32。
func parseSessionResume(body []byte) (sess uint64, gapMS uint32, err error) {
	if len(body) < 12 {
		return 0, 0, errShortFrame("SessionResume")
	}
	return binary.LittleEndian.Uint64(body[0:8]),
		binary.LittleEndian.Uint32(body[8:12]), nil
}

// parseData 解析 0x10 的 body,返回 (sess_id, ch, msg_id, payload)。
// 0x81 Send 的 body 布局与之相同,可直接复用。
func parseData(body []byte) (sess uint64, ch uint8, msgID uint16, payload []byte, err error) {
	if len(body) < 11 {
		return 0, 0, 0, nil, errShortFrame("Data")
	}
	return binary.LittleEndian.Uint64(body[0:8]), body[8],
		binary.LittleEndian.Uint16(body[9:11]), body[11:], nil
}

// parseSessionStats 解析 0x20 的 body。
func parseSessionStats(body []byte) (sess uint64, rttMS, lossPermille, outKbps uint16, err error) {
	if len(body) < 14 {
		return 0, 0, 0, 0, errShortFrame("SessionStats")
	}
	return binary.LittleEndian.Uint64(body[0:8]),
		binary.LittleEndian.Uint16(body[8:10]),
		binary.LittleEndian.Uint16(body[10:12]),
		binary.LittleEndian.Uint16(body[12:14]), nil
}

// parseOverload 解析 0x2F 的 body。
func parseOverload(body []byte) (droppedUp, droppedDown uint32, err error) {
	if len(body) < 8 {
		return 0, 0, errShortFrame("Overload")
	}
	return binary.LittleEndian.Uint32(body[0:4]),
		binary.LittleEndian.Uint32(body[4:8]), nil
}

// decodeAddr 把框架的 18 字节地址解码成 "ip:port" 字符串(尽力而为)。
func decodeAddr(b []byte) string {
	port := binary.LittleEndian.Uint16(b[16:18])
	v4 := true
	for _, x := range b[4:16] {
		if x != 0 {
			v4 = false
			break
		}
	}
	if v4 {
		ip := net.IPv4(b[0], b[1], b[2], b[3])
		return net.JoinHostPort(ip.String(), strconv.Itoa(int(port)))
	}
	ip := net.IP(b[:16])
	return net.JoinHostPort(ip.String(), strconv.Itoa(int(port)))
}

// errShortFrame 构造"帧体过短"错误。
func errShortFrame(what string) error {
	return errors.New("soup: " + what + " 帧体过短")
}
