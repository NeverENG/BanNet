package soup

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

// 帧编解码单测:长度前缀分帧,覆盖半包/粘包与各 parse 函数。

func TestWriteReadFrameRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	body := []byte{1, 2, 3, 4, 5}
	if err := WriteFrame(&buf, FrameSend, body); err != nil {
		t.Fatal(err)
	}
	typ, got, err := ReadFrame(&buf, make([]byte, 1024))
	if err != nil {
		t.Fatal(err)
	}
	if typ != FrameSend {
		t.Fatalf("type = 0x%02X, want 0x%02X", typ, FrameSend)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body = %v, want %v", got, body)
	}
}

// 粘包:多帧连成一段流,循环 ReadFrame 必须全部消化。
func TestReadFrameStickyAndSplit(t *testing.T) {
	var raw bytes.Buffer
	_ = WriteFrame(&raw, FrameEngineHello, []byte{0x00, 0x01})
	_ = WriteFrame(&raw, FrameDataUp, []byte{0xAA, 0xBB, 0xCC})
	all := raw.Bytes()

	rd := bytes.NewReader(all)
	readBuf := make([]byte, 1024)
	var got []byte
	for {
		typ, body, err := ReadFrame(rd, readBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, typ)
		got = append(got, body...)
	}
	want := append([]byte{FrameEngineHello, 0x00, 0x01}, FrameDataUp, 0xAA, 0xBB, 0xCC)
	if !bytes.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// 半包:帧头拆成两段到达,TryDecodeFrame 先报未就绪(不消费),补全后成功。
func TestReadFrameHalfPacket(t *testing.T) {
	var raw bytes.Buffer
	_ = WriteFrame(&raw, FrameSessionStats, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14})
	all := raw.Bytes()
	half := len(all) / 2

	acc := &bytes.Buffer{}
	readBuf := make([]byte, 1024)
	acc.Write(all[:half])
	if _, _, err := TryDecodeFrame(acc, readBuf); err != ErrNeedMore {
		t.Fatalf("半包应报 ErrNeedMore,got %v", err)
	}
	if acc.Len() != half {
		t.Fatalf("ErrNeedMore 不应消费缓冲:len=%d want %d", acc.Len(), half)
	}
	acc.Write(all[half:])
	typ, body, err := TryDecodeFrame(acc, readBuf)
	if err != nil {
		t.Fatal(err)
	}
	if typ != FrameSessionStats || len(body) != 14 {
		t.Fatalf("typ=0x%02X body=%d", typ, len(body))
	}
	if acc.Len() != 0 {
		t.Fatalf("完整帧后累积缓冲应为空,got %d", acc.Len())
	}
}

func chunked(b []byte, n int) [][]byte {
	var out [][]byte
	for len(b) > 0 {
		m := n
		if len(b) < m {
			m = len(b)
		}
		out = append(out, b[:m])
		b = b[m:]
	}
	return out
}

func TestParseSessionOpen(t *testing.T) {
	body := make([]byte, 8+18+2+5)
	binary.LittleEndian.PutUint64(body[0:8], 42)
	// addr [18]byte:前 4 字节 IPv4,末尾 2 字节端口
	body[8] = 127
	body[9] = 0
	body[10] = 0
	body[11] = 1
	binary.LittleEndian.PutUint16(body[24:26], 8080)
	binary.LittleEndian.PutUint16(body[26:28], 5)
	copy(body[28:], "token")

	sess, addr, token, err := parseSessionOpen(body)
	if err != nil {
		t.Fatal(err)
	}
	if sess != 42 {
		t.Fatalf("sess = %d", sess)
	}
	if addr != "127.0.0.1:8080" {
		t.Fatalf("addr = %q", addr)
	}
	if string(token) != "token" {
		t.Fatalf("token = %q", token)
	}
}

func TestParseDataAndMulticastShape(t *testing.T) {
	body := make([]byte, 8+1+2+4)
	binary.LittleEndian.PutUint64(body[0:8], 7)
	body[8] = uint8(ChInput)
	binary.LittleEndian.PutUint16(body[9:11], 1234)
	copy(body[11:], "abcd")

	sess, ch, msg, payload, err := parseData(body)
	if err != nil {
		t.Fatal(err)
	}
	if sess != 7 || ch != uint8(ChInput) || msg != 1234 || string(payload) != "abcd" {
		t.Fatalf("sess=%d ch=%d msg=%d payload=%q", sess, ch, msg, payload)
	}
}

func TestParseRejectsShortBody(t *testing.T) {
	if _, _, _, err := parseSessionOpen(make([]byte, 5)); err == nil {
		t.Fatal("短 SessionOpen 应报错")
	}
	if _, _, _, _, err := parseData(make([]byte, 3)); err == nil {
		t.Fatal("短 Data 应报错")
	}
}

func TestBufferEncoders(t *testing.T) {
	b := &Buffer{data: make([]byte, 64)}
	b.PutU8(0xFF)
	b.PutU16(0x1234)
	b.PutU32(0xDEADBEEF)
	b.PutI16(-2)
	b.PutVarint(-300) // zigzag
	b.PutBytes([]byte{9, 8, 7})
	want := []byte{
		0xFF,
		0x34, 0x12,
		0xEF, 0xBE, 0xAD, 0xDE,
		0xFE, 0xFF,
		0xD7, 0x04, // zigzag(-300) = 599 → varint 0xD7 0x04
		3, 0, 9, 8, 7, // PutBytes:u16 长度前缀
	}
	if !bytes.Equal(b.data[:b.len], want) {
		t.Fatalf("buffer = %v, want %v", b.data[:b.len], want)
	}
}
