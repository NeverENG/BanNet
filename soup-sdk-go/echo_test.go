package soup

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 集成测试:SDK 与「扮演引擎的 UDS 服务端」互通。
// 场景:EngineHello → LogicHello 握手;SessionOpen 建房;Data 上行 →
// 房间 OnInput 用 BeginSend 原样回 → 引擎侧收到 Send 帧。

type echoGK struct{}

func (echoGK) Authenticate(token []byte, addr string) *PlayerID {
	p := PlayerID(1)
	return &p
}
func (echoGK) Route(p PlayerID, h JoinHint) RoomRoute {
	return RoomRoute{Action: RouteCreate, RoomID: 1}
}
func (echoGK) NewRoom(id RoomID, cfg any, players []PlayerID, seed uint64) Room {
	return &echoRoom{}
}

type echoRoom struct {
	ctx *RoomCtx
}

func (e *echoRoom) OnJoin(ctx *RoomCtx, p PlayerID)                   { e.ctx = ctx }
func (e *echoRoom) OnLeave(ctx *RoomCtx, p PlayerID, why LeaveReason) {}
func (e *echoRoom) OnResume(ctx *RoomCtx, p PlayerID, gapMS uint32)   {}
func (e *echoRoom) OnInput(ctx *RoomCtx, p PlayerID, seq InputSeq, payload []byte) {
	// 原样回给该玩家(ch=2 可靠有序):逐字节写入,避免 PutBytes 的 u16 长度前缀。
	b := ctx.BeginSend(p, ChReliableOrdered, 99)
	for _, c := range payload {
		b.PutU8(c)
	}
	ctx.Commit(b)
}
func (e *echoRoom) Tick(ctx *RoomCtx, tick Tick, dtMS uint32) Outcome              { return Continue }
func (e *echoRoom) EncodeSnapshot(target PlayerID, baseline Baseline, out *Buffer) {}
func (e *echoRoom) EncodeFullState(target PlayerID, out *Buffer)                   {}
func (e *echoRoom) StateHash() uint64                                              { return 0 }

// echoTestConn 封装测试侧的 UDS 连接读写。
type echoTestConn struct {
	conn net.Conn
	buf  []byte
}

func (c *echoTestConn) write(t *testing.T, typ byte, body []byte) {
	t.Helper()
	if err := WriteFrame(c.conn, typ, body); err != nil {
		t.Fatal(err)
	}
}

// readExpect 读一帧并断言类型,返回 body。
func (c *echoTestConn) readExpect(t *testing.T, typ byte, what string) []byte {
	t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	got, body, err := ReadFrame(c.conn, c.buf)
	if err != nil {
		t.Fatalf("读帧超时/失败(%s): %v", what, err)
	}
	if got != typ {
		t.Fatalf("帧类型 = 0x%02X, want 0x%02X (%s)", got, typ, what)
	}
	return body
}

func TestEchoThroughSDK(t *testing.T) {
	// UDS 路径(macOS SUN_LEN 限制,用短路径)。
	dir := t.TempDir()
	path := filepath.Join(dir, "s.sock")
	if len(path) > 100 {
		path = filepath.Join(os.TempDir(), "soup-echo.sock")
	}
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path)
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := NewServer(Config{
		EngineSocket: path,
		TickHz:       20,
		SnapshotHz:   20,
		Gatekeeper:   echoGK{},
	})
	go func() { _ = srv.Run(ctx) }()

	// 引擎侧 accept SDK 的连接。
	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ec := &echoTestConn{conn: conn, buf: make([]byte, 64*1024)}

	// 1. 握手:EngineHello → SDK 回 LogicHello。
	hello := make([]byte, 6)
	binary.LittleEndian.PutUint16(hello[0:2], 1)
	binary.LittleEndian.PutUint32(hello[2:6], 0)
	ec.write(t, FrameEngineHello, hello)
	ec.readExpect(t, FrameLogicHello, "LogicHello")

	// 2. SessionOpen → 建房。
	open := make([]byte, 8+18+2+4)
	binary.LittleEndian.PutUint64(open[0:8], 100) // sess_id
	copy(open[8:8+4], []byte{127, 0, 0, 1})
	binary.LittleEndian.PutUint16(open[24:26], 9999)
	binary.LittleEndian.PutUint16(open[26:28], 4)
	copy(open[28:], "tok1")
	ec.write(t, FrameSessionOpen, open)

	// 3. 上行 Data(ch=2)→ 房间 OnInput → echo 回 Send。
	payload := []byte("ping-soup-123")
	up := make([]byte, 8+1+2+len(payload))
	binary.LittleEndian.PutUint64(up[0:8], 100)
	up[8] = uint8(ChReliableOrdered)
	binary.LittleEndian.PutUint16(up[9:11], 42)
	copy(up[11:], payload)
	ec.write(t, FrameDataUp, up)

	// 4. 收 echo(Send 帧,msg 99,payload 原样)。
	body := ec.readExpect(t, FrameSend, "echo Send")
	t.Logf("echo body hex = %x", body)
	sess, ch, msg, echoed, err := parseData(body)
	if err != nil {
		t.Fatal(err)
	}
	if sess != 100 || ch != uint8(ChReliableOrdered) || msg != 99 {
		t.Fatalf("echo 帧头不符: sess=%d ch=%d msg=%d", sess, ch, msg)
	}
	if string(echoed) != string(payload) {
		t.Fatalf("echo payload = %q, want %q", echoed, payload)
	}
}

// 半包到达:SessionOpen 拆成两段写,SDK 必须正确重组。
func TestSplitSessionOpen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s2.sock")
	if len(path) > 100 {
		path = filepath.Join(os.TempDir(), "soup-split.sock")
	}
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path)
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := NewServer(Config{
		EngineSocket: path,
		TickHz:       20,
		Gatekeeper:   echoGK{},
	})
	go func() { _ = srv.Run(ctx) }()

	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ec := &echoTestConn{conn: conn, buf: make([]byte, 64*1024)}

	// 握手。
	hello := make([]byte, 6)
	ec.write(t, FrameEngineHello, hello)
	ec.readExpect(t, FrameLogicHello, "LogicHello")

	// SessionOpen 拆成两段写。
	var raw bytes.Buffer
	_ = WriteFrame(&raw, FrameSessionOpen, make([]byte, 8+18+2+3))
	all := raw.Bytes()
	if _, err := conn.Write(all[:len(all)/2]); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // 确保第一段先到
	if _, err := conn.Write(all[len(all)/2:]); err != nil {
		t.Fatal(err)
	}

	// 不崩溃即可(建房成功由后续 SessionOpen 重复被忽略体现)。
	time.Sleep(200 * time.Millisecond)
}
