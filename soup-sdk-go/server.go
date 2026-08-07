package soup

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"log"
	"sync"
	"time"
)

// 内部调优常量。
const (
	maxFrameLen          = 64 * 1024              // 单帧上限(超出丢弃)
	defaultInboxCap      = 256                    // 房间入站队列容量
	defaultOutboxCap     = 4096                   // 出站队列容量(掉线期间积压上限)
	defaultReconnectBase = 500 * time.Millisecond // 重连退避起点
)

// Config 是 Server 的装配参数,全部字段均有安全默认值(见 NewServer)。
type Config struct {
	// EngineSocket 是框架监听的 UDS 路径。
	EngineSocket string
	// TickHz 是房间 tick 频率(Hz)。
	TickHz int
	// SnapshotHz 是快照下发频率(Hz),可低于 TickHz。
	SnapshotHz int
	// JitterBufferTicks 是输入抖动缓冲深度(S3 生效,默认 2)。
	JitterBufferTicks int
	// KeyframeIntervalTicks 是强制全量关键帧的间隔(S3 生效,默认 100)。
	KeyframeIntervalTicks int
	// BaselineRingSize 是每个客户端的基线环形缓冲大小(S3 生效,默认 32)。
	BaselineRingSize int
	// MaxRooms 是单进程最大房间数。
	MaxRooms int
	// BudgetKbpsPerClient 是单客户端带宽预算 kbps(S3 降级生效,默认 24)。
	BudgetKbpsPerClient int
	// Gatekeeper 是鉴权/路由/建房工厂,必填。
	Gatekeeper Gatekeeper
	// ReplayOut 是回放录制文件路径;非空时把交付的输入(含 seed/tickHz)
	// 录制到该文件,配合 Replay() 离线重放(S4)。
	ReplayOut string
}

// Server 是逻辑服 SDK 的入口。创建一个 Server 后调用 Run 进入服务循环。
type Server struct {
	cfg  Config
	ctx  context.Context
	conn *engineConn

	roomsMu  sync.Mutex
	rooms    map[RoomID]*room
	sessions map[uint64]*sessionRef

	metrics Metrics

	readPool sync.Pool // 读缓冲池([]byte,容量 maxFrameLen)
	bufPool  sync.Pool // 出站 Buffer 池
	outbox   chan outFrame

	maxFrameLen   int
	inboxCap      int
	reconnectBase time.Duration
}

// outFrame 是出站队列元素。buf 非 nil 时 data 是其内部切片,
// 写完(或丢弃)后必须归还 bufPool。
type outFrame struct {
	data []byte
	buf  *Buffer
}

// NewServer 创建 Server 并填充默认配置。
func NewServer(cfg Config) *Server {
	if cfg.TickHz <= 0 {
		cfg.TickHz = 20
	}
	if cfg.SnapshotHz <= 0 {
		cfg.SnapshotHz = 20
	}
	if cfg.JitterBufferTicks <= 0 {
		cfg.JitterBufferTicks = 2
	}
	if cfg.KeyframeIntervalTicks <= 0 {
		cfg.KeyframeIntervalTicks = 100
	}
	if cfg.BaselineRingSize <= 0 {
		cfg.BaselineRingSize = 32
	}
	if cfg.MaxRooms <= 0 {
		cfg.MaxRooms = 1024
	}
	if cfg.BudgetKbpsPerClient <= 0 {
		cfg.BudgetKbpsPerClient = 24
	}
	s := &Server{
		cfg:           cfg,
		conn:          newEngineConn(cfg.EngineSocket, defaultReconnectBase),
		rooms:         make(map[RoomID]*room),
		sessions:      make(map[uint64]*sessionRef),
		outbox:        make(chan outFrame, defaultOutboxCap),
		maxFrameLen:   maxFrameLen,
		inboxCap:      defaultInboxCap,
		reconnectBase: defaultReconnectBase,
	}
	s.readPool.New = func() any { return make([]byte, s.maxFrameLen) }
	s.bufPool.New = func() any { return &Buffer{data: make([]byte, poolBufferCap)} }
	return s
}

// Run 启动服务并阻塞,直到 ctx 被取消(正常退出,返回 nil)。
//
// 启动后立即进入重连循环:框架未就绪时按指数退避持续拨号。
func (s *Server) Run(ctx context.Context) error {
	if s.cfg.Gatekeeper == nil {
		return errors.New("soup: Config.Gatekeeper 不能为 nil")
	}
	s.ctx = ctx
	go s.readLoop(ctx)
	go s.writeLoop(ctx)
	<-ctx.Done()
	s.closeAll()
	return nil
}

// Metrics 返回运行时计数器(可安全并发读取)。
func (s *Server) Metrics() *Metrics { return &s.metrics }

// readLoop 是帧读取 goroutine:拨号 → 累积缓冲分帧 → 分发;断线后自动重连。
// 半包/粘包由 TryDecodeFrame 的累积缓冲天然处理(与框架 Rust 侧同款语义)。
func (s *Server) readLoop(ctx context.Context) {
	tmp := make([]byte, 64*1024)
	for {
		conn, err := s.conn.ensure(ctx)
		if err != nil {
			return // ctx 取消
		}
		acc := &bytes.Buffer{}
		for {
			n, rerr := conn.Read(tmp)
			if rerr != nil {
				s.conn.markDead(conn)
				break // 断线,回到外层重连
			}
			acc.Write(tmp[:n])
			for {
				raw := s.readPool.Get().([]byte)
				typ, body, derr := TryDecodeFrame(acc, raw)
				if derr == ErrNeedMore {
					s.readPool.Put(raw)
					break // 等更多数据
				}
				if derr != nil {
					s.readPool.Put(raw)
					acc.Reset() // 畸形流,丢弃累积
					break
				}
				s.dispatch(typ, body, raw)
			}
		}
	}
}

// writeLoop 是帧写入 goroutine:从出站队列取帧写入 UDS。
// 连接失效时丢弃当帧(计数 out_drops)并重连,重连成功后继续。
func (s *Server) writeLoop(ctx context.Context) {
	for {
		conn, err := s.conn.ensure(ctx)
		if err != nil {
			return // ctx 取消
		}
		select {
		case <-ctx.Done():
			return
		case f := <-s.outbox:
			if _, err := conn.Write(f.data); err != nil {
				s.metrics.OutDrops.Add(1)
				s.conn.markDead(conn)
			}
			if f.buf != nil {
				s.bufPool.Put(f.buf)
			}
		}
	}
}

// dispatch 按帧类型分发。raw 是读缓冲,除投递给房间的 Data 帧外,
// 其余帧在本函数内归还读池。
func (s *Server) dispatch(typ byte, body, raw []byte) {
	switch typ {
	case FrameEngineHello:
		// 连接建立首帧:回复 LogicHello(重连后框架会重发,SDK 随之重新握手)
		if _, _, err := parseEngineHello(body); err != nil {
			break
		}
		s.sendLogicHello()
		s.readPool.Put(raw)

	case FrameSessionOpen:
		s.handleSessionOpen(body)
		s.readPool.Put(raw)

	case FrameSessionClose:
		sess, reason, err := parseSessionClose(body)
		s.readPool.Put(raw)
		if err != nil {
			return
		}
		s.routeToRoom(inEvent{kind: inClose, sess: sess, reason: reason})

	case FrameSessionResume:
		sess, gap, err := parseSessionResume(body)
		s.readPool.Put(raw)
		if err != nil {
			return
		}
		s.routeToRoom(inEvent{kind: inResume, sess: sess, gapMS: gap})

	case FrameDataUp:
		sess, ch, msg, payload, err := parseData(body)
		if err != nil {
			s.readPool.Put(raw)
			return
		}
		s.routeToRoom(inEvent{
			kind: inData, sess: sess, ch: Channel(ch), msg: MsgID(msg),
			payload: payload, raw: raw,
		})

	case FrameSessionStats:
		sess, rtt, loss, _, err := parseSessionStats(body)
		s.readPool.Put(raw)
		if err != nil {
			return
		}
		s.routeToRoom(inEvent{kind: inStats, sess: sess, rtt: rtt, loss: loss})

	case FrameOverload:
		// 全局降频:通知所有房间进入降级(S3),打日志并计数。
		if up, down, err := parseOverload(body); err == nil {
			log.Printf("soup: 框架 Overload dropped_up=%d dropped_down=%d,快照全局降频", up, down)
			s.roomsMu.Lock()
			for _, r := range s.rooms {
				r.setOverload(true)
			}
			s.roomsMu.Unlock()
		}
		s.readPool.Put(raw)

	default:
		log.Printf("soup: 未知帧类型 0x%02X,丢弃", typ)
		s.readPool.Put(raw)
	}
}

// handleSessionOpen 处理 0x01:Authenticate → Route → Join/Create → 投 open 事件。
func (s *Server) handleSessionOpen(body []byte) {
	sess, addr, token, err := parseSessionOpen(body)
	if err != nil {
		return
	}
	p := s.cfg.Gatekeeper.Authenticate(token, addr)
	if p == nil {
		s.sendKick(sess, 0)
		return
	}
	route := s.cfg.Gatekeeper.Route(*p, JoinHint{SessionID: sess, Addr: addr, Token: token})
	if route.Action == RouteReject {
		s.sendKick(sess, 0)
		return
	}

	s.roomsMu.Lock()
	defer s.roomsMu.Unlock()
	if _, dup := s.sessions[sess]; dup {
		return // 重复 SessionOpen,忽略
	}
	r := s.rooms[route.RoomID]
	if r == nil {
		if route.Action != RouteCreate {
			s.sendKick(sess, 0)
			return
		}
		if len(s.rooms) >= s.cfg.MaxRooms {
			s.sendKick(sess, 0)
			return
		}
		nr, err := s.newRoom(route, *p)
		if err != nil {
			s.sendKick(sess, 0)
			return
		}
		s.rooms[route.RoomID] = nr
		s.metrics.RoomsActive.Add(1)
		go nr.run(s.ctx)
		r = nr
	}
	ref := sessionRef{roomID: r.id, player: *p}
	s.sessions[sess] = &ref
	select {
	case r.inbox <- inEvent{kind: inOpen, sess: sess, player: *p}:
	default:
		delete(s.sessions, sess)
		s.metrics.InboxDrops.Add(1)
		s.sendKick(sess, 0)
	}
}

// routeToRoom 按 sess_id 路由事件到对应房间;会话不存在或队列满时丢弃。
func (s *Server) routeToRoom(ev inEvent) {
	s.roomsMu.Lock()
	ref, ok := s.sessions[ev.sess]
	var r *room
	if ok {
		r = s.rooms[ref.roomID]
	}
	s.roomsMu.Unlock()
	if !ok || r == nil {
		if ev.raw != nil {
			s.readPool.Put(ev.raw)
		}
		return
	}
	ev.player = ref.player
	select {
	case r.inbox <- ev:
	default:
		s.metrics.InboxDrops.Add(1)
		if ev.raw != nil {
			s.readPool.Put(ev.raw)
		}
	}
}

// newRoom 通过 Gatekeeper 工厂建房。seed 为 0 时用 crypto/rand 生成
// (仅房间元数据,不影响游戏确定性)。
func (s *Server) newRoom(route RoomRoute, first PlayerID) (*room, error) {
	seed := route.Seed
	if seed == 0 {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return nil, err
		}
		seed = binary.LittleEndian.Uint64(b[:])
	}
	impl := s.cfg.Gatekeeper.NewRoom(route.RoomID, route.Config, []PlayerID{first}, seed)
	r := &room{
		srv:        s,
		id:         route.RoomID,
		impl:       impl,
		rand:       NewDetRand(seed),
		inbox:      make(chan inEvent, s.inboxCap),
		players:    make(map[PlayerID]*pstate, 8),
		sessOf:     make(map[uint64]PlayerID, 8),
		dtMS:       uint32(1000 / s.cfg.TickHz),
		outFrames:  make([]*Buffer, 0, 16),
		pendingBuf: make([]pendingDeliver, 0, 8),
	}
	if s.cfg.ReplayOut != "" {
		path := s.cfg.ReplayOut
		if w, err := newReplayWriter(path, seed, s.cfg.TickHz); err == nil {
			r.replay = w
		} else {
			log.Printf("soup: 回放录制打开失败 %s: %v", path, err)
		}
	}
	r.ctx = &RoomCtx{r: r}
	return r, nil
}

// removeRoom 在房间 goroutine 退出时清理 rooms/sessions 映射。
func (s *Server) removeRoom(r *room) {
	s.roomsMu.Lock()
	if s.rooms[r.id] == r {
		delete(s.rooms, r.id)
	}
	for sess, ref := range s.sessions {
		if ref.roomID == r.id {
			delete(s.sessions, sess)
		}
	}
	s.roomsMu.Unlock()
	s.metrics.RoomsActive.Add(-1)
}

// removeSession 移除单个会话映射(玩家离开但房间仍在时调用)。
func (s *Server) removeSession(sess uint64) {
	s.roomsMu.Lock()
	delete(s.sessions, sess)
	s.roomsMu.Unlock()
}

// closeAll 在 Run 退出时清空房间与会话映射;
// 房间 goroutine 通过 ctx.Done 分支退出,其 defer removeRoom 幂等无害。
func (s *Server) closeAll() {
	s.roomsMu.Lock()
	s.rooms = make(map[RoomID]*room)
	s.sessions = make(map[uint64]*sessionRef)
	s.roomsMu.Unlock()
}

// sendLogicHello 发送握手帧 0x90(version u16 · caps u32)。
func (s *Server) sendLogicHello() {
	var body [6]byte
	binary.LittleEndian.PutUint16(body[0:2], protocolVersion)
	binary.LittleEndian.PutUint32(body[2:6], protocolCaps)
	s.trySend(FrameLogicHello, body[:])
}

// sendKick 发送踢出帧 0x83。
func (s *Server) sendKick(sess uint64, reason uint8) {
	var body [9]byte
	binary.LittleEndian.PutUint64(body[0:8], sess)
	body[8] = reason
	s.trySend(FrameKick, body[:])
}

// trySend 把一帧投入出站队列(非阻塞);队列满或掉线时丢弃并计数 out_drops。
func (s *Server) trySend(typ byte, body []byte) {
	frame := make([]byte, FrameHeaderLen+len(body))
	binary.LittleEndian.PutUint32(frame[0:4], uint32(len(body)))
	frame[4] = typ
	copy(frame[5:], body)
	select {
	case s.outbox <- outFrame{data: frame}:
	default:
		s.metrics.OutDrops.Add(1)
	}
}
