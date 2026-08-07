package soup

// 本文件定义 SDK 的基础类型与领域对象:玩家、tick、房间路由等。
// 这些类型构成 Room 实现者与 SDK 之间的契约,改动需与文档 T0003 同步。

// PlayerID 是逻辑服内部的玩家标识,由 Gatekeeper.Authenticate 分配。
// 它与框架侧的 sess_id 是两个世界:SDK 负责两者之间的映射。
type PlayerID uint32

// Tick 是房间内的权威 tick 序号,从 0 开始递增。
type Tick uint32

// InputSeq 是客户端输入包的序号,用于去重与和解(sequenced 通道)。
type InputSeq uint16

// MsgID 是业务消息标识。
// 注意:0 与 1 被 SDK 保留(快照与全量状态),房间业务消息请从 2 起。
type MsgID uint16

// Channel 是框架提供的四通道之一。
type Channel uint8

const (
	ChUnreliable        Channel = 0 // 快照下行(增量/全量)
	ChInput             Channel = 1 // 输入上行(sequenced)
	ChReliableOrdered   Channel = 2 // 房间事件(可靠有序)
	ChReliableUnordered Channel = 3 // 一次性通知(可靠无序)
)

// SDK 保留的消息号。
const (
	// MsgSnapshot 是快照通道(ChUnreliable)上的消息号,由 SDK 调度。
	MsgSnapshot MsgID = 0
	// MsgFullState 是重连/中途加入时推送全量状态用的消息号(ChReliableOrdered)。
	MsgFullState MsgID = 1
)

// Baseline 描述快照的增量基准,是值类型,传参不分配。
type Baseline struct {
	Tick  Tick
	Valid bool // false 表示必须输出可独立解码的全量
}

// Outcome 是房间 Tick 的返回结果。
type Outcome uint8

const (
	// Continue 表示房间继续运行。
	Continue Outcome = iota
	// End 表示房间结束,SDK 将停止 tick 并回收房间。
	End
)

// LeaveReason 描述玩家离开房间的原因。
type LeaveReason uint8

const (
	// LeaveTimeout 表示超时断线(框架宽限期超时)。
	LeaveTimeout LeaveReason = iota
	// LeaveKicked 表示被踢出。
	LeaveKicked
	// LeaveQuit 表示主动退出或房间结束。
	LeaveQuit
)

// RoomID 是房间的唯一标识,由 Gatekeeper 决定。
type RoomID uint64

// RouteAction 是 Gatekeeper.Route 的裁决结果。
type RouteAction uint8

const (
	// RouteJoin 表示加入已存在的房间(房间不存在则拒绝)。
	RouteJoin RouteAction = iota
	// RouteCreate 表示创建新房间(已存在则直接加入)。
	RouteCreate
	// RouteReject 表示拒绝该玩家进入。
	RouteReject
)

// RoomRoute 是 Gatekeeper.Route 的返回值。
//
// Seed 用于播种房间的确定性随机(DetRand)。为 0 时由 SDK 用 crypto/rand
// 生成——它只决定房间元数据(如出生点),不参与任何会影响确定性的逻辑;
// 需要可复现的房间时,Gatekeeper 可自行指定非零 Seed。
type RoomRoute struct {
	Action RouteAction
	RoomID RoomID
	Config any
	Seed   uint64
}

// JoinHint 携带会话上下文,供 Gatekeeper.Route 决策。
type JoinHint struct {
	// SessionID 是框架侧的会话 id。
	SessionID uint64
	// Addr 是尽力解码出的客户端地址("ip:port";解析失败时为空串)。
	Addr string
	// Token 是框架原样透传的认证令牌(与 Authenticate 收到的一致)。
	Token []byte
}

// Result 是房间结束时的结果,由 RoomCtx.End 传入。
type Result struct {
	// Aborted 表示房间异常中止(而非正常打完)。
	Aborted bool
	// Reason 是可选的结束原因说明。
	Reason string
}

// sessionRef 记录框架会话(sess_id)与房间内玩家的映射,由 Server 维护。
type sessionRef struct {
	roomID RoomID
	player PlayerID
}
