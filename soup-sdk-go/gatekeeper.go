package soup

// Gatekeeper 把匹配、鉴权、建房工厂挡在 SDK 之外。
// 匹配、房间码、组队全部由实现方决定,SDK 不关心任何业务细节。
//
// 注意:Gatekeeper 的方法在 SDK 的帧读取 goroutine 上被同步调用,
// 实现应保持轻量,不要做阻塞 IO。
type Gatekeeper interface {
	// Authenticate 校验会话令牌。
	// token 由框架原样透传,addr 是尽力解码的客户端地址("ip:port")。
	// 返回 nil 表示拒绝,框架会话将被 SDK 踢出。
	Authenticate(token []byte, addr string) *PlayerID

	// Route 决定玩家进哪个房间。hint 携带会话上下文(见 JoinHint)。
	Route(p PlayerID, hint JoinHint) RoomRoute

	// NewRoom 是建房工厂。players 是创建时已在房间内的玩家(至少一人)。
	// seed 用于确定性随机(见 RoomRoute.Seed),房间实现应把它播种进
	// 自身的确定性逻辑(可通过 ctx.Rand() 使用)。
	NewRoom(roomID RoomID, cfg any, players []PlayerID, seed uint64) Room
}

// GatekeeperFuncs 是用函数字段实现 Gatekeeper 的便捷类型:
// 免去为每个方法写空实现,常用于快速原型与示例。
// 字段为 nil 时对应方法返回安全默认值(拒绝 / 默认加入 / 空房间)。
type GatekeeperFuncs struct {
	// AuthenticateFn 对应 Gatekeeper.Authenticate;nil 表示拒绝所有连接。
	AuthenticateFn func(token []byte, addr string) *PlayerID
	// RouteFn 对应 Gatekeeper.Route;nil 表示默认加入 room 0。
	RouteFn func(p PlayerID, hint JoinHint) RoomRoute
	// NewRoomFn 对应 Gatekeeper.NewRoom;nil 表示建房失败。
	NewRoomFn func(roomID RoomID, cfg any, players []PlayerID, seed uint64) Room
}

func (f GatekeeperFuncs) Authenticate(token []byte, addr string) *PlayerID {
	if f.AuthenticateFn == nil {
		return nil
	}
	return f.AuthenticateFn(token, addr)
}

func (f GatekeeperFuncs) Route(p PlayerID, hint JoinHint) RoomRoute {
	if f.RouteFn == nil {
		return RoomRoute{Action: RouteJoin, RoomID: 0}
	}
	return f.RouteFn(p, hint)
}

func (f GatekeeperFuncs) NewRoom(roomID RoomID, cfg any, players []PlayerID, seed uint64) Room {
	if f.NewRoomFn == nil {
		return nil
	}
	return f.NewRoomFn(roomID, cfg, players, seed)
}
