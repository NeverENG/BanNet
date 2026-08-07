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
