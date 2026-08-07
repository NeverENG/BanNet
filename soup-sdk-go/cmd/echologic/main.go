// echologic —— 真实 Go 逻辑服示例:收到 ch=2 输入原样 echo 回该玩家。
// 用于跨语言联调:引擎(Rust)只做网络,这里是"游戏规则"。
//
// 用法:go run ./cmd/echologic --socket /tmp/soup-interop.sock
package main

import (
	"context"
	"flag"
	"log"

	soup "soup-sdk-go"
)

// echoRoom 实现 Room 接口:回显输入。
type echoRoom struct{}

func (r *echoRoom) OnJoin(ctx *soup.RoomCtx, p soup.PlayerID) {
	log.Printf("player %d joined", p)
}
func (r *echoRoom) OnResume(ctx *soup.RoomCtx, p soup.PlayerID, gap uint32) {
	log.Printf("player %d resumed (gap %dms)", p, gap)
}
func (r *echoRoom) OnLeave(ctx *soup.RoomCtx, p soup.PlayerID, why soup.LeaveReason) {
	log.Printf("player %d left (%v)", p, why)
}
func (r *echoRoom) OnInput(ctx *soup.RoomCtx, p soup.PlayerID, _ soup.InputSeq, payload []byte) {
	b := ctx.BeginSend(p, soup.ChReliableOrdered, 1)
	for _, c := range payload {
		b.PutU8(c)
	}
	ctx.Commit(b)
}
func (r *echoRoom) Tick(ctx *soup.RoomCtx, t soup.Tick, dtMS uint32) soup.Outcome {
	return soup.Continue
}
func (r *echoRoom) EncodeSnapshot(target soup.PlayerID, b soup.Baseline, out *soup.Buffer) {}
func (r *echoRoom) EncodeFullState(target soup.PlayerID, out *soup.Buffer)                 {}
func (r *echoRoom) StateHash() uint64                                                      { return 0 }

func main() {
	socket := flag.String("socket", "/tmp/soup-interop.sock", "引擎监听的 UDS 路径")
	flag.Parse()

	srv := soup.NewServer(soup.Config{
		EngineSocket: *socket,
		TickHz:       20,
		SnapshotHz:   10,
		Gatekeeper: soup.GatekeeperFuncs{
			AuthenticateFn: func(token []byte, addr string) *soup.PlayerID {
				p := soup.PlayerID(1)
				return &p
			},
			RouteFn: func(p soup.PlayerID, h soup.JoinHint) soup.RoomRoute {
				return soup.RoomRoute{Action: soup.RouteCreate, RoomID: 1}
			},
			NewRoomFn: func(id soup.RoomID, cfg any, players []soup.PlayerID, seed uint64) soup.Room {
				return &echoRoom{}
			},
		},
	})
	log.Printf("soup-sdk-go 逻辑服启动,监听 %s", *socket)
	srv.Run(context.Background())
}
