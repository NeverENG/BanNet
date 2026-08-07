package soup

import "sync/atomic"

// Metrics 是 SDK 的原子计数器集合,可被监控系统并发读取。
// 计数器只有 Add/读取,不提供 Reset(避免与房间运行期竞态)。
type Metrics struct {
	RoomsActive   atomic.Int64 // 当前活跃房间数
	PlayersOnline atomic.Int64 // 当前在线玩家数(已加入房间)
	TickSkipped   atomic.Int64 // 因落后超过 3 tick 被丢弃的 tick 数
	InboxDrops    atomic.Int64 // 房间入站队列溢出丢弃的事件数
	OutDrops      atomic.Int64 // 出站帧丢弃数(UDS 掉线或出站队列满)
	InputStarved  atomic.Int64 // 输入饥饿计数(S3 抖动缓冲启用后生效)
}

// MetricsSnapshot 是 Metrics 的一致性快照(纯值,无原子操作)。
type MetricsSnapshot struct {
	RoomsActive   int64
	PlayersOnline int64
	TickSkipped   int64
	InboxDrops    int64
	OutDrops      int64
	InputStarved  int64
}

// Snapshot 返回当前计数的一致性快照。
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		RoomsActive:   m.RoomsActive.Load(),
		PlayersOnline: m.PlayersOnline.Load(),
		TickSkipped:   m.TickSkipped.Load(),
		InboxDrops:    m.InboxDrops.Load(),
		OutDrops:      m.OutDrops.Load(),
		InputStarved:  m.InputStarved.Load(),
	}
}
