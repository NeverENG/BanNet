package soup

// Option 是 Server 装配的可选参数(函数式选项模式)。
// 用法:NewServer(WithEngineSocket("/tmp/s.sock"), WithTickHz(20), ...)。
// 所有未显式设置的项都有安全默认值;字段级配置仍可用 WithConfig 整体覆盖。
type Option func(*Config)

// WithEngineSocket 设置框架监听的 UDS 路径。
func WithEngineSocket(path string) Option {
	return func(c *Config) { c.EngineSocket = path }
}

// WithTickHz 设置房间 tick 频率(Hz,默认 20)。
func WithTickHz(hz int) Option {
	return func(c *Config) { c.TickHz = hz }
}

// WithSnapshotHz 设置快照下发频率(Hz,默认 10;0 表示禁用快照)。
func WithSnapshotHz(hz int) Option {
	return func(c *Config) { c.SnapshotHz = hz }
}

// WithJitterBufferTicks 设置输入抖动缓冲深度(tick 数,默认 2)。
func WithJitterBufferTicks(n int) Option {
	return func(c *Config) { c.JitterBufferTicks = n }
}

// WithKeyframeIntervalTicks 设置强制全量关键帧的间隔(tick,默认 100)。
func WithKeyframeIntervalTicks(n int) Option {
	return func(c *Config) { c.KeyframeIntervalTicks = n }
}

// WithBaselineRingSize 设置每个客户端的基线环形缓冲大小(默认 32)。
func WithBaselineRingSize(n int) Option {
	return func(c *Config) { c.BaselineRingSize = n }
}

// WithMaxRooms 设置单进程最大房间数(默认 1024)。
func WithMaxRooms(n int) Option {
	return func(c *Config) { c.MaxRooms = n }
}

// WithBudgetKbpsPerClient 设置单客户端带宽预算 kbps(默认 24)。
func WithBudgetKbpsPerClient(n int) Option {
	return func(c *Config) { c.BudgetKbpsPerClient = n }
}

// WithGatekeeper 设置鉴权/路由/建房工厂(必填,否则 Run 报错)。
func WithGatekeeper(gk Gatekeeper) Option {
	return func(c *Config) { c.Gatekeeper = gk }
}

// WithReplayOut 设置回放录制文件路径;非空时把交付的输入录制到该文件,
// 配合 Replay() 离线重放(S4)。
func WithReplayOut(path string) Option {
	return func(c *Config) { c.ReplayOut = path }
}

// WithConfig 用一份完整 Config 整体覆盖配置(与其它 Option 混用时,
// 按调用顺序,后者覆盖前者)。
func WithConfig(cfg Config) Option {
	return func(c *Config) { *c = cfg }
}

// defaultConfig 返回全部安全默认值。
func defaultConfig() Config {
	return Config{
		TickHz:                20,
		SnapshotHz:            10,
		JitterBufferTicks:     2,
		KeyframeIntervalTicks: 100,
		BaselineRingSize:      32,
		MaxRooms:              1024,
		BudgetKbpsPerClient:   24,
	}
}
