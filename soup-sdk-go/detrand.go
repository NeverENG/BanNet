package soup

// DetRand 是基于 splitmix64 的确定性伪随机数生成器。
//
// 它只依赖 seed 与调用次数,与平台、时间、并发无关,因此可以保证
// 同 seed 的两次对局产生完全一致的随机序列——这是确定性回放的基础。
// 房间内请只通过 ctx.Rand() 获取随机数,禁止使用 math/rand。
type DetRand struct {
	state uint64
}

// NewDetRand 以指定 seed 创建确定性随机源。
// seed 为 0 同样合法(splitmix64 对任意 seed 都有效)。
func NewDetRand(seed uint64) *DetRand {
	return &DetRand{state: seed}
}

// Uint64 返回下一个 64 位随机数(splitmix64)。
func (r *DetRand) Uint64() uint64 {
	r.state += 0x9E3779B97F4A7C15
	z := r.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// Uint32 返回下一个 32 位随机数(取 Uint64 的高 32 位)。
func (r *DetRand) Uint32() uint32 {
	return uint32(r.Uint64() >> 32)
}

// Intn 返回 [0, n) 内的随机整数。n <= 1 时恒返回 0。
// 使用取模而非拒绝采样,保证各平台结果完全一致。
func (r *DetRand) Intn(n int) int {
	if n <= 1 {
		return 0
	}
	return int(r.Uint64() % uint64(n))
}
