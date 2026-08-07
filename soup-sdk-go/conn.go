package soup

import (
	"context"
	"net"
	"sync"
	"time"
)

// engineConn 管理与框架(UDS)的连接。
//
// 设计要点:
//   - 读/写两个 goroutine 各自在需要时调用 ensure 获取当前连接;
//   - 检测到断线(读 EOF 或写失败)的一方调用 markDead 使连接失效,
//     另一方的下一次写会走 ensure 重新拨号,双方不会同时拨号(dialMu 串行);
//   - 拨号失败按指数退避重试,直到成功或 ctx 取消。
type engineConn struct {
	addr string
	base time.Duration // 退避起点(测试可调小)

	dialMu sync.Mutex // 串行化拨号
	mu     sync.Mutex // 保护 cur
	cur    net.Conn
}

func newEngineConn(addr string, base time.Duration) *engineConn {
	return &engineConn{addr: addr, base: base}
}

// ensure 返回当前可用连接;连接不存在或已失效时拨号重连。
func (c *engineConn) ensure(ctx context.Context) (net.Conn, error) {
	c.dialMu.Lock()
	defer c.dialMu.Unlock()

	c.mu.Lock()
	if c.cur != nil {
		conn := c.cur
		c.mu.Unlock()
		return conn, nil
	}
	c.mu.Unlock()

	delay := c.base
	for {
		conn, err := net.Dial("unix", c.addr)
		if err == nil {
			c.mu.Lock()
			c.cur = conn
			c.mu.Unlock()
			return conn, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
		if delay > 5*time.Second {
			delay = 5 * time.Second
		}
	}
}

// markDead 使连接失效并关闭底层 socket。
// 只有持有该连接的调用方(读/写 goroutine)会调用,幂等。
func (c *engineConn) markDead(conn net.Conn) {
	c.mu.Lock()
	if c.cur == conn {
		c.cur = nil
		conn.Close()
	}
	c.mu.Unlock()
}
