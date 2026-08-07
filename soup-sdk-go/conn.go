package soup

import (
	"context"
	"net"
	"os"
	"sync"
	"time"
)

// engineConn 管理与框架(UDS)的连接 —— 逻辑服是**监听端**:
//
// 架构(规格书 T0002M04F06):引擎(Rust)主动连接逻辑服,逻辑服掉线后
// 由引擎按指数退避重连 —— 因此 Go SDK 必须 bind + listen + accept,
// 引擎每次(重)连接都会走进来。
//
// 设计要点:
//   - 读/写两个 goroutine 各自在需要时调用 ensure 获取当前连接;
//   - 连接被引擎断开(读 EOF 或写失败)后由 markDead 失效,
//     下一次 ensure 重新 accept(引擎会按退避重连过来);
//   - 首次 ensure 时 bind + listen;ctx 取消时关闭 listener。
type engineConn struct {
	addr string
	base time.Duration // 保留(接口兼容;监听模式无退避,由引擎侧退避)

	onDead func() // 连接断开回调(引擎掉线 → SDK 清理会话)

	mu  sync.Mutex // 保护 cur / ln
	ln  net.Listener
	cur net.Conn
}

func newEngineConn(addr string, base time.Duration) *engineConn {
	return &engineConn{addr: addr, base: base}
}

// ensure 返回当前连接;无连接时监听并 accept 引擎的连接(阻塞等待,
// 引擎侧 connect + 指数退避重连自动对齐)。
func (c *engineConn) ensure(ctx context.Context) (net.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cur != nil {
		return c.cur, nil
	}
	if c.ln == nil {
		_ = os.Remove(c.addr) // 清理残留 socket 文件(引擎重启会重新 connect)
		ln, err := net.Listen("unix", c.addr)
		if err != nil {
			return nil, err
		}
		c.ln = ln
	}
	type result struct {
		conn net.Conn
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		conn, err := c.ln.Accept()
		ch <- result{conn, err}
	}()
	select {
	case <-ctx.Done():
		// 关闭 listener 让 Accept goroutine 退出,避免泄漏。
		_ = c.ln.Close()
		return nil, ctx.Err()
	case r := <-ch:
		if r.err != nil {
			return nil, r.err
		}
		c.cur = r.conn
		return r.conn, nil
	}
}

// markDead 使连接失效并关闭底层 socket;引擎掉线时触发 onDead
// (SDK 据此清理所有会话 —— 引擎进程死亡意味着客户端会话全部消失)。
func (c *engineConn) markDead(conn net.Conn) {
	c.mu.Lock()
	if c.cur == conn {
		c.cur = nil
		conn.Close()
		dead := c.onDead
		c.mu.Unlock()
		if dead != nil {
			dead()
		}
		return
	}
	c.mu.Unlock()
}
