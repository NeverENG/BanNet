//! 单个连接的抽象。
//!
//! 阶段 0(当前):最朴素形态 —— 读到什么,原样写回去(裸字节 echo,不解析协议)。
//! 阶段 2 会把它升级成"读/写两条流水线 + mpsc 解耦",现在先不碰。

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};

/// 一条客户端连接。阶段 0 只持有 stream + 对端地址。
pub struct Connection {
    stream: TcpStream,
    peer: SocketAddr,
}

impl Connection {
    /// 用一条 accept 到的 TcpStream 构造 Connection。
    pub fn new(stream: TcpStream, peer: SocketAddr) -> Self {
        Self { stream, peer }
    }
    /// 处理这条连接:循环"读一点 → 原样写回",直到对端关闭。
    ///
    /// 签名是 `mut self`(按值 + 可变):连接的所有权被移进这个 task,
    /// task 独占它、负责它的整个生命周期。`mut` 是因为 read/write 要改 stream 内部状态。
    ///
    /// TODO(你来):实现 echo 循环。步骤:
    ///   1. 建一个读缓冲: let mut buf = [0u8; 1024];
    ///   2. loop {
    ///        let n = self.stream.read(&mut buf).await?;   // 读到 n 字节
    ///        if n == 0 { break; }                          // n==0 = 对端已关闭(EOF)
    ///        self.stream.write_all(&buf[..n]).await?;       // 只写回读到的那 n 字节
    ///      }
    ///   3. 上面的 `?` 需要函数能返回 Result;但本函数返回 ()。
    ///      所以你有两个选择,自己权衡:
    ///        (a) 把循环包进一个 `if let Err(e) = self.echo_loop().await { eprintln!(...) }`
    ///        (b) 或直接在 loop 里 match/处理错误后 break。
    ///      —— 记住哲学第 6 条:别 panic,一条连接崩了不该拖垮别人。
    ///
    /// ⚠️ 关键坑:第 2 步为什么写回是 `&buf[..n]` 而不是 `&buf`?
    ///    buf 有 1024 字节,但这次可能只读到 3 字节。写回整个 buf,会把后面
    ///    1021 个垃圾零字节也发给客户端。`&buf[..n]` = "只发这次真正读到的部分"。
    pub fn start(mut self) {
        let mut buf = [0u8; 1024];
        loop {
            let n = match self.stream.read(&mut buf) {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("[BanNet]read error from {}: {}", self.peer, e);
                    break;
                }
            };
            if n == 0 {
                break;
            }
            // todo 暂时是回写
            if let Err(e) = self.stream.write_all(&buf[..n])
             {
                eprintln!("[BanNet]write error to {}: {}", self.peer, e);
                break;
            }
        }
    }
    pub fn stop(&mut self) {
        // TODO(阶段 2):优雅关闭连接,先 shutdown 写半边,再读完对端剩余数据,最后 close。
    }
}
