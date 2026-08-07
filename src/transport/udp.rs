//! 客户端 UDP 传输(规格书 T0002M02)。
//!
//! - 接收:SO_REUSEPORT 多 socket,每个 recv task 一个 socket,内核负载均衡。
//! - 发送:共享一个 `UdpSocket`(`send_to(&self, ...)` 可并发调用)。
//! - 批量收发(recvmmsg/sendmmsg)是 Linux 优化项,`cfg(target_os = "linux")`
//!   预留 TODO;macOS / 通用路径先走单包 recv_from/send_to,压测后再优化。

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::UdpSocket;

use crate::error::{Error, Result};

/// 接收回调:`(数据, 来源地址)`。
/// 由 engine 注入(它持有会话表与上行队列);这里保持传输层无业务。
pub type PacketHandler = dyn Fn(&[u8], SocketAddr) + Send + Sync + 'static;

/// UDP 接收端:一个或多个 SO_REUSEPORT socket。
#[derive(Clone)]
pub struct UdpReceiver {
    sockets: Vec<Arc<UdpSocket>>,
}

/// 用 socket2 预绑定 SO_REUSEPORT 后再转 tokio socket。
fn bind_reuseport(addr: SocketAddr) -> Result<UdpSocket> {
    let domain = match addr {
        SocketAddr::V4(_) => socket2::Domain::IPV4,
        SocketAddr::V6(_) => socket2::Domain::IPV6,
    };
    let socket = socket2::Socket::new(domain, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))?;
    // SO_REUSEPORT:同一端口多个 socket,内核按四元组负载均衡。
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "ios", target_os = "freebsd", target_os = "netbsd", target_os = "openbsd"))]
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    let std_socket: std::net::UdpSocket = socket.into();
    UdpSocket::from_std(std_socket).map_err(Error::Io)
}

impl UdpReceiver {
    /// 绑定 `count` 个 SO_REUSEPORT socket 到同一地址。
    pub async fn bind(addr: SocketAddr, count: usize) -> Result<Self> {
        let count = count.max(1);
        let mut sockets = Vec::with_capacity(count);
        for _ in 0..count {
            sockets.push(Arc::new(bind_reuseport(addr)?));
        }
        Ok(Self { sockets })
    }

    /// 本地实际监听地址(0 端口时返回内核分配的端口)。
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.sockets[0].local_addr().map_err(Error::Io)
    }

    /// 为每个 socket 拉起一个 recv task。数据到达后回调 `handler`。
    ///
    /// 全部 task 结束后返回(正常情况不会结束)。
    pub async fn run(&self, handler: Arc<PacketHandler>) -> Result<()> {
        let mut tasks = Vec::new();
        for (i, socket) in self.sockets.iter().enumerate() {
            let socket = socket.clone();
            let handler = handler.clone();
            tasks.push(tokio::spawn(async move {
                let mut buf = vec![0u8; crate::protocol::types::MTU];
                loop {
                    let (n, peer) = match socket.recv_from(&mut buf).await {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!(recv_task = i, error = %e, "UDP recv 失败");
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                            continue;
                        }
                    };
                    if n > 0 {
                        handler(&buf[..n], peer);
                    }
                }
            }));
        }
        // 只有收到退出信号才结束(engine 用 select 驱动)。
        for t in tasks {
            t.await.map_err(|e| Error::Send(format!("recv task 失败: {e}")))?;
        }
        Ok(())
    }
}

/// UDP 发送端:共享一个 socket,多 task 并发 send。
#[derive(Clone)]
pub struct UdpSender {
    socket: Arc<UdpSocket>,
}

impl UdpSender {
    pub async fn bind(addr: SocketAddr) -> Result<Self> {
        // 发送 socket 不设 SO_REUSEPORT 也可与接收共存(绑定同一端口时需设置)。
        let socket = Arc::new(bind_reuseport(addr)?);
        Ok(Self { socket })
    }

    pub async fn send(&self, data: &[u8], peer: SocketAddr) -> Result<()> {
        self.socket.send_to(data, peer).await?;
        Ok(())
    }

    /// 非阻塞发送(同步回调路径用,握手回包/心跳)。
    pub fn try_send(&self, data: &[u8], peer: SocketAddr) -> Result<()> {
        self.socket.try_send_to(data, peer)?;
        Ok(())
    }
}
