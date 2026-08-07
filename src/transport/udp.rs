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

/// UDP 发送端:复用接收端同一批 SO_REUSEPORT socket(收发合一)。
///
/// ⚠️ 为什么必须收发同一批 socket:客户端回包(ACK/上行)发往**收到的包的
/// 源地址**。若发送走独立 socket(哪怕绑定引擎端口),要么参与 REUSEPORT
/// 分发却从不 recv(分到的包堆积丢失),要么随机端口(客户端 ACK 发往
/// 随机端口没人收)。只有发送源端口 == 监听端口、且该端口上的所有 socket
/// 都参与 recv,链路才成立。
pub struct UdpSender {
    sockets: Vec<Arc<UdpSocket>>,
    idx: Arc<std::sync::atomic::AtomicUsize>,
}

impl Clone for UdpSender {
    fn clone(&self) -> Self {
        Self {
            sockets: self.sockets.clone(),
            idx: self.idx.clone(),
        }
    }
}

impl UdpSender {
    /// 从接收端的 socket 池构造发送端(共享同一批 socket)。
    pub fn from_receiver(receiver: &UdpReceiver) -> Self {
        Self {
            sockets: receiver.sockets.clone(),
            idx: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// 轮询选择一个 socket 发送(源端口始终 = 引擎监听端口)。
    fn pick(&self) -> &Arc<UdpSocket> {
        let i = self.idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % self.sockets.len();
        &self.sockets[i]
    }

    pub async fn send(&self, data: &[u8], peer: SocketAddr) -> Result<()> {
        self.pick().send_to(data, peer).await?;
        Ok(())
    }

    /// 非阻塞发送(同步回调路径用,握手回包/心跳)。
    pub fn try_send(&self, data: &[u8], peer: SocketAddr) -> Result<()> {
        self.pick().try_send_to(data, peer)?;
        Ok(())
    }
}
