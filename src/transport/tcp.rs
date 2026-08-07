//! 客户端 TCP 传输 —— 与 UDP 传输共享同一套 datagram 协议:
//!
//! 每条 TCP 连接 = 一个客户端虚拟 peer。客户端把 UDP 报文
//! (16B datagram 头 + frames + 4B HMAC)经 TCP 流发送,每条报文前加
//! 4B 长度前缀(不含自身,与 UDS 帧流同款语义)。引擎侧拆包后走与
//! UDP 完全相同的 `handle_datagram` 路径 —— 握手 / HMAC / 会话 /
//! 可靠层 / NAT 漂移(带 conn_id 重连)全部复用,零协议分叉。
//!
//! 适用场景:内网部署(逻辑服到引擎的旁路直连)、弱网兜底(丢包高时
//! TCP 有序可靠优于 UDP+自研可靠)、防火墙后(仅开 TCP 443/8443)。
//!
//! 生命周期:连接断开 = 客户端消失,会话进入正常宽限期回收;
//! 客户端带 conn_id 重连(新连接)即恢复,与 UDP 的 NAT 漂移同一套语义。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};

use crate::error::{Error, Result};

/// 接收回调:`(数据, 虚拟 peer 地址)` —— 与 UDP 的 PacketHandler 同型。
pub type PacketHandler = dyn Fn(&[u8], SocketAddr) + Send + Sync + 'static;

/// TCP 接收端:一个 TcpListener + 每连接一对读/写 task。
/// 并发共享请用 `Arc<TcpReceiver>`(内部字段已可共享)。
pub struct TcpReceiver {
    listener: TcpListener,
    /// 虚拟 peer 地址生成器(127.0.0.2:port,port 自增)。
    next_port: AtomicU16,
    /// 虚拟 peer → 该连接的写通道。
    writers: Arc<Mutex<HashMap<SocketAddr, mpsc::Sender<Vec<u8>>>>>,
    /// 当前活跃连接数(诊断/压测)。
    conns: Arc<AtomicU16>,
}

/// 虚拟 peer 的 IP 前缀(仅作内部寻址,不与真实网络交互)。
const VIRTUAL_IP: [u8; 4] = [127, 0, 0, 2];

/// 判断一个 peer 是否是 TCP 虚拟 peer(精确 127.0.0.2)。
pub fn is_virtual_peer(peer: SocketAddr) -> bool {
    matches!(
        peer,
        SocketAddr::V4(v4) if v4.ip().octets() == VIRTUAL_IP
    )
}

impl TcpReceiver {
    pub async fn bind(addr: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(addr).await.map_err(Error::Io)?;
        Ok(Self {
            listener,
            writers: Arc::new(Mutex::new(HashMap::new())),
            next_port: AtomicU16::new(1),
            conns: Arc::new(AtomicU16::new(0)),
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.listener.local_addr().map_err(Error::Io)
    }

    /// 当前 TCP 客户端连接数。
    pub fn conns(&self) -> u16 {
        self.conns.load(Ordering::Relaxed)
    }

    /// 每连接读循环:拆包(4B len + datagram 报文)→ 回调 handler。
    /// 写通道把 send_back 的路由到对应连接。
    async fn conn_loop(
        stream: TcpStream,
        peer: SocketAddr,
        writers: Arc<Mutex<HashMap<SocketAddr, mpsc::Sender<Vec<u8>>>>>,
        handler: Arc<PacketHandler>,
        conns: Arc<AtomicU16>,
    ) {
        let (mut reader, writer) = stream.into_split();
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(256);
        writers.lock().await.insert(peer, tx);
        conns.fetch_add(1, Ordering::Relaxed);

        // 写 task:消费 send_back 投递的报文。
        let write_task = tokio::spawn(async move {
            let mut writer = writer;
            while let Some(bytes) = rx.recv().await {
                let mut out = Vec::with_capacity(4 + bytes.len());
                out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(&bytes);
                if writer.write_all(&out).await.is_err() {
                    break; // 对端关闭
                }
            }
            let _ = writer.shutdown().await;
        });

        // 读循环:4B len + 报文。
        let mut len_buf = [0u8; 4];
        loop {
            if reader.read_exact(&mut len_buf).await.is_err() {
                break; // EOF / 对端关闭
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            if len == 0 || len > 65535 {
                break; // 非法报文,断开
            }
            let mut buf = vec![0u8; len];
            if reader.read_exact(&mut buf).await.is_err() {
                break;
            }
            handler(&buf, peer);
        }

        // 清理:移除写通道,断开写 task,会话由引擎宽限期回收。
        writers.lock().await.remove(&peer);
        conns.fetch_sub(1, Ordering::Relaxed);
        write_task.abort();
    }

    /// 拉起 accept 循环,持续到 listener 关闭(正常不会返回)。
    pub async fn run(&self, handler: Arc<PacketHandler>) -> Result<()> {
        loop {
            let (stream, _) = match self.listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::debug!(error = %e, "TCP accept 失败");
                    continue;
                }
            };
            let _ = stream.set_nodelay(true);
            let port = self.next_port.fetch_add(1, Ordering::Relaxed);
            let peer = SocketAddr::from((VIRTUAL_IP, port));
            tracing::debug!(%peer, "TCP 客户端接入");
            let writers = self.writers.clone();
            let conns = self.conns.clone();
            let handler = handler.clone();
            tokio::spawn(async move {
                Self::conn_loop(stream, peer, writers, handler, conns).await;
            });
        }
    }

    /// 非阻塞把报文发给指定虚拟 peer;连接不存在或写满时丢弃(客户端会重试)。
    pub async fn try_send(&self, bytes: &[u8], peer: SocketAddr) {
        let writers = self.writers.lock().await;
        if let Some(tx) = writers.get(&peer) {
            let _ = tx.try_send(bytes.to_vec());
        }
    }
}
