//! 客户端 WebSocket 传输 —— 面向浏览器/H5 客户端。
//!
//! 与 TCP 传输同一套模式:每条 WS 连接 = 一个虚拟 peer(127.0.0.3/8),
//! 每个二进制消息 = 一个 datagram 报文(16B 头 + frames + 4B HMAC,
//! WS 自带消息边界,无需长度前缀)。握手 / HMAC / 会话 / 可靠层全部复用,
//! 协议零分叉。
//!
//! 适用场景:浏览器/H5 客户端(WebSocket 是浏览器唯一原生双向通道)、
//! 网页观战/调试面板、混合端(移动原生走 UDP,网页走 WS)。
//!
//! 注意:WebSocket 基于 TCP,消息有序可靠 —— 与 UDP 客户端共享同一套
//! 会话表与可靠层,但 WS 客户端天然无丢包,重传基本不触发。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::Message;

use crate::error::{Error, Result};

/// 接收回调:`(数据, 虚拟 peer 地址)` —— 与 UDP 的 PacketHandler 同型。
pub type PacketHandler = dyn Fn(&[u8], SocketAddr) + Send + Sync + 'static;

/// WebSocket 接收端:一个 TcpListener(HTTP Upgrade 由 accept_async 处理)。
pub struct WsReceiver {
    listener: TcpListener,
    next_port: AtomicU16,
    writers: Arc<Mutex<HashMap<SocketAddr, mpsc::Sender<Vec<u8>>>>>,
    conns: Arc<AtomicU16>,
}

/// WS 虚拟 peer 的 IP 前缀(与 TCP 的 127.0.0.2 区分)。
const VIRTUAL_IP: [u8; 4] = [127, 0, 0, 3];

/// 判断一个 peer 是否是 WS 虚拟 peer(精确 127.0.0.3)。
pub fn is_virtual_peer(peer: SocketAddr) -> bool {
    matches!(
        peer,
        SocketAddr::V4(v4) if v4.ip().octets() == VIRTUAL_IP
    )
}

impl WsReceiver {
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

    /// 当前 WS 客户端连接数。
    pub fn conns(&self) -> u16 {
        self.conns.load(Ordering::Relaxed)
    }

    /// 每连接循环:HTTP 升级 → 二进制消息 = 报文 → handler。
    async fn conn_loop(
        stream: TcpStream,
        peer: SocketAddr,
        writers: Arc<Mutex<HashMap<SocketAddr, mpsc::Sender<Vec<u8>>>>>,
        handler: Arc<PacketHandler>,
        conns: Arc<AtomicU16>,
    ) {
        let ws = match tokio_tungstenite::accept_async(stream).await {
            Ok(ws) => ws,
            Err(e) => {
                tracing::debug!(%peer, error = %e, "WS 升级失败");
                return;
            }
        };
        let (mut ws_tx, mut ws_rx) = ws.split();
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(256);
        writers.lock().await.insert(peer, tx);
        conns.fetch_add(1, Ordering::Relaxed);

        // 写 task:消费 send_back 投递的报文 → 二进制消息。
        let write_task = tokio::spawn(async move {
            while let Some(bytes) = rx.recv().await {
                if ws_tx.send(Message::Binary(bytes.into())).await.is_err() {
                    break; // 对端关闭
                }
            }
            let _ = ws_tx.close().await;
        });

        // 读循环:二进制消息即报文。
        while let Some(msg) = ws_rx.next().await {
            match msg {
                Ok(Message::Binary(b)) => handler(&b, peer),
                Ok(Message::Close(_)) | Ok(Message::Ping(_)) | Err(_) => break,
                _ => {} // Text / Pong / Frame:忽略
            }
        }

        writers.lock().await.remove(&peer);
        conns.fetch_sub(1, Ordering::Relaxed);
        write_task.abort();
    }

    /// 拉起 accept 循环(正常不会返回)。
    pub async fn run(&self, handler: Arc<PacketHandler>) -> Result<()> {
        loop {
            let (stream, _) = match self.listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::debug!(error = %e, "WS accept 失败");
                    continue;
                }
            };
            let _ = stream.set_nodelay(true);
            let port = self.next_port.fetch_add(1, Ordering::Relaxed);
            let peer = SocketAddr::from((VIRTUAL_IP, port));
            tracing::debug!(%peer, "WS 客户端接入");
            let writers = self.writers.clone();
            let conns = self.conns.clone();
            let handler = handler.clone();
            tokio::spawn(async move {
                Self::conn_loop(stream, peer, writers, handler, conns).await;
            });
        }
    }

    /// 非阻塞把报文发给指定虚拟 peer;连接不存在或写满时丢弃。
    pub async fn try_send(&self, bytes: &[u8], peer: SocketAddr) {
        let writers = self.writers.lock().await;
        if let Some(tx) = writers.get(&peer) {
            let _ = tx.try_send(bytes.to_vec());
        }
    }
}
