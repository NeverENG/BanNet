//! 单个连接的抽象。
//!
//! 阶段 0(当前):最朴素形态 —— 读到什么,原样写回去(裸字节 echo,不解析协议)。
//! 阶段 2 会把它升级成"读/写两条流水线 + mpsc 解耦",现在先不碰。

use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

/// 一条客户端连接。阶段 0 只持有 stream + 对端地址。
pub struct Connection {
    stream: TcpStream,
    peer: SocketAddr,
    exit: tokio::sync::watch::Receiver<bool>,
}

impl Connection {
    /// 用一条 accept 到的 TcpStream 构造 Connection。
    pub fn new(
        stream: TcpStream,
        peer: SocketAddr,
        exit: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        Self { stream, peer, exit }
    }

    pub async fn start(mut self) {
        let (reader, writer) = tokio::io::split(self.stream);
        let (tx, rx) = mpsc::channel::<Vec<u8>>(100);
        let peer = self.peer;

        tokio::spawn(async move {
            Self::start_read(reader, peer, tx).await;
        });

        tokio::spawn(async move {
            Self::start_write(writer, rx).await;
        });

        loop {
            tokio::select! {
                _ = self.exit.changed() => {
                    eprintln!("「BanNet」客户端 {} 收到退出信号,准备关闭连接...", self.peer);
                    break;
                }
            }
        }
    }

    //pub fn stop(&mut self) {// TODO(阶段 2):优雅关闭连接,先 shutdown 写半边,再读完对端剩余数据,最后 close。}

    async fn start_read(
        mut reader: ReadHalf<TcpStream>,
        peer: SocketAddr,
        tx: mpsc::Sender<Vec<u8>>,
    ) {
        let mut buf = [0u8; 1024];

        loop {
            match reader.read(&mut buf).await {
                Ok(0) => {
                    eprintln!("「BanNet」客户端 {} 关闭了连接", peer);
                    break;
                }
                Ok(n) => {
                    if tx.send(buf[..n].to_vec()).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("「BanNet」客户端 {} 读取数据失败: {}", peer, e);
                    break;
                }
            }
        }
    }

    async fn start_write(mut writer: WriteHalf<TcpStream>, mut rx: mpsc::Receiver<Vec<u8>>) {
        while let Some(data) = rx.recv().await {
            if let Err(e) = writer.write_all(&data).await {
                eprintln!("「BanNet」写入数据失败: {}", e);
                break;
            }
        }
    }
}
