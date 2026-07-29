//! 单个连接的抽象。
//!
//! 阶段 0(当前):读 -> 交给 [`Handler`] -> handler 通过 `req.reply()` 回包。
//! 已经是「读/写两条流水线 + mpsc 解耦」的形态:
//!
//! ```text
//!            ┌──────────────┐   Request(含 tx)   ┌──────────┐
//!  socket ──▶│  read task   │ ──────────────────▶│ Handler  │
//!            └──────────────┘                    └────┬─────┘
//!                                                     │ reply → tx
//!            ┌──────────────┐        mpsc             ▼
//!  socket ◀──│  write task  │◀────────────────────────┘
//!            └──────────────┘
//! ```
//!
//! 读、写各占一个 task,两边都监听 `exit`(watch 通道)做优雅退出。
//! 只有写 task 持有 `WriteHalf`,所以永远不会有两个 task 抢着写同一条流。

use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};

use super::handler::SharedHandler;
use super::req::Request;
use crate::protocol::DataPack;

/// 回包队列深度。handler 生产回包比 socket 消费快时,先在这里排队。
const WRITE_QUEUE_CAP: usize = 100;

/// 一次 read 的缓冲区大小。
const READ_BUF_SIZE: usize = 1024;

/// 一条客户端连接。
pub struct Connection {
    conn_id: u32,
    stream: TcpStream,
    peer: SocketAddr,
    /// 全局退出信号。Server 收到 Ctrl-C 时广播。
    exit: watch::Receiver<bool>,
    /// 业务处理器。注意是 `Arc<dyn Handler>` 而不是 `Option<fn(..)>`:
    /// 一定有值(没注册就是 EchoHandler),所以调用点不用 `unwrap`。
    handler: SharedHandler,
}

impl Connection {
    /// 用一条 accept 到的 TcpStream 构造 Connection。
    pub fn new(
        conn_id: u32,
        stream: TcpStream,
        peer: SocketAddr,
        exit: watch::Receiver<bool>,
        handler: SharedHandler,
    ) -> Self {
        Self {
            conn_id,
            stream,
            peer,
            exit,
            handler,
        }
    }

    /// 启动这条连接:拉起读、写两个 task,等它们都结束后返回。
    pub async fn start(self) {
        // 先解构 self,把各字段的所有权分发给两个 task。
        // (不能写 `tokio::io::split(self.stream)` 之后再用 `self.handler` ——
        //  那是把 self 部分移走后又整体借用,编译不过。)
        let Connection {
            conn_id,
            stream,
            peer,
            exit,
            handler,
        } = self;

        let (reader, writer) = tokio::io::split(stream);
        let (tx, rx) = mpsc::channel::<Vec<u8>>(WRITE_QUEUE_CAP);

        eprintln!("「BanNet」连接 #{conn_id} 建立,对端 {peer}");

        let read_task = tokio::spawn(Self::start_read(reader, peer, tx, handler, exit.clone()));
        let write_task = tokio::spawn(Self::start_write(writer, rx, exit));

        // 读 task 结束时会 drop 掉它持有的 tx,通道随之关闭,写 task 自然收尾。
        let _ = tokio::join!(read_task, write_task);

        eprintln!("「BanNet」连接 #{conn_id} 已关闭,对端 {peer}");
    }

    /// 读流水线:socket -> Request -> Handler。
    ///
    /// handler 是 **inline await** 的(不是 spawn):同一条连接上的消息严格按序
    /// 处理,一个慢 handler 会挡住这条连接后续的读。阶段 0 要的就是这个语义
    /// (简单、有序)。将来要并发处理,把这一行换成 `tokio::spawn` 即可,
    /// 但那时就得自己保证回包顺序。
    async fn start_read(
        mut reader: ReadHalf<TcpStream>,
        peer: SocketAddr,
        tx: mpsc::Sender<Vec<u8>>,
        handler: SharedHandler,
        mut exit: watch::Receiver<bool>,
    ) {
        let mut buf = [0u8; READ_BUF_SIZE];
        let mut pending = Vec::new();
        let packer = DataPack::new();

        loop {
            let n = tokio::select! {
                _ = exit.changed() => {
                    eprintln!("「BanNet」客户端 {peer} 收到退出信号,停止读取");
                    break;
                }
                result = reader.read(&mut buf) => match result {
                    Ok(0) => {
                        eprintln!("「BanNet」客户端 {peer} 关闭了连接");
                        break;
                    }
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("「BanNet」客户端 {peer} 读取数据失败: {e}");
                        break;
                    }
                },
            };

            pending.extend_from_slice(&buf[..n]);

            while let Ok(Some((message, consumed))) = packer.unpack(&pending) {
                pending.drain(..consumed);
                let req = Request::new(message, tx.clone());
                if let Err(e) = handler.handle(req).await {
                    eprintln!("「BanNet」处理客户端 {peer} 的请求失败: {e}");
                }
            }
        }
    }

    /// 写流水线:mpsc -> socket。全连接唯一持有 `WriteHalf` 的地方。
    async fn start_write(
        mut writer: WriteHalf<TcpStream>,
        mut rx: mpsc::Receiver<Vec<u8>>,
        mut exit: watch::Receiver<bool>,
    ) {
        loop {
            tokio::select! {
                _ = exit.changed() => {
                    // 优雅退出:不再接收新回包,但把已经排队的写完再走。
                    rx.close();
                    while let Some(data) = rx.recv().await {
                        if writer.write_all(&data).await.is_err() {
                            break;
                        }
                    }
                    break;
                }
                maybe_data = rx.recv() => match maybe_data {
                    // 通道关闭 = 读 task 已退出,没人再会回包了。
                    None => break,
                    Some(data) => {
                        if let Err(e) = writer.write_all(&data).await {
                            eprintln!("「BanNet」写入数据失败: {e}");
                            break;
                        }
                    }
                },
            }
        }

        // 主动发 FIN,让对端知道我们不会再写了。
        let _ = writer.shutdown().await;
    }
}

// 访问器。阶段 4 的 ConnManager 会用到,现在先留着。
#[allow(dead_code)]
impl Connection {
    pub fn id(&self) -> u32 {
        self.conn_id
    }

    pub fn peer(&self) -> SocketAddr {
        self.peer
    }

    pub fn set_id(&mut self, id: u32) {
        self.conn_id = id;
    }

    /// 换掉这条连接的业务处理器(必须在 `start()` 之前调用)。
    pub fn set_handler(&mut self, handler: SharedHandler) {
        self.handler = handler;
    }
}
