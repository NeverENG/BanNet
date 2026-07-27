//! 服务器 —— 框架的门面(用户第一个接触的类型)。
//!
//! 职责:bind 端口 -> 循环 accept -> 每来一个连接就造一个 Connection 并 start。
//! 同时持有路由表和(阶段 4)连接管理器。
//!
//! 阶段 0 目标(最小可跑):
//!   - Server::new(addr)
//!   - run().await   bind + accept 循环,先做裸字节 echo
//! 阶段 3 目标:
//!   - add_router(id, router)   注册业务处理器
//!
//! TODO(阶段 0 起步)。

use crate::transport::Connection;
use tokio::net::TcpListener;
use tokio::sync::watch;

pub struct Server {
    listener: TcpListener,
    addr: String,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl Server {
    pub async fn new(addr: String) -> Server {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let listener = TcpListener::bind(&addr).await.unwrap();

        Server {
            listener,
            addr,
            shutdown_tx,
            shutdown_rx,
        }
    }

    pub async fn start(&self) {
        loop {
            let (stream, addr) = self.listener.accept().await.unwrap();
            let conn = Connection::new(stream, addr, self.shutdown_rx.clone());
            tokio::spawn(async move {
                conn.start().await;
            });
        }
    }

    pub async fn server(self) {
        let shutdown_tx = self.shutdown_tx.clone();
        let addr = self.addr.clone();

        tokio::spawn(async move {
            self.start().await;
        });

        eprintln!("BanNet echo server 启动中,监听 {} ...", addr);

        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                shutdown_tx.send(true).unwrap();
                eprintln!("「BanNet」收到 Ctrl-C,准备退出...");
            }
            Err(e) => {
                eprintln!("「BanNet」监听 Ctrl-C 失败: {}", e);
            }
        }
    }
}
