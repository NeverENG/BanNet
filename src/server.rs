//! 服务器 —— 框架的门面(用户第一个接触的类型)。
//!
//! 职责:bind 端口 -> 循环 accept -> 每来一个连接就造一个 Connection 并 start。
//! 同时持有业务 handler 和(阶段 4)连接管理器。
//!
//! 阶段 0 目标(最小可跑):
//!   - `Server::new(addr)`          默认 echo
//!   - `Server::with_handler(addr, h)` / `server.on(h)`   注册业务处理器
//!   - `run().await`                accept 循环 + Ctrl-C 优雅退出
//!
//! TODO(阶段 3):`on()` 换成 `add_router(msg_id, router)`,按 msgID 分发。

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::error::Result;
use crate::routing::Router;
use crate::transport::{Connection, Handler, SharedHandler};

pub struct Server {
    listener: TcpListener,
    addr: String,
    /// 业务处理器,所有连接共享同一个 `Arc`。
    handler: SharedHandler,
    /// 只要 `Server::new` 走默认构造,这个 Router 就存在并可注册路由。
    router: Router,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl Server {
    /// bind 端口,使用默认的 Router(echo fallback)。
    pub async fn new(addr: impl Into<String>) -> Result<Server> {
        let router = Router::new();
        let handler = Arc::new(router.clone()) as SharedHandler;
        Self::with_router(addr, router, handler).await
    }

    async fn with_router(
        addr: impl Into<String>,
        router: Router,
        handler: SharedHandler,
    ) -> Result<Server> {
        let addr = addr.into();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let listener = TcpListener::bind(&addr).await?;

        Ok(Server {
            listener,
            addr,
            handler,
            router,
            shutdown_tx,
            shutdown_rx,
        })
    }

    /// bind 端口,并注册业务处理器。
    ///
    /// `handler` 可以是:
    /// - 一个 async 闭包 `|req| async move { req.reply(...).await }`
    /// - 任何 `impl Handler` 的类型(带状态的处理器 / 阶段 3 的 Router)
    pub async fn with_handler<H: Handler>(addr: impl Into<String>, handler: H) -> Result<Server> {
        let addr = addr.into();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let listener = TcpListener::bind(&addr).await?;

        Ok(Server {
            listener,
            addr,
            handler: Arc::new(handler),
            router: Router::new(),
            shutdown_tx,
            shutdown_rx,
        })
    }

    /// 注册一个路由处理器, msg_id 将由 Router 分发。
    pub fn on<H: Handler>(&mut self, msg_id: u32, handler: H) -> &mut Self {
        self.router.add_route(msg_id, handler);
        self.handler = Arc::new(self.router.clone()) as SharedHandler;
        self
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// accept 循环。收到退出信号后停止接受新连接并返回。
    pub async fn start(&self) {
        let mut shutdown = self.shutdown_rx.clone();
        let mut next_conn_id: u32 = 1;

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    eprintln!("「BanNet」停止接受新连接");
                    break;
                }
                accepted = self.listener.accept() => match accepted {
                    Ok((stream, peer)) => {
                        let conn = Connection::new(
                            next_conn_id,
                            stream,
                            peer,
                            self.shutdown_rx.clone(),
                            self.handler.clone(),
                        );
                        next_conn_id = next_conn_id.wrapping_add(1);
                        tokio::spawn(conn.start());
                    }
                    Err(e) => {
                        // accept 失败(比如 fd 耗尽)不该直接搞死整个 server。
                        eprintln!("「BanNet」accept 失败: {e}");
                    }
                },
            }
        }
    }

    /// 跑起来:后台 accept,前台等 Ctrl-C,收到后广播退出信号。
    pub async fn run(self) -> Result<()> {
        let shutdown_tx = self.shutdown_tx.clone();
        let addr = self.addr.clone();

        let accept_task = tokio::spawn(async move {
            self.start().await;
        });

        eprintln!("「BanNet」echo server 启动,监听 {addr} ...(Ctrl-C 退出)");

        match tokio::signal::ctrl_c().await {
            Ok(()) => eprintln!("「BanNet」收到 Ctrl-C,准备退出..."),
            Err(e) => eprintln!("「BanNet」监听 Ctrl-C 失败: {e}"),
        }

        // 广播给 accept 循环和所有连接的读/写 task。
        // send 失败只可能是没有接收者了,忽略即可。
        let _ = shutdown_tx.send(true);
        let _ = accept_task.await;

        eprintln!("「BanNet」已退出");
        Ok(())
    }
}
