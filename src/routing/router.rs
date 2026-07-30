//! 路由。
//!
//! Router 持有一张 msgID -> Handler 的路由表。它本身也实现了
//! `transport::Handler`,因此可以直接传给 `Server` 作为业务处理器。
//!
//! 这样 Connection 不需要改动,收到请求后仍然调用 `handler.handle(req)`。
//! 只不过现在的 `handler` 是一个 `Router`,它会根据 `req.id()` 分发到
//! 不同的业务 handler。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::transport::{EchoHandler, Handler, Request, SharedHandler};

/// 按 msgID 分发的路由器。
#[derive(Clone)]
pub struct Router {
    inner: Arc<Mutex<RouterInner>>,
}

struct RouterInner {
    routes: HashMap<u32, SharedHandler>,
    default: SharedHandler,
}

impl Router {
    /// 使用默认 echo handler 创建 Router。
    pub fn new() -> Self {
        Self::with_default_handler(EchoHandler)
    }

    /// 使用指定默认 handler 创建 Router。
    pub fn with_default_handler<H: Handler>(default: H) -> Self {
        let default = Arc::new(default) as SharedHandler;
        let inner = RouterInner {
            routes: HashMap::new(),
            default,
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    /// 注册一个 msgID 的路由处理器。
    pub fn add_route<H: Handler>(&self, msg_id: u32, handler: H) -> &Self {
        let mut inner = self.inner.lock().unwrap();
        inner.routes.insert(msg_id, Arc::new(handler));
        self
    }
}

impl Handler for Router {
    fn handle(&self, req: Request) -> crate::transport::BoxFuture {
        let target = {
            let inner = self.inner.lock().unwrap();
            inner
                .routes
                .get(&req.id())
                .cloned()
                .unwrap_or_else(|| inner.default.clone())
        };
        target.handle(req)
    }
}
