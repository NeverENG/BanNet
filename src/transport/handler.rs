//! Handler 抽象 —— 框架与用户业务逻辑之间的契约。
//!
//! ## 为什么不能用 `fn(Request) -> Result<()>`
//!
//! 之前 Connection 里存的是一个裸函数指针:
//!
//! ```ignore
//! handle: Option<fn(Request) -> Result<(), Error>>
//! ```
//!
//! 三个问题:
//! 1. **不是 async**。`req.reply()` 要 `.await`,同步函数里根本调不了。
//! 2. **存不下闭包**。`fn` 指针只能接「不捕获环境」的函数,用户想写
//!    `let db = ...; move |req| { db.query(...) }` 这种带状态的 handler 就废了。
//! 3. **`Option` + `.unwrap()`**。没注册 handler 时直接 panic 掉整条连接。
//!
//! ## 现在的形态
//!
//! 一个对象安全(object-safe)的 trait,可以塞进 `Arc<dyn Handler>`:
//!
//! ```ignore
//! trait Handler {
//!     fn handle(&self, req: Request) -> BoxFuture;
//! }
//! ```
//!
//! trait 里不能直接写 `async fn` 又同时做成 `dyn Trait`(返回的 Future 类型
//! 每个实现都不一样,大小不定),所以手动把 Future 装箱成
//! `Pin<Box<dyn Future + Send>>`。这就是 `async_trait` 宏在背后干的事,
//! 我们自己写一遍,省一个依赖,也顺便看清它的原理。
//!
//! 再加一个 **blanket impl**:任何 `Fn(Request) -> impl Future` 的闭包都自动
//! 是 `Handler`。于是用户可以直接写:
//!
//! ```ignore
//! Server::new(addr, |req| async move { req.reply(req.data().to_vec()).await })
//! ```
//!
//! 阶段 3 的 Router(按 msgID 分发)也只是「一个内部持有 HashMap 的 Handler」,
//! 到时候直接 `impl Handler for Router` 就能接进来,Connection 一行都不用改。

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::req::Request;
use crate::error::Result;

/// 装箱后的 Future。`'static` 是因为它要被 spawn / 跨 await 持有。
pub type BoxFuture = Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;

/// 业务处理器。框架每收到一条消息就调一次 `handle`。
///
/// `Send + Sync + 'static`:handler 会被多条连接的多个 task 并发共享
/// (放在 `Arc` 里),所以必须能跨线程共享。
pub trait Handler: Send + Sync + 'static {
    fn handle(&self, req: Request) -> BoxFuture;
}

/// 共享的 handler 句柄。Server / Connection 都存这个。
pub type SharedHandler = Arc<dyn Handler>;

/// blanket impl:让 async 闭包 / async fn 自动成为 Handler。
///
/// 这样用户既可以「实现 trait」写复杂的有状态 handler,也可以「甩一个闭包」
/// 写两行的简单逻辑,两种写法在框架内部是同一个类型。
impl<F, Fut> Handler for F
where
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    fn handle(&self, req: Request) -> BoxFuture {
        Box::pin(self(req))
    }
}

/// 兜底 handler:没注册业务逻辑时用它,原样回显。
///
/// 比 `Option<Handler>` + `unwrap()` 好在:调用点没有分支、不会 panic,
/// 而且它本身就是阶段 0 想要的 echo 行为。
pub struct EchoHandler;

impl Handler for EchoHandler {
    fn handle(&self, req: Request) -> BoxFuture {
        Box::pin(async move {
            let data = req.data().to_vec();
            req.reply(data).await
        })
    }
}
