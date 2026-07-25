//! 路由。
//!
//! Router 是一个 trait(接口),用户实现它来写业务逻辑:
//!
//!   trait Router {
//!       async fn handle(&self, req: Request);
//!   }
//!
//! Server 内部维护一张表 msgID(u32) -> Box<dyn Router>,收到消息后按 id 分发。
//!
//! 注意点(阶段 3 会讲):trait 里带 async fn + 要做成 `dyn Router` 存进
//! HashMap,涉及 trait object 的对象安全问题,可能要用 async_trait 或
//! Box<dyn Future>。这是一个很好的 Rust 深水区学习点。
//!
//! TODO(阶段 3)。
