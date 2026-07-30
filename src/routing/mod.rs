//! # 路由层
//!
//! 把"收到的消息"分发给"用户写的业务逻辑"。这一层定义框架与用户之间的契约:
//! 用户实现 [`router::Router`] trait,框架按 msgID 找到对应实现并调用,
//! 调用时把 [`request::Request`](连接 + 消息)交给它。
//!
//! - [`request`] 请求上下文 `{ conn, message }`
//! - [`router`]  Router trait + 路由表

mod request;
mod router;

// ── 本层对外暴露的 API ──
pub use router::Router;
