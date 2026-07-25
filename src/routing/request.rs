//! 请求上下文。
//!
//! 把"是哪个连接发来的"(conn)和"消息内容"(message)打包成一个 Request,
//! 交给用户的 handler。这样 handler 既能读消息,又能通过 conn 回消息。
//!
//! 阶段 3 目标:
//!   pub struct Request { conn: ..., message: Message }
//!   - conn() -> &Connection
//!   - message() -> &Message
//!
//! TODO(阶段 3)。
