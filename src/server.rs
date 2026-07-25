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
