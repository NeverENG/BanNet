//! 单个连接的抽象。
//!
//! 包住一条 TcpStream,内部跑两条异步流水线:
//!   - 读流水线:不断 read_exact 头+body -> 组装 Message -> 交给 Router
//!   - 写流水线:从一个 mpsc channel 收 Message -> pack -> 写回 socket
//!
//! 这样"谁都能发消息、但只有一个任务真正持有写端",是 Rust 里
//! 处理"共享写"最干净的模式(读/写分离 + channel)。这是阶段 2 的重头戏。
//!
//! 阶段 2 目标:
//!   - Connection::new(...)
//!   - send(&self, id: u32, data: &[u8])   把消息塞进写 channel
//!   - start()                              启动读/写两个 task
//!
//! TODO(阶段 2)。
