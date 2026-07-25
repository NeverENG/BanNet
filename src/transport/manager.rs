//! 连接管理器 —— "1 个 Server 管 N 个 Connection" 里的那个 N 的容器。
//!
//! 维护一张 connID(u32) -> Connection 的表,支持增/删/查/群发。
//! 会被多个 task 并发访问,所以要用 Arc<Mutex<HashMap<..>>>(或 DashMap)。
//! 这是学习 Rust "共享可变状态" 的核心场景。
//!
//! 阶段 4 目标:
//!   - add / remove / get / len / clear
//!
//! TODO(阶段 4)。
