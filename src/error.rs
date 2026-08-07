//! 框架统一错误类型。
//!
//! 延续 BanNet 的单一 `Error` 枚举风格,扩展出协议解析 / 会话等变体。
//! 所有解析路径都返回 `Err` 而不是 panic —— 畸形包必须走错误分支
//! (见规格书 T0002M07:任何输入都不得 panic)。

use std::fmt;

#[derive(Debug)]
pub enum Error {
    /// 底层 IO 错误(read / write / bind / connect)。
    Io(std::io::Error),
    /// 通道发送失败:对端 task 已退出。
    Send(String),
    /// 协议解析错误:畸形包 / 非法字段。带说明,不 panic。
    Protocol(String),
    /// 会话不存在(已经关闭或从未建立)。
    SessionNotFound(u64),
    /// 通道已关闭。
    Closed(String),
    /// 配置错误。
    Config(String),
}

/// 全框架统一的 Result。
pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "IO 错误: {e}"),
            Error::Send(msg) => write!(f, "发送失败: {msg}"),
            Error::Protocol(msg) => write!(f, "协议错误: {msg}"),
            Error::SessionNotFound(id) => write!(f, "会话不存在: sess_id={id}"),
            Error::Closed(msg) => write!(f, "通道已关闭: {msg}"),
            Error::Config(msg) => write!(f, "配置错误: {msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

/// 有了它,IO 相关的地方可以直接用 `?`。
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}
