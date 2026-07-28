//! 框架统一错误类型。
//!
//! 之前代码里借用了 `std::fmt::Error` 当错误类型 —— 那是给 `Display` 用的,
//! 它没有任何 variant,承载不了信息。这里定义 BanNet 自己的 `Error`,
//! 并给出 `bannet::Result<T>` 别名(lib 文档里承诺过的那个)。

use std::fmt;

#[derive(Debug)]
pub enum Error {
    /// 底层 IO 错误(read / write / bind / accept)。
    Io(std::io::Error),
    /// 回包失败:写半边的 task 已经退出,mpsc 通道关闭了。
    Send(String),
}

/// 全框架统一的 Result。用户 handler 也返回它。
pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "IO 错误: {e}"),
            Error::Send(msg) => write!(f, "回包失败: {msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Send(_) => None,
        }
    }
}

/// 有了它,IO 相关的地方可以直接用 `?`。
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}
