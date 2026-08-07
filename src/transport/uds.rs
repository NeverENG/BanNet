//! 逻辑服 UDS 传输(规格书 T0002M04F01)。
//!
//! v1 选型:Unix Domain Socket,`SOCK_STREAM` + 长度前缀分帧 —— 简单可靠,
//! Go 侧零依赖,可用 `socat` 直接抓包调试。v2 的共享内存只在压测证明
//! UDS 是瓶颈时再做(规格书明确「不要一上来就写 shm」)。
//!
//! 本模块只提供连接与帧流原语;连接管理与帧分发在 [`crate::engine`]。

use std::path::Path;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::UnixStream;

use crate::error::{Error, Result};
use crate::protocol::frame::{self, Frame};
use crate::protocol::types::MAX_FRAME_BODY;

/// 一条到逻辑服的 UDS 连接(读写分离)。
///
/// `SOCK_STREAM` 上长度前缀分帧;read/write 各自由独立 task 驱动,
/// 互不阻塞(engine 里 read task 与 write task 并行)。
pub struct UdsConnection {
    pub reader: UdsReader,
    pub writer: UdsWriter,
}

impl UdsConnection {
    /// 连接逻辑服的 socket 文件。
    pub async fn connect(path: &Path) -> Result<Self> {
        let stream = UnixStream::connect(path).await?;
        let (read_half, write_half) = tokio::io::split(stream);
        Ok(Self {
            reader: UdsReader::new(read_half),
            writer: UdsWriter::new(write_half),
        })
    }
}

/// UDS 读半边:长度前缀分帧(天然处理半包/粘包)。
pub struct UdsReader {
    stream: ReadHalf<UnixStream>,
    read_buf: BytesMut,
}

impl UdsReader {
    pub fn new(stream: ReadHalf<UnixStream>) -> Self {
        Self {
            stream,
            read_buf: BytesMut::with_capacity(1024),
        }
    }

    /// 读一帧。连接关闭返回 `Ok(None)`。
    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            if let Some(frame) = frame::try_decode(&mut self.read_buf)? {
                return Ok(Some(frame));
            }
            // 缓冲区可能被恶意大帧撑大,限制上限。
            if self.read_buf.len() > MAX_FRAME_BODY as usize + frame::FRAME_HEADER_LEN {
                return Err(Error::Protocol("UDS 帧流缓冲超限".into()));
            }
            let mut chunk = [0u8; 4096];
            let n = self.stream.read(&mut chunk).await?;
            if n == 0 {
                // 对端关闭。
                return Ok(None);
            }
            self.read_buf.extend_from_slice(&chunk[..n]);
        }
    }
}

/// UDS 写半边:串行写帧(全连接唯一写者)。
pub struct UdsWriter {
    stream: WriteHalf<UnixStream>,
}

impl UdsWriter {
    pub fn new(stream: WriteHalf<UnixStream>) -> Self {
        Self { stream }
    }

    /// 写一帧。
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        let mut out = BytesMut::with_capacity(frame.body.len() + frame::FRAME_HEADER_LEN);
        frame::encode(frame.ty, &frame.body, &mut out)?;
        self.stream.write_all(&out).await?;
        Ok(())
    }

    /// 直接写已编码字节(批量合并路径:调用方拼好再写)。
    pub async fn write_raw(&mut self, bytes: &[u8]) -> Result<()> {
        self.stream.write_all(bytes).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::FRAME_SEND;

    /// 本地回环测试:两个 UdsFramed 互通一帧。
    #[tokio::test]
    async fn framed_roundtrip() {
        // 注意:macOS 的 SUN_LEN 限制 104 字节,temp_dir 路径过长,必须用短路径。
        let path = std::env::temp_dir().join(format!("soup{}-a.sock", std::process::id() % 100000));
        let _ = std::fs::remove_file(&path);

        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (rh, wh) = tokio::io::split(stream);
            let mut srv_reader = UdsReader::new(rh);
            let mut srv_writer = UdsWriter::new(wh);
            let f = srv_reader.read_frame().await.unwrap().unwrap();
            // 原样回写
            srv_writer.write_frame(&f).await.unwrap();
            f
        });

        let mut conn = UdsConnection::connect(&path).await.unwrap();
        let frame = Frame::new(FRAME_SEND, vec![1, 2, 3, 4]);
        conn.writer.write_frame(&frame).await.unwrap();
        let back = conn.reader.read_frame().await.unwrap().unwrap();
        assert_eq!(back, frame);

        let echoed = server_task.await.unwrap();
        assert_eq!(echoed, frame);

        let _ = std::fs::remove_file(&path);
    }

    /// 大帧(10KB)往返:验证长度前缀处理大 body。
    #[tokio::test]
    async fn large_frame_roundtrip() {
        let path = std::env::temp_dir().join(format!("soup{}-b.sock", std::process::id() % 100000));
        let _ = std::fs::remove_file(&path);

        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (rh, wh) = tokio::io::split(stream);
            let mut srv_reader = UdsReader::new(rh);
            let mut srv_writer = UdsWriter::new(wh);
            let f = srv_reader.read_frame().await.unwrap().unwrap();
            srv_writer.write_frame(&f).await.unwrap();
            f
        });

        let mut conn = UdsConnection::connect(&path).await.unwrap();
        let big = vec![0xABu8; 10 * 1024];
        let frame = Frame::new(FRAME_SEND, big);
        conn.writer.write_frame(&frame).await.unwrap();
        let back = conn.reader.read_frame().await.unwrap().unwrap();
        assert_eq!(back, frame);

        let echoed = server_task.await.unwrap();
        assert_eq!(echoed.body.len(), 10 * 1024);

        let _ = std::fs::remove_file(&path);
    }
}
