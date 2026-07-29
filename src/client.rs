//! 通用 TLV 客户端。
//!
//! 这个模块封装了连接、发送、接收和解包逻辑，方便在 `src` 里直接调用。

use std::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::protocol::{DataPack, Message};

pub struct Client {
    stream: TcpStream,
    packer: DataPack,
    pending: Vec<u8>,
}

impl Client {
    pub async fn connect(addr: impl AsRef<str>) -> Result<Self, Box<dyn Error>> {
        let stream = TcpStream::connect(addr.as_ref()).await?;
        Ok(Self {
            stream,
            packer: DataPack::new(),
            pending: Vec::new(),
        })
    }

    pub async fn send(
        &mut self,
        msg_id: u32,
        data: impl Into<Vec<u8>>,
    ) -> Result<(), Box<dyn Error>> {
        let message = Message::new(msg_id, data.into());
        let packet = self.packer.pack(&message);
        self.stream.write_all(&packet).await?;
        Ok(())
    }

    /// 接收一个完整消息。
    ///
    /// 如果当前缓存中已有完整包，就直接返回；否则继续从 socket 读数据，直到完成一个包。
    pub async fn recv_one(&mut self) -> Result<Option<Message>, Box<dyn Error>> {
        let mut buf = [0u8; 1024];

        loop {
            if let Some((message, consumed)) = self.packer.unpack(&self.pending)? {
                self.pending.drain(..consumed);
                return Ok(Some(message));
            }

            let n = self.stream.read(&mut buf).await?;
            if n == 0 {
                return Ok(None);
            }

            self.pending.extend_from_slice(&buf[..n]);
        }
    }

    /// 接收所有当前可用消息，不阻塞等待新消息。
    pub fn recv_available(&mut self) -> Result<Vec<Message>, Box<dyn Error>> {
        let mut items = Vec::new();
        while let Some((message, consumed)) = self.packer.unpack(&self.pending)? {
            self.pending.drain(..consumed);
            items.push(message);
        }
        Ok(items)
    }

    pub async fn request(
        &mut self,
        msg_id: u32,
        data: impl Into<Vec<u8>>,
    ) -> Result<Option<Message>, Box<dyn Error>> {
        self.send(msg_id, data).await?;
        self.recv_one().await
    }
}
