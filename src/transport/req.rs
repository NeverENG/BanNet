//! 请求上下文 —— 交给用户 handler 的那个东西。
//!
//! 一个 `Request` = 「这次收到的消息」 + 「怎么把回包送出去」。
//! 回包不是直接写 socket,而是 send 进 mpsc:写半边由独立 task 串行消费,
//! 这样 handler 里永远不用碰 `WriteHalf`,也不会有两个 task 抢着写同一条流。

use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::protocol::{DataPack, Message};

/// 一次请求的上下文。
pub struct Request {
    /// 本次收到的协议消息。
    message: Message,
    /// 通往「写半边 task」的发送端。
    tx: mpsc::Sender<Vec<u8>>,
}

impl Request {
    pub(crate) fn new(message: Message, tx: mpsc::Sender<Vec<u8>>) -> Self {
        Request { message, tx }
    }

    /// 本次收到的数据。
    pub fn data(&self) -> &[u8] {
        self.message.data()
    }

    /// 消息 ID。
    pub fn id(&self) -> u32 {
        self.message.id()
    }

    /// 回包。数据会被打包成 TLV 并发送给写半边。
    ///
    /// 接受任何能变成 `Vec<u8>` 的东西,所以 `req.reply(b"pong")`、
    /// `req.reply("pong")`、`req.reply(vec)` 都能直接写。
    pub async fn reply(&self, data: impl Into<Vec<u8>>) -> Result<()> {
        let response = Message::new(self.id(), data.into());
        let packet = DataPack::new().pack(&response);
        self.tx
            .send(packet)
            .await
            .map_err(|e| Error::Send(e.to_string()))
    }
}
