//! 请求上下文 —— 交给用户 handler 的那个东西。
//!
//! 一个 `Request` = 「这次收到的数据」 + 「怎么把回包送出去」。
//! 回包不是直接写 socket,而是 send 进 mpsc:写半边由独立 task 串行消费,
//! 这样 handler 里永远不用碰 `WriteHalf`,也不会有两个 task 抢着写同一条流。
//!
//! 阶段 1 接上 TLV 之后,`data` 会换成 `protocol::Message`,`id` 就是 msgID。
//! 现在(阶段 0)`data` 是裸字节,`id` 先固定填 1。

use tokio::sync::mpsc;

use crate::error::{Error, Result};

/// 一次请求的上下文。
pub struct Request {
    /// 本次读到的数据(阶段 0 是裸字节)。
    data: Vec<u8>,
    /// 消息 ID(阶段 1 起由 TLV 头解析得到)。
    id: u32,
    /// 通往「写半边 task」的发送端。
    tx: mpsc::Sender<Vec<u8>>,
}

impl Request {
    pub(crate) fn new(data: Vec<u8>, id: u32, tx: mpsc::Sender<Vec<u8>>) -> Self {
        Request { data, id, tx }
    }

    /// 本次收到的数据。
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// 消息 ID。
    pub fn id(&self) -> u32 {
        self.id
    }

    /// 回包。数据交给写半边 task,本函数不直接碰 socket。
    ///
    /// 接受任何能变成 `Vec<u8>` 的东西,所以 `req.reply(b"pong")`、
    /// `req.reply("pong")`、`req.reply(vec)` 都能直接写。
    pub async fn reply(&self, data: impl Into<Vec<u8>>) -> Result<()> {
        self.tx
            .send(data.into())
            .await
            .map_err(|e| Error::Send(e.to_string()))
    }
}
