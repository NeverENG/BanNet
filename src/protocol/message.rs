//! 协议消息体。
//!
//! `Message` 只关注 `msgID` 和消息数据,长度由 data 自动计算。

#[derive(Debug, Clone)]
pub struct Message {
    id: u32,
    data: Vec<u8>,
}

impl Message {
    pub fn new(id: u32, data: Vec<u8>) -> Self {
        Self { id, data }
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn len(&self) -> u32 {
        self.data.len() as u32
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }
}
