//! 消息本体。
//!
//! 一个应用层消息 = 消息 ID + 数据。这是框架里流动的最小语义单位。
//! Message 拥有自己的数据(Vec<u8>),因为它要跨 task 移动、活得足够久。

/// 一条应用层消息。
///
/// 对应 TLV 协议里的 `msgID` + `data` 两部分(dataLen 由 data.len() 推导)。
#[derive(Debug, Clone)]
pub struct Message {
    /// 消息类型 ID —— 路由时用它找 handler。宽度 u32,和线上协议的 4 字节对齐。
    id: u32,
    /// 消息体。Message 持有所有权(见模块文档:要跨 task 移动)。
    data: Vec<u8>,
}

impl Message {
    /// 构造一条消息。
    ///
    /// TODO(你来):用 id 和 data 构造 Self。
    /// 提示:字段名和参数名相同时,Rust 允许 `Self { id, data }` 简写。
    pub fn new(id: u32, data: Vec<u8>) -> Self {
        todo!()
    }

    /// 消息类型 ID。
    ///
    /// TODO(你来):返回 id。u32 是 Copy 类型,直接返回值,不用借用。
    pub fn id(&self) -> u32 {
        todo!()
    }

    /// 消息体(只读借用)。
    ///
    /// TODO(你来):返回 data 的只读切片。
    /// 提示:`Vec<u8>` 能自动解引用成 `&[u8]`,想想返回 `&self.data` 行不行。
    pub fn data(&self) -> &[u8] {
        todo!()
    }

    /// 消息体字节长度 —— 封包时要把它写进 TLV 头部的 dataLen 字段。
    ///
    /// TODO(你来):返回 data 的长度。注意返回类型是 u32,
    /// 而 Vec::len() 给的是 usize,需要一次转换(想想 `as` 还是 `try_into`,
    /// 以及为什么协议头用固定 4 字节而不是 usize)。
    pub fn len(&self) -> u32 {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_round_trips() {
        let msg = Message::new(1, vec![0xAA, 0xBB, 0xCC]);
        assert_eq!(msg.id(), 1);
        assert_eq!(msg.data(), &[0xAA, 0xBB, 0xCC]);
        assert_eq!(msg.len(), 3);
    }
}
