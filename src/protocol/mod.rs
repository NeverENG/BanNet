//! # 协议层
//!
//! 定义"消息长什么样"以及"字节流怎么编解码"。这一层不碰网络 IO,
//! 只关心数据结构和 TLV 编码规则,是最纯粹、最好测试的一层。
//!
//! - [`message`]  消息本体 `{ id, data }`
//! - [`datapack`] TLV 封包/拆包

mod message;
mod datapack;

// ── 本层对外暴露的 API ──
// TODO(阶段 1):类型写好后打开,把它们提到本层门口。
// pub use message::Message;
// pub use datapack::DataPack;
