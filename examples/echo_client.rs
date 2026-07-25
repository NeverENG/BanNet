//! echo_client —— 测试用客户端,给 echo_server 发消息、收回包。
//!
//! 运行方式:
//!   cargo run --example echo_client
//!
//! 客户端会直接用 tokio 的 TcpStream + 我们的 TLV 协议手写(不依赖框架的
//! Server 部分),这样能独立验证协议编解码是否正确。阶段 1 后开始填充。

fn main() {
    println!("[占位] BanNet echo_client —— 阶段 1 后,这里会按 TLV 协议发/收消息。");
}
