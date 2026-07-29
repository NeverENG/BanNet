//! 通用客户端示例。
//!
//! 直接使用 `bannet::Client` 发送 TLV 请求并接收响应。
//!
//! 运行后输入一行文本回车即可发送，输入 `quit` 或 `exit` 退出。

use bannet::Client;
use std::error::Error;
use std::io::{self, Write};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut client = Client::connect("127.0.0.1:8999").await?;
    println!("connected to server 127.0.0.1:8999");

    loop {
        print!("message> ");
        io::stdout().flush()?;

        let mut line = String::new();
        let bytes = io::stdin().read_line(&mut line)?;
        if bytes == 0 {
            println!("stdin closed, exiting");
            break;
        }

        let input = line.trim_end();
        if input.is_empty() {
            continue;
        }
        if input.eq_ignore_ascii_case("quit") || input.eq_ignore_ascii_case("exit") {
            println!("bye");
            break;
        }

        client.send(1, input.to_string()).await?;
        if let Some(resp) = client.recv_one().await? {
            println!(
                "recv id={} len={} data={:?}",
                resp.id(),
                resp.len(),
                resp.data()
            );
        } else {
            println!("recv incomplete or closed");
            break;
        }
    }

    Ok(())
}
