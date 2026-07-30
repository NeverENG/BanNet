//! router_server —— 用 BanNet 写的路由服务端示例。
//!
//! 运行方式:
//!   cargo run --example echo_server
//!
//! 这个示例展示了如何把 `msgID` 映射到不同的业务逻辑。

use bannet::Server;

#[tokio::main]
async fn main() {
    let mut server = Server::new("127.0.0.1:8999")
        .await
        .expect("Failed to create server");

    server.on(1, |req: bannet::Request| async move {
        let text = String::from_utf8_lossy(req.data());
        println!("route 1 got: {}", text);
        let reply = format!("route1 echo: {}", text);
        req.reply(reply).await
    });

    server.on(2, |req: bannet::Request| async move {
        let text = String::from_utf8_lossy(req.data());
        println!("route 2 got: {}", text);
        let reply = format!("route2 ack: {}", text);
        req.reply(reply).await
    });

    println!("BanNet router server listening on 127.0.0.1:8999");
    _ = server.run().await;
}
