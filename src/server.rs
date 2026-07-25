//! 服务器 —— 框架的门面(用户第一个接触的类型)。
//!
//! 职责:bind 端口 -> 循环 accept -> 每来一个连接就造一个 Connection 并 start。
//! 同时持有路由表和(阶段 4)连接管理器。
//!
//! 阶段 0 目标(最小可跑):
//!   - Server::new(addr)
//!   - run().await   bind + accept 循环,先做裸字节 echo
//! 阶段 3 目标:
//!   - add_router(id, router)   注册业务处理器
//!
//! TODO(阶段 0 起步)。

use std::net::TcpListener;
use crate::transport::Connection;

pub struct Server {
  listener: TcpListener,
  addr: String,
  max_conns: usize,
  workers: usize,
}

impl Server{
  pub fn new(addr: String, max_conns: usize, workers: usize) -> Server{
    Server{
      listener: TcpListener::bind(&addr).unwrap(),
      addr,
      max_conns,
      workers,
    }
  }
  pub fn start(&self){
    // todo 这里启动和管理所有的连接，负责链接客户端，并包装成Connection对
    for stream in self.listener.incoming(){
        match stream{
            Ok(stream) => {
                let peer = stream.peer_addr().unwrap();
                let conn = Connection::new(stream, peer);
                conn.start();
            }
            Err(e) => {
                eprintln!("「BanNet」accept error: {}", e);
            }
        }
    }
  }
  pub fn stop(&self){
    // todo 这里停止所有的连接，负责关闭所有的连接，
  }
}
