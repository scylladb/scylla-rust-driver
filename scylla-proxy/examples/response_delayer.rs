use std::{io::Read as _, net::SocketAddr, str::FromStr, time::Duration};

use scylla_proxy::{
    Condition, Node, Proxy, Reaction, ResponseReaction, ResponseRule, ShardAwareness,
};

fn pause() {
    println!("Press Enter to stop proxy...");
    std::io::stdin().read_exact(&mut [0]).unwrap();
    println!();
}

#[tokio::main]
async fn main() {
    let node1_real_addr = SocketAddr::from_str("127.0.0.1:9042").unwrap();
    let node1_proxy_addr = SocketAddr::from_str("127.0.0.2:9042").unwrap();
    let proxy = Proxy::new([Node::new(
        node1_real_addr,
        node1_proxy_addr,
        ShardAwareness::QueryNode,
        None,
        Some(vec![ResponseRule(
            Condition::ConnectionSeqNo(1),
            ResponseReaction::delay(Duration::from_millis(100)),
        )]),
    )]);
    let running_proxy = proxy.run().await.unwrap();
    pause();
    running_proxy.finish().await.unwrap();
}
