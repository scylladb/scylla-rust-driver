//! Command line-configurable transparent proxy.
//!
//! Useful for testing e.g. address translation or DNS related functionalities,
//! such as alterations of hostname mapping combined with IP changes.
//!
//! How to use:
//! cargo run --example cmdline -- [node_addr] [proxy_addr]
//! This starts a proxy that forwards all traffic between a driver and [proxy_addr] to [node_addr].

use std::{
    net::{Ipv4Addr, SocketAddr},
    str::FromStr,
};

use scylla_proxy::{Node, Proxy, ShardAwareness};
use tracing::instrument::WithSubscriber;

fn init_logger() {
    tracing_subscriber::fmt::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .without_time()
        .init();
}

async fn pause() {
    println!("Press Ctrl-C to stop the proxy...");
    tokio::signal::ctrl_c().await.unwrap();
}

#[tokio::main]
async fn main() {
    init_logger();
    let mut args = std::env::args();
    args.next(); // skip name
    let real_addr = args.next();
    let proxy_addr = args.next();
    let node_real_addr = SocketAddr::new(
        std::net::IpAddr::V4(
            Ipv4Addr::from_str(real_addr.as_deref().unwrap_or("127.0.0.2")).unwrap(),
        ),
        9042,
    );
    let node_proxy_addr = SocketAddr::new(
        std::net::IpAddr::V4(
            Ipv4Addr::from_str(proxy_addr.as_deref().unwrap_or("127.0.0.66")).unwrap(),
        ),
        9042,
    );
    let proxy = Proxy::new([Node::new(
        node_real_addr,
        node_proxy_addr,
        ShardAwareness::QueryNode,
        None,
        None,
    )]);
    let running_proxy = proxy.run().with_current_subscriber().await.unwrap();

    pause().await;
    running_proxy.finish().await.unwrap();
}
