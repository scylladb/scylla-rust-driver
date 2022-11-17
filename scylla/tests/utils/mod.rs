use futures::Future;
use itertools::Itertools;
use scylla::load_balancing::LoadBalancingPolicy;
use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::str::FromStr;
use tracing::instrument::WithSubscriber;

use scylla_proxy::{Node, Proxy, ProxyError, RunningProxy, ShardAwareness};

pub fn init_logger() {
    let _ = tracing_subscriber::fmt::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .without_time()
        .try_init();
}

#[derive(Debug)]
pub struct FixedOrderLoadBalancer;
impl LoadBalancingPolicy for FixedOrderLoadBalancer {
    fn pick<'a>(
        &'a self,
        _info: &'a scylla::load_balancing::RoutingInfo,
        cluster: &'a scylla::transport::ClusterData,
    ) -> Option<scylla::transport::NodeRef<'a>> {
        cluster
            .get_nodes_info()
            .iter()
            .sorted_by(|node1, node2| Ord::cmp(&node1.address, &node2.address))
            .next()
    }

    fn fallback<'a>(
        &'a self,
        _info: &'a scylla::load_balancing::RoutingInfo,
        cluster: &'a scylla::transport::ClusterData,
    ) -> scylla::load_balancing::FallbackPlan<'a> {
        Box::new(
            cluster
                .get_nodes_info()
                .iter()
                .sorted_by(|node1, node2| Ord::cmp(&node1.address, &node2.address)),
        )
    }

    fn name(&self) -> String {
        "FixedOrderLoadBalancer".to_string()
    }
}

pub async fn test_with_3_node_cluster<F, Fut>(
    shard_awareness: ShardAwareness,
    test: F,
) -> Result<(), ProxyError>
where
    F: FnOnce([String; 3], HashMap<SocketAddr, SocketAddr>, RunningProxy) -> Fut,
    Fut: Future<Output = RunningProxy>,
{
    init_logger();
    let real1_uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let proxy1_uri = format!("{}:9042", scylla_proxy::get_exclusive_local_address());
    let real2_uri = env::var("SCYLLA_URI2").unwrap_or_else(|_| "127.0.0.2:9042".to_string());
    let proxy2_uri = format!("{}:9042", scylla_proxy::get_exclusive_local_address());
    let real3_uri = env::var("SCYLLA_URI3").unwrap_or_else(|_| "127.0.0.3:9042".to_string());
    let proxy3_uri = format!("{}:9042", scylla_proxy::get_exclusive_local_address());

    let real1_addr = SocketAddr::from_str(real1_uri.as_str()).unwrap();
    let proxy1_addr = SocketAddr::from_str(proxy1_uri.as_str()).unwrap();
    let real2_addr = SocketAddr::from_str(real2_uri.as_str()).unwrap();
    let proxy2_addr = SocketAddr::from_str(proxy2_uri.as_str()).unwrap();
    let real3_addr = SocketAddr::from_str(real3_uri.as_str()).unwrap();
    let proxy3_addr = SocketAddr::from_str(proxy3_uri.as_str()).unwrap();

    let proxy = Proxy::new(
        [
            (proxy1_addr, real1_addr),
            (proxy2_addr, real2_addr),
            (proxy3_addr, real3_addr),
        ]
        .map(|(proxy_addr, real_addr)| {
            Node::builder()
                .real_address(real_addr)
                .proxy_address(proxy_addr)
                .shard_awareness(shard_awareness)
                .build()
        }),
    );

    let translation_map = proxy.translation_map();
    let running_proxy = proxy.run().with_current_subscriber().await.unwrap();

    let running_proxy = test(
        [proxy1_uri, proxy2_uri, proxy3_uri],
        translation_map,
        running_proxy,
    )
    .await;

    running_proxy.finish().await
}
