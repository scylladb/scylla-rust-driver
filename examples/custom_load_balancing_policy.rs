use anyhow::Result;
use scylla::{
    load_balancing::{LoadBalancingPolicy, QueryInfo},
    transport::{ClusterData, Node},
    Session, SessionBuilder,
};
use std::{env, sync::Arc};

/// Example load balancing policy that prefers nodes from favorite datacenter
struct CustomLoadBalancingPolicy {
    fav_datacenter_name: String,
}

impl LoadBalancingPolicy for CustomLoadBalancingPolicy {
    fn plan<'a>(
        &self,
        _statement: &QueryInfo,
        cluster: &'a ClusterData,
    ) -> Box<dyn Iterator<Item = Arc<Node>> + Send + Sync + 'a> {
        let fav_dc_info = cluster
            .get_datacenters_info()
            .get(&self.fav_datacenter_name);

        match fav_dc_info {
            Some(info) => Box::new(info.nodes.iter().cloned()),
            // If there is no dc with provided name, fallback to other datacenters
            None => Box::new(cluster.get_nodes_info().iter().cloned()),
        }
    }

    fn name(&self) -> String {
        "CustomPolicy".to_string()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    let custom_load_balancing = CustomLoadBalancingPolicy {
        fav_datacenter_name: "PL".to_string(),
    };

    let _session: Session = SessionBuilder::new()
        .known_node(uri)
        .load_balancing(Arc::new(custom_load_balancing))
        .build()
        .await?;

    Ok(())
}
