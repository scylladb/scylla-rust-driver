use anyhow::Result;
use scylla::{
    load_balancing::{LoadBalancingPolicy, RoutingInfo},
    transport::{ClusterData, ExecutionProfile, Node},
    Session, SessionBuilder,
};
use std::{env, sync::Arc};

/// Example load balancing policy that prefers nodes from favorite datacenter
#[derive(Debug)]
struct CustomLoadBalancingPolicy {
    fav_datacenter_name: String,
}

impl LoadBalancingPolicy for CustomLoadBalancingPolicy {
    fn plan<'a>(
        &self,
        _info: &RoutingInfo,
        cluster: &'a ClusterData,
    ) -> Box<dyn Iterator<Item = Arc<Node>> + Send + Sync + 'a> {
        let fav_dc_nodes = cluster
            .replica_locator()
            .unique_nodes_in_datacenter_ring(&self.fav_datacenter_name);

        match fav_dc_nodes {
            Some(nodes) => Box::new(nodes.iter().cloned()),
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

    let profile = ExecutionProfile::builder()
        .load_balancing_policy(Arc::new(custom_load_balancing))
        .build();

    let _session: Session = SessionBuilder::new()
        .known_node(uri)
        .default_execution_profile_handle(profile.into_handle())
        .build()
        .await?;

    Ok(())
}
