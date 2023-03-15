use anyhow::Result;
use scylla::{
    load_balancing::{LoadBalancingPolicy, RoutingInfo},
    transport::{ClusterData, ExecutionProfile},
    Legacy08Session, SessionBuilder,
};
use std::{env, sync::Arc};

/// Example load balancing policy that prefers nodes from favorite datacenter
#[derive(Debug)]
struct CustomLoadBalancingPolicy {
    fav_datacenter_name: String,
}

impl LoadBalancingPolicy for CustomLoadBalancingPolicy {
    fn pick<'a>(
        &'a self,
        _info: &'a RoutingInfo,
        cluster: &'a ClusterData,
    ) -> Option<scylla::transport::NodeRef<'a>> {
        self.fallback(_info, cluster).next()
    }

    fn fallback<'a>(
        &'a self,
        _info: &'a RoutingInfo,
        cluster: &'a ClusterData,
    ) -> scylla::load_balancing::FallbackPlan<'a> {
        let fav_dc_nodes = cluster
            .replica_locator()
            .unique_nodes_in_datacenter_ring(&self.fav_datacenter_name);

        match fav_dc_nodes {
            Some(nodes) => Box::new(nodes.iter()),
            // If there is no dc with provided name, fallback to other datacenters
            None => Box::new(cluster.get_nodes_info().iter()),
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

    let _session: Legacy08Session = SessionBuilder::new()
        .known_node(uri)
        .default_execution_profile_handle(profile.into_handle())
        .build()
        .await?;

    Ok(())
}
