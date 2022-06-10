use anyhow::Result;
use scylla::{
    load_balancing::{LoadBalancingPolicy, RoundRobinPolicy, Statement},
    query::Query,
    transport::{ClusterData, Node},
    Session, SessionBuilder,
};
use std::{env, sync::Arc};
use tracing::info;

/// Example load balancing policy that prefers nodes from favorite datacenter
struct CustomLoadBalancingPolicy {
    fav_datacenter_name: String,
}

impl LoadBalancingPolicy for CustomLoadBalancingPolicy {
    fn plan<'a>(
        &self,
        _statement: &Statement,
        cluster: &'a ClusterData,
    ) -> Box<dyn Iterator<Item = Arc<Node>> + Send + Sync + 'a> {
        info!("Yay, picking my favorite DC again!");
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
    tracing_subscriber::fmt::init();
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    let custom_load_balancing = Arc::new(CustomLoadBalancingPolicy {
        fav_datacenter_name: "PL".to_string(),
    });

    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .load_balancing(custom_load_balancing.clone())
        .build()
        .await?;

    session
        .query("SELECT host_id FROM system.local", &[])
        .await
        .unwrap();

    let mut config = scylla::statement::StatementConfig {
        load_balancing_policy: Some(Arc::new(RoundRobinPolicy::new())),
        ..Default::default()
    };

    session
        .query(
            Query::with_config("SELECT host_id FROM system.local", config.clone()),
            &[],
        )
        .await
        .unwrap();

    config.load_balancing_policy = Some(custom_load_balancing);
    session
        .query(
            Query::with_config("SELECT host_id FROM system.local", config),
            &[],
        )
        .await
        .unwrap();

    Ok(())
}
