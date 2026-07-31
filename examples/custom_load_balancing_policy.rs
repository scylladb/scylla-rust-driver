//! Shows how to plug a hand-written load balancing policy into a session.
//!
//! The policy here is datacenter-aware: it only ever returns nodes from one
//! chosen datacenter. To make that observable, the example starts its own
//! two-datacenter cluster, points the policy at the second datacenter, and
//! then checks, for every request it sends, which node actually coordinated
//! it — a policy that picks the wrong nodes (or a datacenter that does not
//! exist) makes the example fail rather than pass quietly.

use anyhow::Result;
use rand::Rng;
use rand::rng;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::cluster::ClusterState;
use scylla::cluster::NodeRef;
use scylla::policies::load_balancing::{LoadBalancingPolicy, RoutingInfo};
use scylla::routing::Shard;
use scylla_ccm_bridge::CLUSTER_VERSION;
use scylla_ccm_bridge::cluster::{Cluster, ClusterOptions};
use std::collections::HashSet;
use std::sync::Arc;

/// Example load balancing policy that prefers nodes from favorite datacenter
/// This is, of course, very naive, as it is completely non token-aware.
/// For more realistic implementation, see [`DefaultPolicy`](scylla::policies::load_balancing::DefaultPolicy).
#[derive(Debug)]
struct CustomLoadBalancingPolicy {
    fav_datacenter_name: String,
}

fn with_random_shard(node: NodeRef) -> (NodeRef, Option<Shard>) {
    let nr_shards = node
        .sharder()
        .map(|sharder| sharder.nr_shards.get())
        .unwrap_or(1);
    (node, Some(rng().random_range(0..nr_shards) as Shard))
}

impl LoadBalancingPolicy for CustomLoadBalancingPolicy {
    fn pick<'a>(
        &'a self,
        _info: &'a RoutingInfo,
        cluster: &'a ClusterState,
    ) -> Option<(NodeRef<'a>, Option<Shard>)> {
        self.fallback(_info, cluster).next()
    }

    fn fallback<'a>(
        &'a self,
        _info: &'a RoutingInfo,
        cluster: &'a ClusterState,
    ) -> scylla::policies::load_balancing::FallbackPlan<'a> {
        let fav_dc_nodes = cluster
            .replica_locator()
            .unique_nodes_in_datacenter_ring(&self.fav_datacenter_name);

        match fav_dc_nodes {
            Some(nodes) => Box::new(nodes.iter().map(with_random_shard)),
            // If there is no dc with provided name, fallback to other datacenters
            None => Box::new(cluster.get_nodes_info().iter().map(with_random_shard)),
        }
    }

    fn name(&self) -> String {
        "CustomPolicy".to_string()
    }
}

/// How many requests to send. `pick()` returns the first node of the plan, so
/// these all land on the same node; the loop makes the coordinator visible
/// rather than adding confidence. The check with real teeth is the one over
/// the policy's whole plan, below.
const REQUESTS: i32 = 8;

#[tokio::main]
async fn main() -> Result<()> {
    // --- CI setup ---------------------------------------------------------
    // Everything down to the matching banner exists only so that this example
    // can run unattended in CI, against a cluster nobody had to configure by
    // hand: it starts a throwaway two-datacenter cluster with `scylla-ccm-bridge`.
    // It is not what the example teaches. If you already have such a cluster,
    // this is the block you replace with your own contact points.
    let mut cluster = Cluster::new(ClusterOptions {
        name: "examples_custom_lbp".to_string(),
        version: CLUSTER_VERSION.clone(),
        // Two datacenters, so that preferring one of them means something.
        nodes_per_dc: vec![2, 2],
        ..Default::default()
    })
    .await?;
    cluster.init().await?;
    cluster.start(None).await?;
    let session_builder = cluster.make_session_builder().await;
    // --- end of CI setup --------------------------------------------------

    // CCM names the datacenters after their position in `nodes_per_dc`, so the
    // cluster above has `dc1` and `dc2`. Prefer the second one.
    let fav_datacenter_name = "dc2".to_string();

    let policy = Arc::new(CustomLoadBalancingPolicy {
        fav_datacenter_name: fav_datacenter_name.clone(),
    });

    let profile = ExecutionProfile::builder()
        .load_balancing_policy(Arc::clone(&policy) as Arc<dyn LoadBalancingPolicy>)
        .build();

    let session: Session = session_builder
        .default_execution_profile_handle(profile.into_handle())
        .build()
        .await?;

    // The policy above falls back to *every* node if it does not recognise the
    // datacenter name, so a typo would make the rest of this example pass for
    // the wrong reason. Compare the name against the topology the driver
    // actually discovered before trusting anything below.
    let cluster_state = session.get_cluster_state();
    let datacenters: HashSet<&str> = cluster_state
        .get_nodes_info()
        .iter()
        .filter_map(|node| node.datacenter.as_deref())
        .collect();
    println!("Datacenters reported by the cluster: {datacenters:?}");
    anyhow::ensure!(
        datacenters.contains(fav_datacenter_name.as_str()),
        "Favourite datacenter {fav_datacenter_name} does not exist; the policy would silently \
         fall back to all the other datacenters. Known datacenters: {datacenters:?}"
    );

    // `pick` only ever returns the first node of the plan, so hammering the
    // session would keep hitting that one node and would not notice a policy
    // that quietly offered the wrong datacenter as its second choice. Ask the
    // policy for its entire plan instead, and hold every node in it to the
    // favourite datacenter.
    let routing_info = RoutingInfo::default();
    let planned: Vec<_> = policy
        .fallback(&routing_info, &cluster_state)
        .map(|(node, _shard)| node)
        .collect();

    let fav_dc_nodes = cluster_state
        .get_nodes_info()
        .iter()
        .filter(|node| node.datacenter.as_deref() == Some(fav_datacenter_name.as_str()))
        .count();

    for node in &planned {
        let datacenter = node.datacenter.as_deref().unwrap_or("<unknown>");
        println!(
            "the policy is willing to use {} in datacenter {datacenter}",
            node.address
        );
        anyhow::ensure!(
            datacenter == fav_datacenter_name,
            "the policy offered {} in {datacenter}, outside the favourite datacenter \
             {fav_datacenter_name}",
            node.address
        );
    }
    anyhow::ensure!(
        planned.len() == fav_dc_nodes,
        "the policy offered {} of the {fav_dc_nodes} nodes in {fav_datacenter_name}; it should \
         be willing to use all of them",
        planned.len()
    );

    session
        .query_unpaged(
            "CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = \
             {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}",
            &[],
        )
        .await?;
    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.custom_load_balancing_policy \
             (a int primary key, b int)",
            &[],
        )
        .await?;

    // Prepared statements carry a token, so a token-aware policy would spread
    // these over the whole cluster. This one ignores the token and keeps every
    // request inside the favourite datacenter.
    let insert = session
        .prepare("INSERT INTO examples_ks.custom_load_balancing_policy (a, b) VALUES (?, ?)")
        .await?;

    for a in 0..REQUESTS {
        let result = session.execute_unpaged(&insert, (a, 2 * a)).await?;

        // The coordinator is the node that executed the request, so it tells us
        // where the load balancing policy actually sent it.
        let coordinator = result.request_coordinator().node();
        let datacenter = coordinator.datacenter.as_deref().unwrap_or("<unknown>");
        println!(
            "request {a} was coordinated by {} in datacenter {datacenter}",
            coordinator.address
        );

        anyhow::ensure!(
            datacenter == fav_datacenter_name,
            "request {a} was coordinated in {datacenter}, but the policy should have kept it \
             in {fav_datacenter_name}"
        );
    }

    println!("All {REQUESTS} requests were served by datacenter {fav_datacenter_name}.");

    Ok(())
}
