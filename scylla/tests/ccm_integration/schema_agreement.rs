use std::sync::Arc;

use crate::ccm::cluster::{Cluster, ClusterOptions};
use crate::ccm::{run_ccm_test, CLUSTER_VERSION};
use crate::common::utils::{setup_tracing, unique_keyspace_name};

use scylla::client::execution_profile::ExecutionProfile;
use scylla::cluster::{ClusterState, Node, NodeRef};
use scylla::policies::load_balancing::{FallbackPlan, LoadBalancingPolicy, RoutingInfo};
use scylla::query::Query;
use tokio::sync::Mutex;

fn cluster_3_nodes() -> ClusterOptions {
    ClusterOptions {
        name: "schema_agreement_test".to_string(),
        version: CLUSTER_VERSION.clone(),
        nodes: vec![3],
        ..ClusterOptions::default()
    }
}

#[derive(Debug)]
struct SingleTargetLBP {
    target: (Arc<Node>, Option<u32>),
}

impl LoadBalancingPolicy for SingleTargetLBP {
    fn pick<'a>(
        &'a self,
        _query: &'a RoutingInfo,
        _cluster: &'a ClusterState,
    ) -> Option<(NodeRef<'a>, Option<u32>)> {
        Some((&self.target.0, self.target.1))
    }

    fn fallback<'a>(
        &'a self,
        _query: &'a RoutingInfo,
        _cluster: &'a ClusterState,
    ) -> FallbackPlan<'a> {
        Box::new(std::iter::empty())
    }

    fn name(&self) -> String {
        "SingleTargetLBP".to_owned()
    }
}

#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn test_schema_agreement() {
    setup_tracing();
    run_ccm_test(cluster_3_nodes, test_schema_agreement_all_nodes).await;
    run_ccm_test(cluster_3_nodes, test_schema_agreement_with_stopped_node).await;
    run_ccm_test(cluster_3_nodes, test_schema_agreement_with_paused_node).await;
}

async fn test_schema_agreement_all_nodes(cluster: Arc<Mutex<Cluster>>) {
    let cluster = cluster.lock().await;
    let session = cluster.make_session_builder().await.build().await.unwrap();

    // Create keyspace
    let keyspace = unique_keyspace_name();
    session
        .query_unpaged(
            format!(
                "CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}",
                keyspace
            ),
            &[],
        )
        .await
        .unwrap();

    // Use keyspace
    session.use_keyspace(keyspace, true).await.unwrap();

    // Create a table and check schema agreement
    let _result = session
        .query_unpaged(
            "CREATE TABLE test_schema_agreement_all (k int primary key, v int)",
            &[],
        )
        .await
        .unwrap();

    // Check if schema is in agreement
    let schema_agreement = session.check_schema_agreement().await.unwrap();
    assert!(schema_agreement.is_some());
}

async fn test_schema_agreement_with_stopped_node(cluster: Arc<Mutex<Cluster>>) {
    let cluster = cluster.lock().await;

    // Create keyspace
    let session = cluster.make_session_builder().await.build().await.unwrap();

    let keyspace = unique_keyspace_name();
    session
        .query_unpaged(
            format!(
                "CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}",
                keyspace
            ),
            &[],
        )
        .await
        .unwrap();

    // Use keyspace
    session.use_keyspace(keyspace, true).await.unwrap();

    // Stop node 2
    let node = cluster.nodes().get_by_id(2).await.unwrap();
    node.write().await.stop(None).await.unwrap();

    // Create a table while one node is stopped
    let _result = session
        .query_unpaged(
            "CREATE TABLE test_schema_agreement_stopped (k int primary key, v int)",
            &[],
        )
        .await
        .unwrap();

    // Schema agreement should succeed with remaining up nodes
    let schema_agreement = session.check_schema_agreement().await.unwrap();
    assert!(schema_agreement.is_some());

    // Start the node back
    node.write().await.start(None).await.unwrap();
    let schema_agreement = session.check_schema_agreement().await.unwrap();
    assert!(schema_agreement.is_some());
}

async fn test_schema_agreement_with_paused_node(cluster: Arc<Mutex<Cluster>>) {
    let cluster = cluster.lock().await;

    let session = cluster.make_session_builder().await.build().await.unwrap();

    let keyspace = unique_keyspace_name();
    session
        .query_unpaged(
            format!(
                "CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}",
                keyspace
            ),
            &[],
        )
        .await
        .unwrap();

    session.use_keyspace(keyspace, true).await.unwrap();

    // Stop node 2
    let node_id = 2;
    let ccm_node = cluster.nodes().get_by_id(node_id).await.unwrap();
    let ccm_node_addr = ccm_node.read().await.broadcast_rpc_address().clone();
    ccm_node.write().await.pause().await.unwrap();

    // Find the corresponding Scylla node from the session to avoid querying it directly
    let cluster_state = session.get_cluster_state();
    let scylla_node = cluster_state
        .get_nodes_info()
        .iter()
        .find(|n| n.address.ip() != ccm_node_addr)
        .expect("Could not find unpaused Scylla node for querying");

    let policy = SingleTargetLBP {
        target: (scylla_node.clone(), Some(0)),
    };
    let execution_profile = ExecutionProfile::builder()
        .load_balancing_policy(Arc::new(policy))
        .build();
    let mut stmt =
        Query::new("CREATE TABLE test_schema_agreement_paused (k int primary key, v int)");
    stmt.set_execution_profile_handle(Some(execution_profile.into_handle()));
    // Create a table while one node is paused
    let _result = session.query_unpaged(stmt, &[]).await.unwrap();

    // Schema agreement should succeed with remaining up nodes
    let schema_agreement = session.check_schema_agreement().await.unwrap();
    assert!(schema_agreement.is_some());

    // Start the node back
    ccm_node.write().await.resume().await.unwrap();

    let schema_agreement = session.check_schema_agreement().await.unwrap();
    assert!(schema_agreement.is_some());
}
