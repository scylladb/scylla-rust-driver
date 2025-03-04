use std::sync::Arc;
use std::time::Duration;

use crate::ccm::cluster::{Cluster, ClusterOptions};
use crate::ccm::{run_ccm_test, CLUSTER_VERSION};
use crate::common::utils::{setup_tracing, unique_keyspace_name};

use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::cluster::{ClusterState, Node, NodeRef};
use scylla::policies::load_balancing::{FallbackPlan, LoadBalancingPolicy, RoutingInfo};
use scylla::query::Query;
use tokio::sync::Mutex;
use tracing::info;
use uuid::Uuid;

/// Creates a cluster configuration with 3 nodes for schema agreement tests.
fn cluster_3_nodes() -> ClusterOptions {
    ClusterOptions {
        name: "schema_agreement_test".to_string(),
        version: CLUSTER_VERSION.clone(),
        nodes: vec![3],
        ..ClusterOptions::default()
    }
}

/// A load balancing policy that targets a single node.
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

/// Waits for schema agreement with a timeout and retries.
async fn wait_for_schema_agreement(
    session: &Session,
    timeout: Duration,
    retries: u32,
) -> Result<Option<Uuid>, anyhow::Error> {
    let retry_interval = Duration::from_millis(500);
    let mut attempts = 0;

    tokio::time::timeout(timeout, async {
        loop {
            match session.check_schema_agreement().await {
                Ok(Some(agreement)) => return Ok(Some(agreement)),
                Ok(None) => {
                    attempts += 1;
                    if attempts > retries {
                        return Err(anyhow::anyhow!(
                            "Schema agreement not reached after {} retries",
                            retries
                        ));
                    }
                    info!(
                        "Schema agreement not yet reached, retrying ({}/{})",
                        attempts, retries
                    );
                    tokio::time::sleep(retry_interval).await;
                }
                Err(e) => return Err(anyhow::anyhow!("Failed to check schema agreement: {}", e)),
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("Schema agreement timed out after {:?}", timeout))?
}

/// Sets up a keyspace with a given replication factor.
async fn setup_keyspace(
    session: &Session,
    keyspace: &str,
    replication_factor: u32,
) -> Result<(), anyhow::Error> {
    let query = format!(
        "CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : {}}}",
        keyspace, replication_factor
    );
    session.query_unpaged(query, &[]).await?;
    session.use_keyspace(keyspace, true).await?;
    Ok(())
}

#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn test_schema_agreement() {
    setup_tracing();
    run_ccm_test(cluster_3_nodes, test_schema_agreement_all_nodes).await;
    run_ccm_test(cluster_3_nodes, test_schema_agreement_with_stopped_node).await;
    run_ccm_test(cluster_3_nodes, test_schema_agreement_with_paused_node).await;
    // TODO - multidc cases
}

/// Tests schema agreement with all nodes running.
async fn test_schema_agreement_all_nodes(cluster: Arc<Mutex<Cluster>>) {
    let cluster = cluster.lock().await;
    let session = cluster
        .make_session_builder()
        .await
        .build()
        .await
        .expect("Failed to create session");

    let keyspace = unique_keyspace_name();
    setup_keyspace(&session, &keyspace, 3)
        .await
        .expect("Failed to setup keyspace");

    info!("Creating table in test_schema_agreement_all_nodes");
    session
        .query_unpaged("CREATE TABLE test_table (k int primary key, v int)", &[])
        .await
        .expect("Failed to create table");

    let agreement = wait_for_schema_agreement(&session, Duration::from_secs(10), 20)
        .await
        .expect("Schema agreement failed");
    assert!(agreement.is_some(), "Schema agreement should be reached");
    info!("Schema agreement achieved with all nodes");
}

/// Tests schema agreement with one node stopped.
async fn test_schema_agreement_with_stopped_node(cluster: Arc<Mutex<Cluster>>) {
    let cluster = cluster.lock().await;
    let session = cluster
        .make_session_builder()
        .await
        .build()
        .await
        .expect("Failed to create session");

    let keyspace = unique_keyspace_name();
    setup_keyspace(&session, &keyspace, 3)
        .await
        .expect("Failed to setup keyspace");

    let node = cluster
        .nodes()
        .get_by_id(2)
        .await
        .expect("Failed to get node 2");
    info!("Stopping node 2");
    node.write()
        .await
        .stop(None)
        .await
        .expect("Failed to stop node");

    info!("Creating table with one node stopped");
    session
        .query_unpaged("CREATE TABLE test_table (k int primary key, v int)", &[])
        .await
        .expect("Failed to create table");

    let agreement = wait_for_schema_agreement(&session, Duration::from_secs(10), 20)
        .await
        .expect("Schema agreement failed with stopped node");
    assert!(
        agreement.is_some(),
        "Schema agreement should be reached with remaining nodes"
    );

    info!("Restarting node 2");
    node.write()
        .await
        .start(None)
        .await
        .expect("Failed to restart node");
    let agreement = wait_for_schema_agreement(&session, Duration::from_secs(10), 20)
        .await
        .expect("Schema agreement failed after restart");
    assert!(
        agreement.is_some(),
        "Schema agreement should be reached after node restart"
    );
    info!("Schema agreement achieved after node restart");
}

/// Tests schema agreement with one node paused.
async fn test_schema_agreement_with_paused_node(cluster: Arc<Mutex<Cluster>>) {
    let cluster = cluster.lock().await;
    let session = cluster
        .make_session_builder()
        .await
        .build()
        .await
        .expect("Failed to create session");

    let keyspace = unique_keyspace_name();
    setup_keyspace(&session, &keyspace, 3)
        .await
        .expect("Failed to setup keyspace");

    let node_id = 2;
    let ccm_node = cluster
        .nodes()
        .get_by_id(node_id)
        .await
        .expect("Failed to get node 2");
    let ccm_node_addr = ccm_node.read().await.broadcast_rpc_address().clone();
    info!("Pausing node 2");
    ccm_node
        .write()
        .await
        .pause()
        .await
        .expect("Failed to pause node");

    let cluster_state = session.get_cluster_state();
    let running_scylla_node = cluster_state
        .get_nodes_info()
        .iter()
        .find(|n| n.address.ip() != ccm_node_addr)
        .expect("Could not find unpaused Scylla node");

    let policy = SingleTargetLBP {
        target: (running_scylla_node.clone(), Some(0)),
    };
    let execution_profile = ExecutionProfile::builder()
        .load_balancing_policy(Arc::new(policy))
        .build();
    let mut stmt = Query::new("CREATE TABLE test_table (k int primary key, v int)");
    stmt.set_execution_profile_handle(Some(execution_profile.into_handle()));

    info!("Creating table with one node paused");
    session
        .query_unpaged(stmt, &[])
        .await
        .expect("Failed to create table");

    let agreement = wait_for_schema_agreement(&session, Duration::from_secs(10), 20)
        .await
        .expect("Schema agreement failed with paused node");
    assert!(
        agreement.is_some(),
        "Schema agreement should be reached with remaining nodes"
    );

    info!("Resuming node 2");
    ccm_node
        .write()
        .await
        .resume()
        .await
        .expect("Failed to resume node");

    let agreement = wait_for_schema_agreement(&session, Duration::from_secs(10), 20)
        .await
        .expect("Schema agreement failed after resume");
    assert!(
        agreement.is_some(),
        "Schema agreement should be reached after node resume"
    );
    info!("Schema agreement achieved after node resume");
}
