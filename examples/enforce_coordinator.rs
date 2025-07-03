//! This example shows:
//! - how to enforce the target the request is sent to;
//! - how to inspect the coordinator node that executed the request.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use anyhow::Result;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::cluster::Node;
use scylla::policies::load_balancing::{NodeIdentifier, SingleTargetLoadBalancingPolicy};
use scylla::statement::prepared::PreparedStatement;

/// Executes "SELECT host_id, rpc_address FROM system.local" query with `node` as the enforced target.
/// Checks whether the result matches the expected values (i.e. ones stored in peers metadata).
/// Additionally, it verifies that the coordinator node (as observed by the driver based on the metadata
/// fetched from the cluster previosly) is the same as the one we enforced.
async fn query_system_local_and_verify(
    session: &Session,
    enforced_node: &Arc<Node>,
    query_local: &PreparedStatement,
) {
    let result = session
        .execute_unpaged(query_local, ())
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();

    // "Actual" host_id and node_ip are the ones returned by the query, i.e. the ones stored in system.local.
    let (actual_host_id, actual_node_ip) = result.single_row::<(uuid::Uuid, IpAddr)>().unwrap();
    println!("queried host_id: {actual_host_id}; queried node_ip: {actual_node_ip}");

    assert_eq!(enforced_node.host_id, actual_host_id);
    assert_eq!(enforced_node.address.ip(), actual_node_ip);

    // "Coordinator" is the node that executed the request, as observed by the driver
    // based on the metadata fetched from the cluster previously.
    let coordinator_node = result.request_coordinator().node();
    assert_eq!(coordinator_node.host_id, actual_host_id);
    assert_eq!(coordinator_node.address.ip(), actual_node_ip);
}

#[tokio::main]
async fn main() -> Result<()> {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

    let state = session.get_cluster_state();
    let node = state
        .get_nodes_info()
        .first()
        .ok_or_else(|| anyhow::anyhow!("No nodes in metadata!"))?;

    let expected_host_id = node.host_id;
    let expected_node_ip = node.address.ip();

    let mut query_local = session
        .prepare("SELECT host_id, rpc_address FROM system.local where key='local'")
        .await?;

    // Enforce the node using `Arc<Node>`.
    {
        let node_identifier = NodeIdentifier::Node(Arc::clone(node));
        println!("Enforcing target using {node_identifier:?}...");
        query_local.set_load_balancing_policy(Some(SingleTargetLoadBalancingPolicy::new(
            node_identifier,
            None,
        )));

        query_system_local_and_verify(&session, node, &query_local).await;
    }

    // Enforce the node using host_id.
    {
        let node_identifier = NodeIdentifier::HostId(expected_host_id);
        println!("Enforcing target using {node_identifier:?}...");
        query_local.set_load_balancing_policy(Some(SingleTargetLoadBalancingPolicy::new(
            node_identifier,
            None,
        )));

        query_system_local_and_verify(&session, node, &query_local).await;
    }

    // Enforce the node using **untranslated** node address.
    {
        let node_identifier =
            NodeIdentifier::NodeAddress(SocketAddr::new(expected_node_ip, node.address.port()));
        println!("Enforcing target using {node_identifier:?}...");
        query_local.set_load_balancing_policy(Some(SingleTargetLoadBalancingPolicy::new(
            node_identifier,
            None,
        )));

        query_system_local_and_verify(&session, node, &query_local).await;
    }

    Ok(())
}
