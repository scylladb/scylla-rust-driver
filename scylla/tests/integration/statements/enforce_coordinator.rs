use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;

use assert_matches::assert_matches;
use futures::StreamExt;
use scylla::client::session::Session;
use scylla::errors::ExecutionError;

use scylla::policies::load_balancing::{NodeIdentifier, SingleTargetLoadBalancingPolicy};
use scylla::statement::Statement;
use uuid::Uuid;

use crate::utils::{create_new_session_builder, setup_tracing};

#[tokio::test]
async fn test_enforce_request_coordinator() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    async fn query_system_local_and_verify(
        session: &Session,
        node_identifier: NodeIdentifier,
        expected_node_id: Uuid,
        expected_node_ip: IpAddr,
    ) {
        let mut statement =
            Statement::new("SELECT host_id, rpc_address FROM system.local WHERE key='local'");
        statement.set_load_balancing_policy(Some(SingleTargetLoadBalancingPolicy::new(
            node_identifier,
            None,
        )));

        // Check query_unpaged
        let (actual_node_id, actual_node_ip) = session
            .query_unpaged(statement.clone(), ())
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .single_row::<(uuid::Uuid, IpAddr)>()
            .unwrap();
        assert_eq!(expected_node_id, actual_node_id);
        assert_eq!(expected_node_ip, actual_node_ip);

        // Check query_iter
        let (actual_node_id, actual_node_ip) = session
            .query_iter(statement, ())
            .await
            .unwrap()
            .rows_stream::<(uuid::Uuid, IpAddr)>()
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expected_node_id, actual_node_id);
        assert_eq!(expected_node_ip, actual_node_ip);
    }

    let cluster_state = session.get_cluster_state();
    for node in cluster_state.get_nodes_info() {
        let (node_id, node_address) = (
            node.host_id,
            SocketAddr::new(node.address.ip(), node.address.port()),
        );

        query_system_local_and_verify(
            &session,
            NodeIdentifier::Node(Arc::clone(node)),
            node_id,
            node_address.ip(),
        )
        .await;

        query_system_local_and_verify(
            &session,
            NodeIdentifier::HostId(node_id),
            node_id,
            node_address.ip(),
        )
        .await;

        query_system_local_and_verify(
            &session,
            NodeIdentifier::NodeAddress(SocketAddr::new(node_address.ip(), node_address.port())),
            node_id,
            node_address.ip(),
        )
        .await;
    }
}

#[tokio::test]
async fn test_enforce_non_existent_request_coordinator() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let mut statement =
        Statement::new("SELECT host_id, rpc_address FROM system.local WHERE key='local'");
    statement.set_load_balancing_policy(Some(SingleTargetLoadBalancingPolicy::new(
        NodeIdentifier::NodeAddress(SocketAddr::from_str("1.1.1.1:9042").unwrap()),
        None,
    )));

    let result = session.query_unpaged(statement, ()).await;
    assert_matches!(result, Err(ExecutionError::EmptyPlan))
}
