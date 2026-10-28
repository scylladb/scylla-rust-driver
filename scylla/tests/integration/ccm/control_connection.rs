use std::net::IpAddr;
use std::num::NonZeroUsize;

use scylla::client::PoolSize;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla_ccm_bridge::cluster::{Cluster, ClusterOptions};
use scylla_ccm_bridge::{CLUSTER_VERSION, run_ccm_test};
use tracing::{info, warn};

use crate::utils::{session_client_options, setup_tracing, system_clients_has_client_options};

/// dc1: node1; dc2: node2, node3.
fn cluster_2_dcs() -> ClusterOptions {
    ClusterOptions {
        name: "cluster_2_dcs".to_string(),
        version: CLUSTER_VERSION.clone(),
        nodes_per_dc: vec![1, 2],
        ..ClusterOptions::default()
    }
}

/// Whether `session`'s control connection is open to the node at `address`.
async fn hosts_control_connection(session: &Session, address: IpAddr) -> bool {
    let cluster_state = session.get_cluster_state();
    let node = cluster_state
        .get_nodes_info()
        .iter()
        .find(|node| node.address.ip() == address)
        .unwrap();
    // Only the control connection sends DRIVER_CONFIG.
    session_client_options(session, node)
        .await
        .iter()
        .any(|options| options.contains_key("DRIVER_CONFIG"))
}

/// A control connection that has to be re-established goes to the preferred datacenter.
///
/// This needs a real multi-DC cluster: the DC of every node is read from the cluster's metadata.
#[tokio::test]
async fn control_connection_is_reestablished_in_preferred_datacenter() {
    setup_tracing();

    async fn test(cluster: &mut Cluster) {
        let address = |id| {
            cluster
                .nodes()
                .get_by_id(id)
                .unwrap()
                .broadcast_rpc_address()
        };
        let (dc1_node, dc2_node) = (address(1), address(2));

        // The only contact point is in dc2, so the first control connection goes there.
        let session = SessionBuilder::new()
            .known_node(cluster.nodes().get_by_id(2).unwrap().contact_endpoint())
            .prefer_datacenter("dc1".to_owned())
            .fetch_schema_metadata(false)
            .pool_size(PoolSize::PerHost(NonZeroUsize::new(1).unwrap()))
            .build()
            .await
            .unwrap();

        if !system_clients_has_client_options(&session).await {
            warn!(
                "skipping: this cluster's system.clients has no client_options column, \
                 so the control connection cannot be told apart from other connections"
            );
            return;
        }
        assert!(hosts_control_connection(&session, dc2_node).await);

        info!("Stopping node2 to break the control connection");
        cluster
            .nodes_mut()
            .get_mut_by_id(2)
            .unwrap()
            .stop(None)
            .await
            .unwrap();
        // Answered only after a new control connection has fetched metadata.
        session.refresh_metadata().await.unwrap();

        // Without the preference, node3 (dc2) would be just as likely.
        assert!(
            hosts_control_connection(&session, dc1_node).await,
            "the control connection was not re-established in the preferred datacenter"
        );
    }

    run_ccm_test(cluster_2_dcs, test).await;
}
