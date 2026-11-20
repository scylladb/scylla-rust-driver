use std::net::IpAddr;
use std::num::NonZeroUsize;
use std::time::Duration;

use scylla::client::PoolSize;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla_ccm_bridge::cluster::{Cluster, ClusterOptions};
use scylla_ccm_bridge::{CLUSTER_VERSION, run_ccm_test};
use tracing::{info, warn};

use crate::utils::{
    session_client_options, setup_tracing, system_clients_has_client_options,
    wait_until_all_nodes_are_connected,
};

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

/// How long the control connection may take to move to a reachable node in the
/// preferred datacenter. Generous, because it is only reached when the driver
/// does not move it at all.
const MOVE_TIMEOUT: Duration = Duration::from_secs(30);

/// A control connection on a remote node moves to the preferred datacenter on
/// its own, once a node there is reachable.
///
/// This needs a real multi-DC cluster: the DC of every node is read from the cluster's metadata.
#[tokio::test]
async fn control_connection_moves_to_preferred_datacenter() {
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
        let dc2_contact_point = cluster.nodes().get_by_id(2).unwrap().contact_endpoint();

        // Checked while node1 is up: queries of a session that prefers dc1 don't
        // fail over to dc2.
        let probe_session = SessionBuilder::new()
            .known_node(&dc2_contact_point)
            .fetch_schema_metadata(false)
            .build()
            .await
            .unwrap();
        if !system_clients_has_client_options(&probe_session).await {
            warn!(
                "skipping: this cluster's system.clients has no client_options column, \
                 so the control connection cannot be told apart from other connections"
            );
            return;
        }
        drop(probe_session);

        info!("Stopping node1, the only node in dc1");
        cluster
            .nodes_mut()
            .get_mut_by_id(1)
            .unwrap()
            .stop(None)
            .await
            .unwrap();

        // The only contact point is in dc2, so the first control connection goes
        // there, and stays there while dc1 is down.
        let session = SessionBuilder::new()
            .known_node(&dc2_contact_point)
            .prefer_datacenter("dc1".to_owned())
            .fetch_schema_metadata(false)
            .pool_size(PoolSize::PerHost(NonZeroUsize::new(1).unwrap()))
            .build()
            .await
            .unwrap();
        assert!(hosts_control_connection(&session, dc2_node).await);

        info!("Starting node1");
        cluster
            .nodes_mut()
            .get_mut_by_id(1)
            .unwrap()
            .start(None)
            .await
            .unwrap();
        // Needed to read node1's system.clients.
        wait_until_all_nodes_are_connected(3, &session).await;

        let moved = async {
            while !hosts_control_connection(&session, dc1_node).await
                || hosts_control_connection(&session, dc2_node).await
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::time::timeout(MOVE_TIMEOUT, moved)
            .await
            .expect("the control connection did not move to the preferred datacenter");
    }

    run_ccm_test(cluster_2_dcs, test).await;
}
