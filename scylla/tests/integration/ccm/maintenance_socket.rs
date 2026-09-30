//! CCM test of maintenance mode over ScyllaDB's own maintenance socket.
//!
//! ScyllaDB opens a Unix domain socket for maintenance work (the
//! `maintenance_socket` option of `scylla.yaml`), and talking to a single node
//! through it is what maintenance mode exists for. This has to be a CCM test:
//! a CCM cluster runs its nodes as local processes, so that socket lands on the
//! test's own filesystem, whereas in the dockerized cluster the other
//! integration tests use it stays inside the container, unreachable.

use std::path::PathBuf;

use scylla::client::maintenance_mode::MaintenanceModeEndpoint;
use scylla::client::session_builder::MaintenanceSessionBuilder;
use scylla_ccm_bridge::cluster::{Cluster, ClusterOptions};
use scylla_ccm_bridge::{CLUSTER_VERSION, run_ccm_test_with_configuration};

use crate::utils::setup_tracing;

fn cluster_1_node() -> ClusterOptions {
    ClusterOptions {
        name: "maintenance_socket".to_string(),
        version: CLUSTER_VERSION.clone(),
        nodes_per_dc: vec![1],
        ..ClusterOptions::default()
    }
}

/// Where the node is told to open its maintenance socket.
fn maintenance_socket_path(cluster: &Cluster) -> PathBuf {
    let unique = cluster
        .cluster_dir()
        .parent()
        .and_then(|config_dir| config_dir.file_name())
        .expect("the cluster directory is `<config dir>/<cluster name>`")
        .to_string_lossy()
        .into_owned();

    std::env::temp_dir().join(format!("{unique}.sock"))
}

/// Asserts that a maintenance mode session can talk to a node over ScyllaDB's
/// maintenance socket.
#[tokio::test]
async fn test_maintenance_mode_over_maintenance_socket() {
    setup_tracing();

    // Runs after the cluster is initialised but before it is started, which is
    // when `scylla.yaml` can still be changed.
    async fn configure(mut cluster: Cluster) -> Cluster {
        let socket_path = maintenance_socket_path(&cluster);
        cluster
            .updateconf([(
                "maintenance_socket",
                socket_path
                    .to_str()
                    .expect("the node directory path is valid UTF-8"),
            )])
            .await
            .expect("failed to configure the maintenance socket");

        cluster
    }

    async fn test(cluster: &mut Cluster) {
        let socket_path = maintenance_socket_path(cluster);

        let session =
            MaintenanceSessionBuilder::new(MaintenanceModeEndpoint::unix_socket(&socket_path))
                .build()
                .await
                .unwrap();

        // One node, whose address is the documented placeholder: a Unix socket has
        // no address of its own.
        let cluster_state = session.get_cluster_state();
        let nodes = cluster_state.get_nodes_info();
        assert_eq!(nodes.len(), 1, "expected exactly one node, got {nodes:?}");
        assert!(
            nodes[0].address.ip().is_unspecified() && nodes[0].address.port() == 0,
            "expected the Unix socket placeholder address, got {}",
            nodes[0].address
        );

        // Requests are served over the socket.
        let (key,): (String,) = session
            .query_unpaged("SELECT key FROM system.local WHERE key = 'local'", &())
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .single_row()
            .unwrap();
        assert_eq!(key, "local");

        // `ccm remove` deletes the node directory, not a socket outside it.
        drop(session);
        let _ = std::fs::remove_file(&socket_path);
    }

    run_ccm_test_with_configuration(cluster_1_node, configure, test).await;
}
