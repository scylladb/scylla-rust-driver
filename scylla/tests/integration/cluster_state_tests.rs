use std::env;

use hashbrown::HashSet;

use crate::utils::{create_new_session_builder, setup_tracing};

#[tokio::test]
#[ntest::timeout(60000)]
#[cfg(not(scylla_cloud_tests))]
async fn test_session_should_have_topology_metadata() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let state = session.get_cluster_state();
    let keys = ["SCYLLA_URI", "SCYLLA_URI2", "SCYLLA_URI3"];
    let expected_addresses: HashSet<String> = keys
        .iter()
        .map(|key| env::var(key).unwrap_or_else(|_| panic!("{} not set", key)))
        .collect();

    let got_addresses: HashSet<String> = state
        .get_nodes_info()
        .iter()
        .map(|node| node.address.to_string())
        .collect();

    assert_eq!(
        got_addresses, expected_addresses,
        "Cluster node addresses do not match environment variables"
    );
}
