use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::policies::retry::DowngradingConsistencyRetryPolicy;
use scylla::statement::Consistency;

use crate::ccm::cluster::{Cluster, ClusterOptions};
use crate::ccm::{run_ccm_test, CLUSTER_VERSION};
use crate::common::utils::setup_tracing;

fn cluster_config() -> ClusterOptions {
    ClusterOptions {
        name: "ccm_retry_policies".to_string(),
        version: CLUSTER_VERSION.clone(),
        nodes: vec![3],
        ..ClusterOptions::default()
    }
}

async fn cql_read_cl_all(session: &Session) {
    let cql = "SELECT range_end FROM system_distributed_everywhere.cdc_generation_descriptions_v2";
    for is_idempotent in [false, true] {
        let mut prep_stmt = session.prepare(cql).await.unwrap();
        prep_stmt.set_is_idempotent(is_idempotent);
        prep_stmt.set_retry_policy(Some(Arc::new(DowngradingConsistencyRetryPolicy::new())));
        prep_stmt.set_consistency(Consistency::All);
        session
            .execute_unpaged(&prep_stmt, &[])
            .await
            .expect("failed to execute CL=ALL read query");
    }
}

async fn get_alive_nodes_number(session: &Session) -> usize {
    let cluster_state = session.get_cluster_state();
    let alive_nodes: HashSet<_> = cluster_state
        .get_nodes_info()
        .iter()
        .filter(|node| node.is_connected())
        .map(|node| node.address)
        .collect();
    alive_nodes.len()
}

#[tokio::test]
#[ntest::timeout(30000)]
#[cfg_attr(not(ccm_tests), ignore)]
async fn test_downgrading_cl_dbnode_unavailable() {
    // NOTE: whole test takes 15-20 seconds
    setup_tracing();
    async fn test(cluster: Arc<Mutex<Cluster>>) {
        let handle = ExecutionProfile::builder()
            .retry_policy(Arc::new(DowngradingConsistencyRetryPolicy::new()))
            .consistency(Consistency::All)
            .build()
            .into_handle();
        let cluster = cluster.lock().await;
        let session = cluster
            .make_session_builder()
            .await
            .default_execution_profile_handle(handle)
            .build()
            .await
            .unwrap();

        cql_read_cl_all(&session).await;
        let target_node = cluster.nodes().iter().next();

        let alive_nodes_num = get_alive_nodes_number(&session).await;
        let all_nodes_num: usize = cluster_config().nodes.iter().map(|&n| n as usize).sum();
        assert_eq!(all_nodes_num, alive_nodes_num);

        println!("Going to stop first node");
        target_node
            .expect("failed to get DB node")
            .write()
            .await
            .stop(None)
            .await
            .unwrap();

        // NOTE: make sure we have "ALL-1" active DB nodes
        let alive_nodes_num_after_stop = get_alive_nodes_number(&session).await;
        assert_eq!(all_nodes_num, alive_nodes_num_after_stop + 1);

        // NOTE: make a CL=ALL query, it should succeed having "ALL-1" alive nodes
        cql_read_cl_all(&session).await;

        println!("Going to start first node");
        target_node
            .expect("failed to get DB node")
            .write()
            .await
            .start(None)
            .await
            .unwrap();

        // NOTE: wait while driver detects the node availability back again.
        //       During the test development the waiting loop was taking ~1.2s
        let loop_start_time = Instant::now();
        loop {
            let alive_nodes_num_after_start = get_alive_nodes_number(&session).await;
            if alive_nodes_num_after_start == all_nodes_num {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        println!(
            "Waiting for the node availability took {:#?}",
            Instant::now() - loop_start_time
        );
    }
    run_ccm_test(cluster_config, test).await;
}
