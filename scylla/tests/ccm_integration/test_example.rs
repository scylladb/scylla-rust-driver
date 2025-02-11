use std::sync::Arc;

use crate::ccm::cluster::{Cluster, ClusterOptions};
use crate::ccm::{run_ccm_test, CLUSTER_VERSION};
use crate::common::utils::setup_tracing;

use tokio::sync::Mutex;

fn cluster_1_node() -> ClusterOptions {
    ClusterOptions {
        name: "cluster_1_node".to_string(),
        version: CLUSTER_VERSION.clone(),
        nodes: vec![1],
        ..ClusterOptions::default()
    }
}

#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn test_cluster_lifecycle1() {
    setup_tracing();
    async fn test(cluster: Arc<Mutex<Cluster>>) {
        let cluster = cluster.lock().await;
        let session = cluster.make_session_builder().await.build().await.unwrap();

        let rows = session
            .query_unpaged("select data_center from system.local", &[])
            .await
            .expect("failed to execute query")
            .into_rows_result()
            .expect("failed to get rows")
            .rows::<(String,)>()
            .expect("failed to deserialize rows")
            .map(|res| res.map(|row| row.0))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        println!("{:?}", rows);
    }
    run_ccm_test(cluster_1_node, test).await;
}

#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn test_cluster_lifecycle2() {
    setup_tracing();
    async fn test(cluster: Arc<Mutex<Cluster>>) {
        let cluster = cluster.lock().await;
        let session = cluster.make_session_builder().await.build().await.unwrap();

        let rows = session
            .query_unpaged("select data_center from system.local", &[])
            .await
            .expect("failed to execute query")
            .into_rows_result()
            .expect("failed to get rows")
            .rows::<(String,)>()
            .expect("failed to deserialize rows")
            .map(|res| res.map(|row| row.0))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        println!("{:?}", rows);
    }
    run_ccm_test(cluster_1_node, test).await;
}
