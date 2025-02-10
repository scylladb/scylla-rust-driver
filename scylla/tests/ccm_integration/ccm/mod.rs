pub(crate) mod cluster;
mod logged_cmd;
pub(crate) mod node_config;

use std::sync::Arc;

use anyhow::{Context, Error};
use cluster::{Cluster, ClusterOptions};
use lazy_static::lazy_static;
use tokio::sync::RwLock;

lazy_static! {
    pub static ref CLUSTER_VERSION: String =
        std::env::var("SCYLLA_TEST_CLUSTER").unwrap_or("release:6.2.2".to_string());
    static ref TEST_KEEP_CLUSTER_ON_FAILURE: bool = !std::env::var("TEST_KEEP_CLUSTER_ON_FAILURE")
        .unwrap_or("".to_string())
        .parse::<bool>()
        .unwrap_or(false);
}

pub(crate) async fn cluster_1_node() -> Arc<RwLock<Cluster>> {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "cluster_1_node".to_string(),
        version: CLUSTER_VERSION.clone(),
        nodes: vec![1],
        ..ClusterOptions::default()
    })
    .await
    .context("Failed to create cluster")
    .unwrap();

    cluster
        .init()
        .await
        .context("failed to initialize cluster")
        .unwrap();
    cluster
        .start(None)
        .await
        .context("failed to start cluster")
        .unwrap();
    Arc::new(RwLock::new(cluster))
}

pub(crate) async fn run_ccm_test<C, T, CFut, TFut>(cb: C, test_body: T)
where
    C: FnOnce() -> CFut,
    CFut: std::future::Future<Output = Arc<RwLock<Cluster>>>,
    T: FnOnce(Arc<RwLock<Cluster>>) -> TFut,
    TFut: std::future::Future<Output = Result<(), Error>>,
{
    let cluster_arc = cb().await;
    {
        let res = test_body(cluster_arc.clone()).await;
        if res.is_err() && *TEST_KEEP_CLUSTER_ON_FAILURE {
            println!("Test failed, keep cluster alive, TEST_KEEP_CLUSTER_ON_FAILURE=true");
            cluster_arc.write().await.set_keep_on_drop(true);
        }
    }
}
