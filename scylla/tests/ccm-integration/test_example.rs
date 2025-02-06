use crate::cluster::{Cluster, ClusterOptions};
use anyhow::{Context, Error};
use lazy_static::lazy_static;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::RwLock;

type ClusterBuilder = dyn Fn() -> Cluster;

lazy_static! {
    static ref CLUSTER_VERSION: String =
        std::env::var("SCYLLA_TEST_CLUSTER").unwrap_or("release:6.2.2".to_string());
    static ref TEST_KEEP_CLUSTER_ON_FAILURE: bool = !std::env::var("TEST_KEEP_CLUSTER_ON_FAILURE")
        .unwrap_or("".to_string())
        .parse::<bool>()
        .unwrap();
}

async fn cluster_1_node() -> Arc<RwLock<Cluster>> {
    let mut cluster = Cluster::new(ClusterOptions {
        name: "cluster_1_node".to_string(),
        version: CLUSTER_VERSION.clone(),
        nodes: [1].to_vec(),
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

async fn run_ccm_test<CB, TB, CBFut, TBFut>(cb: CB, test_body: TB)
where
    CB: FnOnce() -> CBFut,
    CBFut: std::future::Future<Output = Arc<RwLock<Cluster>>>,
    TB: FnOnce(Arc<RwLock<Cluster>>) -> TBFut,
    TBFut: std::future::Future<Output = Result<(), Error>>,
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

async fn get_session(cluster: &Cluster) -> Session {
    SessionBuilder::new()
        .known_nodes(cluster.nodes.get_contact_endpoints().await)
        .build()
        .await
        .unwrap()
}

#[tokio::test]
async fn test_cluster_lifecycle() {
    run_ccm_test(cluster_1_node, async move |cluster| -> Result<(), Error> {
        let cluster_arc = cluster.clone();
        let cluster_lock = cluster_arc.write().await;
        let cluster = cluster_lock.deref();
        let session = get_session(cluster).await;

        let rows: Vec<String> = session
            .query_unpaged("select data_center from system.local", &[])
            .await
            .context("failed to execute query")?
            .into_rows_result()
            .context("failed to get rows")?
            .rows::<(String,)>()
            .context("failed to deserialize rows")?
            .map(|row| row.unwrap().0)
            .collect();
        println!("{:?}", rows);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn test_cluster_lifecycle2() {
    run_ccm_test(cluster_1_node, async move |cluster| -> Result<(), Error> {
        let cluster_arc = cluster.clone();
        let cluster_lock = cluster_arc.write().await;
        let cluster = cluster_lock.deref();
        let session = get_session(cluster).await;

        let rows: Vec<String> = session
            .query_unpaged("select data_center from system.local", &[])
            .await
            .context("failed to execute query")?
            .into_rows_result()
            .context("failed to get rows")?
            .rows::<(String,)>()
            .context("failed to deserialize rows")?
            .map(|row| row.unwrap().0)
            .collect();
        println!("{:?}", rows);
        Ok(())
    })
    .await;
}
