use crate::ccm::cluster::{Cluster, ClusterOptions};
use crate::ccm::{cluster_1_node, run_ccm_test, CLUSTER_VERSION};
use anyhow::{Context, Error};

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::RwLock;

async fn cluster_1_options() -> ClusterOptions {
    ClusterOptions {
        name: "cluster_1_node".to_string(),
        version: CLUSTER_VERSION.clone(),
        nodes: vec![1],
        ..ClusterOptions::default()
    }
}

async fn get_session(cluster: &Cluster) -> Session {
    let endpoints = cluster.nodes.get_contact_endpoints().await;
    SessionBuilder::new()
        .known_nodes(endpoints)
        .build()
        .await
        .unwrap()
}

#[tokio::test]
async fn test_cluster_lifecycle1() {
    run_ccm_test(cluster_1_node, |cluster| async move {
        let cluster_arc = cluster.clone();
        let cluster_lock = cluster_arc.write().await;
        let cluster = cluster_lock.deref();
        let session = get_session(cluster).await;

        let rows = session
            .query_unpaged("select data_center from system.local", &[])
            .await
            .context("failed to execute query")?
            .into_rows_result()
            .context("failed to get rows")?
            .rows::<(String,)>()
            .context("failed to deserialize rows")?
            .try_fold(Vec::new(), |mut out, rec| match rec {
                Ok(val) => {
                    out.push(val.0);
                    Ok(out)
                }
                Err(err) => Err(err),
            })?;
        println!("{:?}", rows);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn test_cluster_lifecycle2() {
    async fn test(cluster: Arc<RwLock<Cluster>>) -> Result<(), Error> {
        let cluster_arc = cluster.clone();
        let cluster_lock = cluster_arc.write().await;
        let cluster = cluster_lock.deref();
        let session = get_session(cluster).await;

        let rows = session
            .query_unpaged("select data_center from system.local", &[])
            .await
            .context("failed to execute query")?
            .into_rows_result()
            .context("failed to get rows")?
            .rows::<(String,)>()
            .context("failed to deserialize rows")?
            .try_fold(Vec::new(), |mut out, rec| match rec {
                Ok(val) => {
                    out.push(val.0);
                    Ok(out)
                }
                Err(err) => Err(err),
            })?;
        println!("{:?}", rows);
        Ok(())
    }
    run_ccm_test(cluster_1_node, test).await;
}
