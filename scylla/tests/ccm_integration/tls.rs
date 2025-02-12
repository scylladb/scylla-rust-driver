use std::path::PathBuf;
use std::sync::Arc;

use openssl::ssl::{SslContextBuilder, SslMethod, SslVerifyMode};
use tokio::sync::Mutex;

use crate::ccm::cluster::{Cluster, ClusterOptions};
use crate::ccm::{run_ccm_test_with_configuration, CA_TLS_CERT_PATH, CLUSTER_VERSION};
use crate::common::utils::setup_tracing;

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
async fn test_tls_cluster_one_node_connects() {
    setup_tracing();
    async fn test(cluster: Arc<Mutex<Cluster>>) {
        let mut context_builder =
            SslContextBuilder::new(SslMethod::tls()).expect("Failed to create ssl context builder");
        let ca_dir = tokio::fs::canonicalize(PathBuf::from(&*CA_TLS_CERT_PATH))
            .await
            .expect("Failed to find ca cert file");
        context_builder
            .set_ca_file(ca_dir.as_path())
            .expect("Failed to set ca file");
        context_builder.set_verify(SslVerifyMode::PEER);

        let cluster = cluster.lock().await;
        let session = cluster
            .make_session_builder()
            .await
            .ssl_context(Some(context_builder.build()))
            .build()
            .await
            .unwrap();

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

    run_ccm_test_with_configuration(
        cluster_1_node,
        |cluster| async move {
            cluster
                .configure_tls()
                .await
                .expect("failed to configure tls");
            cluster
        },
        test,
    )
    .await;
}
