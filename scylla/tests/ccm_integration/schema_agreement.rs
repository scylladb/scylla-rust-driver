use std::sync::Arc;

use crate::ccm::cluster::{Cluster, ClusterOptions};
use crate::ccm::{run_ccm_test, CLUSTER_VERSION};
use crate::common::utils::{setup_tracing, unique_keyspace_name};

use tokio::sync::Mutex;

fn cluster_3_nodes() -> ClusterOptions {
    ClusterOptions {
        name: "schema_agreement_test".to_string(),
        version: CLUSTER_VERSION.clone(),
        nodes: vec![3],
        ..ClusterOptions::default()
    }
}

#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn test_schema_agreement_all_nodes() {
    setup_tracing();
    async fn test(cluster: Arc<Mutex<Cluster>>) {
        let cluster = cluster.lock().await;
        let session = cluster.make_session_builder().await.build().await.unwrap();

        // Create keyspace
        let keyspace = unique_keyspace_name();
        session
            .query_unpaged(
                format!(
                    "CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}",
                    keyspace
                ),
                &[],
            )
            .await
            .unwrap();

        // Use keyspace
        session.use_keyspace(keyspace, true).await.unwrap();

        // Create a table and check schema agreement
        let _result = session
            .query_unpaged(
                "CREATE TABLE test_schema_agreement_all (k int primary key, v int)",
                &[],
            )
            .await
            .unwrap();

        // Check if schema is in agreement
        let schema_agreement = session.check_schema_agreement().await.unwrap();
        assert!(schema_agreement.is_some());
    }
    run_ccm_test(cluster_3_nodes, test).await;
}

#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn test_schema_agreement_with_stopped_node() {
    setup_tracing();
    async fn test(cluster: Arc<Mutex<Cluster>>) {
        let cluster = cluster.lock().await;

        // Create keyspace
        let session = cluster.make_session_builder().await.build().await.unwrap();

        let keyspace = unique_keyspace_name();
        session
            .query_unpaged(
                format!(
                    "CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}",
                    keyspace
                ),
                &[],
            )
            .await
            .unwrap();

        // Use keyspace
        session.use_keyspace(keyspace, true).await.unwrap();

        // Stop node 2
        let node = cluster.nodes().get_by_id(2).await.unwrap();
        node.write().await.stop(None).await.unwrap();

        // Create a table while one node is stopped
        let _result = session
            .query_unpaged(
                "CREATE TABLE test_schema_agreement_stopped (k int primary key, v int)",
                &[],
            )
            .await
            .unwrap();

        // Schema agreement should succeed with remaining up nodes
        let schema_agreement = session.check_schema_agreement().await.unwrap();
        assert!(schema_agreement.is_some());

        // Start the node back
        node.write().await.start(None).await.unwrap();
        let schema_agreement = session.check_schema_agreement().await.unwrap();
        assert!(schema_agreement.is_some());
    }
    run_ccm_test(cluster_3_nodes, test).await;
}


#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn test_schema_agreement_with_paused_node() {
    setup_tracing();
    async fn test(cluster: Arc<Mutex<Cluster>>) {
        let cluster = cluster.lock().await;

        // Create keyspace
        let session = cluster.make_session_builder().await.build().await.unwrap();

        let keyspace = unique_keyspace_name();
        session
            .query_unpaged(
                format!(
                    "CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}",
                    keyspace
                ),
                &[],
            )
            .await
            .unwrap();

        // Use keyspace
        session.use_keyspace(keyspace, true).await.unwrap();

        // Stop node 2
        let node = cluster.nodes().get_by_id(2).await.unwrap();
        node.write().await.pause().await.unwrap();

        // Try a simple query to verify database is responsive
        let result = session
            .query_unpaged("SELECT * FROM system.local", &[])
            .await
            .unwrap();
        println!("Result after pause: {result:?}");

        // Create a table while one node is paused
        let _result = session
            .query_unpaged(
                "CREATE TABLE test_schema_agreement_paused (k int primary key, v int)",
                &[],
            )
            .await
            .unwrap();

        // Schema agreement should succeed with remaining up nodes
        let schema_agreement = session.check_schema_agreement().await.unwrap();
        assert!(schema_agreement.is_some());

        // Start the node back
        node.write().await.resume().await.unwrap();
        let schema_agreement = session.check_schema_agreement().await.unwrap();
        assert!(schema_agreement.is_some());
    }
    run_ccm_test(cluster_3_nodes, test).await;
}
