pub(crate) mod cluster;
mod logged_cmd;
pub(crate) mod node_config;

use std::ops::AsyncFnOnce;
use std::sync::Arc;

use cluster::Cluster;
use cluster::ClusterOptions;
use lazy_static::lazy_static;
use tokio::sync::Mutex;

lazy_static! {
    pub static ref CLUSTER_VERSION: String =
        std::env::var("SCYLLA_TEST_CLUSTER").unwrap_or("release:6.2.2".to_string());
    static ref TEST_KEEP_CLUSTER_ON_FAILURE: bool = !std::env::var("TEST_KEEP_CLUSTER_ON_FAILURE")
        .unwrap_or("".to_string())
        .parse::<bool>()
        .unwrap_or(false);
}

pub(crate) async fn run_ccm_test<C, T>(make_cluster_options: C, test_body: T)
where
    C: FnOnce() -> ClusterOptions,
    T: AsyncFnOnce(Arc<Mutex<Cluster>>) -> (),
{
    let cluster_options = make_cluster_options();
    let mut cluster = Cluster::new(cluster_options)
        .await
        .expect("Failed to create cluster");
    cluster.init().await.expect("failed to initialize cluster");
    cluster.start(None).await.expect("failed to start cluster");

    struct ClusterWrapper(Arc<Mutex<Cluster>>);
    impl Drop for ClusterWrapper {
        fn drop(&mut self) {
            if std::thread::panicking() && *TEST_KEEP_CLUSTER_ON_FAILURE {
                println!("Test failed, keep cluster alive, TEST_KEEP_CLUSTER_ON_FAILURE=true");
                self.0.blocking_lock().set_keep_on_drop(true);
            }
        }
    }
    let wrapper = ClusterWrapper(Arc::new(Mutex::new(cluster)));
    test_body(Arc::clone(&wrapper.0)).await;
    std::mem::drop(wrapper);
}
