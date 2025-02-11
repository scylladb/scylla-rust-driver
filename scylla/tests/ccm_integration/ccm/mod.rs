pub(crate) mod cluster;
mod ip_allocator;
mod logged_cmd;

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;

use cluster::Cluster;
use cluster::ClusterOptions;
use ip_allocator::IpAllocator;
use tracing::info;

pub(crate) static CLUSTER_VERSION: LazyLock<String> =
    LazyLock::new(|| std::env::var("SCYLLA_TEST_CLUSTER").unwrap_or("release:6.2.2".to_string()));

static TEST_KEEP_CLUSTER_ON_FAILURE: LazyLock<bool> = LazyLock::new(|| {
    std::env::var("TEST_KEEP_CLUSTER_ON_FAILURE")
        .unwrap_or("".to_string())
        .parse::<bool>()
        .unwrap_or(false)
});

/// A global IP allocator for CCM tests. Each cluster requires a unique 127.x.x.x/24 subnet. For this, we implemented
/// a global allocator which allows to allocate and free IP addresses. The allocator is thread safe, and can be used
/// in test environment (the tests are run in parallel).
static IP_ALLOCATOR: LazyLock<std::sync::Mutex<IpAllocator>> = LazyLock::new(|| {
    let ip_allocator = IpAllocator::new().expect("Failed to create IP allocator");
    std::sync::Mutex::new(ip_allocator)
});

/// CCM does not allow to have one active cluster within one config directory
/// To have more than two active CCM cluster at the same time we isolate each cluster into separate
/// config director, each config directory is created in `ROOT_CCM_DIR`.
static ROOT_CCM_DIR: LazyLock<String> = LazyLock::new(|| {
    let cargo_manifest_dir = env!("CARGO_MANIFEST_DIR");
    let ccm_root_dir_env = std::env::var("CCM_ROOT_DIR");
    let ccm_root_dir = match ccm_root_dir_env {
        Ok(x) => x,
        Err(e) => {
            info!(
                "CCM_ROOT_DIR env malformed or not present: {}. Using {}/ccm_data for ccm data.",
                e, cargo_manifest_dir
            );
            cargo_manifest_dir.to_string() + "/ccm_data"
        }
    };
    let path = PathBuf::from(&ccm_root_dir);
    if !path.try_exists().unwrap() {
        info!("Directory {:?} not found, creating", path);
        std::fs::create_dir(path).unwrap();
    }

    ccm_root_dir
});

pub(crate) async fn run_ccm_test<C, T, Fut>(make_cluster_options: C, test_body: T)
where
    C: FnOnce() -> ClusterOptions,
    T: FnOnce(Arc<tokio::sync::Mutex<Cluster>>) -> Fut,
    Fut: Future<Output = ()>,
{
    let cluster_options = make_cluster_options();
    let mut cluster = Cluster::new(cluster_options)
        .await
        .expect("Failed to create cluster");
    cluster.init().await.expect("failed to initialize cluster");
    cluster.start(None).await.expect("failed to start cluster");

    struct ClusterWrapper(Arc<tokio::sync::Mutex<Cluster>>);
    impl Drop for ClusterWrapper {
        fn drop(&mut self) {
            if std::thread::panicking() && *TEST_KEEP_CLUSTER_ON_FAILURE {
                println!("Test failed, keep cluster alive, TEST_KEEP_CLUSTER_ON_FAILURE=true");
                self.0.blocking_lock().set_keep_on_drop(true);
            }
        }
    }
    let wrapper = ClusterWrapper(Arc::new(tokio::sync::Mutex::new(cluster)));
    test_body(Arc::clone(&wrapper.0)).await;
    std::mem::drop(wrapper);
}
