mod cli_wrapper;
pub(crate) mod cluster;
mod ip_allocator;
mod logged_cmd;
pub(crate) mod node;

use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::LazyLock;

use cluster::Cluster;
use cluster::ClusterOptions;
use futures::FutureExt;
use ip_allocator::IpAllocator;
use tracing::info;

pub(crate) static CLUSTER_VERSION: LazyLock<String> = LazyLock::new(|| {
    std::env::var("SCYLLA_TEST_CLUSTER").unwrap_or("release:2025.3.3".to_string())
});

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

pub(crate) async fn run_ccm_test<C, T>(make_cluster_options: C, test_body: T)
where
    C: FnOnce() -> ClusterOptions,
    T: AsyncFnOnce(&mut Cluster) -> (),
{
    run_ccm_test_with_configuration(
        make_cluster_options,
        |cluster| async move { cluster },
        test_body,
    )
    .await
}

/// Run a CCM test with a custom configuration logic before the cluster starts.
///
/// ### Example
/// ```
/// # use crate::ccm::cluster::Cluster;
/// # use crate::ccm::run_ccm_test_with_configuration;
/// # use std::sync::{Arc, Mutex};
/// async fn configure_cluster(cluster: Cluster) -> Cluster {
///    // Do some configuration here
///    cluster.updateconf([("foo", "bar")]).await.expect("failed to update conf");
///    cluster
/// }
///
/// async fn test(cluster: Arc<Mutex<Cluster>>) {
///     let cluster = cluster.lock().await;
///     let session = cluster.make_session_builder().await.build().await.unwrap();
///
///     println!("Succesfully connected to the cluster!");
/// }
///
/// run_ccm_test_with_configuration(ClusterOptions::default, configure_cluster, test).await;
/// ```
pub(crate) async fn run_ccm_test_with_configuration<C, Conf, T>(
    make_cluster_options: C,
    configure: Conf,
    test_body: T,
) where
    C: FnOnce() -> ClusterOptions,
    Conf: AsyncFnOnce(Cluster) -> Cluster,
    T: AsyncFnOnce(&mut Cluster) -> (),
{
    let cluster_options = make_cluster_options();
    let mut cluster = Cluster::new(cluster_options)
        .await
        .expect("Failed to create cluster");
    cluster.init().await.expect("failed to initialize cluster");
    cluster = configure(cluster).await;
    cluster.start(None).await.expect("failed to start cluster");

    let result = AssertUnwindSafe(test_body(&mut cluster))
        .catch_unwind()
        .await;
    match result {
        Ok(()) => (),
        Err(err) => {
            if *TEST_KEEP_CLUSTER_ON_FAILURE {
                println!("Test failed, keep cluster alive, TEST_KEEP_CLUSTER_ON_FAILURE=true");
                cluster.set_keep_on_drop(true);
            }
            std::panic::resume_unwind(err);
        }
    }
}
