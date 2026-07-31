//! CCM (Cassandra Cluster Manager) bridge for testing ScyllaDB Drivers.

mod cli_wrapper;
/// Client routes integration tests utilities.
#[cfg(feature = "unstable-client-routes")]
pub(crate) mod client_routes;
/// Cluster management types and operations.
pub(crate) mod cluster;
mod ip_allocator;
mod logged_cmd;
/// Node management types and operations.
pub(crate) mod node;

use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::LazyLock;

use cluster::Cluster;
use cluster::ClusterOptions;
use futures::FutureExt;
use ip_allocator::IpAllocator;
use tracing::info;

/// The version of the cluster to use for tests (e.g., "release:2026.1.0").
/// Can be overridden with the `SCYLLA_TEST_CLUSTER` environment variable.
pub(crate) static CLUSTER_VERSION: LazyLock<String> = LazyLock::new(|| {
    std::env::var("SCYLLA_TEST_CLUSTER").unwrap_or("release:2026.1.0".to_string())
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
        std::fs::create_dir_all(path).unwrap();
    }

    ccm_root_dir
});

/// Run a CCM test with default configuration.
///
/// # Arguments
/// * `make_cluster_options` - A function that returns cluster configuration
/// * `test_body` - The test function to execute
///
/// # Example
/// ```
/// use crate::ccm::lib::{run_ccm_test, cluster::ClusterOptions};
///
/// fn cluster_options() -> ClusterOptions {
///     ClusterOptions {
///         name: "test_cluster".to_string(),
///         nodes_per_dc: vec![1],
///         ..Default::default()
///     }
/// }
///
/// #[tokio::test]
/// async fn test_example() {
///     run_ccm_test(cluster_options, |cluster| async {
///         let session = cluster.make_session_builder().await.build().await.unwrap();
///         // test code here
///     }).await;
/// }
/// ```
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

/// Run a CCM test with custom configuration logic before the cluster starts.
///
/// # Arguments
/// * `make_cluster_options` - A function that returns cluster configuration
/// * `configure` - Configuration function to customize the cluster before start
/// * `test_body` - The test function to execute
///
/// # Example
/// ```
/// use crate::ccm::lib::{run_ccm_test_with_configuration, cluster::{Cluster, ClusterOptions}};
///
/// async fn configure_cluster(mut cluster: Cluster) -> Cluster {
///    // Do some configuration here
///    cluster.updateconf([("foo", "bar")]).await.expect("failed to update conf");
///    cluster
/// }
///
/// async fn test(cluster: &mut Cluster) {
/// #   let _c = cluster;
///     // test code here
/// }
///
/// #[tokio::test]
/// async fn test_example() {
///     run_ccm_test_with_configuration(
///         ClusterOptions::default,
///         configure_cluster,
///         test
///     ).await;
/// }
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
    cluster
        .init()
        .await
        .inspect_err(|_| cluster.mark_as_failed())
        .expect("failed to initialize cluster");
    cluster = configure(cluster).await;
    cluster
        .start(None)
        .await
        .inspect_err(|_| cluster.mark_as_failed())
        .expect("failed to start cluster");

    let result = AssertUnwindSafe(test_body(&mut cluster))
        .catch_unwind()
        .await;
    match result {
        Ok(()) => (),
        Err(err) => {
            cluster.mark_as_failed();
            std::panic::resume_unwind(err);
        }
    }
}
