//! CCM (Cassandra Cluster Manager) bridge for testing ScyllaDB Drivers.

mod cli_wrapper;
/// Client routes integration tests utilities.
#[cfg(feature = "unstable-client-routes")]
pub mod client_routes;
/// Cluster management types and operations.
pub mod cluster;
mod ip_allocator;
mod logged_cmd;
/// Node management types and operations.
pub mod node;

use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::LazyLock;

use cluster::Cluster;
use cluster::ClusterOptions;
use futures::FutureExt;
use ip_allocator::IpAllocator;
use tracing::info;

/// The version of the cluster to use for tests (e.g., "release:2026.2.2").
///
/// The default is derived at run time from `scylla_version.env` in the
/// repository root, which is the single source of truth for the ScyllaDB
/// version used in testing.
///
/// Can be overridden with the `SCYLLA_TEST_CLUSTER` environment variable. The
/// override is used verbatim, so it must be a full ccm version string -
/// including the `release:` prefix (e.g. `release:2026.2.2`), or another ccm
/// version scheme such as `unstable/master:<id>`.
pub static CLUSTER_VERSION: LazyLock<String> = LazyLock::new(|| {
    std::env::var("SCYLLA_TEST_CLUSTER").unwrap_or_else(|_| {
        let file_contents = read_scylla_version_env();
        format!("release:{}", parse_scylla_version(&file_contents))
    })
});

/// Path to `scylla_version.env`, resolved at compile time relative to this
/// crate's manifest. The crate is test-only and always built from a checkout,
/// so this keeps the lookup independent of the process working directory -
/// which matters because cargo and nextest run test binaries from varying
/// directories.
const SCYLLA_VERSION_ENV_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../scylla_version.env");

/// Reads `scylla_version.env`.
fn read_scylla_version_env() -> String {
    std::fs::read_to_string(SCYLLA_VERSION_ENV_PATH).unwrap_or_else(|e| {
        panic!("Failed to read the ScyllaDB version file `{SCYLLA_VERSION_ENV_PATH}`: {e}")
    })
}

/// Extracts and validates the `SCYLLA_VERSION` value from the contents of
/// `scylla_version.env`. Panics if the key is missing, the value is empty, or
/// the version is not a full three-component version.
fn parse_scylla_version(file_contents: &str) -> &str {
    let version = file_contents
        .lines()
        .filter_map(|line| line.trim().strip_prefix("SCYLLA_VERSION="))
        .map(str::trim)
        .find(|value| !value.is_empty())
        .expect(
            "scylla_version.env must contain a non-empty `SCYLLA_VERSION=<version>` line \
             (e.g. `SCYLLA_VERSION=2026.2.2`)",
        );

    let mut components = version.split('.');
    let three_components = std::array::from_fn::<_, 3, _>(|_| components.next());
    let valid = components.next().is_none()
        && three_components.iter().all(|component| {
            component.is_some_and(|component| {
                !component.is_empty() && component.bytes().all(|b| b.is_ascii_digit())
            })
        });
    assert!(
        valid,
        "SCYLLA_VERSION in scylla_version.env must be a full three-component numeric version \
         (e.g. `2026.2.2`), got `{version}`. A truncated version such as `2026.2` is accepted \
         by ccm, but it makes every single ccm invocation - even against an already-running \
         cluster - query AWS to resolve the latest patch number, which slows the tests down \
         badly.",
    );

    version
}

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
/// use scylla_ccm_bridge::{run_ccm_test, cluster::ClusterOptions};
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
pub async fn run_ccm_test<C, T>(make_cluster_options: C, test_body: T)
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
/// use scylla_ccm_bridge::{run_ccm_test_with_configuration, cluster::{Cluster, ClusterOptions}};
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
pub async fn run_ccm_test_with_configuration<C, Conf, T>(
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

#[cfg(test)]
mod tests {
    use super::parse_scylla_version;
    use super::read_scylla_version_env;

    #[test]
    fn parses_version_ignoring_comments_and_blank_lines() {
        let contents = "# a comment\n\n#SCYLLA_VERSION=1.2.3\nMY_SCYLLA_VERSION=3.2.1\nSCYLLA_VERSION=2026.2.2\n";
        assert_eq!(parse_scylla_version(contents), "2026.2.2");
    }

    #[test]
    #[should_panic(expected = "three-component")]
    fn rejects_truncated_version() {
        parse_scylla_version("SCYLLA_VERSION=2026.2\n");
    }

    #[test]
    #[should_panic(expected = "SCYLLA_VERSION")]
    fn rejects_missing_key() {
        parse_scylla_version("# no version here\n#SCYLLA_VERSION=1.2.3\nMY_SCYLLA_VERSION=1.2.3\n");
    }

    /// The regression guard that matters: the checked-in file is found at the
    /// compile-time-resolved path and its contents parse.
    #[test]
    fn checked_in_env_file_parses() {
        let contents = read_scylla_version_env();
        parse_scylla_version(&contents);
    }
}
