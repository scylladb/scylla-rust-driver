#[cfg(test)]
use crate::session::session_builder::{GenericSessionBuilder, SessionBuilderKind};
#[cfg(test)]
use crate::Session;
#[cfg(test)]
use std::{num::NonZeroU32, time::Duration};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static UNIQUE_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn unique_keyspace_name() -> String {
    let cnt = UNIQUE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let name = format!(
        "test_rust_{}_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        cnt
    );
    println!("Unique name: {}", name);
    name
}

#[cfg(test)]
pub(crate) async fn supports_feature(session: &Session, feature: &str) -> bool {
    // Cassandra doesn't have a concept of features, so first detect
    // if there is the `supported_features` column in system.local

    let meta = session.get_cluster_data();
    let system_local = meta
        .keyspaces
        .get("system")
        .unwrap()
        .tables
        .get("local")
        .unwrap();

    if !system_local.columns.contains_key("supported_features") {
        return false;
    }

    let (features,): (Option<String>,) = session
        .query("SELECT supported_features FROM system.local", ())
        .await
        .unwrap()
        .single_row_typed()
        .unwrap();

    features
        .unwrap_or_default()
        .split(',')
        .any(|f| f == feature)
}

// Creates a generic session builder based on conditional compilation configuration
// For SessionBuilder of DefaultMode type, adds localhost to known hosts, as all of the tests
// connect to localhost.
#[cfg(test)]
pub fn create_new_session_builder() -> GenericSessionBuilder<impl SessionBuilderKind> {
    let session_builder = {
        #[cfg(not(scylla_cloud_tests))]
        {
            use crate::SessionBuilder;

            let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

            SessionBuilder::new().known_node(uri)
        }

        #[cfg(scylla_cloud_tests)]
        {
            use crate::session::session_builder::CloudMode;
            use crate::CloudSessionBuilder;
            use std::path::Path;

            std::env::var("CLOUD_CONFIG_PATH")
                .map(|config_path| CloudSessionBuilder::new(Path::new(&config_path)))
                .expect("Failed to initialize CloudSessionBuilder")
                .expect("CLOUD_CONFIG_PATH environment variable is missing")
        }
    };

    // The reason why we enable so long waiting for TracingInfo is... Cassandra. (Yes, again.)
    // In Cassandra Java Driver, the wait time for tracing info is 10 seconds, so here we do the same.
    // However, as Scylla usually gets TracingInfo ready really fast (our default interval is hence 3ms),
    // we stick to a not-so-much-terribly-long interval here.
    session_builder
        .tracing_info_fetch_attempts(NonZeroU32::new(50).unwrap())
        .tracing_info_fetch_interval(Duration::from_millis(200))
}
