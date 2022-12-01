use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
use crate::Session;

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
