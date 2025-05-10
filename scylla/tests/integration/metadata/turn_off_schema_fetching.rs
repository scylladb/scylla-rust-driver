use std::collections::HashMap;

use crate::utils::{
    create_new_session_builder, setup_tracing, unique_keyspace_name, PerformDDL as _,
};
use scylla::cluster::metadata::Strategy;

#[tokio::test]
async fn test_turning_off_schema_fetching() {
    setup_tracing();
    let session = create_new_session_builder()
        .fetch_schema_metadata(false)
        .build()
        .await
        .unwrap();
    let ks = unique_keyspace_name();

    session
        .ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks))
        .await
        .unwrap();

    session
        .query_unpaged(format!("USE {}", ks), &[])
        .await
        .unwrap();

    session
        .ddl(
            "CREATE TYPE IF NOT EXISTS type_a (
                    a map<frozen<list<int>>, text>,
                    b frozen<map<frozen<list<int>>, frozen<set<text>>>>
                   )",
        )
        .await
        .unwrap();

    session
        .ddl("CREATE TYPE IF NOT EXISTS type_b (a int, b text)")
        .await
        .unwrap();

    session
        .ddl("CREATE TYPE IF NOT EXISTS type_c (a map<frozen<set<text>>, frozen<type_b>>)")
        .await
        .unwrap();

    session
        .ddl(
            "CREATE TABLE IF NOT EXISTS table_a (
                    a frozen<type_a> PRIMARY KEY,
                    b type_b,
                    c frozen<type_c>,
                    d map<text, frozen<list<int>>>,
                    e tuple<int, text>
                  )",
        )
        .await
        .unwrap();

    session.refresh_metadata().await.unwrap();
    let cluster_state = &session.get_cluster_state();
    let keyspace = cluster_state.get_keyspace(&ks).unwrap();

    let datacenter_repfactors: HashMap<String, usize> = cluster_state
        .replica_locator()
        .datacenter_names()
        .iter()
        .map(|dc_name| (dc_name.to_owned(), 1))
        .collect();

    assert_eq!(
        keyspace.strategy,
        Strategy::NetworkTopologyStrategy {
            datacenter_repfactors
        }
    );
    assert_eq!(keyspace.tables.len(), 0);
    assert_eq!(keyspace.user_defined_types.len(), 0);
}
