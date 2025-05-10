use std::sync::Arc;

use scylla::cluster::metadata::{ColumnType, NativeType, UserDefinedType};

use crate::utils::{create_new_session_builder, unique_keyspace_name, PerformDDL as _};

#[tokio::test]
async fn test_refresh_metadata_after_schema_agreement() {
    let session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();
    session.use_keyspace(ks.clone(), false).await.unwrap();

    session
        .ddl("CREATE TYPE udt (field1 int, field2 uuid, field3 text)")
        .await
        .unwrap();

    let cluster_state = session.get_cluster_state();
    let keyspace_metadata = cluster_state.get_keyspace(ks.as_str());
    assert_ne!(keyspace_metadata, None);

    let udt = keyspace_metadata.unwrap().user_defined_types.get("udt");
    assert_eq!(
        udt,
        Some(&Arc::new(UserDefinedType {
            keyspace: ks.into(),
            name: "udt".into(),
            field_types: Vec::from([
                ("field1".into(), ColumnType::Native(NativeType::Int)),
                ("field2".into(), ColumnType::Native(NativeType::Uuid)),
                ("field3".into(), ColumnType::Native(NativeType::Text))
            ])
        }))
    );
}
