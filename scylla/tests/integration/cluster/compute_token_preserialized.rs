#![cfg(all(scylla_unstable, feature = "unstable-csharp-rs"))]

use crate::utils::{PerformDDL, create_new_session_builder, setup_tracing, unique_keyspace_name};
use scylla::cluster::metadata::{ColumnType, NativeType};
use scylla::errors::ClusterStateTokenError;
use scylla::frame::response::result::ColumnSpec;
use scylla::frame::response::result::TableSpec;
use scylla::routing::Token;
use scylla::serialize::row::RowSerializationContext;
use scylla_cql::serialize::row::SerializedValues;

#[tokio::test]
async fn test_compute_token_preserialized_single_and_multi() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session.use_keyspace(ks.as_str(), true).await.unwrap();

    // Single-column partition key
    session
        .ddl("CREATE TABLE IF NOT EXISTS t1 (a text primary key)")
        .await
        .unwrap();

    let v = ("hello",);
    // build column spec for partition key
    let col_specs = [ColumnSpec::borrowed(
        "a",
        ColumnType::Native(NativeType::Text),
        TableSpec::borrowed(&ks, "t1"),
    )];
    let ctx = RowSerializationContext::from_specs(&col_specs);
    let sv: SerializedValues = SerializedValues::from_serializable(&ctx, &v).unwrap();

    // compute tokens
    let token_preser = session
        .get_cluster_state()
        .compute_token_preserialized(&ks, "t1", &sv)
        .unwrap();
    let token_direct = session
        .get_cluster_state()
        .compute_token(&ks, "t1", &(&v.0,))
        .unwrap();
    assert_eq!(token_preser, token_direct);

    // Also compare with DB token
    session
        .query_unpaged("INSERT INTO t1 (a) VALUES (?)", (&v.0,))
        .await
        .unwrap();
    let (value,): (i64,) = session
        .query_unpaged("SELECT token(a) FROM t1 WHERE a = ?", (&v.0,))
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .single_row::<(i64,)>()
        .unwrap();
    assert_eq!(token_preser, Token::new(value));

    // Composite partition key ((a,b,c))
    session
        .ddl("CREATE TABLE IF NOT EXISTS complex_pk (a int, b int, c text, d int, PRIMARY KEY ((a,b,c), d))")
        .await
        .unwrap();
    session.await_schema_agreement().await.unwrap();
    session.refresh_metadata().await.unwrap();

    let all_values_in_query_order = (17_i32, 16_i32, "I'm prepared!!!", 7_i32);
    session
        .query_unpaged(
            "INSERT INTO complex_pk (a, b, c, d) VALUES (?, ?, ?, ?)",
            &all_values_in_query_order,
        )
        .await
        .unwrap();

    let pk_values = (17_i32, 16_i32, "I'm prepared!!!");
    let col_specs2 = [
        ColumnSpec::borrowed(
            "a",
            ColumnType::Native(NativeType::Int),
            TableSpec::borrowed(&ks, "complex_pk"),
        ),
        ColumnSpec::borrowed(
            "b",
            ColumnType::Native(NativeType::Int),
            TableSpec::borrowed(&ks, "complex_pk"),
        ),
        ColumnSpec::borrowed(
            "c",
            ColumnType::Native(NativeType::Text),
            TableSpec::borrowed(&ks, "complex_pk"),
        ),
    ];
    let ctx2 = RowSerializationContext::from_specs(&col_specs2);
    let sv2: SerializedValues = SerializedValues::from_serializable(&ctx2, &pk_values).unwrap();

    let token_preser2 = session
        .get_cluster_state()
        .compute_token_preserialized(&ks, "complex_pk", &sv2)
        .unwrap();
    let token_direct2 = session
        .get_cluster_state()
        .compute_token(&ks, "complex_pk", &pk_values)
        .unwrap();
    assert_eq!(token_preser2, token_direct2);

    // Also compare with DB token
    let (value2,): (i64,) = session
        .query_unpaged(
            "SELECT token(a,b,c) FROM complex_pk WHERE a = ? AND b = ? AND c = ? AND d = ?",
            (pk_values.0, pk_values.1, pk_values.2, 7_i32),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .single_row::<(i64,)>()
        .unwrap();
    assert_eq!(token_preser2, Token::new(value2));

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
async fn test_compute_token_preserialized_count_mismatch() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session.use_keyspace(ks.as_str(), true).await.unwrap();

    session
        .ddl("CREATE TABLE IF NOT EXISTS t2 (a int, b int, c text, primary key ((a, b)))")
        .await
        .unwrap();
    session.await_schema_agreement().await.unwrap();
    session.refresh_metadata().await.unwrap();

    // Serialize only a single value while table expects two partition key components
    let col_specs = [ColumnSpec::borrowed(
        "a",
        ColumnType::Native(NativeType::Int),
        TableSpec::borrowed(&ks, "t2"),
    )];
    let ctx = RowSerializationContext::from_specs(&col_specs);
    let sv: SerializedValues = SerializedValues::from_serializable(&ctx, &(17_i32,)).unwrap();

    let res = session
        .get_cluster_state()
        .compute_token_preserialized(&ks, "t2", &sv);
    match res {
        Err(ClusterStateTokenError::PartitionKeyCountMismatch {
            keyspace: _,
            table: _,
            received,
            expected,
        }) => {
            assert_eq!(received, 1usize);
            assert_eq!(expected, 2usize);
        }
        Ok(token) => panic!(
            "compute_token_preserialized unexpectedly returned Ok({:?})",
            token
        ),
        Err(e) => panic!(
            "compute_token_preserialized returned unexpected error: {:?}",
            e
        ),
    }

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}
