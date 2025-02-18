use scylla::cluster::metadata::{ColumnType, NativeType};
use scylla::frame::response::result::{ColumnSpec, TableSpec};

use crate::utils::{create_new_session_builder, setup_tracing, unique_keyspace_name, PerformDDL};

#[tokio::test]
async fn test_prepared_statement_col_specs() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();
    session
        .ddl(format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = 
            {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}",
            ks
        ))
        .await
        .unwrap();
    session.use_keyspace(&ks, false).await.unwrap();

    session
        .ddl(
            "CREATE TABLE t (k1 int, k2 varint, c1 timestamp,
            a tinyint, b text, c smallint, PRIMARY KEY ((k1, k2), c1))",
        )
        .await
        .unwrap();

    let spec = |name: &'static str, typ: ColumnType<'static>| -> ColumnSpec<'_> {
        ColumnSpec::borrowed(name, typ, TableSpec::borrowed(&ks, "t"))
    };

    let prepared = session
        .prepare("SELECT * FROM t WHERE k1 = ? AND k2 = ? AND c1 > ?")
        .await
        .unwrap();

    let variable_col_specs = prepared.get_variable_col_specs().as_slice();
    let expected_variable_col_specs = &[
        spec("k1", ColumnType::Native(NativeType::Int)),
        spec("k2", ColumnType::Native(NativeType::Varint)),
        spec("c1", ColumnType::Native(NativeType::Timestamp)),
    ];
    assert_eq!(variable_col_specs, expected_variable_col_specs);

    let result_set_col_specs = prepared.get_result_set_col_specs().as_slice();
    let expected_result_set_col_specs = &[
        spec("k1", ColumnType::Native(NativeType::Int)),
        spec("k2", ColumnType::Native(NativeType::Varint)),
        spec("c1", ColumnType::Native(NativeType::Timestamp)),
        spec("a", ColumnType::Native(NativeType::TinyInt)),
        spec("b", ColumnType::Native(NativeType::Text)),
        spec("c", ColumnType::Native(NativeType::SmallInt)),
    ];
    assert_eq!(result_set_col_specs, expected_result_set_col_specs);
}
