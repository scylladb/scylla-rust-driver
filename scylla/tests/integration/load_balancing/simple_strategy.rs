use crate::utils::{create_new_session_builder, unique_keyspace_name, PerformDDL as _};

/// It's recommended to use NetworkTopologyStrategy everywhere, so most tests use only NetworkTopologyStrategy.
/// We still support SimpleStrategy, so to make sure that SimpleStrategy works correctly this test runs
/// a few queries in a SimpleStrategy keyspace.
#[tokio::test]
async fn simple_strategy_test() {
    let ks = unique_keyspace_name();
    let session = create_new_session_builder().build().await.unwrap();

    session
        .ddl(format!(
            "CREATE KEYSPACE {ks} WITH REPLICATION = \
                {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
        ))
        .await
        .unwrap();

    session
        .ddl(format!(
            "CREATE TABLE {ks}.tab (p int, c int, r int, PRIMARY KEY (p, c, r))"
        ))
        .await
        .unwrap();

    session
        .query_unpaged(
            format!("INSERT INTO {ks}.tab (p, c, r) VALUES (1, 2, 3)"),
            (),
        )
        .await
        .unwrap();

    session
        .query_unpaged(
            format!("INSERT INTO {ks}.tab (p, c, r) VALUES (?, ?, ?)"),
            (4, 5, 6),
        )
        .await
        .unwrap();

    let prepared = session
        .prepare(format!("INSERT INTO {ks}.tab (p, c, r) VALUES (?, ?, ?)"))
        .await
        .unwrap();

    session.execute_unpaged(&prepared, (7, 8, 9)).await.unwrap();

    let mut rows: Vec<(i32, i32, i32)> = session
        .query_unpaged(format!("SELECT p, c, r FROM {ks}.tab"), ())
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(i32, i32, i32)>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect::<Vec<(i32, i32, i32)>>();
    rows.sort();

    assert_eq!(rows, vec![(1, 2, 3), (4, 5, 6), (7, 8, 9)]);
}
