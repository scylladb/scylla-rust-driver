use scylla::serialize::value::SerializeValue;

use crate::utils::{
    create_new_session_builder, setup_tracing, unique_keyspace_name, PerformDDL as _,
};

#[tokio::test]
async fn test_unusual_valuelists() {
    setup_tracing();

    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session
        .ddl("CREATE TABLE IF NOT EXISTS tab (a int, b int, c varchar, primary key (a, b, c))")
        .await
        .unwrap();

    let insert_a_b_c = session
        .prepare("INSERT INTO tab (a, b, c) VALUES (?, ?, ?)")
        .await
        .unwrap();

    let values_dyn: Vec<&dyn SerializeValue> = vec![
        &1 as &dyn SerializeValue,
        &2 as &dyn SerializeValue,
        &"&dyn" as &dyn SerializeValue,
    ];
    session
        .execute_unpaged(&insert_a_b_c, values_dyn)
        .await
        .unwrap();

    let values_box_dyn: Vec<Box<dyn SerializeValue>> = vec![
        Box::new(1) as Box<dyn SerializeValue>,
        Box::new(3) as Box<dyn SerializeValue>,
        Box::new("Box dyn") as Box<dyn SerializeValue>,
    ];
    session
        .execute_unpaged(&insert_a_b_c, values_box_dyn)
        .await
        .unwrap();

    let mut all_rows: Vec<(i32, i32, String)> = session
        .query_unpaged("SELECT a, b, c FROM tab", ())
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(i32, i32, String)>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    all_rows.sort();
    assert_eq!(
        all_rows,
        vec![
            (1i32, 2i32, "&dyn".to_owned()),
            (1, 3, "Box dyn".to_owned())
        ]
    );
}
