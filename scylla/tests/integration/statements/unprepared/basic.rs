use scylla::{
    response::{PagingState, PagingStateResponse},
    statement::Statement,
};

use crate::utils::{
    create_new_session_builder, setup_tracing, unique_keyspace_name, PerformDDL as _,
};

#[tokio::test]
async fn test_unprepared_statement() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {}.t (a int, b int, c text, primary key (a, b))",
            ks
        ))
        .await
        .unwrap();

    session
        .query_unpaged(
            format!("INSERT INTO {}.t (a, b, c) VALUES (1, 2, 'abc')", ks),
            &[],
        )
        .await
        .unwrap();
    session
        .query_unpaged(
            format!("INSERT INTO {}.t (a, b, c) VALUES (7, 11, '')", ks),
            &[],
        )
        .await
        .unwrap();
    session
        .query_unpaged(
            format!("INSERT INTO {}.t (a, b, c) VALUES (1, 4, 'hello')", ks),
            &[],
        )
        .await
        .unwrap();

    let query_result = session
        .query_unpaged(format!("SELECT a, b, c FROM {}.t", ks), &[])
        .await
        .unwrap();

    let rows = query_result.into_rows_result().unwrap();

    let col_specs = rows.column_specs();
    assert_eq!(col_specs.get_by_name("a").unwrap().0, 0);
    assert_eq!(col_specs.get_by_name("b").unwrap().0, 1);
    assert_eq!(col_specs.get_by_name("c").unwrap().0, 2);
    assert!(col_specs.get_by_name("d").is_none());

    let mut results = rows
        .rows::<(i32, i32, String)>()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    results.sort();
    assert_eq!(
        results,
        vec![
            (1, 2, String::from("abc")),
            (1, 4, String::from("hello")),
            (7, 11, String::from(""))
        ]
    );
    let query_result = session
        .query_iter(format!("SELECT a, b, c FROM {}.t", ks), &[])
        .await
        .unwrap();
    let specs = query_result.column_specs();
    assert_eq!(specs.len(), 3);
    for (spec, name) in specs.iter().zip(["a", "b", "c"]) {
        assert_eq!(spec.name(), name); // Check column name.
        assert_eq!(spec.table_spec().ks_name(), ks);
    }
    let mut results_from_manual_paging = vec![];
    let query = Statement::new(format!("SELECT a, b, c FROM {}.t", ks)).with_page_size(1);
    let mut paging_state = PagingState::start();
    let mut watchdog = 0;
    loop {
        let (rs_manual, paging_state_response) = session
            .query_single_page(query.clone(), &[], paging_state)
            .await
            .unwrap();
        let mut page_results = rs_manual
            .into_rows_result()
            .unwrap()
            .rows::<(i32, i32, String)>()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        results_from_manual_paging.append(&mut page_results);
        match paging_state_response {
            PagingStateResponse::HasMorePages { state } => {
                paging_state = state;
            }
            _ if watchdog > 30 => break,
            PagingStateResponse::NoMorePages => break,
        }
        watchdog += 1;
    }
    assert_eq!(results_from_manual_paging, results);
}
