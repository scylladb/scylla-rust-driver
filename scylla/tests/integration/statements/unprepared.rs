use crate::utils::{
    PerformDDL as _, create_new_session_builder, setup_tracing, test_with_3_node_cluster,
    unique_keyspace_name,
};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::response::{PagingState, PagingStateResponse};
use scylla::statement::unprepared::Statement;
use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestOpcode, RequestReaction, RequestRule, ShardAwareness,
    WorkerError,
};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_unprepared_statement() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {ks}.t (a int, b int, c text, primary key (a, b))"
        ))
        .await
        .unwrap();

    session
        .query_unpaged(
            format!("INSERT INTO {ks}.t (a, b, c) VALUES (1, 2, 'abc')"),
            &[],
        )
        .await
        .unwrap();
    session
        .query_unpaged(
            format!("INSERT INTO {ks}.t (a, b, c) VALUES (7, 11, '')"),
            &[],
        )
        .await
        .unwrap();
    session
        .query_unpaged(
            format!("INSERT INTO {ks}.t (a, b, c) VALUES (1, 4, 'hello')"),
            &[],
        )
        .await
        .unwrap();

    let query_result = session
        .query_unpaged(format!("SELECT a, b, c FROM {ks}.t"), &[])
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
        .query_iter(format!("SELECT a, b, c FROM {ks}.t"), &[])
        .await
        .unwrap();
    let specs = query_result.column_specs();
    assert_eq!(specs.len(), 3);
    for (spec, name) in specs.iter().zip(["a", "b", "c"]) {
        assert_eq!(spec.name(), name); // Check column name.
        assert_eq!(spec.table_spec().ks_name(), ks);
    }
    let mut results_from_manual_paging = vec![];
    let query = Statement::new(format!("SELECT a, b, c FROM {ks}.t")).with_page_size(1);
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

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
#[ntest::timeout(30000)]
async fn test_prepare_query_with_values() {
    setup_tracing();
    // unprepared query with non empty values should be prepared
    const TIMEOUT_PER_REQUEST: Duration = Duration::from_millis(1000);

    let res = test_with_3_node_cluster(ShardAwareness::QueryNode, |proxy_uris, translation_map, mut running_proxy| async move {
        // DB preparation phase
        let session: Session = SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .address_translator(Arc::new(translation_map))
            .build()
            .await
            .unwrap();

        let ks = unique_keyspace_name();
        session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}")).await.unwrap();
        session.use_keyspace(&ks, false).await.unwrap();
        session
            .ddl("CREATE TABLE t (a int primary key)")
            .await
            .unwrap();

        let s: Statement = Statement::from("INSERT INTO t (a) VALUES (?)");

        let drop_unprepared_frame_rule = RequestRule(
            Condition::RequestOpcode(RequestOpcode::Query)
                .and(Condition::BodyContainsCaseSensitive(Box::new(*b"t"))),
            RequestReaction::drop_frame(),
        );

        running_proxy.running_nodes[2]
        .change_request_rules(Some(vec![drop_unprepared_frame_rule]));

        tokio::select! {
            _res = session.query_unpaged(s, (0,)) => (),
            _ = tokio::time::sleep(TIMEOUT_PER_REQUEST) => panic!("Rules did not work: no received response"),
        };

        running_proxy.turn_off_rules();
        session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();

        running_proxy
    }).await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}

#[tokio::test]
#[ntest::timeout(30000)]
async fn test_query_with_no_values() {
    setup_tracing();
    // unprepared query with empty values should not be prepared
    const TIMEOUT_PER_REQUEST: Duration = Duration::from_millis(1000);

    let res = test_with_3_node_cluster(ShardAwareness::QueryNode, |proxy_uris, translation_map, mut running_proxy| async move {
        // DB preparation phase
        let session: Session = SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .address_translator(Arc::new(translation_map))
            .build()
            .await
            .unwrap();

        let ks = unique_keyspace_name();
        session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}")).await.unwrap();
        session.use_keyspace(&ks, false).await.unwrap();
        session
            .ddl("CREATE TABLE t (a int primary key)")
            .await
            .unwrap();

        let s: Statement = Statement::from("INSERT INTO t (a) VALUES (1)");

        let drop_prepared_frame_rule = RequestRule(
            Condition::RequestOpcode(RequestOpcode::Prepare)
                .and(Condition::BodyContainsCaseSensitive(Box::new(*b"t"))),
            RequestReaction::drop_frame(),
        );

        running_proxy.running_nodes[2]
        .change_request_rules(Some(vec![drop_prepared_frame_rule]));

        tokio::select! {
            _res = session.query_unpaged(s, ()) => (),
            _ = tokio::time::sleep(TIMEOUT_PER_REQUEST) => panic!("Rules did not work: no received response"),
        };

        session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();

        running_proxy
    }).await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
