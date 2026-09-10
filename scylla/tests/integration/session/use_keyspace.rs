use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use scylla::{
    client::session::Session,
    client::session_builder::SessionBuilder,
    errors::{BadKeyspaceName, UseKeyspaceError},
};
use scylla_proxy::{
    Condition, ProxyError, Reaction as _, RequestFrame, RequestOpcode, RequestReaction,
    RequestRule, ResponseReaction, ResponseRule, RunningProxy, ShardAwareness, WorkerError,
};
use tokio::sync::mpsc;

use crate::utils::{
    HEALTHCHECK_QUERY, PerformDDL as _, create_new_session_builder, setup_tracing,
    test_with_3_node_cluster, unique_keyspace_name,
};

#[tokio::test]
async fn test_use_keyspace() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();

    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {ks}.tab (a text primary key)"
        ))
        .await
        .unwrap();

    session
        .query_unpaged(format!("INSERT INTO {ks}.tab (a) VALUES ('test1')"), &[])
        .await
        .unwrap();

    session.use_keyspace(ks.clone(), false).await.unwrap();

    session
        .query_unpaged("INSERT INTO tab (a) VALUES ('test2')", &[])
        .await
        .unwrap();

    let mut rows: Vec<String> = session
        .query_unpaged("SELECT * FROM tab", &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(String,)>()
        .unwrap()
        .map(|res| res.unwrap().0)
        .collect();

    rows.sort();

    assert_eq!(rows, vec!["test1".to_string(), "test2".to_string()]);

    // Test that invalid keyspaces get rejected
    assert!(matches!(
        session.use_keyspace("", false).await,
        Err(UseKeyspaceError::BadKeyspaceName(BadKeyspaceName::Empty))
    ));

    let long_name: String = ['a'; 49].iter().collect();
    assert!(matches!(
        session.use_keyspace(long_name, false).await,
        Err(UseKeyspaceError::BadKeyspaceName(BadKeyspaceName::TooLong(
            _,
            _
        )))
    ));

    assert!(matches!(
        session.use_keyspace("abcd;dfdsf", false).await,
        Err(UseKeyspaceError::BadKeyspaceName(
            BadKeyspaceName::IllegalCharacter(_, ';')
        ))
    ));

    // Make sure that use_keyspace on SessionBuiler works
    let session2: Session = create_new_session_builder()
        .use_keyspace(ks.clone(), false)
        .build()
        .await
        .unwrap();

    let mut rows2: Vec<String> = session2
        .query_unpaged("SELECT * FROM tab", &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(String,)>()
        .unwrap()
        .map(|res| res.unwrap().0)
        .collect();

    rows2.sort();

    assert_eq!(rows2, vec!["test1".to_string(), "test2".to_string()]);

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();

    // Trying to use a nonexisting keyspace fails, and leaves the session unable to execute
    // requests: the connections that did not switch are closed, and the ones opened to
    // replace them cannot switch either. This is why it is done last here.
    assert!(
        session
            .use_keyspace("this_keyspace_does_not_exist_at_all", false)
            .await
            .is_err()
    );
    assert!(session.query_unpaged(HEALTHCHECK_QUERY, &[]).await.is_err());
}

#[tokio::test]
async fn test_use_keyspace_case_sensitivity() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks_lower = unique_keyspace_name().to_lowercase();
    let ks_upper = ks_lower.to_uppercase();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS \"{ks_lower}\" WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS \"{ks_upper}\" WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();

    session
        .ddl(format!("CREATE TABLE {ks_lower}.tab (a text primary key)"))
        .await
        .unwrap();

    session
        .ddl(format!(
            "CREATE TABLE \"{ks_upper}\".tab (a text primary key)"
        ))
        .await
        .unwrap();

    session
        .query_unpaged(
            format!("INSERT INTO {ks_lower}.tab (a) VALUES ('lowercase')"),
            &[],
        )
        .await
        .unwrap();

    session
        .query_unpaged(
            format!("INSERT INTO \"{ks_upper}\".tab (a) VALUES ('uppercase')"),
            &[],
        )
        .await
        .unwrap();

    // Use uppercase keyspace without case sensitivity
    // Should select the lowercase one
    session.use_keyspace(ks_upper.clone(), false).await.unwrap();

    let rows: Vec<String> = session
        .query_unpaged("SELECT * from tab", &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(String,)>()
        .unwrap()
        .map(|row| row.unwrap().0)
        .collect();

    assert_eq!(rows, vec!["lowercase".to_string()]);

    // Use uppercase keyspace with case sensitivity
    // Should select the uppercase one
    session.use_keyspace(&ks_upper, true).await.unwrap();

    let rows: Vec<String> = session
        .query_unpaged("SELECT * from tab", &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(String,)>()
        .unwrap()
        .map(|row| row.unwrap().0)
        .collect();

    assert_eq!(rows, vec!["uppercase".to_string()]);

    session
        .ddl(format!("DROP KEYSPACE \"{ks_lower}\""))
        .await
        .unwrap();
    session
        .ddl(format!("DROP KEYSPACE \"{ks_upper}\""))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_raw_use_keyspace() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();

    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {ks}.tab (a text primary key)"
        ))
        .await
        .unwrap();

    session
        .query_unpaged(format!("INSERT INTO {ks}.tab (a) VALUES ('raw_test')"), &[])
        .await
        .unwrap();

    session
        .query_unpaged(format!("use    \"{ks}\"    ;"), &[])
        .await
        .unwrap();

    let rows: Vec<String> = session
        .query_unpaged("SELECT * FROM tab", &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(String,)>()
        .unwrap()
        .map(|res| res.unwrap().0)
        .collect();

    assert_eq!(rows, vec!["raw_test".to_string()]);

    // Check if case sensitivity is correctly detected
    assert!(
        session
            .query_unpaged(format!("use    \"{}\"    ;", ks.to_uppercase()), &[])
            .await
            .is_err()
    );

    assert!(
        session
            .query_unpaged(format!("use    {}    ;", ks.to_uppercase()), &[])
            .await
            .is_ok()
    );

    // Test pager APIs
    {
        let _pager = session.query_iter("use    system    ;", &[]).await.unwrap();
        session
            .query_unpaged("SELECT host_id FROM local WHERE key = 'local'", ())
            .await
            .expect("Keyspace seen to not have been correctly set by Pager execution");
    }

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
async fn test_get_keyspace_name() {
    setup_tracing();

    let ks = unique_keyspace_name();

    // Create the keyspace
    // No keyspace is set in config, so get_keyspace() should return None.
    let session = create_new_session_builder().build().await.unwrap();
    assert_eq!(session.get_keyspace(), None);
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    assert_eq!(session.get_keyspace(), None);

    // Call use_keyspace(), get_keyspace now should return the new keyspace name
    session.use_keyspace(&ks, true).await.unwrap();
    assert_eq!(*session.get_keyspace().unwrap(), ks);

    // Creating a new session with the keyspace set in config should cause
    // get_keyspace to return that name
    let session = create_new_session_builder()
        .use_keyspace(&ks, true)
        .build()
        .await
        .unwrap();
    assert_eq!(*session.get_keyspace().unwrap(), ks);

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

/// Two `use_keyspace` calls must not have their `USE` fanouts in flight at the same time,
/// or the connections can be left split between the two keyspaces.
///
/// The responses to the first keyspace's `USE` are delayed by the proxy, so a second fanout
/// that was allowed to start while the first one is still running would finish first.
#[tokio::test]
async fn test_concurrent_use_keyspace_calls_are_serialized() {
    setup_tracing();

    const USE_RESPONSE_DELAY: Duration = Duration::from_secs(1);

    let test_fut = |proxy_uris: [String; 3],
                    translation_map: HashMap<SocketAddr, SocketAddr>,
                    mut running_proxy: RunningProxy| async move {
        let session: Session = SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .address_translator(Arc::new(translation_map))
            // Schema metadata responses mention keyspace names too, and the response rule
            // installed below matches on the name alone.
            .fetch_schema_metadata(false)
            .build()
            .await
            .unwrap();

        let first_ks = unique_keyspace_name();
        let second_ks = unique_keyspace_name();
        for ks in [&first_ks, &second_ks] {
            session
                .ddl(format!(
                    "CREATE KEYSPACE {ks} WITH REPLICATION = \
                     {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"
                ))
                .await
                .unwrap();
        }

        let (use_tx, mut use_rx) = mpsc::unbounded_channel::<(RequestFrame, Option<u16>)>();
        for node in &mut running_proxy.running_nodes {
            node.change_request_rules(Some(vec![RequestRule(
                Condition::RequestOpcode(RequestOpcode::Query).and(
                    Condition::BodyContainsCaseSensitive(Box::from(first_ks.as_bytes())),
                ),
                RequestReaction::noop().with_feedback_when_performed(use_tx.clone()),
            )]));
            node.change_response_rules(Some(vec![ResponseRule(
                Condition::BodyContainsCaseSensitive(Box::from(first_ks.as_bytes())),
                ResponseReaction::delay(USE_RESPONSE_DELAY),
            )]));
        }

        let session = Arc::new(session);
        let finish_order = Arc::new(Mutex::new(Vec::new()));

        let first_call = tokio::spawn({
            let session = Arc::clone(&session);
            let finish_order = Arc::clone(&finish_order);
            let first_ks = first_ks.clone();
            async move {
                session.use_keyspace(first_ks, false).await.unwrap();
                finish_order.lock().unwrap().push("first");
            }
        });

        // Only start the second call once the first fanout is observably in progress, so that
        // the order in which the two requests reach the cluster worker is not left to a race.
        use_rx.recv().await.unwrap();

        session.use_keyspace(&second_ks, false).await.unwrap();
        finish_order.lock().unwrap().push("second");
        first_call.await.unwrap();

        assert_eq!(
            *finish_order.lock().unwrap(),
            ["first", "second"],
            "the second use_keyspace finished first, so the two fanouts overlapped"
        );

        // The rules would delay the DROPs below as well.
        for node in &mut running_proxy.running_nodes {
            node.change_request_rules(None);
            node.change_response_rules(None);
        }
        for ks in [&first_ks, &second_ks] {
            session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
        }

        running_proxy
    };

    let res = test_with_3_node_cluster(ShardAwareness::QueryNode, test_fut).await;
    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
