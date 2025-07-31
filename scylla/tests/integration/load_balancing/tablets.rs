use std::sync::Arc;

use crate::utils::{
    execute_prepared_statement_everywhere, execute_unprepared_statement_everywhere,
    scylla_supports_tablets, setup_tracing, supports_feature, test_with_3_node_cluster,
    unique_keyspace_name, PerformDDL,
};

use futures::future::try_join_all;
use futures::TryStreamExt;
use itertools::Itertools;
use scylla::client::session::Session;
use scylla::cluster::Node;
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::unprepared::Statement;

use scylla_proxy::{
    Condition, ProxyError, Reaction, ResponseFrame, ResponseOpcode, ResponseReaction, ResponseRule,
    ShardAwareness, TargetShard, WorkerError,
};

use tokio::sync::mpsc::{self, UnboundedReceiver};
use tracing::info;
use uuid::Uuid;

#[derive(scylla::DeserializeRow)]
struct SelectedTablet {
    last_token: i64,
    replicas: Vec<(Uuid, i32)>,
}

#[derive(Eq, PartialEq)]
struct Tablet {
    first_token: i64,
    last_token: i64,
    replicas: Vec<(Arc<Node>, i32)>,
}

async fn get_tablets(session: &Session, ks: &str, table: &str) -> Vec<Tablet> {
    let cluster_state = session.get_cluster_state();
    let endpoints = cluster_state.get_nodes_info();
    for endpoint in endpoints.iter() {
        info!(
            "Endpoint id: {}, address: {}",
            endpoint.host_id,
            endpoint.address.ip()
        );
    }

    let selected_tablets_response = session.query_iter(
        "select last_token, replicas from system.tablets WHERE keyspace_name = ? and table_name = ? ALLOW FILTERING",
        &(ks, table)).await.unwrap();

    let mut selected_tablets: Vec<SelectedTablet> = selected_tablets_response
        .rows_stream::<SelectedTablet>()
        .unwrap()
        .into_stream()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    selected_tablets.sort_unstable_by(|a, b| a.last_token.cmp(&b.last_token));

    let (tablets, _) = selected_tablets.iter().fold(
        (Vec::new(), i64::MIN),
        |(mut tablets, first_token), tablet| {
            let replicas = tablet
                .replicas
                .iter()
                .map(|(uuid, shard)| {
                    (
                        Arc::clone(
                            endpoints
                                .get(endpoints.iter().position(|e| e.host_id == *uuid).unwrap())
                                .unwrap(),
                        ),
                        *shard,
                    )
                })
                .collect();
            let raw_tablet = Tablet {
                first_token,
                last_token: tablet.last_token,
                replicas,
            };

            tablets.push(raw_tablet);
            (tablets, tablet.last_token.wrapping_add(1))
        },
    );

    // Print information about tablets, useful for debugging
    for tablet in tablets.iter() {
        info!(
            "Tablet: [{}, {}]: {:?}",
            tablet.first_token,
            tablet.last_token,
            tablet
                .replicas
                .iter()
                .map(|(replica, shard)| { (replica.address.ip(), shard) })
                .collect::<Vec<_>>()
        );
    }

    tablets
}

// Takes a prepared statements which accepts 2 arguments: i32 pk and i32 ck,
// and calculates an example key for each of the tablets in the table.
fn calculate_key_per_tablet(tablets: &[Tablet], prepared: &PreparedStatement) -> Vec<(i32, i32)> {
    // Here we calculate a PK per token
    let mut present_tablets = vec![false; tablets.len()];
    let mut value_lists = vec![];
    for i in 0..1000 {
        let token_value = prepared.calculate_token(&(i, 1)).unwrap().unwrap().value();
        let tablet_idx = tablets
            .iter()
            .position(|tablet| {
                tablet.first_token <= token_value && token_value <= tablet.last_token
            })
            .unwrap();
        if !present_tablets[tablet_idx] {
            let values = (i, 1);
            let tablet = &tablets[tablet_idx];
            info!(
                "Values: {:?}, token: {}, tablet index: {}, tablet: [{}, {}], replicas: {:?}",
                values,
                token_value,
                tablet_idx,
                tablet.first_token,
                tablet.last_token,
                tablet
                    .replicas
                    .iter()
                    .map(|(replica, shard)| { (replica.address.ip(), shard) })
                    .collect::<Vec<_>>()
            );
            value_lists.push(values);
            present_tablets[tablet_idx] = true;
        }
    }

    // This function tries 1000 keys and assumes that it is enough to cover all
    // tablets. This is just a random number that seems to work in the tests,
    // so the assert is here to catch the problem early if 1000 stops being enough
    // for some reason.
    assert!(present_tablets.iter().all(|x| *x));

    value_lists
}

fn frame_has_tablet_feedback(frame: ResponseFrame) -> bool {
    let response =
        scylla_cql::frame::parse_response_body_extensions(frame.params.flags, None, frame.body)
            .unwrap();
    match response.custom_payload {
        Some(map) => map.contains_key("tablets-routing-v1"),
        None => false,
    }
}

fn count_tablet_feedbacks(
    rx: &mut mpsc::UnboundedReceiver<(ResponseFrame, Option<TargetShard>)>,
) -> usize {
    std::iter::from_fn(|| rx.try_recv().ok())
        .map(|(frame, _shard)| frame_has_tablet_feedback(frame))
        .filter(|b| *b)
        .count()
}

async fn prepare_schema(session: &Session, ks: &str, table: &str, tablet_count: usize) {
    let supports_table_tablet_options = supports_feature(session, "TABLET_OPTIONS").await;
    let (keyspace_tablet_opts, table_tablet_opts) = if supports_table_tablet_options {
        (
            "AND tablets = { 'enabled': true }".to_string(),
            format!("WITH tablets = {{ 'min_tablet_count': {tablet_count} }}"),
        )
    } else {
        (
            format!("AND tablets = {{ 'initial': {tablet_count} }}"),
            String::new(),
        )
    };

    session
        .ddl(format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks}
            WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 2}}
            {keyspace_tablet_opts}"
        ))
        .await
        .unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {ks}.{table} (a int, b int, c text, primary key (a, b))
            {table_tablet_opts}"
        ))
        .await
        .unwrap();
}

async fn populate_internal_driver_tablet_info(
    session: &Session,
    prepared: &PreparedStatement,
    value_per_tablet: &[(i32, i32)],
    feedback_rxs: &mut [UnboundedReceiver<(ResponseFrame, Option<u16>)>],
) -> Result<(), String> {
    let mut total_tablets_with_feedback = 0;
    for values in value_per_tablet.iter() {
        info!(
            "First loop, trying key {:?}, token: {}",
            values,
            prepared.calculate_token(&values).unwrap().unwrap().value()
        );
        execute_prepared_statement_everywhere(
            session,
            &session.get_cluster_state(),
            prepared,
            values,
        )
        .await
        .unwrap();
        let feedbacks: usize = feedback_rxs.iter_mut().map(count_tablet_feedbacks).sum();
        if feedbacks > 0 {
            total_tablets_with_feedback += 1;
        }
    }

    if total_tablets_with_feedback == value_per_tablet.len() {
        Ok(())
    } else {
        Err(format!(
            "Expected feedback for {} tablets, got it for {}",
            value_per_tablet.len(),
            total_tablets_with_feedback
        ))
    }
}

async fn verify_queries_routed_to_correct_tablets(
    session: &Session,
    prepared: &PreparedStatement,
    value_per_tablet: &[(i32, i32)],
    feedback_rxs: &mut [UnboundedReceiver<(ResponseFrame, Option<u16>)>],
) -> Result<(), String> {
    for values in value_per_tablet.iter() {
        info!(
            "Second loop, trying key {:?}, token: {}",
            values,
            prepared.calculate_token(&values).unwrap().unwrap().value()
        );
        try_join_all((0..100).map(|_| async { session.execute_unpaged(prepared, values).await }))
            .await
            .unwrap();
        let feedbacks: usize = feedback_rxs.iter_mut().map(count_tablet_feedbacks).sum();
        if feedbacks != 0 {
            return Err(format!("Expected 0 tablet feedbacks, received {feedbacks}"));
        }
    }

    Ok(())
}

async fn run_test_default_policy_is_tablet_aware_attempt(
    session: &Session,
    prepared: &PreparedStatement,
    value_per_tablet: &[(i32, i32)],
    feedback_rxs: &mut [UnboundedReceiver<(ResponseFrame, Option<u16>)>],
) -> Result<(), String> {
    populate_internal_driver_tablet_info(session, prepared, value_per_tablet, feedback_rxs).await?;

    // Now we must have info about all the tablets. It should not be
    // possible to receive any feedback if DefaultPolicy is properly
    // tablet-aware.
    verify_queries_routed_to_correct_tablets(session, prepared, value_per_tablet, feedback_rxs)
        .await
}

/// Tests that, when:
/// - Using DefaultPolicy with TokenAwareness
/// - Querying table that uses tablets
/// - Driver has all driver info fetched locally
/// Then we'll never receive tablet info.
///
/// The test first sends, to each possible target, one insert request per tablet.
/// It expects to get tablet feedback for each tablet.
/// After that we know we have all the info. The test sends the same insert request
/// per tablet, this time using DefaultPolicy, and expects to not get any feedbacks.
#[cfg_attr(scylla_cloud_tests, ignore)]
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_default_policy_is_tablet_aware() {
    setup_tracing();
    const TABLET_COUNT: usize = 16;

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let session = scylla::client::session_builder::SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .build()
                .await
                .unwrap();

            if !scylla_supports_tablets(&session).await {
                tracing::warn!("Skipping test because this Scylla version doesn't support tablets");
                return running_proxy;
            }

            let ks = unique_keyspace_name();

            /* Prepare schema */
            prepare_schema(&session, &ks, "t", TABLET_COUNT).await;

            let prepared = session
                .prepare(format!("INSERT INTO {ks}.t (a, b, c) VALUES (?, ?, 'abc')"))
                .await
                .unwrap();

            let (feedback_txs, mut feedback_rxs): (Vec<_>, Vec<_>) = (0..3)
                .map(|_| mpsc::unbounded_channel::<(ResponseFrame, Option<TargetShard>)>())
                .unzip();
            for (i, tx) in feedback_txs.iter().cloned().enumerate() {
                running_proxy.running_nodes[i].change_response_rules(Some(vec![ResponseRule(
                    Condition::ResponseOpcode(ResponseOpcode::Result)
                        .and(Condition::not(Condition::ConnectionRegisteredAnyEvent)),
                    ResponseReaction::noop().with_feedback_when_performed(tx),
                )]));
            }

            // Test attempt can fail because of tablet migrations.
            // Let's try a few times if there are migrations.
            let mut last_error = None;
            for _ in 0..5 {
                let tablets = get_tablets(&session, &ks, "t").await;
                let value_per_tablet = calculate_key_per_tablet(&tablets, &prepared);
                match run_test_default_policy_is_tablet_aware_attempt(
                    &session,
                    &prepared,
                    &value_per_tablet,
                    &mut feedback_rxs,
                )
                .await
                {
                    Ok(_) => return running_proxy, // Test succeeded
                    Err(e) => {
                        let new_tablets = get_tablets(&session, &ks, "t").await;
                        if tablets == new_tablets {
                            // We failed, but there was no migration.
                            panic!("Test attempt failed despite no migration. Error: {e}");
                        }
                        last_error = Some(e);
                        // There was a migration, let's try again
                    }
                }
            }
            panic!(
                "There was a tablet migration during each attempt! Last error: {}",
                last_error.unwrap()
            );
        },
    )
    .await;
    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}

/// This test verifies that Scylla never sends tablet info when receiving unprepared
/// query - as it would make no sens to send it (driver can't do token awareness
/// for unprepared queries).
///
/// The test sends a query to each shard of every node and verifies that no
/// tablet info was sent in response.
#[cfg_attr(scylla_cloud_tests, ignore)]
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_tablet_feedback_not_sent_for_unprepared_queries() {
    setup_tracing();
    const TABLET_COUNT: usize = 16;

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let session = scylla::client::session_builder::SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .build()
                .await
                .unwrap();

            if !scylla_supports_tablets(&session).await {
                tracing::warn!("Skipping test because this Scylla version doesn't support tablets");
                return running_proxy;
            }

            let ks = unique_keyspace_name();

            /* Prepare schema */
            prepare_schema(&session, &ks, "t", TABLET_COUNT).await;

            let (feedback_txs, mut feedback_rxs): (Vec<_>, Vec<_>) = (0..3)
                .map(|_| mpsc::unbounded_channel::<(ResponseFrame, Option<TargetShard>)>())
                .unzip();
            for (i, tx) in feedback_txs.iter().cloned().enumerate() {
                running_proxy.running_nodes[i].change_response_rules(Some(vec![ResponseRule(
                    Condition::ResponseOpcode(ResponseOpcode::Result)
                        .and(Condition::not(Condition::ConnectionRegisteredAnyEvent)),
                    ResponseReaction::noop().with_feedback_when_performed(tx),
                )]));
            }

            // I expect Scylla to not send feedback for unprepared queries,
            // as such queries cannot be token-aware anyway
            execute_unprepared_statement_everywhere(
                &session,
                session.get_cluster_state().as_ref(),
                &Statement::new(format!("INSERT INTO {ks}.t (a, b, c) VALUES (1, 1, 'abc')")),
                &(),
            )
            .await
            .unwrap();

            let feedbacks: usize = feedback_rxs.iter_mut().map(count_tablet_feedbacks).sum();
            assert_eq!(feedbacks, 0);

            running_proxy
        },
    )
    .await;
    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}

/// Verifies that LWT optimization (sending LWT queries to the replicas in a fixed order)
/// works correctly for tablet tables when we have all tablet info locally.
///
/// The test first fetches all tablet info: by sending a query to each shard of each node,
/// for every tablet.
/// After that it sends 100 queries fro each tablet and verifies that only 1 shard on 1 node
/// recevied requests for a given tablet.
///
/// TODO: Remove #[ignore] once LWTs are supported with tablets.
/// Below cfg_attr is commented out because:
/// - This test should be always ignored for now
/// - Having both attrs results in warning about `#[ignore]` being unused
/// - I don't want to fully remove cfg_attr because it will be needed after we remove `#[ignore]`
// #[cfg_attr(scylla_cloud_tests, ignore)]
#[tokio::test]
#[ntest::timeout(30000)]
#[ignore]
async fn test_lwt_optimization_works_with_tablets() {
    setup_tracing();
    const TABLET_COUNT: usize = 16;

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let session = scylla::client::session_builder::SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .build()
                .await
                .unwrap();

            if !scylla_supports_tablets(&session).await {
                tracing::warn!("Skipping test because this Scylla version doesn't support tablets");
                return running_proxy;
            }

            let ks = unique_keyspace_name();

            /* Prepare schema */
            prepare_schema(&session, &ks, "t", TABLET_COUNT).await;

            let tablets = get_tablets(&session, &ks, "t").await;

            let prepared_insert = session
                .prepare(format!("INSERT INTO {ks}.t (a, b, c) VALUES (?, ?, null)"))
                .await
                .unwrap();

            let prepared_lwt_update = session
                .prepare(format!(
                    "UPDATE {ks}.t SET c = ? WHERE a = ? and b = ? IF c != null"
                ))
                .await
                .unwrap();

            let value_lists = calculate_key_per_tablet(&tablets, &prepared_insert);

            let (feedback_txs, mut feedback_rxs): (Vec<_>, Vec<_>) = (0..3)
                .map(|_| mpsc::unbounded_channel::<(ResponseFrame, Option<TargetShard>)>())
                .unzip();
            for (i, tx) in feedback_txs.iter().cloned().enumerate() {
                running_proxy.running_nodes[i].change_response_rules(Some(vec![ResponseRule(
                    Condition::ResponseOpcode(ResponseOpcode::Result)
                        .and(Condition::not(Condition::ConnectionRegisteredAnyEvent)),
                    ResponseReaction::noop().with_feedback_when_performed(tx),
                )]));
            }

            // Unlike test_tablet_awareness_works_with_full_info I use "send_statement_everywhere",
            // in order to make the test faster (it sends one request per shard, not 100 requests).
            for values in value_lists.iter() {
                info!(
                    "First loop, trying key {:?}, token: {}",
                    values,
                    prepared_insert
                        .calculate_token(&values)
                        .unwrap()
                        .unwrap()
                        .value()
                );
                execute_prepared_statement_everywhere(
                    &session,
                    session.get_cluster_state().as_ref(),
                    &prepared_insert,
                    values,
                )
                .await
                .unwrap();
                let feedbacks: usize = feedback_rxs.iter_mut().map(count_tablet_feedbacks).sum();
                assert!(feedbacks > 0);
            }

            // We have all the info about tablets.
            // Executing LWT queries should not yield any more feedbacks.
            // All queries for given key should also land in a single replica.
            for (a, b) in value_lists.iter() {
                info!(
                    "Second loop, trying key {:?}, token: {}",
                    (a, b),
                    prepared_insert
                        .calculate_token(&(a, b))
                        .unwrap()
                        .unwrap()
                        .value()
                );
                try_join_all((0..100).map(|_| async {
                    session
                        .execute_unpaged(&prepared_lwt_update, &("abc", a, b))
                        .await
                }))
                .await
                .unwrap();

                let mut queried_nodes = 0;
                feedback_rxs.iter_mut().for_each(|rx| {
                    let frames = std::iter::from_fn(|| rx.try_recv().ok()).collect::<Vec<_>>();
                    let feedbacks_count = frames
                        .iter()
                        .map(|(frame, _shard)| frame_has_tablet_feedback(frame.clone()))
                        .filter(|b| *b)
                        .count();
                    assert_eq!(feedbacks_count, 0);

                    let queried_shards =
                        frames.iter().map(|(_frame, shard)| *shard).unique().count();
                    assert!(queried_shards <= 1);

                    if queried_shards == 1 {
                        queried_nodes += 1;
                    }
                });

                assert_eq!(queried_nodes, 1);
            }

            running_proxy
        },
    )
    .await;
    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
