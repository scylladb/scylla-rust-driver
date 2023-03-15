use std::sync::Arc;

use crate::utils::setup_tracing;
use crate::utils::test_with_3_node_cluster;

use futures::future::try_join_all;
use futures::TryStreamExt;
use itertools::Itertools;
use scylla::load_balancing::FallbackPlan;
use scylla::load_balancing::LoadBalancingPolicy;
use scylla::load_balancing::RoutingInfo;
use scylla::prepared_statement::PreparedStatement;
use scylla::query::Query;
use scylla::serialize::row::SerializeRow;
use scylla::test_utils::unique_keyspace_name;
use scylla::transport::ClusterData;
use scylla::transport::Node;
use scylla::transport::NodeRef;
use scylla::{ExecutionProfile, LegacyQueryResult, LegacySession};

use scylla::transport::errors::QueryError;
use scylla_proxy::{
    Condition, ProxyError, Reaction, ResponseFrame, ResponseOpcode, ResponseReaction, ResponseRule,
    ShardAwareness, TargetShard, WorkerError,
};

use tokio::sync::mpsc;
use tracing::info;
use uuid::Uuid;

#[derive(scylla::FromRow)]
struct SelectedTablet {
    last_token: i64,
    replicas: Vec<(Uuid, i32)>,
}

struct Tablet {
    first_token: i64,
    last_token: i64,
    replicas: Vec<(Arc<Node>, i32)>,
}

async fn get_tablets(session: &LegacySession, ks: &str, table: &str) -> Vec<Tablet> {
    let cluster_data = session.get_cluster_data();
    let endpoints = cluster_data.get_nodes_info();
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

    let mut selected_tablets = selected_tablets_response
        .into_typed::<SelectedTablet>()
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

#[derive(Debug)]
struct SingleTargetLBP {
    target: (Arc<Node>, Option<u32>),
}

impl LoadBalancingPolicy for SingleTargetLBP {
    fn pick<'a>(
        &'a self,
        _query: &'a RoutingInfo,
        _cluster: &'a ClusterData,
    ) -> Option<(NodeRef<'a>, Option<u32>)> {
        Some((&self.target.0, self.target.1))
    }

    fn fallback<'a>(
        &'a self,
        _query: &'a RoutingInfo,
        _cluster: &'a ClusterData,
    ) -> FallbackPlan<'a> {
        Box::new(std::iter::empty())
    }

    fn name(&self) -> String {
        "SingleTargetLBP".to_owned()
    }
}

async fn send_statement_everywhere(
    session: &LegacySession,
    cluster: &ClusterData,
    statement: &PreparedStatement,
    values: &dyn SerializeRow,
) -> Result<Vec<LegacyQueryResult>, QueryError> {
    let tasks = cluster.get_nodes_info().iter().flat_map(|node| {
        let shard_count: u16 = node.sharder().unwrap().nr_shards.into();
        (0..shard_count).map(|shard| {
            let mut stmt = statement.clone();
            let values_ref = &values;
            let policy = SingleTargetLBP {
                target: (node.clone(), Some(shard as u32)),
            };
            let execution_profile = ExecutionProfile::builder()
                .load_balancing_policy(Arc::new(policy))
                .build();
            stmt.set_execution_profile_handle(Some(execution_profile.into_handle()));

            async move { session.execute_unpaged(&stmt, values_ref).await }
        })
    });

    try_join_all(tasks).await
}

async fn send_unprepared_query_everywhere(
    session: &LegacySession,
    cluster: &ClusterData,
    query: &Query,
) -> Result<Vec<LegacyQueryResult>, QueryError> {
    let tasks = cluster.get_nodes_info().iter().flat_map(|node| {
        let shard_count: u16 = node.sharder().unwrap().nr_shards.into();
        (0..shard_count).map(|shard| {
            let mut stmt = query.clone();
            let policy = SingleTargetLBP {
                target: (node.clone(), Some(shard as u32)),
            };
            let execution_profile = ExecutionProfile::builder()
                .load_balancing_policy(Arc::new(policy))
                .build();
            stmt.set_execution_profile_handle(Some(execution_profile.into_handle()));

            async move { session.query_unpaged(stmt, &()).await }
        })
    });

    try_join_all(tasks).await
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

async fn prepare_schema(session: &LegacySession, ks: &str, table: &str, tablet_count: usize) {
    session
        .query_unpaged(
            format!(
                "CREATE KEYSPACE IF NOT EXISTS {} 
            WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 2}}
            AND tablets = {{ 'initial': {} }}",
                ks, tablet_count
            ),
            &[],
        )
        .await
        .unwrap();
    session
        .query_unpaged(
            format!(
                "CREATE TABLE IF NOT EXISTS {}.{} (a int, b int, c text, primary key (a, b))",
                ks, table
            ),
            &[],
        )
        .await
        .unwrap();
}

/// Tests that, when using DefaultPolicy with TokenAwareness and querying table
/// that uses tablets:
/// 1. When querying data that belongs to tablet we didn't receive yet we will
///    receive this tablet at some point.
/// 2. When we have all tablets info locally then we'll never receive tablet info.
///
/// The test first sends 100 queries per tablet and expects to receive tablet info.
/// After that we know we have all the info. The test sends the statements again
/// and expects to not receive any tablet info.
#[cfg(not(scylla_cloud_tests))]
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_default_policy_is_tablet_aware() {
    setup_tracing();
    const TABLET_COUNT: usize = 16;

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let session = scylla::SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .build()
                .await
                .unwrap();

            if !scylla::test_utils::scylla_supports_tablets(&session).await {
                tracing::warn!("Skipping test because this Scylla version doesn't support tablets");
                return running_proxy;
            }

            let ks = unique_keyspace_name();

            /* Prepare schema */
            prepare_schema(&session, &ks, "t", TABLET_COUNT).await;

            let tablets = get_tablets(&session, &ks, "t").await;

            let prepared = session
                .prepare(format!(
                    "INSERT INTO {}.t (a, b, c) VALUES (?, ?, 'abc')",
                    ks
                ))
                .await
                .unwrap();

            let value_lists = calculate_key_per_tablet(&tablets, &prepared);

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

            // When the driver never received tablet info for any tablet in a given table,
            // then it will not be aware that the table is tablet-based and fall back
            // to token-ring routing for this table.
            // After it receives any tablet for this table:
            // - tablet-aware routing will be used for tablets that the driver has locally
            // - non-token-aware routing will be used for other tablets (which basically means
            //   sending the requests to random nodes)
            // In the following code I want to make sure that the driver fetches info
            // about all the tablets in the table.
            // Obvious way to do this would be to, for each tablet, send some requests (here some == 100)
            // and expect that at least one will land on non-replica and return tablet feedback.
            // This mostly works, but there is a problem: initially driver has no
            // tablet information at all for this table so it will fall back to token-ring routing.
            // It is possible that token-ring replicas and tablet replicas are the same
            // for some tokens. If it happens for the first token that we use in this loop,
            // then no matter how many requests we send we are not going to receive any tablet feedback.
            // The solution is to iterate over tablets twice.
            //
            // First iteration guarantees that the driver will receive at least one tablet
            // for this table (it is statistically improbable for all tokens used here to have the same
            // set of replicas for tablets and token-ring). In practice it will receive all or almost all of the tablets.
            //
            // Second iteration will not use token-ring routing (because the driver has some tablets
            // for this table, so it is aware that the table is tablet based),
            // which means that for unknown tablets it will send requests to random nodes,
            // and definitely fetch the rest of the tablets.
            let mut total_tablets_with_feedback = 0;
            for values in value_lists.iter().chain(value_lists.iter()) {
                info!(
                    "First loop, trying key {:?}, token: {}",
                    values,
                    prepared.calculate_token(&values).unwrap().unwrap().value()
                );
                try_join_all(
                    (0..100).map(|_| async { session.execute_unpaged(&prepared, values).await }),
                )
                .await
                .unwrap();
                let feedbacks: usize = feedback_rxs.iter_mut().map(count_tablet_feedbacks).sum();
                if feedbacks > 0 {
                    total_tablets_with_feedback += 1;
                }
            }

            assert_eq!(total_tablets_with_feedback, TABLET_COUNT);

            // Now we must have info about all the tablets. It should not be
            // possible to receive any feedback if DefaultPolicy is properly
            // tablet-aware.
            for values in value_lists.iter() {
                info!(
                    "Second loop, trying key {:?}, token: {}",
                    values,
                    prepared.calculate_token(&values).unwrap().unwrap().value()
                );
                try_join_all(
                    (0..100).map(|_| async { session.execute_unpaged(&prepared, values).await }),
                )
                .await
                .unwrap();
                let feedbacks: usize = feedback_rxs.iter_mut().map(count_tablet_feedbacks).sum();
                assert_eq!(feedbacks, 0);
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

/// This test verifies that Scylla never sends tablet info when receiving unprepared
/// query - as it would make no sens to send it (driver can't do token awareness
/// for unprepared queries).
///
/// The test sends a query to each shard of every node and verifies that no
/// tablet info was sent in response.
#[cfg(not(scylla_cloud_tests))]
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_tablet_feedback_not_sent_for_unprepared_queries() {
    setup_tracing();
    const TABLET_COUNT: usize = 16;

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let session = scylla::SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .build()
                .await
                .unwrap();

            if !scylla::test_utils::scylla_supports_tablets(&session).await {
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
            send_unprepared_query_everywhere(
                &session,
                session.get_cluster_data().as_ref(),
                &Query::new(format!("INSERT INTO {ks}.t (a, b, c) VALUES (1, 1, 'abc')")),
            )
            .await
            .unwrap();

            let feedbacks: usize = feedback_rxs.iter_mut().map(count_tablet_feedbacks).sum();
            assert!(feedbacks == 0);

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
#[cfg(not(scylla_cloud_tests))]
#[tokio::test]
#[ntest::timeout(30000)]
#[ignore]
async fn test_lwt_optimization_works_with_tablets() {
    setup_tracing();
    const TABLET_COUNT: usize = 16;

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let session = scylla::SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .build()
                .await
                .unwrap();

            if !scylla::test_utils::scylla_supports_tablets(&session).await {
                tracing::warn!("Skipping test because this Scylla version doesn't support tablets");
                return running_proxy;
            }

            let ks = unique_keyspace_name();

            /* Prepare schema */
            prepare_schema(&session, &ks, "t", TABLET_COUNT).await;

            let tablets = get_tablets(&session, &ks, "t").await;

            let prepared_insert = session
                .prepare(format!(
                    "INSERT INTO {}.t (a, b, c) VALUES (?, ?, null)",
                    ks
                ))
                .await
                .unwrap();

            let prepared_lwt_update = session
                .prepare(format!(
                    "UPDATE {}.t SET c = ? WHERE a = ? and b = ? IF c != null",
                    ks
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
                send_statement_everywhere(
                    &session,
                    session.get_cluster_data().as_ref(),
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
