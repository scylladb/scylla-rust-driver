use std::sync::Arc;

use crate::utils::setup_tracing;
use crate::utils::test_with_3_node_cluster;

use futures::future::try_join_all;
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
use scylla::ExecutionProfile;
use scylla::QueryResult;
use scylla::{IntoTypedRows, Session};

use scylla_cql::errors::QueryError;
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

async fn get_tablets(session: &Session, ks: String, table: String) -> Vec<Tablet> {
    let cluster_data = session.get_cluster_data();
    let endpoints = cluster_data.get_nodes_info();
    for endpoint in endpoints.iter() {
        info!(
            "Endpoint id: {}, address: {}",
            endpoint.host_id,
            endpoint.address.ip()
        );
    }

    let selected_tablets_rows = session.query(
        "select last_token, replicas from system.tablets WHERE keyspace_name = ? and table_name = ? ALLOW FILTERING",
        &(ks.as_str(), table.as_str())).await.unwrap().rows.unwrap();

    let mut selected_tablets = selected_tablets_rows
        .into_typed::<SelectedTablet>()
        .map(|x| x.unwrap())
        .collect::<Vec<_>>();
    selected_tablets.sort_unstable_by(|a, b| a.last_token.cmp(&b.last_token));

    let mut tablets = Vec::new();
    let mut first_token = i64::MIN;
    for tablet in selected_tablets {
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
        first_token = tablet.last_token.wrapping_add(1);
        tablets.push(raw_tablet);
    }

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
        Box::new(std::iter::once((&self.target.0, self.target.1)))
    }

    fn name(&self) -> String {
        "SingleTargetLBP".to_owned()
    }
}

async fn send_statement_everywhere(
    session: &Session,
    cluster: &ClusterData,
    statement: &PreparedStatement,
    values: impl SerializeRow,
) -> Result<Vec<QueryResult>, QueryError> {
    let mut tasks = vec![];
    for node in cluster.get_nodes_info() {
        let shard_count: u16 = node.sharder().unwrap().nr_shards.into();
        for shard in 0..shard_count {
            let mut stmt = statement.clone();
            let values_ref = &values;
            let policy = SingleTargetLBP {
                target: (node.clone(), Some(shard as u32)),
            };
            let execution_profile = ExecutionProfile::builder()
                .load_balancing_policy(Arc::new(policy))
                .build();
            stmt.set_execution_profile_handle(Some(execution_profile.into_handle()));

            let task = async move { session.execute(&stmt, values_ref).await };

            tasks.push(task);
        }
    }

    try_join_all(tasks).await
}

async fn send_unprepared_query_everywhere(
    session: &Session,
    cluster: &ClusterData,
    query: &Query,
) -> Result<Vec<QueryResult>, QueryError> {
    let mut tasks = vec![];
    for node in cluster.get_nodes_info() {
        let shard_count: u16 = node.sharder().unwrap().nr_shards.into();
        for shard in 0..shard_count {
            let mut stmt = query.clone();
            let policy = SingleTargetLBP {
                target: (node.clone(), Some(shard as u32)),
            };
            let execution_profile = ExecutionProfile::builder()
                .load_balancing_policy(Arc::new(policy))
                .build();
            stmt.set_execution_profile_handle(Some(execution_profile.into_handle()));

            let task = async move { session.query(stmt, &()).await };

            tasks.push(task);
        }
    }

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

async fn prepare_schema(session: &Session, ks: &str, table: &str, tablet_count: usize) {
    session
        .query(
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
        .query(
            format!(
                "CREATE TABLE IF NOT EXISTS {}.{} (a int, b int, c text, primary key (a, b))",
                ks, table
            ),
            &[],
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ntest::timeout(30000)]
async fn test_tablet_awareness_works_with_full_info() {
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
            let ks = unique_keyspace_name();

            /* Prepare schema */
            prepare_schema(&session, &ks, "t", TABLET_COUNT).await;

            let tablets = get_tablets(&session, ks.clone(), "t".to_string()).await;

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

            // At first, driver has no info about tablets. If we send a statement
            // to each shard then we must get at least one tablet feedback - we are
            // running with 3-node cluster and keyspace has replication factor 2,
            // so even if each node has only one shard there must exist at least
            // one non-replica node.
            for values in value_lists.iter() {
                info!(
                    "First loop, trying key {:?}, token: {}",
                    values,
                    prepared.calculate_token(&values).unwrap().unwrap().value()
                );
                send_statement_everywhere(
                    &session,
                    session.get_cluster_data().as_ref(),
                    &prepared,
                    values,
                )
                .await
                .unwrap();
                let feedbacks: usize = feedback_rxs.iter_mut().map(count_tablet_feedbacks).sum();
                assert!(feedbacks > 0);
            }

            // Now we must have info about all the tablets. It should not be
            // possible to receive any feedback if DefaultPolicy is properly
            // tablet-aware.
            for values in value_lists.iter() {
                info!(
                    "Second loop, trying key {:?}, token: {}",
                    values,
                    prepared.calculate_token(&values).unwrap().unwrap().value()
                );
                try_join_all((0..100).map(|_| async { session.execute(&prepared, values).await }))
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

#[tokio::test]
#[ntest::timeout(30000)]
async fn test_tablet_feedback_received_with_default_policy() {
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
            let ks = unique_keyspace_name();

            /* Prepare schema */
            prepare_schema(&session, &ks, "t", TABLET_COUNT).await;

            let tablets = get_tablets(&session, ks.clone(), "t".to_string()).await;

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

            // We can't expect feedback for each query, because replica list may
            // be the same for given token in tablets and in token ring, but we
            // can expect to receive some feedbacks.
            let mut statements_with_feedback = 0;
            for values in value_lists.iter() {
                info!(
                    "First loop, trying key {:?}, token: {}",
                    values,
                    prepared.calculate_token(&values).unwrap().unwrap().value()
                );
                try_join_all((0..100).map(|_| async { session.execute(&prepared, values).await }))
                    .await
                    .unwrap();
                let feedbacks: usize = feedback_rxs.iter_mut().map(count_tablet_feedbacks).sum();
                if feedbacks > 0 {
                    statements_with_feedback += 1;
                }
            }

            // There is no special meaning to "3", I just thought it gives low
            // enough chances of failure.
            assert!(statements_with_feedback > (value_lists.len() / 3));

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

#[tokio::test]
#[ntest::timeout(30000)]
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
            let ks = unique_keyspace_name();

            /* Prepare schema */
            prepare_schema(&session, &ks, "t", TABLET_COUNT).await;

            let tablets = get_tablets(&session, ks.clone(), "t".to_string()).await;

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

            // At first, driver has no info about tablets. If we send a statement
            // to each shard then we must get at least one tablet feedback - we are
            // running with 3-node cluster and keyspace has replication factor 2,
            // so even if each node has only one shard there must exist at least
            // one non-replica node.
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
                    session.execute(&prepared_lwt_update, &("abc", a, b)).await
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
