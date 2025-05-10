use std::{collections::HashMap, sync::Arc};

use assert_matches::assert_matches;
use scylla::{
    errors::{DbError, PrepareError, RequestAttemptError},
    policies::load_balancing::{NodeIdentifier, SingleTargetLoadBalancingPolicy},
};
use scylla_proxy::{
    Condition, ProxyError, Reaction as _, RequestFrame, RequestOpcode, RequestReaction,
    RequestRule, RunningProxy, ShardAwareness, TargetShard, WorkerError,
};
use tokio::sync::mpsc;
use tracing::{debug, info};

use crate::utils::{setup_tracing, test_with_3_node_cluster};

/// Counts number of feedbacks, aggregated per shard.
/// Unknown shards feedbacks are counted too.
fn count_per_shard_feedbacks(
    rx: &mut mpsc::UnboundedReceiver<(RequestFrame, Option<TargetShard>)>,
) -> HashMap<Option<TargetShard>, usize> {
    let per_shard_feedback_counts = std::iter::from_fn(|| rx.try_recv().ok())
        .map(|(_frame, shard)| shard)
        .fold(HashMap::new(), |mut counts, shard| {
            counts
                .entry(shard)
                .and_modify(|count| *count += 1)
                .or_insert(1);
            counts
        });

    debug!(
        "Got per_shard_feedback_counts: {:#?}",
        per_shard_feedback_counts
    );

    per_shard_feedback_counts
}

/// Asserts that there was exactly one preparation attempt on every node.
fn assert_preparation_attempted_exactly_once_on_each_node(
    rxs: [mpsc::UnboundedReceiver<(RequestFrame, Option<TargetShard>)>; 3],
) {
    let per_shard_feedbacks_counts = rxs.map(|mut rx| count_per_shard_feedbacks(&mut rx));

    for (node_num, per_shard_feedbacks_count) in per_shard_feedbacks_counts.into_iter().enumerate()
    {
        let preparation_attempts_to_node: usize = per_shard_feedbacks_count.values().sum();
        assert_eq!(
            1, preparation_attempts_to_node,
            "Unexpected number of preparation attempts on node {}: expected 1, got {}",
            node_num, preparation_attempts_to_node,
        );
    }
}

/// Asserts that there was at least one preparation attempt on every shard.
fn assert_preparation_attempted_on_all_shards(
    rxs: [mpsc::UnboundedReceiver<(RequestFrame, Option<TargetShard>)>; 3],
) {
    let per_shard_feedbacks_counts = rxs.map(|mut rx| count_per_shard_feedbacks(&mut rx));
    for (node_num, per_shard_feedbacks_count) in per_shard_feedbacks_counts.into_iter().enumerate()
    {
        if per_shard_feedbacks_count.contains_key(&None) {
            // We are shard-unaware. This is most likely due to Cassandra being our DB, not ScyllaDB.
            // In such case, nothing to check here.
        } else {
            // We are shard-aware. Then let's make sure that preparation was attempted on all shards.
            for (shard, preparation_attempts_to_shard) in per_shard_feedbacks_count
                .into_iter()
                .filter_map(|(shard, count)| shard.map(|shard| (shard, count)))
            {
                assert!(
                    preparation_attempts_to_shard >= 1,
                    "Preparation was not attempted on node {}, shard {}",
                    node_num,
                    shard
                );
            }
        }
    }
}

/// Assumptions:
/// - Preparation is expected to succeed iff at least one shard sends a successful PREPARED response.
/// - If preparation succeeds, the statement is executed on all nodes and this way repreparation logic is additionally checked.
///
/// Outline:
/// 1. All nodes enabled -> preparation succeeds.
/// 2. One or two nodes fully disabled -> preparation succeeds.
/// 3. All three nodes fully disabled -> preparation fails, and we assert that all shards were attempted.
/// 4. All three nodes disabled once (simulation of only part of shards broken) -> preparation succeeds,
///     and we assert that all shards were attempted.
///
#[cfg_attr(scylla_cloud_tests, ignore)]
#[tokio::test]
#[ntest::timeout(30000)]
async fn test_preparation_logic() {
    setup_tracing();

    const STATEMENT: &str = "SELECT host_id FROM system.local WHERE key='local'";

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let session = scylla::client::session_builder::SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .build()
                .await
                .unwrap();
            let cluster_state = session.get_cluster_state();

            let expect_success = || async {
                let mut prepared = session.prepare(STATEMENT).await.unwrap();

                session.execute_unpaged(&prepared, ()).await.unwrap();
                for node in cluster_state.get_nodes_info() {
                    prepared.set_load_balancing_policy(Some(SingleTargetLoadBalancingPolicy::new(
                        NodeIdentifier::Node(Arc::clone(node)),
                        None,
                    )));
                }
            };
            let expect_failure = || async {
                assert_matches!(
                    session.prepare(STATEMENT).await,
                    Err(PrepareError::AllAttemptsFailed {
                        first_attempt: RequestAttemptError::DbError(DbError::ServerError, _)
                    })
                );
            };

            struct NodeConfig {
                /// How many times should we the node fail preparation until it finally succeeds.
                prepare_rejections_count: usize,
            }

            /// Configures proxy so that nodes reject preparation requested number of times, then always succeed.
            fn configure_proxy(
                running_proxy: &mut RunningProxy,
                node_config: [NodeConfig; 3],
            ) -> [mpsc::UnboundedReceiver<(RequestFrame, Option<TargetShard>)>; 3] {
                let (feedback_txs, feedback_rxs): (Vec<_>, Vec<_>) = (0..3)
                    .map(|_| mpsc::unbounded_channel::<(RequestFrame, Option<TargetShard>)>())
                    .unzip();

                running_proxy
                    .running_nodes
                    .iter_mut()
                    .zip(node_config.iter().zip(feedback_txs))
                    .for_each(|(node, (config, tx))| {
                        let noncontrol_prepare_condition = || {
                            Condition::RequestOpcode(RequestOpcode::Prepare)
                                .and(Condition::not(Condition::ConnectionRegisteredAnyEvent))
                        };
                        let accept_rule = || {
                            RequestRule(
                                noncontrol_prepare_condition(),
                                RequestReaction::noop().with_feedback_when_performed(tx.clone()),
                            )
                        };
                        let reject_rule = |rejections| {
                            RequestRule(
                                noncontrol_prepare_condition()
                                    .and(Condition::TrueForLimitedTimes(rejections)),
                                RequestReaction::forge()
                                    .server_error()
                                    .with_feedback_when_performed(tx.clone()),
                            )
                        };
                        let rules = match config.prepare_rejections_count {
                            0 => vec![accept_rule()],
                            n => {
                                vec![reject_rule(n), accept_rule()]
                            }
                        };

                        node.change_request_rules(Some(rules));
                    });

                feedback_rxs.try_into().unwrap()
            }

            // 1. All nodes enabled -> preparation succeeds, and we assert that each node was attempted preparation only once.
            {
                info!("Test case 1: All nodes enabled.");
                let rxs = configure_proxy(
                    &mut running_proxy,
                    [0, 0, 0].map(|n| NodeConfig {
                        prepare_rejections_count: n,
                    }),
                );
                expect_success().await;
                assert_preparation_attempted_exactly_once_on_each_node(rxs);
            }

            // 2. One or two nodes fully disabled -> preparation succeeds, and we assert that each node was attempted preparation only once.
            {
                info!("Test case 2: Two nodes disabled.");
                let rxs = configure_proxy(
                    &mut running_proxy,
                    [usize::MAX, usize::MAX, 0].map(|n| NodeConfig {
                        prepare_rejections_count: n,
                    }),
                );
                expect_success().await;
                assert_preparation_attempted_exactly_once_on_each_node(rxs);
            }

            // 3. All three nodes fully disabled -> preparation fails, and we assert that all shards were attempted.
            {
                info!("Test case 3: All nodes disabled.");
                let rxs = configure_proxy(
                    &mut running_proxy,
                    [usize::MAX, usize::MAX, usize::MAX].map(|n| NodeConfig {
                        prepare_rejections_count: n,
                    }),
                );
                expect_failure().await;
                assert_preparation_attempted_on_all_shards(rxs);
            }

            // 4. All three nodes disabled once (simulation of only part of shards broken) -> preparation succeeds,
            //    and we assert that all shards per each node were attempted.
            {
                info!("Test case 4: Simulated part of shards disabled on every node.");
                let rxs = configure_proxy(
                    &mut running_proxy,
                    [1, 1, 1].map(|n| NodeConfig {
                        prepare_rejections_count: n,
                    }),
                );
                expect_success().await;
                assert_preparation_attempted_on_all_shards(rxs);
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
