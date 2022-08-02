use futures::{future::RemoteHandle, FutureExt};
use tracing::{error, trace};

use super::{ChildLoadBalancingPolicy, LoadBalancingPolicy, Plan, RoundRobinPolicy, Statement};
use crate::transport::{cluster::ClusterData, node::Node};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Debug)]
struct AtomicDuration(AtomicU64);

impl AtomicDuration {
    pub fn new() -> Self {
        Self(AtomicU64::new(u64::MAX))
    }

    pub fn store(&self, duration: Duration) {
        self.0.store(duration.as_micros() as u64, Ordering::Relaxed)
    }

    pub fn load(&self) -> Option<Duration> {
        let micros = self.0.load(Ordering::Relaxed);
        if micros == u64::MAX {
            None
        } else {
            Some(Duration::from_micros(micros))
        }
    }
}

/// A latency-aware load balancing policy.
#[derive(Debug)]
pub struct LatencyAwarePolicy {
    /// The exclusion threshold controls how much worse the average latency of a node must be
    /// compared to the fastest performing node for it to be penalised by the policy.
    /// For example, if set to 2, the resulting policy excludes nodes that are more than twice
    /// slower than the fastest node.
    pub exclusion_threshold: f64,

    /// The retry period defines how long a node may be penalised by the policy before it is given
    /// a 2nd chance. More precisely, a node is excluded from query plans if both his calculated
    /// average latency is [exclusion_threshold](Self::exclusion_threshold) times slower than
    /// the fastest node average latency (at the time the query plan is computed) **and** his
    /// calculated average latency has been updated since less than [retry_period](Self::retry_period).
    /// Since penalised nodes will likely not see their latency updated, this is basically how long
    /// the policy will exclude a node.
    pub retry_period: Duration,

    /// The update rate defines how often the minimum average latency is recomputed. While the
    /// average latency score of each node is computed iteratively (updated each time a new latency
    /// is collected), the minimum score needs to be recomputed from scratch every time, which is
    /// slightly more costly. For this reason, the minimum is only re-calculated at the given fixed
    /// rate and cached between re-calculation.
    /// The default update rate if **100 milliseconds**, which should be appropriate for most
    /// applications. In particular, note that while we want to avoid to recompute the minimum for
    /// every query, that computation is not particularly intensive either and there is no reason to
    /// use a very slow rate (more than second is probably unnecessarily slow for instance).
    pub update_rate: Duration,

    /// Penalising nodes is based on an average of their recently measured average latency.
    /// This average is only meaningful if a minimum of measurements have been collected.
    /// This is what this option controls. If fewer than [minimum_measurements](Self::minimum_measurements)
    /// data points have been collected for a given host, the policy will never penalise that host.
    /// Note that the number of collected measurements for a given host is reset if the node
    /// is restarted.
    /// The default for this option is **50**.
    pub minimum_measurements: usize,

    /// Last minimum average latency that was noted among the nodes. It is updated every
    /// [update_rate](Self::update_rate).
    last_min_latency: Arc<AtomicDuration>,

    child_policy: Box<dyn ChildLoadBalancingPolicy>,

    nodes: Arc<Mutex<Option<Vec<Arc<Node>>>>>,
    ask_for_nodes: Arc<AtomicBool>,

    _updater_handle: RemoteHandle<()>,
}

impl LatencyAwarePolicy {
    pub fn new(
        exclusion_threshold: f64,
        retry_period: Duration,
        update_rate: Duration,
        minimum_measurements: usize,
        child_policy: Box<dyn ChildLoadBalancingPolicy>,
    ) -> Self {
        let min_latency = Arc::new(AtomicDuration::new());
        let mut update_scheduler = tokio::time::interval(update_rate);
        let ask_for_nodes = Arc::new(AtomicBool::new(true));

        let ask_for_nodes_clone = ask_for_nodes.clone();
        let min_latency_clone = min_latency.clone();
        let nodes = Arc::new(Mutex::new(None));
        let nodes_clone = nodes.clone();

        let (updater_fut, updater_handle) = async move {
            loop {
                ask_for_nodes.store(true, Ordering::Relaxed);
                update_scheduler.tick().await;
                let nodes: Option<Vec<Arc<Node>>> = nodes.lock().unwrap().clone();
                if let Some(nodes) = nodes {
                    if nodes.is_empty() {
                        error!("Empty node list - driver/policy bug!");
                        return;
                    }

                    let min_avg = nodes
                        .iter()
                        .filter_map(|node| {
                            node.average_latency
                                .read()
                                .unwrap()
                                .map(|timestamped_average| timestamped_average.average)
                        })
                        .min();
                    if let Some(min_avg) = min_avg {
                        min_latency.store(min_avg);
                        trace!(
                            "LatencyAwarePolicy: updated min average latency to {} ms",
                            min_avg.as_secs_f64() * 1000.
                        );
                    }
                }
            }
        }
        .remote_handle();
        tokio::task::spawn(updater_fut);

        Self {
            exclusion_threshold,
            retry_period,
            update_rate,
            minimum_measurements,
            last_min_latency: min_latency_clone,
            child_policy,
            nodes: nodes_clone,
            ask_for_nodes: ask_for_nodes_clone,
            _updater_handle: updater_handle,
        }
    }

    fn refresh_last_min_avg_nodes(&self, nodes: &[Arc<Node>]) {
        if self.ask_for_nodes.load(Ordering::Relaxed)
            && self.ask_for_nodes.swap(false, Ordering::Relaxed)
        {
            *self.nodes.lock().unwrap() = Some(nodes.to_owned());
            trace!("LatencyAwarePolicy: updating nodes list for the min average updater");
        }
    }

    fn make_plan<'a>(&self, plan: Vec<Arc<Node>>) -> Plan<'a> {
        let min_avg_latency = match self.last_min_latency.load() {
            Some(min_avg) => min_avg,
            None => return self.child_policy.apply_child_policy(plan), // noop, as no latency data has been collected yet
        };

        Box::new(IteratorWithSkippedNodes::new(
            Box::new(plan.into_iter()),
            self.exclusion_threshold,
            self.retry_period,
            self.minimum_measurements,
            min_avg_latency,
            &*self.child_policy,
        ))
    }
}

impl Default for LatencyAwarePolicy {
    /// Default configuration of the [LatencyAwarePolicy] cycles through both lists
    /// (of fast and slow nodes) using round robin.
    fn default() -> Self {
        Self::new(
            2_f64,
            Duration::from_secs(10),
            Duration::from_millis(100),
            50,
            Box::<RoundRobinPolicy>::default(),
        )
    }
}

impl LoadBalancingPolicy for LatencyAwarePolicy {
    fn plan<'a>(&self, _statement: &Statement, cluster: &'a ClusterData) -> Plan<'a> {
        self.make_plan(cluster.all_nodes.clone())
    }

    fn name(&self) -> String {
        "LatencyAwarePolicy".to_string()
    }

    fn requires_latency_measurements(&self) -> bool {
        true
    }

    fn update_cluster_data(&self, cluster_data: &ClusterData) {
        self.refresh_last_min_avg_nodes(&cluster_data.all_nodes);
        self.child_policy.update_cluster_data(cluster_data);
    }
}

impl ChildLoadBalancingPolicy for LatencyAwarePolicy {
    fn apply_child_policy(
        &self,
        plan: Vec<Arc<Node>>,
    ) -> Box<dyn Iterator<Item = Arc<Node>> + Send + Sync> {
        self.make_plan(plan)
    }
}

struct IteratorWithSkippedNodes<'a> {
    fast_nodes: Plan<'a>,
    penalised_nodes: Plan<'a>,
}

impl<'a> IteratorWithSkippedNodes<'a> {
    fn new(
        nodes: Plan<'a>,
        exclusion_threshold: f64,
        retry_period: Duration,
        minimum_measurements: usize,
        min_avg: Duration,
        child_policy: &dyn ChildLoadBalancingPolicy,
    ) -> Self {
        enum FastEnough {
            Yes,
            No { average: Duration },
        }

        fn fast_enough(
            node: &Node,
            exclusion_threshold: f64,
            retry_period: Duration,
            minimum_measurements: usize,
            min_avg: Duration,
        ) -> FastEnough {
            let avg = match *node.average_latency.read().unwrap() {
                Some(avg) => avg,
                None => return FastEnough::Yes,
            };
            if avg.num_measures >= minimum_measurements
                && avg.timestamp.elapsed() < retry_period
                && avg.average.as_micros() as f64 > exclusion_threshold * min_avg.as_micros() as f64
            {
                FastEnough::No {
                    average: avg.average,
                }
            } else {
                FastEnough::Yes
            }
        }

        let mut fast_nodes = vec![];
        let mut penalised_nodes = vec![];

        for node in nodes {
            match fast_enough(
                &node,
                exclusion_threshold,
                retry_period,
                minimum_measurements,
                min_avg,
            ) {
                FastEnough::Yes => fast_nodes.push(node),
                FastEnough::No { average } => {
                    trace!("Penalising node {{address={}, datacenter={:?}, rack={:?}}} for being on average at least {} times slower (latency: {}ms) than the fastest ({}ms).",
                            node.address, node.datacenter, node.rack, exclusion_threshold, average.as_millis(), min_avg.as_millis());
                    penalised_nodes.push(node);
                }
            }
        }

        Self {
            fast_nodes: child_policy.apply_child_policy(fast_nodes),
            penalised_nodes: child_policy.apply_child_policy(penalised_nodes),
        }
    }
}

impl<'a> Iterator for IteratorWithSkippedNodes<'a> {
    type Item = Arc<Node>;

    fn next(&mut self) -> Option<Self::Item> {
        self.fast_nodes
            .next()
            .or_else(|| self.penalised_nodes.next())
    }
}

#[cfg(test)]
pub use tests::latency_aware_without_round_robin;
#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        load_balancing::{tests::DumbPolicy, TokenAwarePolicy},
        routing::Token,
        transport::{load_balancing::tests, node::TimestampedAverage},
    };
    use std::{collections::HashSet, time::Instant};

    pub fn latency_aware_without_round_robin() -> LatencyAwarePolicy {
        LatencyAwarePolicy::new(
            2_f64,
            Duration::from_secs(10),
            Duration::from_millis(100),
            50,
            Box::new(DumbPolicy {}),
        )
    }

    #[tokio::test]
    async fn latency_aware_policy_is_noop_if_no_latency_info_available_yet() {
        let policy = latency_aware_without_round_robin();
        let cluster = tests::mock_cluster_data_for_round_robin_and_latency_aware_tests().await;

        let plans = (0..16)
            .map(|_| {
                tests::get_plan_and_collect_node_identifiers(
                    &policy,
                    &tests::EMPTY_STATEMENT,
                    &cluster,
                )
            })
            .collect::<HashSet<_>>();

        let expected_plans = vec![vec![1, 2, 3, 4, 5]]
            .into_iter()
            .collect::<HashSet<Vec<_>>>();

        assert_eq!(expected_plans, plans);
    }

    #[tokio::test]
    async fn latency_aware_policy_does_not_penalise_if_not_enough_measurements() {
        let policy = latency_aware_without_round_robin();
        let mut cluster = tests::mock_cluster_data_for_round_robin_and_latency_aware_tests().await;

        let min_avg = Duration::from_millis(10);

        tests::set_nodes_latency_stats(
            &mut cluster,
            &[
                (
                    1,
                    Some(TimestampedAverage {
                        timestamp: Instant::now(),
                        average: Duration::from_secs_f64(
                            policy.exclusion_threshold * 1.5 * min_avg.as_secs_f64(),
                        ),
                        num_measures: policy.minimum_measurements - 1,
                    }),
                ),
                (
                    3,
                    Some(TimestampedAverage {
                        timestamp: Instant::now(),
                        average: min_avg,
                        num_measures: policy.minimum_measurements,
                    }),
                ),
            ],
        );

        let plans = (0..16)
            .map(|_| {
                tests::get_plan_and_collect_node_identifiers(
                    &policy,
                    &tests::EMPTY_STATEMENT,
                    &cluster,
                )
            })
            .collect::<HashSet<_>>();

        let expected_plans = vec![vec![1, 2, 3, 4, 5]]
            .into_iter()
            .collect::<HashSet<Vec<_>>>();

        assert_eq!(expected_plans, plans);
    }

    #[tokio::test]
    async fn latency_aware_policy_does_not_penalise_if_exclusion_threshold_not_crossed() {
        let policy = latency_aware_without_round_robin();
        let mut cluster = tests::mock_cluster_data_for_round_robin_and_latency_aware_tests().await;

        let min_avg = Duration::from_millis(10);

        tests::set_nodes_latency_stats(
            &mut cluster,
            &[
                (
                    1,
                    Some(TimestampedAverage {
                        timestamp: Instant::now(),
                        average: Duration::from_secs_f64(
                            policy.exclusion_threshold * 0.95 * min_avg.as_secs_f64(),
                        ),
                        num_measures: policy.minimum_measurements,
                    }),
                ),
                (
                    3,
                    Some(TimestampedAverage {
                        timestamp: Instant::now(),
                        average: min_avg,
                        num_measures: policy.minimum_measurements,
                    }),
                ),
            ],
        );

        let plans = (0..16)
            .map(|_| {
                tests::get_plan_and_collect_node_identifiers(
                    &policy,
                    &tests::EMPTY_STATEMENT,
                    &cluster,
                )
            })
            .collect::<HashSet<_>>();

        let expected_plans = vec![vec![1, 2, 3, 4, 5]]
            .into_iter()
            .collect::<HashSet<Vec<_>>>();

        assert_eq!(expected_plans, plans);
    }

    #[tokio::test]
    async fn latency_aware_policy_does_not_penalise_if_retry_period_expired() {
        let policy = LatencyAwarePolicy::new(
            2.,
            Duration::from_millis(10),
            Duration::from_millis(10),
            20,
            Box::new(DumbPolicy {}),
        );
        let mut cluster = tests::mock_cluster_data_for_round_robin_and_latency_aware_tests().await;

        let min_avg = Duration::from_millis(10);

        tests::set_nodes_latency_stats(
            &mut cluster,
            &[
                (
                    1,
                    Some(TimestampedAverage {
                        timestamp: Instant::now(),
                        average: Duration::from_secs_f64(
                            policy.exclusion_threshold * 1.5 * min_avg.as_secs_f64(),
                        ),
                        num_measures: policy.minimum_measurements,
                    }),
                ),
                (
                    3,
                    Some(TimestampedAverage {
                        timestamp: Instant::now(),
                        average: min_avg,
                        num_measures: policy.minimum_measurements,
                    }),
                ),
            ],
        );

        tokio::time::sleep(2 * policy.retry_period).await;

        let plans = (0..16)
            .map(|_| {
                tests::get_plan_and_collect_node_identifiers(
                    &policy,
                    &tests::EMPTY_STATEMENT,
                    &cluster,
                )
            })
            .collect::<HashSet<_>>();

        let expected_plans = vec![vec![1, 2, 3, 4, 5]]
            .into_iter()
            .collect::<HashSet<Vec<_>>>();

        assert_eq!(expected_plans, plans);
    }

    #[tokio::test]
    async fn latency_aware_policy_penalises_if_conditions_met() {
        let policy = latency_aware_without_round_robin();
        let mut cluster = tests::mock_cluster_data_for_round_robin_and_latency_aware_tests().await;

        let min_avg = Duration::from_millis(10);

        tests::set_nodes_latency_stats(
            &mut cluster,
            &[
                (
                    1,
                    Some(TimestampedAverage {
                        timestamp: Instant::now(),
                        average: Duration::from_secs_f64(
                            policy.exclusion_threshold * 1.05 * min_avg.as_secs_f64(),
                        ),
                        num_measures: policy.minimum_measurements,
                    }),
                ),
                (
                    3,
                    Some(TimestampedAverage {
                        timestamp: Instant::now(),
                        average: min_avg,
                        num_measures: policy.minimum_measurements,
                    }),
                ),
            ],
        );

        // Await last min average updater.
        policy.refresh_last_min_avg_nodes(&cluster.all_nodes);
        tokio::time::sleep(policy.update_rate).await;

        let plans = (0..16)
            .map(|_| {
                tests::get_plan_and_collect_node_identifiers(
                    &policy,
                    &tests::EMPTY_STATEMENT,
                    &cluster,
                )
            })
            .collect::<HashSet<_>>();

        let expected_plans = vec![vec![2, 3, 4, 5, 1]]
            .into_iter()
            .collect::<HashSet<Vec<_>>>();

        assert_eq!(expected_plans, plans);
    }

    #[tokio::test]
    async fn latency_aware_policy_by_default_performs_round_robin() {
        let policy = LatencyAwarePolicy::default();
        let mut cluster = tests::mock_cluster_data_for_round_robin_and_latency_aware_tests().await;

        let min_avg = Duration::from_millis(10);

        tests::set_nodes_latency_stats(
            &mut cluster,
            &[
                (
                    1,
                    Some(TimestampedAverage {
                        timestamp: Instant::now(),
                        average: Duration::from_secs_f64(
                            policy.exclusion_threshold * 1.05 * min_avg.as_secs_f64(),
                        ),
                        num_measures: policy.minimum_measurements,
                    }),
                ),
                (
                    3,
                    Some(TimestampedAverage {
                        timestamp: Instant::now(),
                        average: min_avg,
                        num_measures: policy.minimum_measurements,
                    }),
                ),
            ],
        );

        // Await last min average updater.
        policy.refresh_last_min_avg_nodes(&cluster.all_nodes);
        tokio::time::sleep(policy.update_rate).await;

        let plans = (0..16)
            .map(|_| {
                tests::get_plan_and_collect_node_identifiers(
                    &policy,
                    &tests::EMPTY_STATEMENT,
                    &cluster,
                )
            })
            .collect::<HashSet<_>>();

        let expected_plans = vec![
            vec![2, 3, 4, 5, 1],
            vec![3, 4, 5, 2, 1],
            vec![4, 5, 2, 3, 1],
            vec![5, 2, 3, 4, 1],
        ]
        .into_iter()
        .collect::<HashSet<Vec<_>>>();

        assert_eq!(expected_plans, plans);
    }

    #[tokio::test]
    async fn latency_aware_policy_stops_penalising_after_min_average_increases_enough_only_after_update_rate_elapses(
    ) {
        let policy = latency_aware_without_round_robin();
        let mut cluster = tests::mock_cluster_data_for_round_robin_and_latency_aware_tests().await;

        let min_avg = Duration::from_millis(10);

        {
            // min_avg is low enough to penalise node 1
            tests::set_nodes_latency_stats(
                &mut cluster,
                &[
                    (
                        1,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: Duration::from_secs_f64(
                                policy.exclusion_threshold * 1.05 * min_avg.as_secs_f64(),
                            ),
                            num_measures: policy.minimum_measurements,
                        }),
                    ),
                    (
                        3,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: min_avg,
                            num_measures: policy.minimum_measurements,
                        }),
                    ),
                ],
            );

            // Await last min average updater.
            policy.refresh_last_min_avg_nodes(&cluster.all_nodes);
            tokio::time::sleep(policy.update_rate).await;

            let plans = (0..16)
                .map(|_| {
                    tests::get_plan_and_collect_node_identifiers(
                        &policy,
                        &tests::EMPTY_STATEMENT,
                        &cluster,
                    )
                })
                .collect::<HashSet<_>>();

            let expected_plans = vec![vec![2, 3, 4, 5, 1]]
                .into_iter()
                .collect::<HashSet<Vec<_>>>();

            assert_eq!(expected_plans, plans);
        }
        // node 3 becomes as slow as node 1
        tests::set_nodes_latency_stats(
            &mut cluster,
            &[(
                3,
                Some(TimestampedAverage {
                    timestamp: Instant::now(),
                    average: Duration::from_secs_f64(
                        policy.exclusion_threshold * min_avg.as_secs_f64(),
                    ),
                    num_measures: policy.minimum_measurements,
                }),
            )],
        );
        {
            // min_avg is still low, because update_rate has not elapsed yet
            let plans = (0..16)
                .map(|_| {
                    tests::get_plan_and_collect_node_identifiers(
                        &policy,
                        &tests::EMPTY_STATEMENT,
                        &cluster,
                    )
                })
                .collect::<HashSet<_>>();

            let expected_plans = vec![vec![2, 3, 4, 5, 1]]
                .into_iter()
                .collect::<HashSet<Vec<_>>>();

            assert_eq!(expected_plans, plans);
        }

        tokio::time::sleep(policy.update_rate).await;
        {
            // min_avg has been updated and is already high enough to stop penalising node 1
            let plans = (0..16)
                .map(|_| {
                    tests::get_plan_and_collect_node_identifiers(
                        &policy,
                        &tests::EMPTY_STATEMENT,
                        &cluster,
                    )
                })
                .collect::<HashSet<_>>();

            let expected_plans = vec![vec![1, 2, 3, 4, 5]]
                .into_iter()
                .collect::<HashSet<Vec<_>>>();

            assert_eq!(expected_plans, plans);
        }
    }

    // ConnectionKeeper (which lives in Node) requires context of Tokio runtime
    #[tokio::test]
    async fn test_token_and_latency_aware_policy() {
        let _ = tracing_subscriber::fmt::try_init();
        let cluster = tests::mock_cluster_data_for_token_aware_tests().await;

        struct Test<'a, 'b> {
            statement: Statement<'a>,
            expected_plan: Vec<u16>,
            preset_min_avg: Option<Duration>,
            latency_stats: &'b [(u16, Option<TimestampedAverage>)],
        }

        let latency_aware_policy_defaults = LatencyAwarePolicy::default();
        let min_avg = Duration::from_millis(10);
        let tests = [
            Test {
                // Latency-aware policy fires up and moves 3 past 1.
                statement: Statement {
                    token: Some(Token { value: 160 }),
                    keyspace: Some("keyspace_with_simple_strategy_replication_factor_2"),
                    is_confirmed_lwt: false,
                },
                latency_stats: &[
                    (
                        1,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: min_avg,
                            num_measures: latency_aware_policy_defaults.minimum_measurements,
                        }),
                    ),
                    (
                        3,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: Duration::from_secs_f64(
                                latency_aware_policy_defaults.exclusion_threshold
                                    * 1.05
                                    * min_avg.as_secs_f64(),
                            ),
                            num_measures: latency_aware_policy_defaults.minimum_measurements,
                        }),
                    ),
                ],
                preset_min_avg: None,
                expected_plan: vec![1, 3, 2],
            },
            Test {
                // Latency-aware policy has old minimum average cached, so does not fire.
                statement: Statement {
                    token: Some(Token { value: 160 }),
                    keyspace: Some("keyspace_with_simple_strategy_replication_factor_2"),
                    is_confirmed_lwt: false,
                },
                latency_stats: &[
                    (
                        1,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: min_avg,
                            num_measures: latency_aware_policy_defaults.minimum_measurements,
                        }),
                    ),
                    (
                        3,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: Duration::from_secs_f64(
                                latency_aware_policy_defaults.exclusion_threshold
                                    * 1.05
                                    * min_avg.as_secs_f64(),
                            ),
                            num_measures: latency_aware_policy_defaults.minimum_measurements,
                        }),
                    ),
                ],
                preset_min_avg: Some(100 * min_avg),
                expected_plan: vec![3, 1, 2],
            },
            Test {
                // Both 1 and 2 are way slower than 3, but only 2 has enough measurements collected, and 2, 3 do not replicate data for this token.
                statement: Statement {
                    token: Some(Token { value: 60 }),
                    keyspace: Some("keyspace_with_simple_strategy_replication_factor_1"),
                    is_confirmed_lwt: false,
                },
                latency_stats: &[
                    (
                        1,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: min_avg * 20,
                            num_measures: latency_aware_policy_defaults.minimum_measurements,
                        }),
                    ),
                    (
                        2,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: min_avg * 10,
                            num_measures: latency_aware_policy_defaults.minimum_measurements,
                        }),
                    ),
                    (
                        3,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: min_avg,
                            num_measures: latency_aware_policy_defaults.minimum_measurements,
                        }),
                    ),
                ],
                preset_min_avg: None,
                expected_plan: vec![1, 3, 2],
            },
            Test {
                // Both 1 and 2 are way slower than 3, and both have enough measurements collected,
                // but because 3 does not replicate data for this token, they are placed first.
                statement: Statement {
                    token: Some(Token { value: 60 }),
                    keyspace: Some("keyspace_with_simple_strategy_replication_factor_2"),
                    is_confirmed_lwt: false,
                },
                latency_stats: &[
                    (
                        1,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: min_avg * 20,
                            num_measures: latency_aware_policy_defaults.minimum_measurements,
                        }),
                    ),
                    (
                        2,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: min_avg * 10,
                            num_measures: latency_aware_policy_defaults.minimum_measurements,
                        }),
                    ),
                    (
                        3,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: min_avg,
                            num_measures: latency_aware_policy_defaults.minimum_measurements,
                        }),
                    ),
                ],
                preset_min_avg: None,
                expected_plan: vec![1, 2, 3],
            },
            Test {
                // No latency stats, so latency-aware policy is a no-op.
                statement: Statement {
                    token: Some(Token { value: 60 }),
                    keyspace: Some("invalid"),
                    is_confirmed_lwt: false,
                },
                latency_stats: &[],
                preset_min_avg: None,
                expected_plan: vec![1, 3, 2],
            },
            Test {
                // 3 is penalised over 2 (in token-aware policy fallback plan) as being much slower than 1.
                statement: Statement {
                    token: Some(Token { value: 60 }),
                    keyspace: None,
                    is_confirmed_lwt: false,
                },
                latency_stats: &[
                    (
                        1,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: min_avg,
                            num_measures: latency_aware_policy_defaults.minimum_measurements,
                        }),
                    ),
                    (
                        3,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: Duration::from_secs_f64(
                                latency_aware_policy_defaults.exclusion_threshold
                                    * 1.05
                                    * min_avg.as_secs_f64(),
                            ),
                            num_measures: latency_aware_policy_defaults.minimum_measurements,
                        }),
                    ),
                ],
                preset_min_avg: None,
                expected_plan: vec![1, 2, 3],
            },
        ];

        for test in &tests {
            let policy = TokenAwarePolicy::new(Box::new(latency_aware_without_round_robin()));
            let mut cluster = cluster.clone();

            if let Some(preset_min_avg) = test.preset_min_avg {
                tests::set_nodes_latency_stats(
                    &mut cluster,
                    &[(
                        1,
                        Some(TimestampedAverage {
                            timestamp: Instant::now(),
                            average: preset_min_avg,
                            num_measures: latency_aware_policy_defaults.minimum_measurements,
                        }),
                    )],
                );
                policy.update_cluster_data(&cluster);
                // Await last min average updater.
                tokio::time::sleep(latency_aware_policy_defaults.update_rate).await;
                tests::set_nodes_latency_stats(&mut cluster, &[(1, None)]);
            }
            tests::set_nodes_latency_stats(&mut cluster, test.latency_stats);

            if test.preset_min_avg.is_none() {
                policy.update_cluster_data(&cluster);
                // Await last min average updater.
                tokio::time::sleep(latency_aware_policy_defaults.update_rate).await;
            }

            let plan =
                tests::get_plan_and_collect_node_identifiers(&policy, &test.statement, &cluster);
            assert_eq!(plan, test.expected_plan);
        }
    }
}
