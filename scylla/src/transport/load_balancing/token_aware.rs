use super::{ChildLoadBalancingPolicy, LoadBalancingPolicy, Plan, Statement};
use crate::routing::Token;
use crate::transport::topology::Strategy;
use crate::transport::{cluster::ClusterData, node::Node};
use itertools::Itertools;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::{collections::HashMap, sync::Arc};
use tracing::trace;

/// A wrapper load balancing policy that adds token awareness to a child policy.
#[derive(Debug)]
pub struct TokenAwarePolicy {
    child_policy: Box<dyn ChildLoadBalancingPolicy>,
}

impl TokenAwarePolicy {
    pub fn new(child_policy: Box<dyn ChildLoadBalancingPolicy>) -> Self {
        Self { child_policy }
    }

    fn simple_strategy_replicas(
        cluster: &ClusterData,
        token: &Token,
        replication_factor: usize,
    ) -> Vec<Arc<Node>> {
        cluster
            .ring_range(token)
            .unique()
            .take(replication_factor)
            .collect()
    }

    fn network_topology_strategy_replicas(
        cluster: &ClusterData,
        token: &Token,
        datacenter_repfactors: &HashMap<String, usize>,
    ) -> Vec<Arc<Node>> {
        let mut acceptable_repeats = datacenter_repfactors
            .iter()
            .map(|(dc_name, repfactor)| {
                let rack_count = cluster
                    .datacenters
                    .get(dc_name)
                    .map(|dc| dc.rack_count)
                    .unwrap_or(0);

                (dc_name.as_str(), repfactor.saturating_sub(rack_count))
            })
            .collect::<HashMap<&str, usize>>();

        let desired_result_len: usize = datacenter_repfactors.values().sum();

        let mut result: Vec<Arc<Node>> = Vec::with_capacity(desired_result_len);
        for node in cluster.ring_range(token).unique() {
            let current_node_dc = match &node.datacenter {
                None => continue,
                Some(dc) => dc,
            };

            let repfactor = match datacenter_repfactors.get(current_node_dc) {
                None => continue,
                Some(r) => r,
            };

            let picked_nodes_from_current_dc = || {
                result
                    .iter()
                    .filter(|node| node.datacenter.as_ref() == Some(current_node_dc))
            };

            if *repfactor == picked_nodes_from_current_dc().count() {
                // found enough nodes in this datacenter
                continue;
            }

            let current_node_rack = node.rack.as_ref();
            let current_node_rack_count = picked_nodes_from_current_dc()
                .filter(|node| node.rack.as_ref() == current_node_rack)
                .count();

            if current_node_rack_count == 0 {
                // new rack
                result.push(node.clone());
            } else {
                // we’ve already found a node in this rack

                // unwrap, because we already know repfactor
                let repeats = acceptable_repeats
                    .get_mut(current_node_dc.as_str())
                    .unwrap();
                if *repeats > 0 {
                    // we must pick multiple nodes in the same rack
                    *repeats -= 1;
                    result.push(node.clone());
                }
            }

            if result.len() == desired_result_len {
                break;
            }
        }

        result
    }

    pub(crate) fn replicas_for_token(
        cluster: &ClusterData,
        token: &Token,
        keyspace_name: Option<&str>,
    ) -> Vec<Arc<Node>> {
        let keyspace = keyspace_name.and_then(|k| cluster.keyspaces.get(k));

        let strategy = keyspace.map(|k| &k.strategy);

        match strategy {
            Some(Strategy::SimpleStrategy { replication_factor }) => {
                Self::simple_strategy_replicas(cluster, token, *replication_factor)
            }
            Some(Strategy::NetworkTopologyStrategy {
                datacenter_repfactors,
            }) => Self::network_topology_strategy_replicas(cluster, token, datacenter_repfactors),
            _ => {
                // default to simple strategy with replication factor = 1
                let replication_factor = 1;
                Self::simple_strategy_replicas(cluster, token, replication_factor)
            }
        }
    }
}

impl LoadBalancingPolicy for TokenAwarePolicy {
    fn plan<'a>(&self, statement: &Statement, cluster: &'a ClusterData) -> Plan<'a> {
        match statement.token {
            Some(token) => {
                let replicas = Self::replicas_for_token(cluster, &token, statement.keyspace);
                trace!(
                    token = token.value,
                    replicas = replicas
                        .iter()
                        .map(|node| node.address.to_string())
                        .collect::<Vec<String>>()
                        .join(",")
                        .as_str(),
                    "TokenAware"
                );

                let fallback_plan = {
                    let replicas_set: HashSet<SocketAddr> =
                        replicas.iter().map(|node| node.address).collect();

                    self.child_policy
                        .plan(&Statement::empty(), cluster)
                        .filter(move |node| !replicas_set.contains(&node.address))
                };

                let plan = self
                    .child_policy
                    .apply_child_policy(replicas)
                    .chain(fallback_plan);
                Box::new(plan)
            }
            // fallback to child policy
            None => {
                trace!("TokenAware: falling back to child policy, no token present");
                self.child_policy.plan(statement, cluster)
            }
        }
    }

    fn name(&self) -> String {
        format!(
            "TokenAwarePolicy{{child_policy: {}}}",
            self.child_policy.name()
        )
    }

    fn requires_latency_measurements(&self) -> bool {
        self.child_policy.requires_latency_measurements()
    }

    fn update_cluster_data(&self, cluster_data: &ClusterData) {
        self.child_policy.update_cluster_data(cluster_data);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::load_balancing::tests::DumbPolicy;
    use crate::transport::load_balancing::tests;
    use crate::transport::topology::Keyspace;
    use crate::transport::topology::Metadata;
    use crate::transport::topology::Peer;
    use crate::transport::topology::Strategy;
    use std::collections::HashMap;

    // ConnectionKeeper (which lives in Node) requires context of Tokio runtime
    #[tokio::test]
    async fn test_token_aware_policy() {
        let cluster = tests::mock_cluster_data_for_token_aware_tests();

        struct Test<'a> {
            statement: Statement<'a>,
            expected_plan: Vec<u16>,
        }

        let tests = [
            Test {
                statement: Statement {
                    token: Some(Token { value: 160 }),
                    keyspace: Some("keyspace_with_simple_strategy_replication_factor_2"),
                },
                expected_plan: vec![3, 1],
            },
            Test {
                statement: Statement {
                    token: Some(Token { value: 60 }),
                    keyspace: Some("keyspace_with_simple_strategy_replication_factor_3"),
                },
                expected_plan: vec![1, 2, 3],
            },
            Test {
                statement: Statement {
                    token: Some(Token { value: 500 }),
                    keyspace: Some("keyspace_with_simple_strategy_replication_factor_3"),
                },
                expected_plan: vec![1, 2, 3],
            },
            Test {
                statement: Statement {
                    token: Some(Token { value: 60 }),
                    keyspace: Some("invalid"),
                },
                expected_plan: vec![1],
            },
            Test {
                statement: Statement {
                    token: Some(Token { value: 60 }),
                    keyspace: None,
                },
                expected_plan: vec![1],
            },
        ];

        for test in &tests {
            let policy = TokenAwarePolicy::new(Box::new(DumbPolicy {}));

            let plan =
                tests::get_plan_and_collect_node_identifiers(&policy, &test.statement, &cluster);
            assert_eq!(plan, test.expected_plan);
        }
    }

    #[tokio::test]
    async fn test_token_aware_policy_with_nts() {
        let cluster = mock_cluster_data_for_nts_token_aware_tests();

        let policy = TokenAwarePolicy::new(Box::new(DumbPolicy {}));

        let statement = Statement {
            token: Some(Token { value: 0 }),
            keyspace: Some("keyspace_with_nts"),
        };

        let plan = tests::get_plan_and_collect_node_identifiers(&policy, &statement, &cluster);
        let expected_plan = vec![1, 5, 6, 4, 8];
        assert_eq!(plan, expected_plan);
    }

    #[tokio::test]
    async fn test_token_aware_fallback_policy() {
        let cluster = tests::mock_cluster_data_for_token_aware_tests();

        let policy = TokenAwarePolicy::new(Box::new(DumbPolicy {}));

        // Returned plan should have 0 length, as DumbPolicy's plan() returns empty iterator
        let plan = tests::get_plan_and_collect_node_identifiers(
            &policy,
            &tests::EMPTY_STATEMENT,
            &cluster,
        );
        assert_eq!(plan.len(), 0);
    }

    // creates ClusterData with info about 8 nodes living in two different datacenters
    //
    // ring field is populated as follows:
    // ring tokens:            50 100 150 200 250 300 400 500 510
    // corresponding node ids: 1  5   2   1   6   4   8   7   3
    //
    // datacenter:       waw
    // nodes in rack r1: 1 2
    // nodes in rack r2: 3 4
    //
    // datacenter:       her
    // nodes in rack r3: 5 6
    // nodes in rack r4: 7 8
    fn mock_cluster_data_for_nts_token_aware_tests() -> ClusterData {
        let peers = [
            Peer {
                datacenter: Some("waw".into()),
                rack: Some("r1".into()),
                address: tests::id_to_invalid_addr(1),
                tokens: vec![Token { value: 50 }, Token { value: 200 }],
                untranslated_address: Some(tests::id_to_invalid_addr(1)),
                up: true,
            },
            Peer {
                datacenter: Some("waw".into()),
                rack: Some("r1".into()),
                address: tests::id_to_invalid_addr(2),
                tokens: vec![Token { value: 150 }],
                untranslated_address: Some(tests::id_to_invalid_addr(2)),
                up: true,
            },
            Peer {
                datacenter: Some("waw".into()),
                rack: Some("r2".into()),
                address: tests::id_to_invalid_addr(3),
                tokens: vec![Token { value: 510 }],
                untranslated_address: Some(tests::id_to_invalid_addr(3)),
                up: true,
            },
            Peer {
                datacenter: Some("waw".into()),
                rack: Some("r2".into()),
                address: tests::id_to_invalid_addr(4),
                tokens: vec![Token { value: 300 }],
                untranslated_address: Some(tests::id_to_invalid_addr(4)),
                up: true,
            },
            Peer {
                datacenter: Some("her".into()),
                rack: Some("r3".into()),
                address: tests::id_to_invalid_addr(5),
                tokens: vec![Token { value: 100 }],
                untranslated_address: Some(tests::id_to_invalid_addr(5)),
                up: true,
            },
            Peer {
                datacenter: Some("her".into()),
                rack: Some("r3".into()),
                address: tests::id_to_invalid_addr(6),
                tokens: vec![Token { value: 250 }],
                untranslated_address: Some(tests::id_to_invalid_addr(6)),
                up: true,
            },
            Peer {
                datacenter: Some("her".into()),
                rack: Some("r4".into()),
                address: tests::id_to_invalid_addr(7),
                tokens: vec![Token { value: 500 }],
                untranslated_address: Some(tests::id_to_invalid_addr(7)),
                up: true,
            },
            Peer {
                datacenter: Some("her".into()),
                rack: Some("r4".into()),
                address: tests::id_to_invalid_addr(8),
                tokens: vec![Token { value: 400 }],
                untranslated_address: Some(tests::id_to_invalid_addr(8)),
                up: true,
            },
        ];

        let keyspaces = [(
            "keyspace_with_nts".into(),
            Keyspace {
                strategy: Strategy::NetworkTopologyStrategy {
                    datacenter_repfactors: [("waw".to_string(), 2), ("her".to_string(), 3)]
                        .iter()
                        .cloned()
                        .collect::<HashMap<_, _>>(),
                },
                tables: HashMap::new(),
                views: HashMap::new(),
                user_defined_types: HashMap::new(),
            },
        )]
        .iter()
        .cloned()
        .collect();

        let info = Metadata {
            peers: Vec::from(peers),
            keyspaces,
        };

        ClusterData::new(info, &Default::default(), &HashMap::new(), &None, None)
    }
}
