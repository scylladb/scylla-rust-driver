use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use uuid::Uuid;

use super::{ReplicaLocator, ReplicaSet};
use crate::routing::Token;
use crate::transport::{
    connection_pool::PoolConfig,
    topology::{Keyspace, Metadata, Peer, Strategy},
    Node,
};
use crate::transport::{NodeAddr, NodeRef};

use std::collections::HashSet;
use std::sync::Arc;
use std::{
    collections::{BTreeSet, HashMap},
    net::SocketAddr,
};

pub(crate) const KEYSPACE_NTS_RF_2: &str = "keyspace_with_nts_rf_2";
pub(crate) const KEYSPACE_NTS_RF_3: &str = "keyspace_with_nts_rf_3";
pub(crate) const KEYSPACE_SS_RF_2: &str = "keyspace_with_ss_rf_2";

pub(crate) const A: u16 = 1;
pub(crate) const B: u16 = 2;
pub(crate) const C: u16 = 3;
pub(crate) const D: u16 = 4;
pub(crate) const E: u16 = 5;
pub(crate) const F: u16 = 6;
pub(crate) const G: u16 = 7;

// Creates `Metadata` with info about 7 nodes living in 2 datacenters, each with 2 racks.
// num | node | DC | rack
// 1     A      eu   r1
// 2     B      eu   r1
// 3     C      eu   r1
// 4     D      us   r1
// 5     E      us   r1
// 6     F      us   r2
// 7     G      eu   r2
//
// Ring created from this metadata field will be populated as follows (tokens 0-900):
// Ring tokens:             50 100 150 200 250 300 350 400 450 500 550 600 650 700 750 800 900
// Corresponding node ids:  A  B   E   F   A   C   D   A   F   G   D   B   C   C   E   G   B
// Corresponding node nums: 1  2   5   6   1   3   4   1   6   7   4   2   3   3   5   7   2
pub(crate) fn mock_metadata_for_token_aware_tests() -> Metadata {
    let peers = [
        Peer {
            // A
            datacenter: Some("eu".into()),
            rack: Some("r1".to_owned()),
            address: id_to_invalid_addr(1),
            tokens: vec![
                Token { value: 50 },
                Token { value: 250 },
                Token { value: 400 },
            ],
            host_id: Uuid::new_v4(),
        },
        Peer {
            // B
            datacenter: Some("eu".into()),
            rack: Some("r1".to_owned()),
            address: id_to_invalid_addr(2),
            tokens: vec![
                Token { value: 100 },
                Token { value: 600 },
                Token { value: 900 },
            ],
            host_id: Uuid::new_v4(),
        },
        Peer {
            // C
            datacenter: Some("eu".into()),
            rack: Some("r1".to_owned()),
            address: id_to_invalid_addr(3),
            tokens: vec![
                Token { value: 300 },
                Token { value: 650 },
                Token { value: 700 },
            ],
            host_id: Uuid::new_v4(),
        },
        Peer {
            // D
            datacenter: Some("us".into()),
            rack: Some("r1".to_owned()),
            address: id_to_invalid_addr(4),
            tokens: vec![Token { value: 350 }, Token { value: 550 }],
            host_id: Uuid::new_v4(),
        },
        Peer {
            // E
            datacenter: Some("us".into()),
            rack: Some("r1".to_owned()),
            address: id_to_invalid_addr(5),
            tokens: vec![Token { value: 150 }, Token { value: 750 }],
            host_id: Uuid::new_v4(),
        },
        Peer {
            // F
            datacenter: Some("us".into()),
            rack: Some("r2".to_owned()),
            address: id_to_invalid_addr(6),
            tokens: vec![Token { value: 200 }, Token { value: 450 }],
            host_id: Uuid::new_v4(),
        },
        Peer {
            // G
            datacenter: Some("eu".into()),
            rack: Some("r2".to_owned()),
            address: id_to_invalid_addr(7),
            tokens: vec![Token { value: 500 }, Token { value: 800 }],
            host_id: Uuid::new_v4(),
        },
    ];

    let keyspaces = [
        (
            KEYSPACE_SS_RF_2.into(),
            Keyspace {
                strategy: Strategy::SimpleStrategy {
                    replication_factor: 2,
                },
                tables: HashMap::new(),
                views: HashMap::new(),
                user_defined_types: HashMap::new(),
            },
        ),
        (
            KEYSPACE_NTS_RF_2.into(),
            Keyspace {
                strategy: Strategy::NetworkTopologyStrategy {
                    datacenter_repfactors: [("eu".to_owned(), 2), ("us".to_owned(), 2)]
                        .into_iter()
                        .collect(),
                },
                tables: HashMap::new(),
                views: HashMap::new(),
                user_defined_types: HashMap::new(),
            },
        ),
        (
            KEYSPACE_NTS_RF_3.into(),
            Keyspace {
                strategy: Strategy::NetworkTopologyStrategy {
                    datacenter_repfactors: [("eu".to_owned(), 3), ("us".to_owned(), 3)]
                        .into_iter()
                        .collect(),
                },
                tables: HashMap::new(),
                views: HashMap::new(),
                user_defined_types: HashMap::new(),
            },
        ),
    ]
    .iter()
    .cloned()
    .collect();

    Metadata {
        peers: Vec::from(peers),
        keyspaces,
    }
}

pub(crate) fn id_to_invalid_addr(id: u16) -> NodeAddr {
    NodeAddr::Translatable(SocketAddr::from(([255, 255, 255, 255], id)))
}

fn assert_same_node_ids<'a>(left: impl Iterator<Item = NodeRef<'a>>, ids: &[u16]) {
    let left: BTreeSet<_> = left.map(|node| node.address.port()).collect();
    assert_eq!(left, ids.iter().copied().collect())
}

fn assert_replica_set_equal_to(nodes: ReplicaSet<'_>, ids: &[u16]) {
    assert_same_node_ids(nodes.into_iter().map(|(node, _shard)| node), ids)
}

pub(crate) fn create_ring(metadata: &Metadata) -> impl Iterator<Item = (Token, Arc<Node>)> {
    let pool_config: PoolConfig = Default::default();
    let mut ring: Vec<(Token, Arc<Node>)> = Vec::new();

    for peer in &metadata.peers {
        let node = Arc::new(Node::new(
            peer.to_peer_endpoint(),
            pool_config.clone(),
            None,
            true,
        ));

        for token in &peer.tokens {
            ring.push((*token, node.clone()));
        }
    }

    ring.into_iter()
}

pub(crate) fn create_locator(metadata: &Metadata) -> ReplicaLocator {
    let ring = create_ring(metadata);
    let strategies = metadata.keyspaces.values().map(|ks| &ks.strategy);

    ReplicaLocator::new(ring, strategies)
}

#[tokio::test]
async fn test_locator() {
    let locator = create_locator(&mock_metadata_for_token_aware_tests());

    test_datacenter_info(&locator);
    test_simple_strategy_replicas(&locator);
    test_network_topology_strategy_replicas(&locator);
    test_replica_set_len(&locator);
    test_replica_set_choose(&locator);
    test_replica_set_choose_filtered(&locator);
}

fn test_datacenter_info(locator: &ReplicaLocator) {
    let names: BTreeSet<_> = locator
        .datacenter_names()
        .iter()
        .map(|name| name.as_str())
        .collect();
    assert_eq!(names, ["eu", "us"].into_iter().collect());

    assert_same_node_ids(
        locator.unique_nodes_in_global_ring().iter(),
        &[A, B, C, D, E, F, G],
    );

    assert_same_node_ids(
        locator
            .unique_nodes_in_datacenter_ring("eu")
            .unwrap()
            .iter(),
        &[A, B, C, G],
    );

    assert_same_node_ids(
        locator
            .unique_nodes_in_datacenter_ring("us")
            .unwrap()
            .iter(),
        &[D, E, F],
    );
}

fn test_simple_strategy_replicas(locator: &ReplicaLocator) {
    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token { value: 450 },
            &Strategy::SimpleStrategy {
                replication_factor: 3,
            },
            None,
        ),
        &[F, G, D],
    );

    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token { value: 450 },
            &Strategy::SimpleStrategy {
                replication_factor: 4,
            },
            None,
        ),
        &[F, G, D, B],
    );

    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token { value: 201 },
            &Strategy::SimpleStrategy {
                replication_factor: 4,
            },
            None,
        ),
        &[A, C, D, F],
    );

    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token { value: 201 },
            &Strategy::SimpleStrategy {
                replication_factor: 0,
            },
            None,
        ),
        &[],
    );

    // Restricting SimpleStrategy to some datacenter does not guarantee that a replica can be found
    // in that dc.
    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token { value: 50 },
            &Strategy::SimpleStrategy {
                replication_factor: 1,
            },
            Some("us"),
        ),
        &[],
    );

    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token { value: 50 },
            &Strategy::SimpleStrategy {
                replication_factor: 3,
            },
            Some("us"),
        ),
        &[E],
    );

    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token { value: 50 },
            &Strategy::SimpleStrategy {
                replication_factor: 3,
            },
            Some("eu"),
        ),
        &[A, B],
    );
}

fn test_network_topology_strategy_replicas(locator: &ReplicaLocator) {
    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token { value: 75 },
            &Strategy::NetworkTopologyStrategy {
                datacenter_repfactors: [("eu".to_owned(), 1), ("us".to_owned(), 1)]
                    .into_iter()
                    .collect(),
            },
            Some("eu"),
        ),
        &[B],
    );

    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token { value: 75 },
            &Strategy::NetworkTopologyStrategy {
                datacenter_repfactors: [("eu".to_owned(), 1), ("us".to_owned(), 1)]
                    .into_iter()
                    .collect(),
            },
            Some("us"),
        ),
        &[E],
    );

    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token { value: 75 },
            &Strategy::NetworkTopologyStrategy {
                datacenter_repfactors: [("eu".to_owned(), 1), ("us".to_owned(), 1)]
                    .into_iter()
                    .collect(),
            },
            None,
        ),
        &[B, E],
    );

    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token { value: 75 },
            &Strategy::NetworkTopologyStrategy {
                datacenter_repfactors: [("eu".to_owned(), 2), ("us".to_owned(), 1)]
                    .into_iter()
                    .collect(),
            },
            None,
        ),
        // Walking the ring from token 75, [B E F A C D A F G] is encountered.
        // NTS takes the first 2 nodes from that list - {B, E} and the last one - G because it is
        // the only eu node that lives on rack r2.
        &[B, E, G],
    );

    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token { value: 75 },
            &Strategy::NetworkTopologyStrategy {
                datacenter_repfactors: [("unknown".to_owned(), 2), ("us".to_owned(), 1)]
                    .into_iter()
                    .collect(),
            },
            None,
        ),
        &[E],
    );

    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token { value: 800 },
            &Strategy::NetworkTopologyStrategy {
                datacenter_repfactors: [("eu".to_owned(), 1), ("us".to_owned(), 1)]
                    .into_iter()
                    .collect(),
            },
            None,
        ),
        &[G, E],
    );
}

fn test_replica_set_len(locator: &ReplicaLocator) {
    let merged_nts_len = locator
        .replicas_for_token(
            Token { value: 75 },
            &Strategy::NetworkTopologyStrategy {
                datacenter_repfactors: [("eu".to_owned(), 2), ("us".to_owned(), 1)]
                    .into_iter()
                    .collect(),
            },
            None,
        )
        .len();
    assert_eq!(merged_nts_len, 3);

    // Request more replicas than there are nodes in each datacenter and see if the returned
    // replica set length was limited.
    let capped_merged_nts_len = locator
        .replicas_for_token(
            Token { value: 75 },
            &Strategy::NetworkTopologyStrategy {
                datacenter_repfactors: [("eu".to_owned(), 69), ("us".to_owned(), 1)]
                    .into_iter()
                    .collect(),
            },
            None,
        )
        .len();
    assert_eq!(capped_merged_nts_len, 5); // 5 = all eu nodes + 1 us node = 4 + 1.

    let filtered_nts_len = locator
        .replicas_for_token(
            Token { value: 450 },
            &Strategy::NetworkTopologyStrategy {
                datacenter_repfactors: [("eu".to_owned(), 2), ("us".to_owned(), 1)]
                    .into_iter()
                    .collect(),
            },
            Some("eu"),
        )
        .len();
    assert_eq!(filtered_nts_len, 2);

    let ss_len = locator
        .replicas_for_token(
            Token { value: 75 },
            &Strategy::SimpleStrategy {
                replication_factor: 3,
            },
            None,
        )
        .len();
    assert_eq!(ss_len, 3);

    // Test if the replica set length was capped when a datacenter name was provided.
    let filtered_ss_len = locator
        .replicas_for_token(
            Token { value: 75 },
            &Strategy::SimpleStrategy {
                replication_factor: 3,
            },
            Some("eu"),
        )
        .len();
    assert_eq!(filtered_ss_len, 1)
}

fn test_replica_set_choose(locator: &ReplicaLocator) {
    let strategies = [
        Strategy::NetworkTopologyStrategy {
            datacenter_repfactors: [("eu".to_owned(), 2137), ("us".to_owned(), 69)]
                .into_iter()
                .collect(),
        },
        Strategy::SimpleStrategy {
            replication_factor: 42,
        },
    ];

    let mut rng = ChaCha8Rng::seed_from_u64(69);

    for strategy in strategies {
        let replica_set_generator =
            || locator.replicas_for_token(Token { value: 75 }, &strategy, None);

        // Verify that after a certain number of random selections, the set of selected replicas
        // will contain all nodes in the ring (replica set was created using a strategy with
        // replication factors higher than node count).
        let mut chosen_replicas = HashSet::new();
        for _ in 0..32 {
            let set = replica_set_generator();
            let (node, _shard) = set
                .choose(&mut rng)
                .expect("choose from non-empty set must return some node");
            chosen_replicas.insert(node.host_id);
        }

        assert_eq!(
            chosen_replicas,
            locator
                .unique_nodes_in_global_ring()
                .iter()
                .map(|node| node.host_id)
                .collect()
        )
    }
}

fn test_replica_set_choose_filtered(locator: &ReplicaLocator) {
    let strategies = [
        Strategy::NetworkTopologyStrategy {
            datacenter_repfactors: [("eu".to_owned(), 2137), ("us".to_owned(), 69)]
                .into_iter()
                .collect(),
        },
        Strategy::SimpleStrategy {
            replication_factor: 42,
        },
    ];

    let mut rng = ChaCha8Rng::seed_from_u64(69);

    for strategy in strategies {
        let replica_set_generator =
            || locator.replicas_for_token(Token { value: 75 }, &strategy, None);

        // Verify that after a certain number of random selections with a dc filter, the set of
        // selected replicas will contain all nodes in the specified dc ring.
        let mut chosen_replicas = HashSet::new();
        for _ in 0..32 {
            let set = replica_set_generator();
            let (node, _shard) = set
                .choose_filtered(&mut rng, |(node, _shard)| {
                    node.datacenter == Some("eu".into())
                })
                .expect("choose from non-empty set must return some node");
            chosen_replicas.insert(node.host_id);
        }

        assert_eq!(
            chosen_replicas,
            locator
                .unique_nodes_in_datacenter_ring("eu")
                .unwrap()
                .iter()
                .map(|node| node.host_id)
                .collect()
        )
    }

    // Check that choosing from an empty set yields no value.
    let empty = locator
        .replicas_for_token(
            Token { value: 75 },
            &Strategy::LocalStrategy,
            Some("unknown_dc_name"),
        )
        .choose_filtered(&mut rng, |_| true);
    assert_eq!(empty, None);
}
