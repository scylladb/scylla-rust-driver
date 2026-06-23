use crate::frame::response::result::TableSpec;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use uuid::Uuid;

use super::tablets::TabletsInfo;
use super::{ReplicaLocator, ReplicaSet};
use crate::cluster::Node;
use crate::cluster::metadata::{Keyspace, Metadata, Peer, Strategy};
use crate::cluster::{NodeAddr, NodeRef};
use crate::network::PoolConfig;
use crate::routing::Token;
use crate::test_utils::setup_tracing;

use std::collections::HashSet;
use std::sync::Arc;
use std::{
    collections::{BTreeSet, HashMap},
    net::SocketAddr,
};

pub(crate) const KEYSPACE_NTS_RF_2: &str = "keyspace_with_nts_rf_2";
pub(crate) const KEYSPACE_NTS_RF_3: &str = "keyspace_with_nts_rf_3";
pub(crate) const KEYSPACE_SS_RF_2: &str = "keyspace_with_ss_rf_2";

// Those are references because otherwise I can't use them in Option without
// additional binding.
pub(crate) const TABLE_NTS_RF_2: &TableSpec<'static> =
    &TableSpec::borrowed(KEYSPACE_NTS_RF_2, "table");
pub(crate) const TABLE_NTS_RF_3: &TableSpec<'static> =
    &TableSpec::borrowed(KEYSPACE_NTS_RF_3, "table");
pub(crate) const TABLE_SS_RF_2: &TableSpec<'static> =
    &TableSpec::borrowed(KEYSPACE_SS_RF_2, "table");
pub(crate) const TABLE_INVALID: &TableSpec<'static> = &TableSpec::borrowed("invalid", "invalid");

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
            tokens: vec![Token::new(50), Token::new(250), Token::new(400)],
            host_id: Uuid::new_v4(),
        },
        Peer {
            // B
            datacenter: Some("eu".into()),
            rack: Some("r1".to_owned()),
            address: id_to_invalid_addr(2),
            tokens: vec![Token::new(100), Token::new(600), Token::new(900)],
            host_id: Uuid::new_v4(),
        },
        Peer {
            // C
            datacenter: Some("eu".into()),
            rack: Some("r1".to_owned()),
            address: id_to_invalid_addr(3),
            tokens: vec![Token::new(300), Token::new(650), Token::new(700)],
            host_id: Uuid::new_v4(),
        },
        Peer {
            // D
            datacenter: Some("us".into()),
            rack: Some("r1".to_owned()),
            address: id_to_invalid_addr(4),
            tokens: vec![Token::new(350), Token::new(550)],
            host_id: Uuid::new_v4(),
        },
        Peer {
            // E
            datacenter: Some("us".into()),
            rack: Some("r1".to_owned()),
            address: id_to_invalid_addr(5),
            tokens: vec![Token::new(150), Token::new(750)],
            host_id: Uuid::new_v4(),
        },
        Peer {
            // F
            datacenter: Some("us".into()),
            rack: Some("r2".to_owned()),
            address: id_to_invalid_addr(6),
            tokens: vec![Token::new(200), Token::new(450)],
            host_id: Uuid::new_v4(),
        },
        Peer {
            // G
            datacenter: Some("eu".into()),
            rack: Some("r2".to_owned()),
            address: id_to_invalid_addr(7),
            tokens: vec![Token::new(500), Token::new(800)],
            host_id: Uuid::new_v4(),
        },
    ];

    let keyspaces = [
        (
            KEYSPACE_SS_RF_2.into(),
            Ok(Keyspace {
                strategy: Strategy::SimpleStrategy {
                    replication_factor: 2,
                },
                durable_writes: true,
                tablet_based: false,
                tables: HashMap::new(),
                views: HashMap::new(),
                user_defined_types: HashMap::new(),
            }),
        ),
        (
            KEYSPACE_NTS_RF_2.into(),
            Ok(Keyspace {
                strategy: Strategy::NetworkTopologyStrategy {
                    datacenter_repfactors: [("eu".to_owned(), 2), ("us".to_owned(), 2)]
                        .into_iter()
                        .collect(),
                },
                durable_writes: true,
                tablet_based: false,
                tables: HashMap::new(),
                views: HashMap::new(),
                user_defined_types: HashMap::new(),
            }),
        ),
        (
            KEYSPACE_NTS_RF_3.into(),
            Ok(Keyspace {
                strategy: Strategy::NetworkTopologyStrategy {
                    datacenter_repfactors: [("eu".to_owned(), 3), ("us".to_owned(), 3)]
                        .into_iter()
                        .collect(),
                },
                durable_writes: true,
                tablet_based: false,
                tables: HashMap::new(),
                views: HashMap::new(),
                user_defined_types: HashMap::new(),
            }),
        ),
    ]
    .iter()
    .cloned()
    .collect();

    Metadata {
        peers: Vec::from(peers),
        keyspaces,
        cluster_name: Some("TestCluster".into()),
        client_routes_updated_hosts: Default::default(),
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

pub(crate) fn create_ring(metadata: &Metadata) -> impl Iterator<Item = (Token, Arc<Node>)> + use<> {
    let pool_config: PoolConfig = Default::default();
    let mut ring: Vec<(Token, Arc<Node>)> = Vec::new();

    let (connectivity_events_sender, _) = tokio::sync::mpsc::unbounded_channel();
    for peer in &metadata.peers {
        let node = Arc::new(Node::new(
            peer.to_peer_endpoint(),
            &pool_config,
            connectivity_events_sender.clone(),
            None,
            #[cfg(feature = "metrics")]
            Default::default(),
        ));

        for token in &peer.tokens {
            ring.push((*token, node.clone()));
        }
    }

    ring.into_iter()
}

pub(crate) fn create_locator(metadata: &Metadata) -> ReplicaLocator {
    let ring = create_ring(metadata);
    let strategies = metadata
        .keyspaces
        .values()
        .map(|ks| &ks.as_ref().unwrap().strategy);

    ReplicaLocator::new(ring, strategies, TabletsInfo::new())
}

#[tokio::test]
async fn test_locator() {
    setup_tracing();
    let locator = create_locator(&mock_metadata_for_token_aware_tests());

    test_datacenter_info(&locator);
    test_simple_strategy_replicas(&locator);
    test_network_topology_strategy_replicas(&locator);
    test_replica_set_len(&locator);
    test_replica_set_choose(&locator);
    test_replica_set_choose_filtered(&locator);
    test_replica_set_iterator_nth(&locator);
    test_replica_set_iterator_nth_large_n_regression(&locator);
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
            Token::new(450),
            &Strategy::SimpleStrategy {
                replication_factor: 3,
            },
            None,
            TABLE_INVALID,
        ),
        &[F, G, D],
    );

    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token::new(450),
            &Strategy::SimpleStrategy {
                replication_factor: 4,
            },
            None,
            TABLE_INVALID,
        ),
        &[F, G, D, B],
    );

    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token::new(201),
            &Strategy::SimpleStrategy {
                replication_factor: 4,
            },
            None,
            TABLE_INVALID,
        ),
        &[A, C, D, F],
    );

    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token::new(201),
            &Strategy::SimpleStrategy {
                replication_factor: 0,
            },
            None,
            TABLE_INVALID,
        ),
        &[],
    );

    // Restricting SimpleStrategy to some datacenter does not guarantee that a replica can be found
    // in that dc.
    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token::new(50),
            &Strategy::SimpleStrategy {
                replication_factor: 1,
            },
            Some("us"),
            TABLE_INVALID,
        ),
        &[],
    );

    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token::new(50),
            &Strategy::SimpleStrategy {
                replication_factor: 3,
            },
            Some("us"),
            TABLE_INVALID,
        ),
        &[E],
    );

    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token::new(50),
            &Strategy::SimpleStrategy {
                replication_factor: 3,
            },
            Some("eu"),
            TABLE_INVALID,
        ),
        &[A, B],
    );
}

fn test_network_topology_strategy_replicas(locator: &ReplicaLocator) {
    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token::new(75),
            &Strategy::NetworkTopologyStrategy {
                datacenter_repfactors: [("eu".to_owned(), 1), ("us".to_owned(), 1)]
                    .into_iter()
                    .collect(),
            },
            Some("eu"),
            TABLE_INVALID,
        ),
        &[B],
    );

    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token::new(75),
            &Strategy::NetworkTopologyStrategy {
                datacenter_repfactors: [("eu".to_owned(), 1), ("us".to_owned(), 1)]
                    .into_iter()
                    .collect(),
            },
            Some("us"),
            TABLE_INVALID,
        ),
        &[E],
    );

    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token::new(75),
            &Strategy::NetworkTopologyStrategy {
                datacenter_repfactors: [("eu".to_owned(), 1), ("us".to_owned(), 1)]
                    .into_iter()
                    .collect(),
            },
            None,
            TABLE_INVALID,
        ),
        &[B, E],
    );

    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token::new(75),
            &Strategy::NetworkTopologyStrategy {
                datacenter_repfactors: [("eu".to_owned(), 2), ("us".to_owned(), 1)]
                    .into_iter()
                    .collect(),
            },
            None,
            TABLE_INVALID,
        ),
        // Walking the ring from token 75, [B E F A C D A F G] is encountered.
        // NTS takes the first 2 nodes from that list - {B, E} and the last one - G because it is
        // the only eu node that lives on rack r2.
        &[B, E, G],
    );

    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token::new(75),
            &Strategy::NetworkTopologyStrategy {
                datacenter_repfactors: [("unknown".to_owned(), 2), ("us".to_owned(), 1)]
                    .into_iter()
                    .collect(),
            },
            None,
            TABLE_INVALID,
        ),
        &[E],
    );

    assert_replica_set_equal_to(
        locator.replicas_for_token(
            Token::new(800),
            &Strategy::NetworkTopologyStrategy {
                datacenter_repfactors: [("eu".to_owned(), 1), ("us".to_owned(), 1)]
                    .into_iter()
                    .collect(),
            },
            None,
            TABLE_INVALID,
        ),
        &[G, E],
    );
}

fn test_replica_set_len(locator: &ReplicaLocator) {
    let merged_nts_len = locator
        .replicas_for_token(
            Token::new(75),
            &Strategy::NetworkTopologyStrategy {
                datacenter_repfactors: [("eu".to_owned(), 2), ("us".to_owned(), 1)]
                    .into_iter()
                    .collect(),
            },
            None,
            TABLE_INVALID,
        )
        .len();
    assert_eq!(merged_nts_len, 3);

    // Request more replicas than there are nodes in each datacenter and see if the returned
    // replica set length was limited.
    let capped_merged_nts_len = locator
        .replicas_for_token(
            Token::new(75),
            &Strategy::NetworkTopologyStrategy {
                datacenter_repfactors: [("eu".to_owned(), 69), ("us".to_owned(), 1)]
                    .into_iter()
                    .collect(),
            },
            None,
            TABLE_INVALID,
        )
        .len();
    assert_eq!(capped_merged_nts_len, 5); // 5 = all eu nodes + 1 us node = 4 + 1.

    let filtered_nts_len = locator
        .replicas_for_token(
            Token::new(450),
            &Strategy::NetworkTopologyStrategy {
                datacenter_repfactors: [("eu".to_owned(), 2), ("us".to_owned(), 1)]
                    .into_iter()
                    .collect(),
            },
            Some("eu"),
            TABLE_INVALID,
        )
        .len();
    assert_eq!(filtered_nts_len, 2);

    let ss_len = locator
        .replicas_for_token(
            Token::new(75),
            &Strategy::SimpleStrategy {
                replication_factor: 3,
            },
            None,
            TABLE_INVALID,
        )
        .len();
    assert_eq!(ss_len, 3);

    // Test if the replica set length was capped when a datacenter name was provided.
    let filtered_ss_len = locator
        .replicas_for_token(
            Token::new(75),
            &Strategy::SimpleStrategy {
                replication_factor: 3,
            },
            Some("eu"),
            TABLE_INVALID,
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
            || locator.replicas_for_token(Token::new(75), &strategy, None, TABLE_INVALID);

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
            || locator.replicas_for_token(Token::new(75), &strategy, None, TABLE_INVALID);

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
            Token::new(75),
            &Strategy::LocalStrategy,
            Some("unknown_dc_name"),
            TABLE_INVALID,
        )
        .choose_filtered(&mut rng, |_| true);
    assert_eq!(empty, None);
}

// Helper: extract the address port from an iterator item, for easy node identification.
fn port(item: Option<(NodeRef<'_>, crate::routing::Shard)>) -> Option<u16> {
    item.map(|(node, _shard)| node.address.port())
}

// Tests for ReplicaSetIterator::nth, covering all iterator variants:
//
//   * Plain        – SimpleStrategy, no DC filter; O(1) index bump.
//   * FilteredSimple – SimpleStrategy filtered to one DC; sequential O(n) skip.
//   * ChainedNTS   – NetworkTopologyStrategy, no DC filter; DC-aware skip that
//                    can cross datacenter boundaries in O(#DCs).
//
// The test topology (mock_metadata_for_token_aware_tests):
//   Ring:  50  100 150 200 250 300 350 400 450 500 550 600 650 700 750 800 900
//   Node:   A   B   E   F   A   C   D   A   F   G   D   B   C   C   E   G   B
//   DC:    eu  eu  us  us  eu  eu  us  eu  us  eu  us  eu  eu  eu  us  eu  eu
//
//   Node–port mapping: A=1, B=2, C=3, D=4, E=5, F=6, G=7
//   DC order in locator: ["eu", "us"]  (first-occurrence order on the ring)
#[expect(clippy::iter_nth_zero)]
fn test_replica_set_iterator_nth(locator: &ReplicaLocator) {
    // -----------------------------------------------------------------------
    // Plain variant (ReplicaSetIteratorInner::Plain)
    // SimpleStrategy RF=4, no DC filter, token 75.
    // Walking the ring from 75: B(100), E(150), F(200), A(250) → [B, E, F, A]
    // -----------------------------------------------------------------------
    {
        let strategy = Strategy::SimpleStrategy {
            replication_factor: 4,
        };
        let make = || locator.replicas_for_token(Token::new(75), &strategy, None, TABLE_INVALID);

        // nth(0) is equivalent to next(): returns the first element.
        let mut iter = make().into_iter();
        assert_eq!(port(iter.nth(0)), Some(B));

        // nth(1) skips B and returns E.
        let mut iter = make().into_iter();
        assert_eq!(port(iter.nth(1)), Some(E));

        // nth(3) reaches the last element A, leaving the iterator empty.
        let mut iter = make().into_iter();
        assert_eq!(port(iter.nth(3)), Some(A));
        assert_eq!(port(iter.next()), None);

        // nth(4) is out of bounds (equal to len) and returns None.
        let mut iter = make().into_iter();
        assert_eq!(port(iter.nth(4)), None);

        // After nth(1) (returns E), next() continues from F, then A, then exhausts.
        let mut iter = make().into_iter();
        assert_eq!(port(iter.nth(1)), Some(E));
        assert_eq!(port(iter.next()), Some(F));
        assert_eq!(port(iter.next()), Some(A));
        assert_eq!(port(iter.next()), None);

        // Two sequential nth calls: nth(1) → E, then nth(1) on [F, A] → A.
        let mut iter = make().into_iter();
        assert_eq!(port(iter.nth(1)), Some(E));
        assert_eq!(port(iter.nth(1)), Some(A));
        assert_eq!(port(iter.next()), None);
    }

    // -----------------------------------------------------------------------
    // FilteredSimple variant (ReplicaSetIteratorInner::FilteredSimple)
    // SimpleStrategy RF=3, DC filter="eu", token 50.
    // All RF=3 replicas for token 50: [A(50,eu), B(100,eu), E(150,us)]
    // After filtering to "eu": [A, B]
    // -----------------------------------------------------------------------
    {
        let strategy = Strategy::SimpleStrategy {
            replication_factor: 3,
        };
        let make =
            || locator.replicas_for_token(Token::new(50), &strategy, Some("eu"), TABLE_INVALID);

        // nth(0) returns the first eu replica: A.
        let mut iter = make().into_iter();
        assert_eq!(port(iter.nth(0)), Some(A));

        // nth(1) skips A and returns B (the last eu replica).
        let mut iter = make().into_iter();
        assert_eq!(port(iter.nth(1)), Some(B));
        assert_eq!(port(iter.next()), None);

        // nth(2) is out of bounds and returns None.
        let mut iter = make().into_iter();
        assert_eq!(port(iter.nth(2)), None);

        // After nth(0) (returns A), next() returns B, then exhausts.
        let mut iter = make().into_iter();
        assert_eq!(port(iter.nth(0)), Some(A));
        assert_eq!(port(iter.next()), Some(B));
        assert_eq!(port(iter.next()), None);
    }

    // -----------------------------------------------------------------------
    // ChainedNTS variant (ReplicaSetIteratorInner::ChainedNTS)
    // NTS eu=2, us=1, no DC filter, token 75.
    // eu replicas: [B, G] (B on rack r1, G on rack r2 — NTS picks distinct racks)
    // us replicas: [E]
    // Iteration order (eu DC first, then us): B, G, E
    // -----------------------------------------------------------------------
    {
        let strategy = Strategy::NetworkTopologyStrategy {
            datacenter_repfactors: [("eu".to_owned(), 2), ("us".to_owned(), 1)]
                .into_iter()
                .collect(),
        };
        let make = || locator.replicas_for_token(Token::new(75), &strategy, None, TABLE_INVALID);

        // nth(0) returns B (first eu replica).
        let mut iter = make().into_iter();
        assert_eq!(port(iter.nth(0)), Some(B));

        // nth(1) skips B and returns G (second eu replica, within the same DC).
        let mut iter = make().into_iter();
        assert_eq!(port(iter.nth(1)), Some(G));

        // nth(2) crosses the DC boundary: skips both eu replicas and returns E (us).
        let mut iter = make().into_iter();
        assert_eq!(port(iter.nth(2)), Some(E));
        assert_eq!(port(iter.next()), None);

        // nth(3) is out of bounds and returns None.
        let mut iter = make().into_iter();
        assert_eq!(port(iter.nth(3)), None);

        // After nth(1) (returns G), next() crosses to the us DC and returns E.
        let mut iter = make().into_iter();
        assert_eq!(port(iter.nth(1)), Some(G));
        assert_eq!(port(iter.next()), Some(E));
        assert_eq!(port(iter.next()), None);

        // Two sequential nth calls: nth(1) → G, then nth(0) on [E] → E.
        let mut iter = make().into_iter();
        assert_eq!(port(iter.nth(1)), Some(G));
        assert_eq!(port(iter.nth(0)), Some(E));
        assert_eq!(port(iter.next()), None);
    }
}

// Regression test for a bug in ReplicaSetIterator::nth for the Plain and PlainSharded variants:
// the original implementation did `*idx += n` with no overflow or bounds check.
//
// Two failure modes:
// 1. Overflow: nth(usize::MAX) wraps the index around (debug build panics; release build
//    silently produces a garbage index and may return a wrong element or corrupt state).
// 2. size_hint underflow: any n that pushes idx past replicas.len() would make size_hint's
//    `replicas.len() - *idx` subtract with underflow, panicking in debug or yielding a huge
//    size estimate in release.
//
// The fix clamps idx to replicas.len() and returns None early when out of bounds, so both
// failure modes are prevented.
fn test_replica_set_iterator_nth_large_n_regression(locator: &ReplicaLocator) {
    // Plain variant: SimpleStrategy RF=4, no DC filter, token 75 → [B, E, F, A] (len = 4)
    let strategy = Strategy::SimpleStrategy {
        replication_factor: 4,
    };
    let make = || locator.replicas_for_token(Token::new(75), &strategy, None, TABLE_INVALID);

    // nth(usize::MAX) must return None without overflowing.
    // Before the fix this would panic (debug) or silently wrap (release).
    {
        let mut iter = make().into_iter();
        assert_eq!(port(iter.nth(usize::MAX)), None);
        // size_hint must not underflow after an out-of-bounds nth.
        assert_eq!(iter.size_hint(), (0, Some(0)));
        // The iterator must stay properly exhausted.
        assert_eq!(port(iter.next()), None);
    }

    // nth(n) where n > len (but no arithmetic overflow) must also clamp correctly.
    // Before the fix, idx would be set to e.g. 100, and size_hint would underflow on
    // `replicas.len() (= 4) - *idx (= 100)`.
    {
        let mut iter = make().into_iter();
        assert_eq!(port(iter.nth(100)), None);
        // Must not underflow.
        assert_eq!(iter.size_hint(), (0, Some(0)));
        assert_eq!(port(iter.next()), None);
    }
}
