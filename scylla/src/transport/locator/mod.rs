mod precomputed_replicas;
mod replicas;
mod replication_info;
pub(crate) mod tablets;
#[cfg(test)]
pub(crate) mod test;
mod token_ring;

use rand::{seq::IteratorRandom, Rng};
use scylla_cql::frame::response::result::TableSpec;
pub use token_ring::TokenRing;

use self::tablets::TabletsInfo;

use super::{topology::Strategy, Node, NodeRef};
use crate::routing::{Shard, Token};
use itertools::Itertools;
use precomputed_replicas::PrecomputedReplicas;
use replicas::{ReplicasArray, EMPTY_REPLICAS};
use replication_info::ReplicationInfo;
use std::{
    cmp,
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tracing::debug;

/// `ReplicaLocator` provides a way to find the set of owning nodes for a given (token,
/// replication strategy, table) tuple. It does so by either using the precomputed
/// token ranges, or doing the computation on the fly (precomputation is configurable).
#[derive(Debug, Clone)]
pub struct ReplicaLocator {
    /// The data based on which `ReplicaLocator` computes replica sets.
    replication_data: ReplicationInfo,

    precomputed_replicas: PrecomputedReplicas,

    datacenters: Vec<String>,

    pub(crate) tablets: TabletsInfo,
}

impl ReplicaLocator {
    /// Creates a new `ReplicaLocator` in which the specified replication strategies
    /// (`precompute_replica_sets_for`) will have its token ranges precomputed. This function can
    /// potentially be CPU-intensive (if a ring & replication factors in given strategies are big).
    pub(crate) fn new<'a>(
        ring_iter: impl Iterator<Item = (Token, Arc<Node>)>,
        precompute_replica_sets_for: impl Iterator<Item = &'a Strategy>,
        tablets: TabletsInfo,
    ) -> Self {
        let replication_data = ReplicationInfo::new(ring_iter);
        let precomputed_replicas =
            PrecomputedReplicas::compute(&replication_data, precompute_replica_sets_for);

        let datacenters = replication_data
            .get_global_ring()
            .iter()
            .filter_map(|(_, node)| node.datacenter.as_deref())
            .unique()
            .map(ToOwned::to_owned)
            .collect();

        Self {
            replication_data,
            precomputed_replicas,
            datacenters,
            tablets,
        }
    }

    /// Returns a set of nodes that are considered to be replicas for a given token, strategy and table.
    /// If the `datacenter` parameter is set, the returned `ReplicaSet` is limited only to replicas
    /// from that datacenter. If a specified datacenter name does not correspond to a valid
    /// datacenter, an empty set will be returned.
    ///
    /// Supported replication strategies: `SimpleStrategy`, 'NetworkTopologyStrategy',
    /// 'LocalStrategy'. If other is specified, it is treated as the `SimpleStrategy` with
    /// replication factor equal to 1.
    ///
    /// If a provided replication strategy did not appear in `precompute_replica_sets_for`
    /// parameter of `Self::new`, invocation of this function will trigger a computation of the
    /// desired replica set (the computation might be delegated in time and start upon interaction
    /// with the returned `ReplicaSet`).
    ///
    /// If the requested table uses Tablets, then a separate code path is taken, which ignores
    /// replication strategies and only uses tablet information stored in ReplicaLocator.
    /// If we don't have info about the tablet that owns the given token, empty set will be returned.
    pub fn replicas_for_token<'a>(
        &'a self,
        token: Token,
        strategy: &'a Strategy,
        datacenter: Option<&'a str>,
        table_spec: &TableSpec,
    ) -> ReplicaSet<'a> {
        if let Some(tablets) = self.tablets.tablets_for_table(table_spec) {
            let replicas: Option<&[(Arc<Node>, Shard)]> = if let Some(datacenter) = datacenter {
                tablets.dc_replicas_for_token(token, datacenter)
            } else {
                tablets.replicas_for_token(token)
            };
            return ReplicaSet {
                inner: ReplicaSetInner::PlainSharded(replicas.unwrap_or(
                    // The table is a tablet table, but we don't have information for given token.
                    // Let's just return empty set in this case.
                    &[],
                )),
                token,
            };
        } else {
            match strategy {
                Strategy::SimpleStrategy { replication_factor } => {
                    if let Some(datacenter) = datacenter {
                        let replicas =
                            self.get_simple_strategy_replicas(token, *replication_factor);

                        return ReplicaSet {
                            inner: ReplicaSetInner::FilteredSimple {
                                replicas,
                                datacenter,
                            },
                            token,
                        };
                    } else {
                        return ReplicaSet {
                            inner: ReplicaSetInner::Plain(
                                self.get_simple_strategy_replicas(token, *replication_factor),
                            ),
                            token,
                        };
                    }
                }
                Strategy::NetworkTopologyStrategy {
                    datacenter_repfactors,
                } => {
                    if let Some(dc) = datacenter {
                        if let Some(repfactor) = datacenter_repfactors.get(dc) {
                            return ReplicaSet {
                                inner: ReplicaSetInner::Plain(
                                    self.get_network_strategy_replicas(token, dc, *repfactor),
                                ),
                                token,
                            };
                        } else {
                            debug!("Datacenter ({}) does not exist!", dc);
                            return ReplicaSet {
                                inner: ReplicaSetInner::Plain(EMPTY_REPLICAS),
                                token,
                            };
                        }
                    } else {
                        return ReplicaSet {
                            inner: ReplicaSetInner::ChainedNTS {
                                datacenter_repfactors,
                                locator: self,
                                token,
                            },
                            token,
                        };
                    }
                }
                Strategy::Other { name, .. } => {
                    debug!("Unknown strategy ({}), falling back to SimpleStrategy with replication_factor = 1", name)
                }
                _ => (),
            }

            // Fallback to simple strategy with replication factor = 1.
            self.replicas_for_token(
                token,
                &Strategy::SimpleStrategy {
                    replication_factor: 1,
                },
                datacenter,
                table_spec,
            )
        }
    }

    /// Gives access to the token ring, based on which all token ranges/replica sets are computed.
    pub fn ring(&self) -> &TokenRing<Arc<Node>> {
        self.replication_data.get_global_ring()
    }

    /// Gives a list of all nodes in the token ring.
    pub fn unique_nodes_in_global_ring(&self) -> &[Arc<Node>] {
        self.replication_data.unique_nodes_in_global_ring()
    }

    /// Gives a list of all known datacenters.
    pub fn datacenter_names(&self) -> &[String] {
        self.datacenters.as_slice()
    }

    /// Gives a list of all nodes in a specified datacenter ring (which is created by filtering the
    /// original ring to only contain nodes living in the specified datacenter).
    pub fn unique_nodes_in_datacenter_ring<'a>(
        &'a self,
        datacenter_name: &str,
    ) -> Option<&'a [Arc<Node>]> {
        self.replication_data
            .unique_nodes_in_datacenter_ring(datacenter_name)
    }

    fn get_simple_strategy_replicas(
        &self,
        token: Token,
        replication_factor: usize,
    ) -> ReplicasArray<'_> {
        if replication_factor == 0 {
            return EMPTY_REPLICAS;
        }

        if let Some(precomputed_replicas) = self
            .precomputed_replicas
            .get_precomputed_simple_strategy_replicas(token, replication_factor)
        {
            precomputed_replicas.into()
        } else {
            ReplicasArray::from_iter(
                self.replication_data
                    .simple_strategy_replicas(token, replication_factor),
            )
        }
    }

    fn get_network_strategy_replicas<'a>(
        &'a self,
        token: Token,
        datacenter: &str,
        datacenter_replication_factor: usize,
    ) -> ReplicasArray<'a> {
        if datacenter_replication_factor == 0 {
            return EMPTY_REPLICAS;
        }

        if let Some(precomputed_replicas) = self
            .precomputed_replicas
            .get_precomputed_network_strategy_replicas(
                token,
                datacenter,
                datacenter_replication_factor,
            )
        {
            ReplicasArray::from(precomputed_replicas)
        } else {
            ReplicasArray::from_iter(self.replication_data.nts_replicas_in_datacenter(
                token,
                datacenter,
                datacenter_replication_factor,
            ))
        }
    }
}

fn with_computed_shard(node: NodeRef, token: Token) -> (NodeRef, Shard) {
    let shard = node
        .sharder()
        .map(|sharder| sharder.shard_of(token))
        .unwrap_or(0);
    (node, shard)
}

#[derive(Debug)]
enum ReplicaSetInner<'a> {
    Plain(ReplicasArray<'a>),

    PlainSharded(&'a [(Arc<Node>, Shard)]),

    // Represents a set of SimpleStrategy replicas that is limited to a specified datacenter.
    FilteredSimple {
        replicas: ReplicasArray<'a>,
        datacenter: &'a str,
    },

    // Represents a set of NetworkTopologyStrategy replicas that is not limited to any specific
    // datacenter. The set is constructed lazily, by invoking
    // `locator.get_network_strategy_replicas()`.
    ChainedNTS {
        datacenter_repfactors: &'a HashMap<String, usize>,
        locator: &'a ReplicaLocator,
        token: Token,
    },
}

/// Represents a set of replicas for a given token and strategy;
///
/// This container can only be created by calling `ReplicaLocator::replicas_for_token`, and it
/// can borrow precomputed replica lists living in the locator.
#[derive(Debug)]
pub struct ReplicaSet<'a> {
    inner: ReplicaSetInner<'a>,
    token: Token,
}

impl<'a> ReplicaSet<'a> {
    /// Chooses a random replica that satisfies the given predicate.
    pub fn choose_filtered<R>(
        self,
        rng: &mut R,
        predicate: impl Fn(&(NodeRef<'a>, Shard)) -> bool,
    ) -> Option<(NodeRef<'a>, Shard)>
    where
        R: Rng + ?Sized,
    {
        let happy = self.choose(rng)?;
        if predicate(&happy) {
            return Some(happy);
        }

        self.into_iter().filter(predicate).choose(rng)
    }

    /// Gets the size of the set.
    ///
    /// If the set represents `SimpleStrategy` replicas that were filtered by datacenter, this
    /// function will have O(R) complexity, where R is the replication factor of that strategy.
    ///
    /// If the set represents `NetworkTopologyStrategy` replicas that were not filtered by
    /// datacenter, this function will have O(D) complexity where D is the number of known
    /// datacenters.
    ///
    /// In all other cases, the complexity is O(1)
    pub fn len(&self) -> usize {
        match &self.inner {
            ReplicaSetInner::Plain(replicas) => replicas.len(),
            ReplicaSetInner::PlainSharded(replicas) => replicas.len(),
            ReplicaSetInner::FilteredSimple {
                replicas,
                datacenter,
            } => replicas
                .iter()
                .filter(|node| node.datacenter.as_deref() == Some(*datacenter))
                .count(),
            ReplicaSetInner::ChainedNTS {
                datacenter_repfactors,
                locator,
                token: _,
            } => datacenter_repfactors
                .iter()
                .map(|(dc, rf)| {
                    let unique_nodes_in_dc_count = locator
                        .unique_nodes_in_datacenter_ring(dc)
                        .map(|nodes| nodes.len())
                        .unwrap_or(0);

                    cmp::min(*rf, unique_nodes_in_dc_count)
                })
                .sum(),
        }
    }

    /// Returns `true` if the replica set contains no elements.
    ///
    /// Complexity same as of `ReplicaSet::len`.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn choose<R>(&self, rng: &mut R) -> Option<(NodeRef<'a>, Shard)>
    where
        R: Rng + ?Sized,
    {
        let len = self.len();
        if len > 0 {
            let index = rng.gen_range(0..len);

            match &self.inner {
                ReplicaSetInner::Plain(replicas) => replicas
                    .get(index)
                    .map(|node| with_computed_shard(node, self.token)),
                ReplicaSetInner::PlainSharded(replicas) => {
                    replicas.get(index).map(|(node, shard)| (node, *shard))
                }
                ReplicaSetInner::FilteredSimple {
                    replicas,
                    datacenter,
                } => replicas
                    .iter()
                    .filter(|node| node.datacenter.as_deref() == Some(*datacenter))
                    .nth(index)
                    .map(|node| with_computed_shard(node, self.token)),
                ReplicaSetInner::ChainedNTS {
                    datacenter_repfactors,
                    locator,
                    token,
                } => {
                    let mut nodes_to_skip = index;
                    for datacenter in locator.datacenters.iter() {
                        let requested_repfactor =
                            *datacenter_repfactors.get(datacenter).unwrap_or(&0);
                        let unique_nodes_in_dc_count = locator
                            .unique_nodes_in_datacenter_ring(datacenter)
                            .map(|nodes| nodes.len())
                            .unwrap_or(0);

                        let repfactor = cmp::min(requested_repfactor, unique_nodes_in_dc_count);

                        if nodes_to_skip < repfactor {
                            return locator
                                .get_network_strategy_replicas(*token, datacenter, repfactor)
                                .get(nodes_to_skip)
                                .map(|node| with_computed_shard(node, self.token));
                        }

                        nodes_to_skip -= repfactor;
                    }

                    None
                }
            }
        } else {
            None
        }
    }
}

impl<'a> IntoIterator for ReplicaSet<'a> {
    type Item = (NodeRef<'a>, Shard);
    type IntoIter = ReplicaSetIterator<'a>;

    /// Converts the replica set into iterator. Order defined by that iterator does not have to
    /// match the order set by the token ring.
    ///
    /// Iterating through `ReplicaSet` using this method is far more efficient than invoking the
    /// `get` method sequentially.
    fn into_iter(self) -> Self::IntoIter {
        let inner = match self.inner {
            ReplicaSetInner::Plain(replicas) => ReplicaSetIteratorInner::Plain { replicas, idx: 0 },
            ReplicaSetInner::PlainSharded(replicas) => {
                ReplicaSetIteratorInner::PlainSharded { replicas, idx: 0 }
            }
            ReplicaSetInner::FilteredSimple {
                replicas,
                datacenter,
            } => ReplicaSetIteratorInner::FilteredSimple {
                replicas,
                datacenter,
                idx: 0,
            },
            ReplicaSetInner::ChainedNTS {
                datacenter_repfactors,
                locator,
                token,
            } => {
                if let Some(datacenter) = locator.datacenters.first() {
                    let repfactor = *datacenter_repfactors.get(datacenter.as_str()).unwrap_or(&0);
                    ReplicaSetIteratorInner::ChainedNTS {
                        replicas: locator
                            .get_network_strategy_replicas(token, datacenter, repfactor),
                        replicas_idx: 0,

                        locator,
                        token,
                        datacenter_idx: 0,
                        datacenter_repfactors,
                    }
                } else {
                    ReplicaSetIteratorInner::Plain {
                        replicas: EMPTY_REPLICAS,
                        idx: 0,
                    }
                }
            }
        };

        ReplicaSetIterator {
            inner,
            token: self.token,
        }
    }
}

enum ReplicaSetIteratorInner<'a> {
    /// Token ring with SimpleStrategy, any datacenter
    Plain {
        replicas: ReplicasArray<'a>,
        idx: usize,
    },
    /// Tablets
    PlainSharded {
        replicas: &'a [(Arc<Node>, Shard)],
        idx: usize,
    },
    /// Token ring with SimpleStrategy, specific datacenter
    FilteredSimple {
        replicas: ReplicasArray<'a>,
        datacenter: &'a str,
        idx: usize,
    },
    /// Token ring with NetworkTopologyStrategy
    ChainedNTS {
        replicas: ReplicasArray<'a>,
        replicas_idx: usize,

        datacenter_repfactors: &'a HashMap<String, usize>,
        locator: &'a ReplicaLocator,
        token: Token,
        datacenter_idx: usize,
    },
}

/// Iterator that returns replicas from some replica set.
pub struct ReplicaSetIterator<'a> {
    inner: ReplicaSetIteratorInner<'a>,
    token: Token,
}

impl<'a> Iterator for ReplicaSetIterator<'a> {
    type Item = (NodeRef<'a>, Shard);

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            ReplicaSetIteratorInner::Plain { replicas, idx } => {
                if let Some(replica) = replicas.get(*idx) {
                    *idx += 1;
                    return Some(with_computed_shard(replica, self.token));
                }

                None
            }
            ReplicaSetIteratorInner::PlainSharded { replicas, idx } => {
                if let Some((replica, shard)) = replicas.get(*idx) {
                    *idx += 1;
                    return Some((replica, *shard));
                }

                None
            }
            ReplicaSetIteratorInner::FilteredSimple {
                replicas,
                datacenter,
                idx,
            } => {
                while let Some(replica) = replicas.get(*idx) {
                    *idx += 1;
                    if replica.datacenter.as_deref() == Some(*datacenter) {
                        return Some(with_computed_shard(replica, self.token));
                    }
                }

                None
            }
            ReplicaSetIteratorInner::ChainedNTS {
                replicas,
                replicas_idx,
                locator,
                token,
                datacenter_idx,
                datacenter_repfactors,
            } => {
                if let Some(replica) = replicas.get(*replicas_idx) {
                    *replicas_idx += 1;
                    Some(with_computed_shard(replica, self.token))
                } else if *datacenter_idx + 1 < locator.datacenters.len() {
                    *datacenter_idx += 1;
                    *replicas_idx = 0;

                    let datacenter = &locator.datacenters[*datacenter_idx];
                    let repfactor = *datacenter_repfactors.get(datacenter).unwrap_or(&0);
                    *replicas =
                        locator.get_network_strategy_replicas(*token, datacenter, repfactor);

                    self.next()
                } else {
                    None
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.inner {
            ReplicaSetIteratorInner::Plain { replicas, idx } => {
                let size = replicas.len() - *idx;

                (size, Some(size))
            }
            ReplicaSetIteratorInner::PlainSharded { replicas, idx } => {
                let size = replicas.len() - *idx;

                (size, Some(size))
            }
            ReplicaSetIteratorInner::FilteredSimple {
                replicas,
                datacenter: _,
                idx,
            } => (0, Some(replicas.len() - *idx)),
            ReplicaSetIteratorInner::ChainedNTS {
                replicas: _,
                replicas_idx: _,
                datacenter_repfactors,
                locator,
                token: _,
                datacenter_idx,
            } => {
                let yielded: usize = locator.datacenter_names()[0..*datacenter_idx]
                    .iter()
                    .filter_map(|name| datacenter_repfactors.get(name))
                    .sum();

                (
                    0,
                    Some(datacenter_repfactors.values().sum::<usize>() - yielded),
                )
            }
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        match &mut self.inner {
            ReplicaSetIteratorInner::Plain { replicas: _, idx }
            | ReplicaSetIteratorInner::PlainSharded { replicas: _, idx } => {
                *idx += n;

                self.next()
            }
            _ => {
                for _i in 0..n {
                    self.next()?;
                }

                self.next()
            }
        }
    }
}

impl<'a> ReplicaSet<'a> {
    pub fn into_replicas_ordered(self) -> ReplicasOrdered<'a> {
        ReplicasOrdered { replica_set: self }
    }
}

/// Represents a sequence of replicas for a given token and strategy,
/// ordered according to the ring order (for token-ring tables) or with the
/// order defined by tablet data (for tablet tables).
///
/// This container can only be created by calling `ReplicaSet::into_replicas_ordered()`,
/// and either it can borrow precomputed replica lists living in the locator (in case of SimpleStrategy)
/// or it must compute them on-demand (in case of NetworkTopologyStrategy).
/// The computation is lazy (performed by `ReplicasOrderedIterator` upon call to `next()`).
/// For obtaining the primary replica, no allocations are needed. Therefore, the first call
/// to `next()` is optimised and does not allocate.
/// For the remaining others, unfortunately, allocation is inevitable.
pub struct ReplicasOrdered<'a> {
    replica_set: ReplicaSet<'a>,
}

/// Iterator that returns replicas from some replica sequence, ordered according to the ring order.
pub struct ReplicasOrderedIterator<'a> {
    inner: ReplicasOrderedIteratorInner<'a>,
}

enum ReplicasOrderedIteratorInner<'a> {
    AlreadyRingOrdered {
        // In case of Plain, PlainSharded and FilteredSimple variants,
        // ReplicaSetIterator respects ring order.
        replica_set_iter: ReplicaSetIterator<'a>,
    },
    PolyDatacenterNTS {
        // In case of ChainedNTS variant, ReplicaSetIterator does not respect ring order,
        // so specific code is needed to yield replicas according to that order.
        replicas_ordered_iter: ReplicasOrderedNTSIterator<'a>,
    },
}

struct ReplicasOrderedNTSIterator<'a> {
    token: Token,
    inner: ReplicasOrderedNTSIteratorInner<'a>,
}

enum ReplicasOrderedNTSIteratorInner<'a> {
    FreshForPick {
        datacenter_repfactors: &'a HashMap<String, usize>,
        locator: &'a ReplicaLocator,
        token: Token,
    },
    Picked {
        datacenter_repfactors: &'a HashMap<String, usize>,
        locator: &'a ReplicaLocator,
        token: Token,
        picked: NodeRef<'a>,
    },
    ComputedFallback {
        replicas: ReplicasArray<'a>,
        idx: usize,
    },
}

impl<'a> Iterator for ReplicasOrderedNTSIterator<'a> {
    type Item = (NodeRef<'a>, Shard);

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner {
            ReplicasOrderedNTSIteratorInner::FreshForPick {
                datacenter_repfactors,
                locator,
                token,
            } => {
                // We're going to find the primary replica for the given token.
                let nodes_on_ring = locator.replication_data.get_global_ring().ring_range(token);
                for node in nodes_on_ring {
                    // If this node's DC has some replicas in this NTS...
                    if let Some(dc) = &node.datacenter {
                        if datacenter_repfactors.get(dc).is_some() {
                            // ...then this node must be the primary replica.
                            self.inner = ReplicasOrderedNTSIteratorInner::Picked {
                                datacenter_repfactors,
                                locator,
                                token,
                                picked: node,
                            };
                            return Some(with_computed_shard(node, self.token));
                        }
                    }
                }
                None
            }
            ReplicasOrderedNTSIteratorInner::Picked {
                datacenter_repfactors,
                locator,
                token,
                picked,
            } => {
                // Clippy can't check that in Eq and Hash impls we don't actually use any field with interior mutability
                // (in Node only `down_marker` is such, being an AtomicBool).
                // https://rust-lang.github.io/rust-clippy/master/index.html#mutable_key_type
                #[allow(clippy::mutable_key_type)]
                let mut all_replicas: HashSet<&'a Arc<Node>> = HashSet::new();
                for (datacenter, repfactor) in datacenter_repfactors.iter() {
                    all_replicas.extend(
                        locator
                            .get_network_strategy_replicas(token, datacenter, *repfactor)
                            .iter(),
                    );
                }
                // It's no use returning a node that was already picked.
                all_replicas.remove(picked);

                let mut replicas_ordered = vec![];
                let nodes_on_ring = locator.replication_data.get_global_ring().ring_range(token);
                for node in nodes_on_ring {
                    if all_replicas.is_empty() {
                        // All replicas were put in order.
                        break;
                    }
                    if all_replicas.remove(node) {
                        replicas_ordered.push(node);
                    }
                }
                assert!(
                    all_replicas.is_empty(),
                    "all_replicas somehow contained a node that wasn't present in the global ring!"
                );

                self.inner = ReplicasOrderedNTSIteratorInner::ComputedFallback {
                    replicas: ReplicasArray::Owned(replicas_ordered),
                    idx: 0,
                };
                self.next()
            }
            ReplicasOrderedNTSIteratorInner::ComputedFallback {
                ref replicas,
                ref mut idx,
            } => {
                if let Some(replica) = replicas.get(*idx) {
                    *idx += 1;
                    Some(with_computed_shard(replica, self.token))
                } else {
                    None
                }
            }
        }
    }
}

impl<'a> Iterator for ReplicasOrderedIterator<'a> {
    type Item = (NodeRef<'a>, Shard);

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            ReplicasOrderedIteratorInner::AlreadyRingOrdered { replica_set_iter } => {
                replica_set_iter.next()
            }
            ReplicasOrderedIteratorInner::PolyDatacenterNTS {
                replicas_ordered_iter,
            } => replicas_ordered_iter.next(),
        }
    }
}

impl<'a> IntoIterator for ReplicasOrdered<'a> {
    type Item = (NodeRef<'a>, Shard);
    type IntoIter = ReplicasOrderedIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        let Self { replica_set } = self;
        Self::IntoIter {
            inner: match replica_set.inner {
                ReplicaSetInner::Plain(_) | ReplicaSetInner::FilteredSimple { .. } => {
                    ReplicasOrderedIteratorInner::AlreadyRingOrdered {
                        replica_set_iter: replica_set.into_iter(),
                    }
                }
                ReplicaSetInner::PlainSharded(_) => {
                    ReplicasOrderedIteratorInner::AlreadyRingOrdered {
                        replica_set_iter: replica_set.into_iter(),
                    }
                }
                ReplicaSetInner::ChainedNTS {
                    datacenter_repfactors,
                    locator,
                    token,
                } => ReplicasOrderedIteratorInner::PolyDatacenterNTS {
                    replicas_ordered_iter: ReplicasOrderedNTSIterator {
                        token: replica_set.token,
                        inner: ReplicasOrderedNTSIteratorInner::FreshForPick {
                            datacenter_repfactors,
                            locator,
                            token,
                        },
                    },
                },
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{routing::Token, test_utils::setup_tracing, transport::locator::test::*};

    #[tokio::test]
    async fn test_replicas_ordered() {
        setup_tracing();
        let metadata = mock_metadata_for_token_aware_tests();
        let locator = create_locator(&metadata);

        // For each case (token, limit_to_dc, strategy), we are checking
        // that ReplicasOrdered yields replicas in the expected order.
        let check = |token, limit_to_dc, strategy, table, expected| {
            let replica_set =
                locator.replicas_for_token(Token::new(token), strategy, limit_to_dc, table);
            let replicas_ordered = replica_set.into_replicas_ordered();
            let ids: Vec<_> = replicas_ordered
                .into_iter()
                .map(|(node, _shard)| node.address.port())
                .collect();
            assert_eq!(expected, ids);
        };

        // In all these tests:
        // going through the ring, we get order: F , A , C , D , G , B , E
        //                                       us  eu  eu  us  eu  eu  us
        //                                       r2  r1  r1  r1  r2  r1  r1
        check(
            160,
            None,
            &metadata.keyspaces.get(KEYSPACE_NTS_RF_3).unwrap().strategy,
            TABLE_NTS_RF_3,
            &[F, A, C, D, G, E][..],
        );
        check(
            160,
            None,
            &metadata.keyspaces.get(KEYSPACE_NTS_RF_2).unwrap().strategy,
            TABLE_NTS_RF_2,
            &[F, A, D, G],
        );
        check(
            160,
            None,
            &metadata.keyspaces.get(KEYSPACE_SS_RF_2).unwrap().strategy,
            TABLE_SS_RF_2,
            &[F, A],
        );

        check(
            160,
            Some("eu"),
            &metadata.keyspaces.get(KEYSPACE_NTS_RF_3).unwrap().strategy,
            TABLE_NTS_RF_3,
            &[A, C, G],
        );
        check(
            160,
            Some("us"),
            &metadata.keyspaces.get(KEYSPACE_NTS_RF_3).unwrap().strategy,
            TABLE_NTS_RF_3,
            &[F, D, E],
        );
        check(
            160,
            Some("eu"),
            &metadata.keyspaces.get(KEYSPACE_SS_RF_2).unwrap().strategy,
            TABLE_SS_RF_2,
            &[A],
        );
    }
}
