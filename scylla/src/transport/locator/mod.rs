mod precomputed_replicas;
mod replicas;
mod replication_info;
mod token_ring;

use rand::{seq::IteratorRandom, Rng};
pub use token_ring::TokenRing;

use super::{topology::Strategy, Node, NodeRef};
use crate::routing::Token;
use itertools::Itertools;
use precomputed_replicas::PrecomputedReplicas;
use replicas::{ReplicasArray, EMPTY_REPLICAS};
use replication_info::ReplicationInfo;
use std::{cmp, collections::HashMap, sync::Arc};
use tracing::debug;

/// `ReplicaLocator` provides a way to find the set of owning nodes for a given (token, replication
/// strategy) pair. It does so by either using the precomputed token ranges, or doing the
/// computation on the fly.
#[derive(Debug, Clone)]
pub struct ReplicaLocator {
    /// the data based on which `ReplicaLocator` computes replica sets.
    replication_data: ReplicationInfo,

    precomputed_replicas: PrecomputedReplicas,

    datacenters: Vec<String>,
}

impl ReplicaLocator {
    /// Creates a new `ReplicaLocator` in which the specified replication strategies
    /// (`precompute_replica_sets_for`) will have its token ranges precomputed. This function can
    /// potentially be CPU-intensive (if a ring & replication factors in given strategies are big).
    pub fn new<'a>(
        ring_iter: impl Iterator<Item = (Token, Arc<Node>)>,
        precompute_replica_sets_for: impl Iterator<Item = &'a Strategy>,
    ) -> Self {
        let replication_data = ReplicationInfo::new(ring_iter);
        let precomputed_replicas =
            PrecomputedReplicas::compute(&replication_data, precompute_replica_sets_for);

        let datacenters = replication_data
            .get_global_ring()
            .iter()
            .filter_map(|(_, node)| node.datacenter.clone())
            .unique()
            .collect();

        Self {
            replication_data,
            precomputed_replicas,
            datacenters,
        }
    }

    /// Returns a set of nodes that are considered to be replicas for a given token and strategy.
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
    pub fn replicas_for_token<'a>(
        &'a self,
        token: Token,
        strategy: &'a Strategy,
        datacenter: Option<&'a str>,
    ) -> ReplicaSet<'a> {
        match strategy {
            Strategy::SimpleStrategy { replication_factor } => {
                if let Some(datacenter) = datacenter {
                    let replicas = self.get_simple_strategy_replicas(token, *replication_factor);

                    return ReplicaSetInner::FilteredSimple {
                        replicas,
                        datacenter,
                    }
                    .into();
                } else {
                    return ReplicaSetInner::Plain(
                        self.get_simple_strategy_replicas(token, *replication_factor),
                    )
                    .into();
                }
            }
            Strategy::NetworkTopologyStrategy {
                datacenter_repfactors,
            } => {
                if let Some(dc) = datacenter {
                    if let Some(repfactor) = datacenter_repfactors.get(dc) {
                        return ReplicaSetInner::Plain(
                            self.get_network_strategy_replicas(token, dc, *repfactor),
                        )
                        .into();
                    } else {
                        debug!("Datacenter ({}) does not exist!", dc);
                        return EMPTY_REPLICAS.into();
                    }
                } else {
                    return ReplicaSetInner::ChainedNTS {
                        datacenter_repfactors,
                        locator: self,
                        token,
                    }
                    .into();
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
        )
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

#[derive(Debug)]
enum ReplicaSetInner<'a> {
    Plain(ReplicasArray<'a>),

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
}

impl<'a> ReplicaSet<'a> {
    /// Chooses a random replica that satisfies the given predicate.
    pub fn choose_filtered<R>(
        self,
        rng: &mut R,
        predicate: impl Fn(NodeRef<'a>) -> bool,
    ) -> Option<NodeRef<'a>>
    where
        R: Rng + ?Sized,
    {
        let happy = self.choose(rng)?;
        if predicate(happy) {
            return Some(happy);
        }

        self.into_iter()
            .filter(|node_ref| predicate(node_ref))
            .choose(rng)
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
            ReplicaSetInner::FilteredSimple {
                replicas,
                datacenter,
            } => replicas
                .iter()
                .filter(|node| node.datacenter.as_deref() == Some(datacenter))
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

    fn choose<R>(&self, rng: &mut R) -> Option<NodeRef<'a>>
    where
        R: Rng + ?Sized,
    {
        let len = self.len();
        if len > 0 {
            let index = rng.gen_range(0..len);

            match &self.inner {
                ReplicaSetInner::Plain(replicas) => replicas.get(index),
                ReplicaSetInner::FilteredSimple {
                    replicas,
                    datacenter,
                } => replicas
                    .iter()
                    .filter(|node| node.datacenter.as_deref() == Some(datacenter))
                    .nth(index),
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
                                .get(nodes_to_skip);
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
    type Item = NodeRef<'a>;
    type IntoIter = ReplicaSetIterator<'a>;

    /// Converts the replica set into iterator. Order defined by that iterator does not have to
    /// match the order set by the token ring.
    ///
    /// Iterating through `ReplicaSet` using this method is far more efficient than invoking the
    /// `get` method sequentially.
    fn into_iter(self) -> Self::IntoIter {
        let inner = match self.inner {
            ReplicaSetInner::Plain(replicas) => ReplicaSetIteratorInner::Plain { replicas, idx: 0 },
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
                if let Some(datacenter) = &locator.datacenters.first() {
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

        ReplicaSetIterator { inner }
    }
}

impl<'a> From<ReplicaSetInner<'a>> for ReplicaSet<'a> {
    fn from(item: ReplicaSetInner<'a>) -> Self {
        Self { inner: item }
    }
}

impl<'a, T> From<T> for ReplicaSet<'a>
where
    T: Into<ReplicasArray<'a>>,
{
    fn from(item: T) -> Self {
        Self {
            inner: ReplicaSetInner::Plain(item.into()),
        }
    }
}

enum ReplicaSetIteratorInner<'a> {
    Plain {
        replicas: ReplicasArray<'a>,
        idx: usize,
    },
    FilteredSimple {
        replicas: ReplicasArray<'a>,
        datacenter: &'a str,
        idx: usize,
    },
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
}

impl<'a> Iterator for ReplicaSetIterator<'a> {
    type Item = NodeRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            ReplicaSetIteratorInner::Plain { replicas, idx } => {
                if let Some(replica) = replicas.get(*idx) {
                    *idx += 1;
                    return Some(replica);
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
                    if replica.datacenter.as_deref() == Some(datacenter) {
                        return Some(replica);
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
                    Some(replica)
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
            ReplicaSetIteratorInner::Plain { replicas: _, idx } => {
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
