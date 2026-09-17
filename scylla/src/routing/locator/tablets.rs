//! Tablet routing information and the tablet-version tracking used to keep it fresh.
//!
//! For a tablet-enabled table the driver caches, per tablet, the token range and the replica
//! set so it can route each request straight to a replica. The server keeps that cache
//! correct by piggy-backing routing information on responses, negotiated through one of two
//! protocol extensions:
//!
//! - `TABLETS_ROUTING_V1`: whenever the driver contacts a shard that does not own the target
//!   tablet, the response carries a `tablets-routing-v1` custom payload describing the tablet
//!   so the driver can correct its cache.
//! - `TABLETS_ROUTING_V2` (experimental; negotiated on the wire as
//!   `TABLETS_ROUTING_V2_EXPERIMENTAL`): V2 subsumes V1 and adds *tablet-version tracking*.
//!   The driver attaches a one-byte "tablet-version block" to every `EXECUTE` (see
//!   [`TabletVersion`]); the server compares it against its own tablet version and returns a
//!   fresh `tablets-routing-v2` payload only when the driver's cached version is stale. The
//!   block encodes one randomly chosen nibble of the version, so successive requests probe
//!   the whole version and detect any change without the driver having to contact a wrong
//!   replica first.
//!
//! The tablet version is an opaque 64-bit hash of the tablet's ordered replica list (not a
//! monotonic counter); only its bit pattern is meaningful. Because V2 is experimental its wire
//! name and payload are subject to change.
//!
//! # Leader-aware routing for strongly-consistent tables
//!
//! For a strongly-consistent (Raft-based) keyspace — one created with `consistency = 'global'`,
//! reflected in [`Keyspace::consistency_mode`] as [`ConsistencyMode::Global`] — one replica of
//! each tablet is the Raft leader that coordinates its writes and its linearizable reads.
//!
//! The server does not publish which replica that is: it is absent from schema and from
//! `system.tablets`, and it can change at any time through a Raft re-election. The one place it
//! surfaces is the `tablets-routing-v2` payload, which lists the leader first. The driver stores
//! the replicas in that payload order, so `replicas[0]` of a cached tablet is the leader, and the
//! built-in load balancing policy uses it to route leader-requiring requests straight there,
//! saving the extra coordinator-to-leader hop.
//!
//! A request to such a keyspace is routed to the leader whenever its consistency level is
//! anything other than `ONE` or `LOCAL_ONE`:
//!
//! - writes to such a keyspace must use `QUORUM`/`LOCAL_QUORUM` (the server rejects
//!   `ONE`/`LOCAL_ONE` writes), so they always reach the leader — and a write sent to a follower
//!   would only be bounced to it anyway;
//! - reads at `ONE`/`LOCAL_ONE` may be served by any replica, so they keep the normal
//!   load-spreading routing;
//! - all other reads go to the leader.
//!
//! The leader outranks *distance*: it is targeted ahead of nearer replicas, a leader in a remote
//! datacenter ahead of one in the preferred rack. Every write and every linearizable read on such
//! a table has to be coordinated by the leader anyway, so contacting a nearer replica only adds a
//! forwarding hop -- and because the table is globally consistent, keeping the request inside one
//! datacenter buys no consistency either.
//!
//! It does not override the policy's own filter, though. A leader the policy would never contact
//! is left alone: with a preferred datacenter and datacenter failover disabled, a leader elsewhere
//! is skipped, the request goes to a local replica, and the server forwards it to the leader --
//! exactly as it would without the extension. Only the leader is promoted; the remaining replicas
//! keep their usual ordering, so retries after it still spread.
//!
//! This mirrors the Python driver's `TokenAwarePolicy` behavior, so the two drivers agree on how
//! leader awareness interacts with locality preferences. The decision itself lives in the load
//! balancing policy.
//!
//! [`Keyspace::consistency_mode`]: crate::cluster::metadata::Keyspace::consistency_mode
//! [`ConsistencyMode::Global`]: crate::cluster::metadata::ConsistencyMode::Global

use crate::cluster::metadata::Keyspace;
use crate::deserialize::value::{DeserializeValue, ListlikeIterator};
use crate::deserialize::{DeserializationError, FrameSlice, TypeCheckError};
use crate::frame::response::result::{CollectionType, ColumnType, NativeType, TableSpec};
use bytes::Bytes;
use thiserror::Error;
use tracing::warn;
use uuid::Uuid;

use crate::cluster::Node;
use crate::routing::{Shard, Token};
use crate::utils::safe_format::IteratorSafeFormatExt;
use rand::Rng as _;
use smallvec::SmallVec;

use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::{Arc, LazyLock};

#[derive(Error, Debug)]
pub(crate) enum TabletParsingError {
    #[error(transparent)]
    Deserialization(#[from] DeserializationError),
    #[error(transparent)]
    TypeCheck(#[from] TypeCheckError),
    #[error("Shard id for tablet is negative: {0}")]
    ShardNum(i32),
    #[error(
        "First element of tablet payload token range must be strictly smaller than the second, but the range is ({0}, {1}]"
    )]
    WrongTokenRange(i64, i64),
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct RawTabletReplicas {
    replicas: Vec<(Uuid, Shard)>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RawTablet {
    /// First token belonging to the tablet, inclusive
    first_token: Token,
    /// Last token belonging to the tablet, inclusive
    last_token: Token,
    replicas: RawTabletReplicas,
    /// Tablet version, present only when the tablet was learned via the
    /// TABLETS_ROUTING_V2 extension. Used to keep the driver's routing cache fresh -
    /// see [`TabletVersion`].
    tablet_version: Option<TabletVersion>,
}

#[cfg(test)]
impl RawTablet {
    /// Builds a raw tablet directly, for tests that need to inject tablet mappings
    /// (with a chosen replica order and version) into a `ClusterState`.
    ///
    /// `tablet_version` is given as the signed `bigint` the server would have sent.
    pub(crate) fn new_for_test(
        first_token: i64,
        last_token: i64,
        replicas: Vec<(Uuid, Shard)>,
        tablet_version: Option<i64>,
    ) -> Self {
        Self {
            first_token: Token::new(first_token),
            last_token: Token::new(last_token),
            replicas: RawTabletReplicas { replicas },
            tablet_version: tablet_version.map(TabletVersion::from_server_value),
        }
    }
}

type RawTabletPayloadV1<'frame, 'metadata> =
    (i64, i64, ListlikeIterator<'frame, 'metadata, (Uuid, i32)>);

/// TABLETS_ROUTING_V2 payload is the V1 tuple with an extra trailing `bigint`
/// carrying the tablet version.
type RawTabletPayloadV2<'frame, 'metadata> = (
    i64,
    i64,
    ListlikeIterator<'frame, 'metadata, (Uuid, i32)>,
    i64,
);

static RAW_TABLETS_V1_CQL_TYPE: LazyLock<ColumnType<'static>> = LazyLock::new(|| {
    ColumnType::Tuple(vec![
        ColumnType::Native(NativeType::BigInt),
        ColumnType::Native(NativeType::BigInt),
        ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Tuple(vec![
                ColumnType::Native(NativeType::Uuid),
                ColumnType::Native(NativeType::Int),
            ]))),
        },
    ])
});

static RAW_TABLETS_V2_CQL_TYPE: LazyLock<ColumnType<'static>> = LazyLock::new(|| {
    ColumnType::Tuple(vec![
        ColumnType::Native(NativeType::BigInt),
        ColumnType::Native(NativeType::BigInt),
        ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Tuple(vec![
                ColumnType::Native(NativeType::Uuid),
                ColumnType::Native(NativeType::Int),
            ]))),
        },
        ColumnType::Native(NativeType::BigInt),
    ])
});

const CUSTOM_PAYLOAD_TABLETS_V1_KEY: &str = "tablets-routing-v1";
const CUSTOM_PAYLOAD_TABLETS_V2_KEY: &str = "tablets-routing-v2";

/// An opaque tablet version learned via the `TABLETS_ROUTING_V2` protocol extension.
///
/// It is a 64-bit hash of the tablet's ordered replica list -- not a numeric counter -- so only
/// its bit pattern is ever meaningful, never its value as an integer. It is therefore stored as
/// the raw big-endian bytes the server sent (a CQL `bigint`), which makes that explicit and
/// keeps the type free of sign-changing casts.
///
/// The driver uses it to keep its tablet-routing cache fresh: a "block" byte sampled from the
/// version is sent with every `EXECUTE`, and the server replies with fresh routing information
/// whenever the sampled nibble disagrees with its own version. See
/// [`block_for`](TabletVersion::block_for).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct TabletVersion([u8; 8]);

impl TabletVersion {
    /// Builds a version from the signed `bigint` the server sends in a TABLETS_ROUTING_V2
    /// payload. Only the bit pattern matters.
    pub(crate) fn from_server_value(raw: i64) -> Self {
        Self(raw.to_be_bytes())
    }

    /// Chooses a tablet-version "block" byte to send with an `EXECUTE` on a
    /// TABLETS_ROUTING_V2 connection.
    ///
    /// The index of the block is randomized so that, over successive requests,
    /// the driver eventually probes every nibble and detects any version change.
    fn choose_block(self) -> u8 {
        let index: u8 = rand::rng().random::<u8>() & 0x0F; // 0..=15
        // Bytes are big-endian, so nibble `index` (counted from the least significant one)
        // lives in byte `7 - index / 2`: its low half for an even index, its high half for an
        // odd one.
        let byte = self.0[7 - (index / 2) as usize];
        let nibble = if index.is_multiple_of(2) {
            byte & 0xF
        } else {
            byte >> 4
        };
        (index << 4) | nibble
    }

    /// Returns a random tablet-version block byte, used when no tablet version
    /// is cached for the target token. A random block maximizes the chance of
    /// a mismatch, which prompts the server to return fresh routing information.
    pub(crate) fn random_block() -> u8 {
        rand::rng().random::<u8>()
    }

    /// Returns the tablet-version block byte to attach to an `EXECUTE` on a
    /// TABLETS_ROUTING_V2 connection: the cached version's block when known (see
    /// [`choose_block`](TabletVersion::choose_block)), or a random probe byte on a cache miss
    /// (see [`random_block`](TabletVersion::random_block)).
    pub(crate) fn block_for(version: Option<Self>) -> u8 {
        match version {
            Some(version) => version.choose_block(),
            None => Self::random_block(),
        }
    }
}

impl RawTablet {
    pub(crate) fn from_custom_payload(
        payload: &HashMap<String, Bytes>,
    ) -> Option<Result<RawTablet, TabletParsingError>> {
        // A V2 connection receives the V2 key, a V1 connection the V1 key. The keys are
        // mutually exclusive and self-describing (V2 carries an extra `bigint` version),
        // so prefer V2 and fall back to V1.
        if let Some(payload) = payload.get(CUSTOM_PAYLOAD_TABLETS_V2_KEY) {
            Some(Self::parse_v2(payload))
        } else {
            let payload = payload.get(CUSTOM_PAYLOAD_TABLETS_V1_KEY)?;
            Some(Self::parse_v1(payload))
        }
    }

    fn parse_v1(payload: &Bytes) -> Result<RawTablet, TabletParsingError> {
        <RawTabletPayloadV1 as DeserializeValue<'_, '_>>::type_check(
            RAW_TABLETS_V1_CQL_TYPE.deref(),
        )?;
        let (first_token, last_token, replicas): RawTabletPayloadV1 =
            <RawTabletPayloadV1 as DeserializeValue<'_, '_>>::deserialize(
                RAW_TABLETS_V1_CQL_TYPE.deref(),
                Some(FrameSlice::new(payload)),
            )?;
        Self::build(first_token, last_token, replicas, None)
    }

    fn parse_v2(payload: &Bytes) -> Result<RawTablet, TabletParsingError> {
        <RawTabletPayloadV2 as DeserializeValue<'_, '_>>::type_check(
            RAW_TABLETS_V2_CQL_TYPE.deref(),
        )?;
        let (first_token, last_token, replicas, tablet_version): RawTabletPayloadV2 =
            <RawTabletPayloadV2 as DeserializeValue<'_, '_>>::deserialize(
                RAW_TABLETS_V2_CQL_TYPE.deref(),
                Some(FrameSlice::new(payload)),
            )?;
        Self::build(
            first_token,
            last_token,
            replicas,
            Some(TabletVersion::from_server_value(tablet_version)),
        )
    }

    fn build(
        first_token: i64,
        last_token: i64,
        replicas: ListlikeIterator<'_, '_, (Uuid, i32)>,
        tablet_version: Option<TabletVersion>,
    ) -> Result<RawTablet, TabletParsingError> {
        // Important invariant. That way we guarantee that:
        // - Token range is not empty.
        // - Token range doesn't cross the i64::MAX/i64::MIN boundary.
        if last_token <= first_token {
            return Err(TabletParsingError::WrongTokenRange(first_token, last_token));
        }

        let replicas = replicas
            .map(|res| {
                res.map_err(TabletParsingError::from)
                    .and_then(|(uuid, shard_num)| match shard_num.try_into() {
                        Ok(s) => Ok((uuid, s)),
                        Err(_) => Err(TabletParsingError::ShardNum(shard_num)),
                    })
            })
            .collect::<Result<Vec<(Uuid, Shard)>, TabletParsingError>>()?;

        Ok(RawTablet {
            // +1 because ScyllaDB sends left-open range, so received
            // number is the last token not belonging to this tablet.
            // This won't overflow because we checked that first token
            // is strictly smaller than the last, so must be at least
            // one less than i64::MAX.
            first_token: Token::new(first_token + 1),
            last_token: Token::new(last_token),
            replicas: RawTabletReplicas { replicas },
            tablet_version,
        })
    }
}

#[derive(Clone, Debug, Default)]
#[cfg_attr(test, derive(Eq))]
struct TabletReplicas {
    /// The replicas in payload order. A datacenter-scoped query is answered
    /// with a mask of positions in this list (see `dc_masks` and
    /// `ReplicaSetInner::MaskedSharded`).
    ///
    /// Behind an `Arc` so that cloning a `Tablet` (which happens for every
    /// tablet of a table whenever one of that table's tablets is updated) is a
    /// single reference count increment rather than a copy of the list.
    all: Arc<[(Arc<Node>, Shard)]>,
    /// One mask per datacenter among the replicas: the positions in `all` of
    /// the replicas in that datacenter. Which datacenter a mask stands for is
    /// told by its first replica, so no name is stored and a datacenter query
    /// costs one comparison per datacenter of the tablet instead of one per
    /// replica - and does not depend on where in the list the datacenter's
    /// replicas are.
    ///
    /// A mask is `Self::mask_words(all.len())` words long (see [`MaskBits`]),
    /// and the masks are stored back to back. A tablet has up to 64 replicas in
    /// practice, so a mask is one word and this holds three datacenters inline.
    ///
    /// Replicas whose node has no datacenter are in no mask: they match no
    /// datacenter query.
    dc_masks: SmallVec<[u64; 3]>,
}

impl TabletReplicas {
    /// Gets raw replica list (which is an array of (Uuid, Shard)) and retrieves
    /// `Node` objects.
    /// In case of failure this function returns Self, but with the problematic nodes skipped,
    /// and a list of skipped uuids - so that the caller can e.g. do some logging.
    pub(crate) fn from_raw_replicas(
        raw_replicas: &RawTabletReplicas,
        replica_translator: impl Fn(Uuid) -> Option<Arc<Node>>,
    ) -> Result<Self, (Self, Vec<Uuid>)> {
        let mut failed = Vec::new();
        // Sized up front: collecting straight into the `Arc` would go through a
        // `Vec` anyway (the filtered iterator's length is not trusted), growing
        // it by doubling and paying an allocation per step.
        let mut all = Vec::with_capacity(raw_replicas.replicas.len());
        for (replica, shard) in raw_replicas.replicas.iter() {
            match replica_translator(*replica) {
                Some(node) => all.push((node, *shard as Shard)),
                None => failed.push(*replica),
            }
        }
        let all: Arc<[(Arc<Node>, Shard)]> = all.into();

        if failed.is_empty() {
            Ok(Self::new(all))
        } else {
            Err((Self::new(all), failed))
        }
    }

    fn new(all: Arc<[(Arc<Node>, Shard)]>) -> Self {
        let dc_masks = Self::index(&all);
        Self { all, dc_masks }
    }

    /// Builds the `dc_masks` of `all`.
    fn index(all: &[(Arc<Node>, Shard)]) -> SmallVec<[u64; 3]> {
        let words = Self::mask_words(all.len());
        let mut dc_masks: SmallVec<[u64; 3]> = SmallVec::new();
        for (i, (node, _)) in all.iter().enumerate() {
            let Some(dc) = node.datacenter.as_deref() else {
                continue;
            };
            let (word, bit) = (i / u64::BITS as usize, 1u64 << (i % u64::BITS as usize));
            match dc_masks
                .chunks_exact_mut(words)
                .find(|mask| Self::datacenter_of_mask(all, mask) == Some(dc))
            {
                Some(mask) => mask[word] |= bit,
                None => {
                    let start = dc_masks.len();
                    dc_masks.resize(start + words, 0);
                    dc_masks[start + word] = bit;
                }
            }
        }
        dc_masks
    }

    /// Number of words in a mask over a replica list of `len` entries.
    ///
    /// At least one, so that an empty list still has a well-defined (and
    /// non-zero) stride for `chunks_exact`.
    fn mask_words(len: usize) -> usize {
        len.div_ceil(u64::BITS as usize).max(1)
    }

    /// The datacenter a mask of `dc_masks` stands for: that of its first replica.
    fn datacenter_of_mask<'a>(all: &'a [(Arc<Node>, Shard)], mask: &[u64]) -> Option<&'a str> {
        // Spelled out rather than `MaskBits::new(mask).next()`: this runs for
        // every datacenter of the tablet on every datacenter-scoped lookup.
        let (word_idx, word) = mask.iter().enumerate().find(|(_, word)| **word != 0)?;
        let first = word_idx * u64::BITS as usize + word.trailing_zeros() as usize;
        all.get(first)?.0.datacenter.as_deref()
    }

    /// Mask of the positions in `all` of the replicas in datacenter `dc`; empty
    /// if there are none.
    // A plain stepping loop instead of `chunks_exact`, which divides by the
    // chunk size up front; measurably cheaper on this hot path.
    #[inline]
    fn dc_mask(&self, dc: &str) -> &[u64] {
        let words = Self::mask_words(self.all.len());
        let masks: &[u64] = &self.dc_masks;
        let mut start = 0;
        while let Some(mask) = masks.get(start..start + words) {
            if Self::datacenter_of_mask(&self.all, mask) == Some(dc) {
                return mask;
            }
            start += words;
        }
        &[]
    }

    #[cfg(test)]
    fn new_for_test(replicas: Vec<Arc<Node>>) -> Self {
        Self::new(replicas.into_iter().map(|r| (r, 0)).collect())
    }
}

// We can't use derive because it would use normal comparision while
// comapring replicas needs to use `Arc::ptr_eq`. It's not enough to compare host ids,
// because different `Node` objects nay have the same host id.
// There is not reason to compare this outside of tests, so the `cfg(test)` is there
// to prevent future contributors from doing something stupid like comparing it
// in non-test driver code.
#[cfg(test)]
impl PartialEq for TabletReplicas {
    fn eq(&self, other: &Self) -> bool {
        if self.all.len() != other.all.len() {
            return false;
        }
        for ((self_node, self_shard), (other_node, other_shard)) in
            self.all.iter().zip(other.all.iter())
        {
            if self_shard != other_shard {
                return false;
            }
            if !Arc::ptr_eq(self_node, other_node) {
                return false;
            }
        }

        true
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) struct Tablet {
    /// First token belonging to the tablet, inclusive
    first_token: Token,
    /// Last token belonging to the tablet, inclusive
    last_token: Token,
    replicas: TabletReplicas,
    /// Present only when the tablet was learned via the
    /// TABLETS_ROUTING_V2 extension.
    tablet_version: Option<TabletVersion>,
    /// If any of the replicas failed to resolve to a Node,
    /// then this field will contain the original list of replicas.
    ///
    /// Boxed because it is rarely present and would otherwise take a `Vec`'s
    /// worth of space in every tablet.
    failed: Option<Box<RawTabletReplicas>>,
}

impl Tablet {
    /// The replica leading this tablet's Raft group, if it is known.
    ///
    /// Precondition: the tablet is strongly-consistent.
    pub(crate) fn known_leader(&self) -> Option<(&Arc<Node>, Shard)> {
        let leader = self.replicas.all.first()?;

        // `failed` holds the *original* replica list, so its first entry is the
        // leader the payload named. If that host is the one `replicas` starts
        // with, the leader resolved fine and the unresolved replicas are all
        // followers; otherwise the leader itself is missing and we know of none.
        if let Some(failed) = self.failed.as_ref() {
            let named_leader = failed.replicas.first()?;
            if named_leader.0 != leader.0.host_id {
                return None;
            }
        }

        Some((&leader.0, leader.1))
    }

    pub(crate) fn from_raw_tablet(
        raw_tablet: RawTablet,
        replica_translator: impl Fn(Uuid) -> Option<Arc<Node>>,
    ) -> Result<Self, (Self, Vec<Uuid>)> {
        let replicas_result =
            TabletReplicas::from_raw_replicas(&raw_tablet.replicas, replica_translator);
        match replicas_result {
            Ok(replicas) => Ok(Self {
                first_token: raw_tablet.first_token,
                last_token: raw_tablet.last_token,
                replicas,
                tablet_version: raw_tablet.tablet_version,
                failed: None,
            }),
            Err((replicas, failed_replicas)) => Err((
                Self {
                    first_token: raw_tablet.first_token,
                    last_token: raw_tablet.last_token,
                    replicas,
                    tablet_version: raw_tablet.tablet_version,
                    failed: Some(Box::new(raw_tablet.replicas)),
                },
                failed_replicas,
            )),
        }
    }

    pub(crate) fn range(&self) -> (Token, Token) {
        (self.first_token, self.last_token)
    }

    /// Whether `raw` carries exactly the routing information `self` does: the
    /// same token range and version, the same replicas (by host id and shard)
    /// in the same order, and all of `self`'s replicas resolved.
    ///
    /// Comparing host ids is exact because a state's tablets only ever hold the
    /// `Node` objects of its `known_nodes` (maintenance replaces recreated ones),
    /// which is also what `raw` would resolve to.
    fn is_same_as(&self, raw: &RawTablet) -> bool {
        self.first_token == raw.first_token
            && self.last_token == raw.last_token
            && self.tablet_version == raw.tablet_version
            && self.failed.is_none()
            && self.replicas.all.len() == raw.replicas.replicas.len()
            && self
                .replicas
                .all
                .iter()
                .zip(raw.replicas.replicas.iter())
                .all(|((node, shard), (host_id, raw_shard))| {
                    node.host_id == *host_id && shard == raw_shard
                })
    }

    // Returns `Ok(())` if after the operation Tablet replicas are fully resolved.
    // Return `Err(replicas)` if some replicas failed to resolve. `replicas` is a
    // list of Uuids that failed to resolve.
    fn re_resolve_replicas(
        &mut self,
        replica_translator: impl Fn(Uuid) -> Option<Arc<Node>>,
    ) -> Result<(), Vec<Uuid>> {
        if let Some(failed) = self.failed.as_ref() {
            match TabletReplicas::from_raw_replicas(failed, replica_translator) {
                Ok(resolved_replicas) => {
                    // We managed to successfully resolve all replicas, all is well.
                    self.replicas = resolved_replicas;
                    self.failed = None;
                    Ok(())
                }
                Err((_, failed)) => Err(failed),
            }
        } else {
            Ok(())
        }
    }

    fn update_stale_nodes(&mut self, recreated_nodes: &HashMap<Uuid, Arc<Node>>) {
        // The replica list is shared with the previous `ClusterState`, so it is
        // only copied (by `make_mut`) if there is something to replace in it.
        let any_stale = self
            .replicas
            .all
            .iter()
            .any(|(node, _)| recreated_nodes.contains_key(&node.host_id));
        if !any_stale {
            return;
        }
        for (node, _) in Arc::make_mut(&mut self.replicas.all).iter_mut() {
            if let Some(new_node) = recreated_nodes.get(&node.host_id) {
                assert!(!Arc::ptr_eq(new_node, node));
                *node = Arc::clone(new_node);
            }
        }
        // A recreated node may be in another datacenter now.
        self.replicas.dc_masks = TabletReplicas::index(&self.replicas.all);
    }

    #[cfg(test)]
    fn new_for_test(token: i64, replicas: Vec<Arc<Node>>, failed: Option<Vec<Uuid>>) -> Self {
        Self {
            first_token: Token::new(token),
            last_token: Token::new(token),
            replicas: TabletReplicas::new_for_test(replicas),
            tablet_version: None,
            failed: failed.map(|vec| {
                Box::new(RawTabletReplicas {
                    replicas: vec.into_iter().map(|id| (id, 0)).collect::<Vec<_>>(),
                })
            }),
        }
    }
}

/// A tablet's replicas together with a mask of the positions of a subset of
/// them, see [`TabletReplicas::dc_mask`].
pub(crate) type MaskedReplicas<'a> = (&'a [(Arc<Node>, Shard)], &'a [u64]);

/// The positions of the set bits of a mask, ascending. Bit `i` of the mask is
/// bit `i % 64` of word `i / 64`.
///
/// `size_hint` is exact, so a replica set backed by a mask has an O(1) `len()`.
/// `nth` is the default loop of `next`: it is only ever asked to skip fewer
/// bits than are set, and a popcount-based skip would cost more than the few
/// steps it saves (the default target has no `popcnt`, so `count_ones` is a
/// dozen instructions).
#[derive(Clone, Debug)]
pub(crate) struct MaskBits<'a> {
    /// The bits of the current word not yielded yet.
    word: u64,
    /// Position of bit 0 of `word`.
    base: usize,
    /// The words after the current one.
    rest: &'a [u64],
}

impl<'a> MaskBits<'a> {
    pub(crate) fn new(mask: &'a [u64]) -> Self {
        let (word, rest) = mask
            .split_first()
            .map_or((0, &[][..]), |(w, rest)| (*w, rest));
        Self {
            word,
            base: 0,
            rest,
        }
    }

    /// Moves to the next word; `false` if there is none (the iterator is then exhausted).
    fn advance_word(&mut self) -> bool {
        let Some((word, rest)) = self.rest.split_first() else {
            return false;
        };
        self.word = *word;
        self.rest = rest;
        self.base += u64::BITS as usize;
        true
    }
}

impl Iterator for MaskBits<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        while self.word == 0 {
            if !self.advance_word() {
                return None;
            }
        }
        let pos = self.base + self.word.trailing_zeros() as usize;
        self.word &= self.word - 1;
        Some(pos)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let ones = |word: &u64| word.count_ones() as usize;
        let n = ones(&self.word) + self.rest.iter().map(ones).sum::<usize>();
        (n, Some(n))
    }
}

impl ExactSizeIterator for MaskBits<'_> {}

/// Container for tablets of a single table.
///
/// It can be viewed as a set of non-overlapping Tablet objects.
/// It has 2 basic operations:
/// 1. Find a tablet for given Token
/// 2. Add a new tablet.
///
/// Adding new Tablet will first remove all tablets that overlap with the new tablet.
#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) struct TableTablets {
    table_spec: TableSpec<'static>,
    tablet_list: Vec<Tablet>,
    /// In order to make typical tablet maintance faster
    /// we remember if there were any tablets that have unrecognized uuids in replica list.
    /// If there were none, and a few other conditions are satisfied, we can skip nearly whole maintanace.
    /// This flag may be falsely true: if we add tablet with unknown replica but later
    /// overwrite it with some other tablet.
    has_unknown_replicas: bool,
}

impl TableTablets {
    fn new(table_spec: TableSpec<'static>) -> Self {
        Self {
            table_spec,
            tablet_list: Default::default(),
            has_unknown_replicas: false,
        }
    }

    fn tablet_for_token(&self, token: Token) -> Option<&Tablet> {
        let idx = self
            .tablet_list
            .partition_point(|tablet| tablet.last_token < token);
        let tablet = self.tablet_list.get(idx);
        tablet.filter(|t| t.first_token <= token)
    }

    pub(crate) fn replicas_for_token(&self, token: Token) -> Option<&[(Arc<Node>, Shard)]> {
        self.tablet_for_token(token)
            .map(|tablet| &*tablet.replicas.all)
    }

    /// The replicas of the tablet owning `token` together with the mask of
    /// those of them that are in datacenter `dc`.
    pub(crate) fn dc_replicas_for_token(
        &self,
        token: Token,
        dc: &str,
    ) -> Option<MaskedReplicas<'_>> {
        self.tablet_for_token(token)
            .map(|tablet| (&*tablet.replicas.all, tablet.replicas.dc_mask(dc)))
    }

    /// Returns the tablet version for the tablet owning `token`, if known.
    ///
    /// `None` means either no tablet is cached for the token, or the cached tablet was
    /// learned via TABLETS_ROUTING_V1 (which carries no version).
    pub(crate) fn tablet_version_for_token(&self, token: Token) -> Option<TabletVersion> {
        self.tablet_for_token(token)
            .and_then(|tablet| tablet.tablet_version)
    }

    /// Returns the replica leading the Raft group of the tablet owning `token`, if it is known.
    ///
    /// The caller must ensure the tablet is strongly-consistent.
    pub(crate) fn leader_for_token(&self, token: Token) -> Option<(&Arc<Node>, Shard)> {
        self.tablet_for_token(token)?.known_leader()
    }

    /// Whether the tablet `raw` describes is already present exactly so (see
    /// [`Tablet::is_same_as`]), so that adding it would change nothing.
    fn contains(&self, raw: &RawTablet) -> bool {
        self.tablet_for_token(raw.first_token)
            .is_some_and(|present| present.is_same_as(raw))
    }

    /// This method:
    /// - first removes all tablets that overlap with `tablet` from `self`
    /// - adds `tablet` to `self`
    ///
    /// This preserves the invariant that all tablets in `self` are non-overlapping.
    fn add_tablet(&mut self, tablet: Tablet) {
        if tablet.failed.is_some() {
            self.has_unknown_replicas = true;
        }
        // Smallest `left_idx` for which `tablet.first_token` is LESS OR EQUAL to `tablet_list[left_idx].last_token`.
        // It implies that `tablet_list[left_idx]` overlaps with `tablet` iff `tablet.last_token`
        // is GREATER OR EQUAL to `tablet_list[left_idx].first_token`.
        let left_idx = self
            .tablet_list
            .partition_point(|t| t.last_token < tablet.first_token);
        // Smallest `right_idx` for which `tablet.last_token` is LESS than `tablet_list[right_idx].first_token`.
        // It means that `right_idx` is the index of first tablet that is "to the right" of `tablet` and doesn't overlap with it.
        // From this it follows that if `tablet_list[left_idx]` turns out to not overlap with `tablet`, then `left_idx == right_idx`
        // and we won't remove any tablets because `tablet` doesn't overlap with any existing tablets.
        let right_idx = self
            .tablet_list
            .partition_point(|t| t.first_token <= tablet.last_token);
        self.tablet_list.drain(left_idx..right_idx);
        self.tablet_list.insert(left_idx, tablet);
    }

    fn perform_maintenance(
        &mut self,
        removed_nodes: &HashSet<Uuid>,
        all_current_nodes: &HashMap<Uuid, Arc<Node>>,
        recreated_nodes: &HashMap<Uuid, Arc<Node>>,
    ) {
        // First we need to re-resolve unknown replicas or remove their tablets.
        // It will make later checks easier because we'll know that `failed` field
        // is `None` for all tablets.
        if self.has_unknown_replicas {
            self.tablet_list.retain_mut(|tablet| {
                let r = tablet.re_resolve_replicas(|id: Uuid| all_current_nodes.get(&id).cloned());
                if let Err(failed) = &r {
                    warn!("Nodes ({}) listed as replicas for a tablet {{ks: {}, table: {}, range: [{}. {}]}} are not present in ClusterState.known_nodes, \
                           despite topology refresh. Removing problematic tablet.",
                           failed.iter().safe_format(", "), self.table_spec.ks_name(), self.table_spec.table_name(), tablet.first_token.value(), tablet.last_token.value());
                }

                r.is_ok()
            });
        }

        // Now we remove all tablets that have replicas on removed nodes.
        if !removed_nodes.is_empty() {
            self.tablet_list.retain(|tablet| {
                tablet
                    .replicas
                    .all
                    .iter()
                    .all(|node| !removed_nodes.contains(&node.0.host_id))
            });
        }

        // The last thing to do is to replace all old `Node` objects.
        // Situations where driver requires this don't happen often:
        // - Node IP change
        // - Node DC change / Rack change
        // so I don't think we should be too concerned about performance of this code.
        if !recreated_nodes.is_empty() {
            for tablet in self.tablet_list.iter_mut() {
                tablet.update_stale_nodes(recreated_nodes);
            }
        }

        // All unknown replicas were either resolved or whole tablets removed.
        self.has_unknown_replicas = false;
    }

    #[cfg(test)]
    fn new_for_test() -> Self {
        Self::new(TableSpec::borrowed("test_ks", "test_table"))
    }
}

/// Needed to query hashbrown::HashMap<TableSpec<'static>, TableTablets>
/// with `TableSpec` of any lifetime.
#[derive(Hash)]
struct TableSpecQueryKey<'a> {
    table_spec: &'a TableSpec<'a>,
}

impl<'key, 'query> hashbrown::Equivalent<TableSpec<'key>> for TableSpecQueryKey<'query> {
    fn equivalent(&self, key: &TableSpec<'key>) -> bool {
        self.table_spec == key
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) struct TabletsInfo {
    // We use hashbrown hashmap instead of std hashmap because with
    // std one it is not possible to query map with key `TableSpec<'static>`
    // using `TableSpec<'a>` for `'a` other than `'static`.
    // This is because `std::hashmap` requires that the key implements `Borrow<Q>`
    // where `&Q` is an argument to `.get(key)` method. It is not possible to write
    // such `Borrow` impl for `TableSpec`.
    // HashBrown on the other hand requires only `Q: Hash + Equivalent<K> + ?Sized`,
    // and it is easy to create a wrapper type with required `Equivalent` impl.
    //
    // Each table's tablets are behind an `Arc`, so that a tablet update - which
    // clones the whole `TabletsInfo` and then changes one table - copies only
    // that table's tablets and shares the rest.
    tablets: hashbrown::HashMap<TableSpec<'static>, Arc<TableTablets>>,
    /// See `has_unknown_replicas` field in `TableTablets`.
    /// The field here will be true if it is true for any TableTablets.
    has_unknown_replicas: bool,
}

impl TabletsInfo {
    pub(crate) fn new() -> Self {
        Self {
            tablets: hashbrown::HashMap::new(),
            has_unknown_replicas: false,
        }
    }

    pub(crate) fn tablets_for_table<'a, 'b>(
        &'a self,
        table_spec: &'b TableSpec<'b>,
    ) -> Option<&'a TableTablets> {
        let query_key = TableSpecQueryKey { table_spec };
        self.tablets.get(&query_key).map(Arc::as_ref)
    }

    /// Whether the tablet `raw` describes is already present for `table_spec`
    /// exactly so, so that adding it would change nothing. See
    /// [`TableTablets::contains`].
    pub(crate) fn contains(&self, table_spec: &TableSpec<'_>, raw: &RawTablet) -> bool {
        self.tablets_for_table(table_spec)
            .is_some_and(|tablets| tablets.contains(raw))
    }

    pub(crate) fn add_tablet(&mut self, table_spec: TableSpec<'static>, tablet: Tablet) {
        if tablet.failed.is_some() {
            self.has_unknown_replicas = true;
        }
        let table_tablets = self.tablets.entry(table_spec).or_insert_with_key(|k| {
            tracing::debug!(
                "Found new tablets table: {}.{}",
                k.ks_name(),
                k.table_name()
            );
            Arc::new(TableTablets::new(k.clone()))
        });
        Arc::make_mut(table_tablets).add_tablet(tablet)
    }

    #[expect(clippy::doc_overindented_list_items)]
    /// This method is supposed to be called when topology is updated.
    /// It goes through tablet info and adjusts it to topology changes, to prevent
    /// a situation where local tablet info and a real one are permanently different.
    /// What is updated:
    /// 1. Info for dropped tables, and tables that are in non-tablet keyspaces
    ///    according to fetched schema. Empty tablet lists are added for tables
    ///    in tablet-based keyspaces, to prevent ReplicaLocator from treating
    ///    them as VNode-based.
    /// 2. Tablets where a removed node was one of replicas are removed.
    ///    Can be skipped if no nodes were removed.
    /// 3. Tablets with unrecognized uuids in replica list are resolved again.
    ///    If this is unsuccessful again then the tablet is removed.
    ///    This can be skipped if we know we have no such tablets.
    /// 4. Rarely, the driver may need to re-create `Node` object for a given node.
    ///    The old object is replaced with the new one in replica lists.
    ///    This can be skipped if there were no re-created `Node` objects.
    ///
    /// In order to not perform unnecessary work during typical schema refresh
    /// we avoid iterating through tablets at all if steps 2-4 can be skipped.
    ///
    /// * `removed_nodes`: Nodes that previously were present in ClusterState but are not anymore.
    ///                    For any such node we should remove all tablets that have it in replica list.
    ///                    This is because otherwise:
    ///                    1. We would keep old `Node` objects, not allowing them to release memory.
    ///                    2. We would return removed nodes in LBP
    ///                    3. When a new node joins and becomes replica for this tablet, we would
    ///                       not use it - instead we would keep querying a subset of replicas.
    ///
    /// * `all_current_nodes`: Map of all nodes. Required to remap unknown replicas.
    ///                        If we didn't try to remap them and instead just skipped them,
    ///                        then we would only query subset of replicas for the tablet,
    ///                        potentially increasing load on those replicas.
    ///                        The alternative is dropping the tablet immediately, but if there are a lot
    ///                        of requests to a range belonging to this tablet, then we would get a
    ///                        lot of unnecessary feedbacks sent. Thus the current solution:
    ///                        skipping unknown replicas and dropping the tablet if we still can't resolve
    ///                        them after topology refresh.
    ///
    /// * `recreated_nodes`: There are some situations (IP change, DC / Rack change) where the driver
    ///                      will create a new `Node` object for some node and drop the old one.
    ///                      Tablet info would still contain the old object, so the driver would not use
    ///                      new connections. That means if there were such nodes then we need to go over
    ///                      tablets and replace `Arc<Node>` objects for recreated nodes.
    ///
    /// There are some situations not handled by this maintanance procedure that could
    /// still result in permanent difference between local and real tablet info:
    ///
    /// * Extending replica list for a tablet: If a new replica is added to replica list,
    ///   then we won't learn about it, because we'll keep querying current replicas, which are
    ///   still replicas. I'm not sure if this can happen. The only scenario where this seems
    ///   possible is increasing RF - I'm not sure if this would only add replicas or make more changes.
    ///   We could probably discover it by comparing replication strategy pre and post topology referesh
    ///   and if it changed then remove tablet info for this keyspace.
    ///
    /// * Removing the keyspace and recreating it immediately without tablets. This seems so absurd
    ///   that we most likely don't need to worry about it, but I'm putting it here as a potential problem
    ///   for completeness.
    pub(crate) fn perform_maintenance(
        &mut self,
        keyspaces: &HashMap<String, Keyspace>,
        removed_nodes: &HashSet<Uuid>,
        all_current_nodes: &HashMap<Uuid, Arc<Node>>,
        recreated_nodes: &HashMap<Uuid, Arc<Node>>,
    ) {
        // First we remove info for all tables that are no longer present,
        // or are in non-tablet keyspace.
        self.tablets.retain(|k, _| {
            let Some(keyspace) = keyspaces.get(k.ks_name()) else {
                return false;
            };
            if !keyspace.tablet_based {
                return false;
            }

            // Materialized views are tablet-based too, but they live in a
            // separate `views` map, so we must check both.
            keyspace.tables.contains_key(k.table_name())
                || keyspace.views.contains_key(k.table_name())
        });

        // Now we add empty entries for all tables in tablet-based keyspaces
        // that don't already have an entry. This prevents ReplicaLocator
        // from returning VNode-based replicas for such tables. Materialized
        // views are tablet-based too, so we include them as well.
        keyspaces
            .iter()
            .filter(|(_, ks)| ks.tablet_based)
            .for_each(|(ks_name, ks)| {
                ks.tables
                    .keys()
                    .chain(ks.views.keys())
                    .for_each(|table_name| {
                        let borrowed_spec =
                            TableSpec::borrowed(ks_name.as_str(), table_name.as_str());
                        let query_key = TableSpecQueryKey {
                            table_spec: &borrowed_spec,
                        };
                        self.tablets
                            .raw_entry_mut()
                            .from_key(&query_key)
                            .or_insert_with(|| {
                                (
                                    borrowed_spec.to_owned(),
                                    Arc::new(TableTablets::new(borrowed_spec.to_owned())),
                                )
                            });
                    })
            });

        if !removed_nodes.is_empty() || !recreated_nodes.is_empty() || self.has_unknown_replicas {
            for (_, table_tablets) in self.tablets.iter_mut() {
                Arc::make_mut(table_tablets).perform_maintenance(
                    removed_nodes,
                    all_current_nodes,
                    recreated_nodes,
                );
            }
        }

        // All unknown replicas were either resolved or whole tablets removed.
        self.has_unknown_replicas = false;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use crate::cluster::metadata::{ConsistencyMode, Keyspace, Strategy, Table};
    use crate::frame::response::result::{CollectionType, ColumnType, NativeType, TableSpec};
    use crate::serialize::value::SerializeValue;
    use crate::serialize::writers::CellWriter;
    use bytes::Bytes;
    use tracing::debug;
    use uuid::Uuid;

    use crate::cluster::Node;
    use crate::routing::locator::tablets::{
        CUSTOM_PAYLOAD_TABLETS_V1_KEY, CUSTOM_PAYLOAD_TABLETS_V2_KEY, MaskBits,
        RAW_TABLETS_V1_CQL_TYPE, RAW_TABLETS_V2_CQL_TYPE, RawTablet, RawTabletReplicas,
        TabletParsingError, TabletVersion,
    };
    use crate::routing::{Shard, Token};
    use crate::test_utils::setup_tracing;
    use crate::value::CqlValue;

    use super::{TableTablets, Tablet, TabletReplicas, TabletsInfo};

    const DC1: &str = "dc1";
    const DC2: &str = "dc2";
    const DC3: &str = "dc3";

    #[test]
    fn test_raw_tablet_deser_empty() {
        let custom_payload = HashMap::new();
        assert!(RawTablet::from_custom_payload(&custom_payload).is_none());
    }

    #[test]
    fn test_raw_tablet_deser_trash() {
        let custom_payload = HashMap::from([(
            CUSTOM_PAYLOAD_TABLETS_V1_KEY.to_string(),
            Bytes::from_static(&[1, 2, 3]),
        )]);
        assert_matches::assert_matches!(
            RawTablet::from_custom_payload(&custom_payload),
            Some(Err(TabletParsingError::Deserialization(_)))
        );
    }

    #[test]
    fn test_raw_tablet_deser_wrong_type() {
        let mut custom_payload = HashMap::new();
        let mut data = vec![];

        let value = CqlValue::Tuple(vec![
            Some(CqlValue::Ascii("asdderty".to_string())),
            Some(CqlValue::BigInt(1234)),
            Some(CqlValue::List(vec![])),
        ]);
        let col_type = ColumnType::Tuple(vec![
            ColumnType::Native(NativeType::Ascii),
            ColumnType::Native(NativeType::BigInt),
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(Box::new(ColumnType::Tuple(vec![
                    ColumnType::Native(NativeType::Uuid),
                    ColumnType::Native(NativeType::Int),
                ]))),
            },
        ]);

        SerializeValue::serialize(&value, &col_type, CellWriter::new(&mut data)).unwrap();
        debug!("{:?}", data);

        custom_payload.insert(CUSTOM_PAYLOAD_TABLETS_V1_KEY.to_string(), Bytes::from(data));

        assert_matches::assert_matches!(
            RawTablet::from_custom_payload(&custom_payload),
            Some(Err(TabletParsingError::Deserialization(_)))
        );
    }

    /// Helper: build a custom payload map from (first_token, last_token, replicas).
    fn make_tablet_custom_payload(first_token: i64, last_token: i64) -> HashMap<String, Bytes> {
        let mut data = vec![];
        let value = CqlValue::Tuple(vec![
            Some(CqlValue::BigInt(first_token)),
            Some(CqlValue::BigInt(last_token)),
            Some(CqlValue::List(vec![])),
        ]);
        SerializeValue::serialize(&value, &RAW_TABLETS_V1_CQL_TYPE, CellWriter::new(&mut data))
            .unwrap();
        // Skip the 4-byte length prefix added by SerializeValue::serialize,
        // because ScyllaDB sends the value without it.
        HashMap::from([(
            CUSTOM_PAYLOAD_TABLETS_V1_KEY.to_string(),
            Bytes::copy_from_slice(&data[4..]),
        )])
    }

    #[test]
    fn test_raw_tablet_deser_wrong_token_range() {
        // last_token < first_token
        let payload = make_tablet_custom_payload(100, -50);
        assert_matches::assert_matches!(
            RawTablet::from_custom_payload(&payload),
            Some(Err(TabletParsingError::WrongTokenRange(100, -50)))
        );

        // last_token == first_token (range would be empty since it's left-open)
        let payload = make_tablet_custom_payload(100, 100);
        assert_matches::assert_matches!(
            RawTablet::from_custom_payload(&payload),
            Some(Err(TabletParsingError::WrongTokenRange(100, 100)))
        );

        // Edge case: i64::MAX as first_token, i64::MIN as last_token
        let payload = make_tablet_custom_payload(i64::MAX, i64::MIN);
        assert_matches::assert_matches!(
            RawTablet::from_custom_payload(&payload),
            Some(Err(TabletParsingError::WrongTokenRange(i64::MAX, i64::MIN)))
        );

        // Sanity check: valid range still works
        let payload = make_tablet_custom_payload(99, 100);
        assert_matches::assert_matches!(RawTablet::from_custom_payload(&payload), Some(Ok(_)));
    }

    #[test]
    fn test_raw_tablet_deser_correct() {
        let mut custom_payload = HashMap::new();
        let mut data = vec![];

        const FIRST_TOKEN: i64 = 1234;
        const LAST_TOKEN: i64 = 5678;

        let value = CqlValue::Tuple(vec![
            Some(CqlValue::BigInt(FIRST_TOKEN)),
            Some(CqlValue::BigInt(LAST_TOKEN)),
            Some(CqlValue::List(vec![
                CqlValue::Tuple(vec![
                    Some(CqlValue::Uuid(Uuid::from_u64_pair(1, 2))),
                    Some(CqlValue::Int(15)),
                ]),
                CqlValue::Tuple(vec![
                    Some(CqlValue::Uuid(Uuid::from_u64_pair(3, 4))),
                    Some(CqlValue::Int(19)),
                ]),
            ])),
        ]);

        SerializeValue::serialize(&value, &RAW_TABLETS_V1_CQL_TYPE, CellWriter::new(&mut data))
            .unwrap();
        tracing::debug!("{:?}", data);

        custom_payload.insert(
            CUSTOM_PAYLOAD_TABLETS_V1_KEY.to_string(),
            // Skipping length because `SerializeValue::serialize` adds length at the
            // start of serialized value while ScyllaDB sends the value without initial
            // length.
            Bytes::copy_from_slice(&data[4..]),
        );

        let tablet = RawTablet::from_custom_payload(&custom_payload)
            .unwrap()
            .unwrap();

        assert_eq!(
            tablet,
            RawTablet {
                first_token: Token::new(FIRST_TOKEN + 1),
                last_token: Token::new(LAST_TOKEN),
                replicas: RawTabletReplicas {
                    replicas: vec![
                        (Uuid::from_u64_pair(1, 2), 15),
                        (Uuid::from_u64_pair(3, 4), 19)
                    ]
                },
                tablet_version: None,
            }
        );
    }

    #[test]
    fn test_choose_tablet_version_block_encoding() {
        // The block byte encodes `(index << 4) | nibble`, where `index` (high nibble)
        // selects one of the 16 nibbles of the version and `nibble` (low nibble) is that
        // nibble's value. This must match ScyllaDB's `compare_tablet_version_block`.
        let versions = [
            0x0000_0000_0000_0000u64,
            0x0123_4567_89AB_CDEFu64,
            0xFFFF_FFFF_FFFF_FFFFu64,
            0xDEAD_BEEF_CAFE_BABEu64,
        ];
        for version in versions {
            let tablet_version = TabletVersion::from_server_value(version as i64);
            let mut seen_indices = HashSet::new();
            for _ in 0..1000 {
                let block = tablet_version.choose_block();
                let index = block >> 4;
                let nibble = block & 0x0F;
                // This invariant holds for every possible RNG outcome, so the assertion
                // is deterministic regardless of which index happened to be drawn.
                let expected_nibble = ((version >> (index * 4)) & 0xF) as u8;
                assert_eq!(
                    nibble, expected_nibble,
                    "version={version:#018x} index={index} block={block:#04x}"
                );
                seen_indices.insert(index);
            }
            // Liveness: confirm the index is actually randomized. With 1000 draws over 16
            // possible indices, seeing only one is impossible in practice
            // (probability < 16 * (1/16)^999).
            assert!(
                seen_indices.len() >= 2,
                "index did not vary across draws for version {version:#018x}"
            );
        }
    }

    #[test]
    fn test_tablet_version_block_for_cache_miss_is_random() {
        // With no cached version there is nothing to probe, so the block is drawn at random -
        // over the whole byte, not just the nibble, since the index is meaningless too.
        let blocks: HashSet<u8> = (0..1000).map(|_| TabletVersion::block_for(None)).collect();
        assert!(
            blocks.len() >= 2,
            "block did not vary across draws on a cache miss"
        );
    }

    #[test]
    fn test_raw_tablet_deser_v2_correct() {
        // TABLETS_ROUTING_V2 payload: the V1 tuple plus a trailing bigint tablet version.
        let mut data = vec![];

        const FIRST_TOKEN: i64 = 1234;
        const LAST_TOKEN: i64 = 5678;
        const TABLET_VERSION: u64 = 0x0123_4567_89AB_CDEF;

        let value = CqlValue::Tuple(vec![
            Some(CqlValue::BigInt(FIRST_TOKEN)),
            Some(CqlValue::BigInt(LAST_TOKEN)),
            Some(CqlValue::List(vec![CqlValue::Tuple(vec![
                Some(CqlValue::Uuid(Uuid::from_u64_pair(1, 2))),
                Some(CqlValue::Int(15)),
            ])])),
            Some(CqlValue::BigInt(TABLET_VERSION as i64)),
        ]);

        SerializeValue::serialize(&value, &RAW_TABLETS_V2_CQL_TYPE, CellWriter::new(&mut data))
            .unwrap();

        let custom_payload = HashMap::from([(
            CUSTOM_PAYLOAD_TABLETS_V2_KEY.to_string(),
            // Skip the 4-byte length prefix, matching what ScyllaDB sends on the wire.
            Bytes::copy_from_slice(&data[4..]),
        )]);

        let tablet = RawTablet::from_custom_payload(&custom_payload)
            .unwrap()
            .unwrap();

        assert_eq!(
            tablet,
            RawTablet {
                // Note: +1 because ScyllaDB sends left-open range, so received
                //       number is the last token not belonging to this tablet.
                //       See `RawTablet::build`, which performs the shift.
                first_token: Token::new(FIRST_TOKEN + 1),
                last_token: Token::new(LAST_TOKEN),
                replicas: RawTabletReplicas {
                    replicas: vec![(Uuid::from_u64_pair(1, 2), 15)],
                },
                tablet_version: Some(TabletVersion::from_server_value(TABLET_VERSION as i64)),
            }
        );
    }

    #[test]
    fn test_raw_tablet_deser_prefers_v2_over_v1() {
        // The server should never send both V1 and V2 payloads.
        // However, let's test that the driver only looks at the
        // V2 one if both are present (as a sanity check).
        const V1_FIRST_TOKEN: i64 = 1;
        const V1_LAST_TOKEN: i64 = 2;
        let mut custom_payload = make_tablet_custom_payload(V1_FIRST_TOKEN, V1_LAST_TOKEN);

        const V2_FIRST_TOKEN: i64 = 1234;
        const V2_LAST_TOKEN: i64 = 5678;
        const TABLET_VERSION: u64 = 0x0123_4567_89AB_CDEF;

        let mut data = vec![];
        let value = CqlValue::Tuple(vec![
            Some(CqlValue::BigInt(V2_FIRST_TOKEN)),
            Some(CqlValue::BigInt(V2_LAST_TOKEN)),
            Some(CqlValue::List(vec![CqlValue::Tuple(vec![
                Some(CqlValue::Uuid(Uuid::from_u64_pair(1, 2))),
                Some(CqlValue::Int(15)),
            ])])),
            Some(CqlValue::BigInt(TABLET_VERSION as i64)),
        ]);
        SerializeValue::serialize(&value, &RAW_TABLETS_V2_CQL_TYPE, CellWriter::new(&mut data))
            .unwrap();

        custom_payload.insert(
            CUSTOM_PAYLOAD_TABLETS_V2_KEY.to_string(),
            Bytes::copy_from_slice(&data[4..]),
        );

        let tablet = RawTablet::from_custom_payload(&custom_payload)
            .unwrap()
            .unwrap();

        assert_eq!(
            tablet,
            RawTablet {
                first_token: Token::new(V2_FIRST_TOKEN + 1),
                last_token: Token::new(V2_LAST_TOKEN),
                replicas: RawTabletReplicas {
                    replicas: vec![(Uuid::from_u64_pair(1, 2), 15)],
                },
                tablet_version: Some(TabletVersion::from_server_value(TABLET_VERSION as i64)),
            }
        );
    }

    #[test]
    fn raw_replicas_resolve_in_order() {
        let nodes: HashMap<Uuid, Arc<Node>> = [
            Node::new_for_test(
                Some(Uuid::from_u64_pair(1, 1)),
                None,
                Some(DC1.to_string()),
                None,
            ),
            Node::new_for_test(
                Some(Uuid::from_u64_pair(1, 2)),
                None,
                Some(DC2.to_string()),
                None,
            ),
            Node::new_for_test(
                Some(Uuid::from_u64_pair(1, 3)),
                None,
                Some(DC3.to_string()),
                None,
            ),
            Node::new_for_test(
                Some(Uuid::from_u64_pair(1, 4)),
                None,
                Some(DC2.to_string()),
                None,
            ),
            Node::new_for_test(
                Some(Uuid::from_u64_pair(1, 5)),
                None,
                Some(DC2.to_string()),
                None,
            ),
            Node::new_for_test(
                Some(Uuid::from_u64_pair(1, 6)),
                None,
                Some(DC1.to_string()),
                None,
            ),
        ]
        .into_iter()
        .map(|node| (node.host_id, Arc::new(node)))
        .collect();

        let translator = |uuid| nodes.get(&uuid).cloned();

        let replicas_uids = [
            Uuid::from_u64_pair(1, 1),
            Uuid::from_u64_pair(1, 2),
            Uuid::from_u64_pair(1, 3),
            Uuid::from_u64_pair(1, 4),
            Uuid::from_u64_pair(1, 5),
            Uuid::from_u64_pair(1, 6),
        ];

        let raw_replicas = RawTabletReplicas {
            replicas: replicas_uids.into_iter().map(|uid| (uid, 1)).collect(),
        };

        let replicas = TabletReplicas::from_raw_replicas(&raw_replicas, translator);

        assert_eq!(
            replicas,
            Ok(TabletReplicas::new(
                replicas_uids
                    .iter()
                    .cloned()
                    .map(|replica| (translator(replica).unwrap(), 1))
                    .collect(),
            ))
        );
    }

    fn node_in_dc(id: u64, dc: Option<&str>) -> Arc<Node> {
        Arc::new(Node::new_for_test(
            Some(Uuid::from_u64_pair(1, id)),
            None,
            dc.map(str::to_owned),
            None,
        ))
    }

    // The per-datacenter masks must cover exactly the replicas in each
    // datacenter, leave nodes without a datacenter out, and follow a node
    // that is recreated in another datacenter.
    #[test]
    fn dc_masks_index_replicas_by_datacenter() {
        let node = node_in_dc;
        let replicas = TabletReplicas::new(
            [
                (node(1, Some(DC1)), 0),
                (node(2, Some(DC2)), 0),
                (node(3, None), 0),
                (node(4, Some(DC1)), 0),
                (node(5, Some(DC2)), 0),
            ]
            .into(),
        );
        assert_eq!(replicas.dc_mask(DC1), [0b01001]);
        assert_eq!(replicas.dc_mask(DC2), [0b10010]);
        assert_eq!(replicas.dc_mask(DC3), [0u64; 0]);
        assert_eq!(replicas.dc_masks.len(), 2);

        // Node 4 moves to DC3 (a recreated `Node` object with the same host id).
        let mut tablet = Tablet {
            first_token: Token::new(0),
            last_token: Token::new(1),
            replicas,
            tablet_version: None,
            failed: None,
        };
        let moved = node(4, Some(DC3));
        tablet.update_stale_nodes(&HashMap::from([(moved.host_id, moved)]));
        assert_eq!(tablet.replicas.dc_mask(DC1), [0b00001]);
        assert_eq!(tablet.replicas.dc_mask(DC2), [0b10010]);
        assert_eq!(tablet.replicas.dc_mask(DC3), [0b01000]);
    }

    // A tablet with more than 64 replicas gets multi-word masks, which must
    // name exactly the same positions a filter would.
    #[test]
    fn dc_masks_span_words_for_large_tablets() {
        let dcs = [DC1, DC2, DC3];
        let all: Vec<(Arc<Node>, Shard)> = (0..150u64)
            .map(|i| (node_in_dc(i, Some(dcs[i as usize % 3])), 0))
            .collect();
        let replicas = TabletReplicas::new(all.into());
        assert_eq!(replicas.dc_masks.len(), 3 * 3);
        for (d, dc) in dcs.iter().enumerate() {
            let mask = replicas.dc_mask(dc);
            assert_eq!(mask.len(), 3);
            let expected: Vec<usize> = (d..150).step_by(3).collect();
            assert_eq!(MaskBits::new(mask).collect::<Vec<_>>(), expected);
        }
        assert_eq!(replicas.dc_mask("dc4"), [0u64; 0]);
    }

    // `MaskBits` must yield positions across word boundaries, keep an exact
    // size, and skip with `nth` the same way repeated `next` would.
    #[test]
    fn mask_bits_iterate_across_words() {
        let mask = [1u64 << 63 | 1, 0, 1 << 5 | 1 << 7];
        let positions = [0, 63, 128 + 5, 128 + 7];

        let mut bits = MaskBits::new(&mask);
        assert_eq!(bits.len(), 4);
        assert_eq!(bits.next(), Some(0));
        assert_eq!(bits.len(), 3);
        assert_eq!(bits.collect::<Vec<_>>(), positions[1..]);

        for n in 0..positions.len() {
            let mut bits = MaskBits::new(&mask);
            assert_eq!(bits.nth(n), Some(positions[n]));
            assert_eq!(bits.len(), positions.len() - n - 1);
            assert_eq!(bits.collect::<Vec<_>>(), positions[n + 1..]);
        }
        let mut bits = MaskBits::new(&mask);
        assert_eq!(bits.nth(positions.len()), None);
        assert_eq!(bits.len(), 0);
        let mut bits = MaskBits::new(&mask);
        assert_eq!(bits.nth(usize::MAX), None);
        assert_eq!(bits.len(), 0);
        assert_eq!(bits.next(), None);

        assert_eq!(MaskBits::new(&[]).next(), None);
        assert_eq!(MaskBits::new(&[0, 0]).len(), 0);
        assert_eq!(MaskBits::new(&[0, 0]).next(), None);
    }

    #[test]
    fn table_tablets_empty() {
        let tablets: TableTablets = TableTablets::new_for_test();
        assert_eq!(tablets.tablet_for_token(Token::new(1)), None);
    }

    fn verify_ranges(tablets: &TableTablets, ranges: &[(i64, i64)]) {
        let mut ranges_iter = ranges.iter();
        for tablet in tablets.tablet_list.iter() {
            let range = ranges_iter.next().unwrap();
            assert_eq!(tablet.first_token.value(), range.0);
            assert_eq!(tablet.last_token.value(), range.1);
        }
        assert_eq!(ranges_iter.next(), None)
    }

    fn insert_ranges(tablets: &mut TableTablets, ranges: &[(i64, i64)]) {
        for (first, last) in ranges.iter() {
            tablets.add_tablet(Tablet {
                first_token: Token::new(*first),
                last_token: Token::new(*last),
                replicas: Default::default(),
                tablet_version: None,
                failed: None,
            });
        }
    }

    #[test]
    fn table_tablets_single() {
        let mut tablets = TableTablets::new_for_test();

        insert_ranges(&mut tablets, &[(-200, 1000)]);
        verify_ranges(&tablets, &[(-200, 1000)]);

        assert_eq!(
            tablets.tablet_for_token(Token::new(-1)),
            Some(&tablets.tablet_list[0])
        );
        assert_eq!(
            tablets.tablet_for_token(Token::new(0)),
            Some(&tablets.tablet_list[0])
        );
        assert_eq!(
            tablets.tablet_for_token(Token::new(1)),
            Some(&tablets.tablet_list[0])
        );
        assert_eq!(
            tablets.tablet_for_token(Token::new(-200)),
            Some(&tablets.tablet_list[0])
        );
        assert_eq!(tablets.tablet_for_token(Token::new(-201)), None);
        assert_eq!(
            tablets.tablet_for_token(Token::new(1000)),
            Some(&tablets.tablet_list[0])
        );
        assert_eq!(tablets.tablet_for_token(Token::new(1001)), None);
    }

    // `TableTablets::contains` must report a tablet as present only when the
    // payload matches it exactly, so that no genuine change is ever skipped.
    #[test]
    fn contains_requires_exact_match() {
        let node = Arc::new(Node::new_for_test(
            Some(Uuid::from_u64_pair(1, 1)),
            None,
            None,
            None,
        ));
        let other = Arc::new(Node::new_for_test(
            Some(Uuid::from_u64_pair(1, 2)),
            None,
            None,
            None,
        ));
        let unknown = Uuid::from_u64_pair(9, 9);
        let translator = |id: Uuid| {
            [&node, &other]
                .into_iter()
                .find(|n| n.host_id == id)
                .cloned()
        };
        let resolve = |raw: RawTablet| match Tablet::from_raw_tablet(raw, translator) {
            Ok(t) | Err((t, _)) => t,
        };
        let raw = |first, last, replicas, version| {
            RawTablet::new_for_test(first, last, replicas, version)
        };
        let replicas = || vec![(node.host_id, 0), (other.host_id, 1)];

        let mut tablets = TableTablets::new_for_test();
        tablets.add_tablet(resolve(raw(0, 100, replicas(), Some(7))));

        assert!(tablets.contains(&raw(0, 100, replicas(), Some(7))));

        let differing = [
            // Range.
            raw(0, 99, replicas(), Some(7)),
            raw(1, 100, replicas(), Some(7)),
            // Version.
            raw(0, 100, replicas(), Some(8)),
            raw(0, 100, replicas(), None),
            // Shard.
            raw(0, 100, vec![(node.host_id, 1), (other.host_id, 1)], Some(7)),
            // Replica order (the first replica is the leader).
            raw(0, 100, vec![(other.host_id, 1), (node.host_id, 0)], Some(7)),
            // Replica count.
            raw(0, 100, vec![(node.host_id, 0)], Some(7)),
            // Another host.
            raw(0, 100, vec![(node.host_id, 0), (unknown, 1)], Some(7)),
        ];
        for tablet in differing {
            assert!(!tablets.contains(&tablet), "{tablet:?}");
        }

        // A present tablet with unresolved replicas never counts as already
        // there: the next feedback may resolve them.
        let with_unknown = || raw(0, 100, vec![(node.host_id, 0), (unknown, 1)], Some(7));
        let mut tablets = TableTablets::new_for_test();
        tablets.add_tablet(resolve(with_unknown()));
        assert!(!tablets.contains(&with_unknown()));
    }

    #[test]
    fn test_adding_tablets_non_overlapping() {
        let mut tablets = TableTablets::new_for_test();
        const RANGES: &[(i64, i64)] = &[
            (-2000000, -1900001),
            (-1900000, -1700001),
            (-1700000, -1),
            (0, 19),
            (20, 10000),
        ];

        insert_ranges(&mut tablets, RANGES);
        verify_ranges(&tablets, RANGES);
    }

    #[test]
    fn test_adding_tablet_same() {
        let mut tablets = TableTablets::new_for_test();

        insert_ranges(&mut tablets, &[(-2000000, -1800000), (-2000000, -1800000)]);
        verify_ranges(&tablets, &[(-2000000, -1800000)]);
    }

    #[test]
    fn test_adding_tablet_overlapping_one() {
        let mut tablets = TableTablets::new_for_test();
        insert_ranges(&mut tablets, &[(-2000000, -1800000)]);
        verify_ranges(&tablets, &[(-2000000, -1800000)]);

        // Replacing a tablet, overlaps right part of the old one
        insert_ranges(&mut tablets, &[(-1900000, -1700000)]);
        verify_ranges(&tablets, &[(-1900000, -1700000)]);

        // Replacing a tablet, overlaps left part of the old one
        insert_ranges(&mut tablets, &[(-2000000, -1800000)]);
        verify_ranges(&tablets, &[(-2000000, -1800000)]);
    }

    #[test]
    fn test_adding_tablet_fill_hole() {
        let mut tablets = TableTablets::new_for_test();

        // Fill a hole between two tablets
        insert_ranges(
            &mut tablets,
            &[
                (-2000000, -1800001),
                (-1600000, -1400000), // Create a hole
                (-1800000, -1600001), // Fully fill this hole
            ],
        );
        verify_ranges(
            &tablets,
            &[
                (-2000000, -1800001),
                (-1800000, -1600001),
                (-1600000, -1400000),
            ],
        );
    }

    #[test]
    fn test_adding_tablet_neighbours_not_removed() {
        let mut tablets = TableTablets::new_for_test();
        insert_ranges(
            &mut tablets,
            &[
                (-2000000, -1800001),
                (-1800000, -1600001),
                (-1600000, -1400000),
            ],
        );

        // Make sure neighbours are not removed when fully replacing tablet in the middle
        insert_ranges(&mut tablets, &[(-1800000, -1600001)]);
        verify_ranges(
            &tablets,
            &[
                (-2000000, -1800001),
                (-1800000, -1600001),
                (-1600000, -1400000),
            ],
        );

        // Make sure neighbours are not removed when new tablet is smaller than old one
        insert_ranges(&mut tablets, &[(-1750000, -1650000)]);
        verify_ranges(
            &tablets,
            &[
                (-2000000, -1800001),
                (-1750000, -1650000),
                (-1600000, -1400000),
            ],
        );
    }

    #[test]
    fn replace_multiple_tablets_middle() {
        let mut tablets = TableTablets::new_for_test();
        insert_ranges(
            &mut tablets,
            &[
                (-2000000, -1800001),
                (-1800000, -1600001),
                (-1600000, -1400001),
                (-1400000, -1200001),
                (-1200000, -1000000),
            ],
        );

        // Replacing 3 middle tablets
        insert_ranges(&mut tablets, &[(-1750000, -1250000)]);
        verify_ranges(
            &tablets,
            &[
                (-2000000, -1800001),
                (-1750000, -1250000),
                (-1200000, -1000000),
            ],
        );
    }

    const SOME_KS: TableSpec<'static> = TableSpec::borrowed("ks", "tbl");

    fn node_map(nodes: &[&Arc<Node>]) -> HashMap<Uuid, Arc<Node>> {
        nodes.iter().map(|n| (n.host_id, Arc::clone(n))).collect()
    }

    #[test]
    fn table_maintenance_tests() {
        setup_tracing();

        let node1 = Arc::new(Node::new_for_test(
            Some(Uuid::from_u128(1)),
            None,
            Some(DC1.to_owned()),
            None,
        ));
        let node2 = Arc::new(Node::new_for_test(
            Some(Uuid::from_u128(2)),
            None,
            Some(DC2.to_owned()),
            None,
        ));
        let node2_v2 = Arc::new(Node::new_for_test(
            Some(Uuid::from_u128(2)),
            None,
            Some(DC2.to_owned()),
            None,
        ));
        let node3 = Arc::new(Node::new_for_test(
            Some(Uuid::from_u128(3)),
            None,
            Some(DC3.to_owned()),
            None,
        ));
        let node3_v2 = Arc::new(Node::new_for_test(
            Some(Uuid::from_u128(3)),
            None,
            Some(DC3.to_owned()),
            None,
        ));

        type MaintenanceArgs = (
            HashSet<Uuid>,
            HashMap<Uuid, Arc<Node>>,
            HashMap<Uuid, Arc<Node>>,
        );
        let tests: &mut [(TableTablets, MaintenanceArgs, TableTablets)] = &mut [
            (
                // [Case 0] Nothing changes, no maintenance required
                TableTablets {
                    table_spec: SOME_KS,
                    tablet_list: vec![
                        Tablet::new_for_test(0, vec![node1.clone(), node2.clone()], None),
                        Tablet::new_for_test(1, vec![node2.clone(), node3.clone()], None),
                        Tablet::new_for_test(2, vec![node3.clone(), node1.clone()], None),
                        Tablet::new_for_test(3, vec![node3.clone()], None),
                    ],
                    has_unknown_replicas: false,
                },
                (
                    HashSet::new(),
                    node_map(&[&node1, &node2, &node3]),
                    HashMap::new(),
                ),
                TableTablets {
                    table_spec: SOME_KS,
                    tablet_list: vec![
                        Tablet::new_for_test(0, vec![node1.clone(), node2.clone()], None),
                        Tablet::new_for_test(1, vec![node2.clone(), node3.clone()], None),
                        Tablet::new_for_test(2, vec![node3.clone(), node1.clone()], None),
                        Tablet::new_for_test(3, vec![node3.clone()], None),
                    ],
                    has_unknown_replicas: false,
                },
            ),
            (
                // [Case 1] Removed node
                TableTablets {
                    table_spec: SOME_KS,
                    tablet_list: vec![
                        Tablet::new_for_test(0, vec![node1.clone(), node2.clone()], None),
                        Tablet::new_for_test(1, vec![node2.clone(), node3.clone()], None),
                        Tablet::new_for_test(2, vec![node3.clone(), node1.clone()], None),
                        Tablet::new_for_test(3, vec![node3.clone()], None),
                    ],
                    has_unknown_replicas: false,
                },
                (
                    HashSet::from([node1.host_id]),
                    node_map(&[&node2, &node3]),
                    HashMap::new(),
                ),
                TableTablets {
                    table_spec: SOME_KS,
                    tablet_list: vec![
                        Tablet::new_for_test(1, vec![node2.clone(), node3.clone()], None),
                        Tablet::new_for_test(3, vec![node3.clone()], None),
                    ],
                    has_unknown_replicas: false,
                },
            ),
            (
                // [Case 2] Multiple removed nodes
                TableTablets {
                    table_spec: SOME_KS,
                    tablet_list: vec![
                        Tablet::new_for_test(0, vec![node1.clone(), node2.clone()], None),
                        Tablet::new_for_test(1, vec![node2.clone(), node3.clone()], None),
                        Tablet::new_for_test(2, vec![node3.clone(), node1.clone()], None),
                        Tablet::new_for_test(3, vec![node3.clone()], None),
                    ],
                    has_unknown_replicas: false,
                },
                (
                    HashSet::from([node1.host_id, node2.host_id]),
                    node_map(&[&node3]),
                    HashMap::new(),
                ),
                TableTablets {
                    table_spec: SOME_KS,
                    tablet_list: vec![Tablet::new_for_test(3, vec![node3.clone()], None)],
                    has_unknown_replicas: false,
                },
            ),
            (
                // [Case 3] Nodes with unresolved replicas
                TableTablets {
                    table_spec: SOME_KS,
                    tablet_list: vec![
                        Tablet::new_for_test(
                            0,
                            vec![node2.clone()],
                            Some(vec![node1.host_id, node2.host_id]),
                        ),
                        Tablet::new_for_test(
                            1,
                            vec![node2.clone()],
                            Some(vec![node2.host_id, node3.host_id]),
                        ),
                        Tablet::new_for_test(2, vec![node3.clone(), node1.clone()], None),
                        Tablet::new_for_test(3, vec![node3.clone()], None),
                    ],
                    has_unknown_replicas: true,
                },
                (
                    HashSet::new(),
                    node_map(&[&node1, &node2, &node3]),
                    HashMap::new(),
                ),
                TableTablets {
                    table_spec: SOME_KS,
                    tablet_list: vec![
                        Tablet::new_for_test(0, vec![node1.clone(), node2.clone()], None),
                        Tablet::new_for_test(1, vec![node2.clone(), node3.clone()], None),
                        Tablet::new_for_test(2, vec![node3.clone(), node1.clone()], None),
                        Tablet::new_for_test(3, vec![node3.clone()], None),
                    ],
                    has_unknown_replicas: false,
                },
            ),
            (
                // [Case 4] Some replicas still unresolved
                TableTablets {
                    table_spec: SOME_KS,
                    tablet_list: vec![
                        Tablet::new_for_test(
                            0,
                            vec![node2.clone()],
                            Some(vec![node1.host_id, node2.host_id]),
                        ),
                        Tablet::new_for_test(
                            1,
                            vec![node2.clone()],
                            Some(vec![node2.host_id, node3.host_id]),
                        ),
                        Tablet::new_for_test(2, vec![], Some(vec![node3.host_id, node1.host_id])),
                        Tablet::new_for_test(3, vec![node2.clone()], None),
                        Tablet::new_for_test(4, vec![], Some(vec![node3.host_id])),
                    ],
                    has_unknown_replicas: true,
                },
                (HashSet::new(), node_map(&[&node2, &node3]), HashMap::new()),
                TableTablets {
                    table_spec: SOME_KS,
                    tablet_list: vec![
                        Tablet::new_for_test(1, vec![node2.clone(), node3.clone()], None),
                        Tablet::new_for_test(3, vec![node2.clone()], None),
                        Tablet::new_for_test(4, vec![node3.clone()], None),
                    ],
                    has_unknown_replicas: false,
                },
            ),
            (
                // [Case 5] Incorrectly set "has_unknown_replicas" - unknown replicas should be ignored,
                // because this stip of the maintenance is skipped.
                TableTablets {
                    table_spec: SOME_KS,
                    tablet_list: vec![
                        Tablet::new_for_test(
                            0,
                            vec![node2.clone()],
                            Some(vec![node1.host_id, node2.host_id]),
                        ),
                        Tablet::new_for_test(
                            1,
                            vec![node2.clone()],
                            Some(vec![node2.host_id, node3.host_id]),
                        ),
                        Tablet::new_for_test(2, vec![node3.clone(), node1.clone()], None),
                        Tablet::new_for_test(3, vec![node3.clone()], None),
                    ],
                    has_unknown_replicas: false,
                },
                (
                    HashSet::new(),
                    node_map(&[&node1, &node2, &node3]),
                    HashMap::new(),
                ),
                TableTablets {
                    table_spec: SOME_KS,
                    tablet_list: vec![
                        Tablet::new_for_test(
                            0,
                            vec![node2.clone()],
                            Some(vec![node1.host_id, node2.host_id]),
                        ),
                        Tablet::new_for_test(
                            1,
                            vec![node2.clone()],
                            Some(vec![node2.host_id, node3.host_id]),
                        ),
                        Tablet::new_for_test(2, vec![node3.clone(), node1.clone()], None),
                        Tablet::new_for_test(3, vec![node3.clone()], None),
                    ],
                    has_unknown_replicas: false,
                },
            ),
            (
                // [Case 6] Recreated one of the nodes
                TableTablets {
                    table_spec: SOME_KS,
                    tablet_list: vec![
                        Tablet::new_for_test(0, vec![node1.clone(), node2.clone()], None),
                        Tablet::new_for_test(1, vec![node2.clone(), node3.clone()], None),
                        Tablet::new_for_test(2, vec![node3.clone(), node1.clone()], None),
                        Tablet::new_for_test(3, vec![node3.clone()], None),
                    ],
                    has_unknown_replicas: false,
                },
                (
                    HashSet::new(),
                    node_map(&[&node1, &node2, &node3_v2]),
                    HashMap::from([(node3_v2.host_id, node3_v2.clone())]),
                ),
                TableTablets {
                    table_spec: SOME_KS,
                    tablet_list: vec![
                        Tablet::new_for_test(0, vec![node1.clone(), node2.clone()], None),
                        Tablet::new_for_test(1, vec![node2.clone(), node3_v2.clone()], None),
                        Tablet::new_for_test(2, vec![node3_v2.clone(), node1.clone()], None),
                        Tablet::new_for_test(3, vec![node3_v2.clone()], None),
                    ],
                    has_unknown_replicas: false,
                },
            ),
            (
                // [Case 7] Recreated multiple nodes
                TableTablets {
                    table_spec: SOME_KS,
                    tablet_list: vec![
                        Tablet::new_for_test(0, vec![node1.clone(), node2.clone()], None),
                        Tablet::new_for_test(1, vec![node2.clone(), node3.clone()], None),
                        Tablet::new_for_test(2, vec![node3.clone(), node1.clone()], None),
                        Tablet::new_for_test(3, vec![node3.clone()], None),
                    ],
                    has_unknown_replicas: false,
                },
                (
                    HashSet::new(),
                    node_map(&[&node1, &node2, &node3_v2]),
                    HashMap::from([
                        (node3_v2.host_id, node3_v2.clone()),
                        (node2_v2.host_id, node2_v2.clone()),
                    ]),
                ),
                TableTablets {
                    table_spec: SOME_KS,
                    tablet_list: vec![
                        Tablet::new_for_test(0, vec![node1.clone(), node2_v2.clone()], None),
                        Tablet::new_for_test(1, vec![node2_v2.clone(), node3_v2.clone()], None),
                        Tablet::new_for_test(2, vec![node3_v2.clone(), node1.clone()], None),
                        Tablet::new_for_test(3, vec![node3_v2.clone()], None),
                    ],
                    has_unknown_replicas: false,
                },
            ),
            (
                // [Case 8] Unknown replica and removed node
                TableTablets {
                    table_spec: SOME_KS,
                    tablet_list: vec![
                        Tablet::new_for_test(
                            0,
                            vec![node2.clone()],
                            Some(vec![node1.host_id, node2.host_id]),
                        ),
                        Tablet::new_for_test(1, vec![node2.clone(), node3.clone()], None),
                        Tablet::new_for_test(
                            2,
                            vec![node3.clone()],
                            Some(vec![node3.host_id, node1.host_id]),
                        ),
                        Tablet::new_for_test(3, vec![node3.clone()], None),
                    ],
                    has_unknown_replicas: true,
                },
                (
                    HashSet::from([node2.host_id]),
                    node_map(&[&node1, &node3]),
                    HashMap::new(),
                ),
                TableTablets {
                    table_spec: SOME_KS,
                    tablet_list: vec![
                        Tablet::new_for_test(2, vec![node3.clone(), node1.clone()], None),
                        Tablet::new_for_test(3, vec![node3.clone()], None),
                    ],
                    has_unknown_replicas: false,
                },
            ),
            (
                // [Case 9] Unknown replica, removed node and recreated node
                TableTablets {
                    table_spec: SOME_KS,
                    tablet_list: vec![
                        Tablet::new_for_test(
                            0,
                            vec![node2.clone()],
                            Some(vec![node1.host_id, node2.host_id]),
                        ),
                        Tablet::new_for_test(1, vec![node2.clone(), node3.clone()], None),
                        Tablet::new_for_test(
                            2,
                            vec![node3.clone()],
                            Some(vec![node3.host_id, node1.host_id]),
                        ),
                        Tablet::new_for_test(3, vec![node3.clone()], None),
                    ],
                    has_unknown_replicas: true,
                },
                (
                    HashSet::from([node2.host_id]),
                    node_map(&[&node1, &node3]),
                    HashMap::from([(node3_v2.host_id, node3_v2.clone())]),
                ),
                TableTablets {
                    table_spec: SOME_KS,
                    tablet_list: vec![
                        Tablet::new_for_test(2, vec![node3_v2.clone(), node1.clone()], None),
                        Tablet::new_for_test(3, vec![node3_v2.clone()], None),
                    ],
                    has_unknown_replicas: false,
                },
            ),
        ];

        for (i, (pre, (removed, all, recreated), post)) in tests.iter_mut().enumerate() {
            tracing::info!("Test case {}", i);
            pre.perform_maintenance(removed, all, recreated);
            assert_eq!(pre, post);
        }
    }

    #[test]
    fn maintenance_keyspace_add_remove_test() {
        const TABLE_1: TableSpec<'static> = TableSpec::borrowed("ks_1", "table_1");
        const TABLE_2: TableSpec<'static> = TableSpec::borrowed("ks_2", "table_2");
        const TABLE_3: TableSpec<'static> = TableSpec::borrowed("ks_3", "table_3");
        const TABLE_DROP: TableSpec<'static> = TableSpec::borrowed("ks_drop", "table_drop");

        let mut pre = TabletsInfo {
            tablets: hashbrown::HashMap::from([
                (
                    TABLE_1.clone(),
                    Arc::new(TableTablets::new(TABLE_1.clone())),
                ),
                (
                    TABLE_DROP.clone(),
                    Arc::new(TableTablets::new(TABLE_DROP.clone())),
                ),
                (
                    TABLE_2.clone(),
                    Arc::new(TableTablets::new(TABLE_2.clone())),
                ),
            ]),
            has_unknown_replicas: false,
        };

        let expected_after = TabletsInfo {
            tablets: hashbrown::HashMap::from([
                (
                    TABLE_1.clone(),
                    Arc::new(TableTablets::new(TABLE_1.clone())),
                ),
                (
                    TABLE_2.clone(),
                    Arc::new(TableTablets::new(TABLE_2.clone())),
                ),
                (
                    TABLE_3.clone(),
                    Arc::new(TableTablets::new(TABLE_3.clone())),
                ),
            ]),
            has_unknown_replicas: false,
        };

        let spec_to_ks_tuple = |spec: TableSpec<'_>| {
            (
                spec.ks_name().to_owned(),
                Keyspace {
                    strategy: Strategy::LocalStrategy,
                    durable_writes: false,
                    tablet_based: true,
                    consistency_mode: ConsistencyMode::Eventual,
                    tables: HashMap::from([(
                        spec.table_name().to_owned(),
                        Table {
                            columns: HashMap::new(),
                            partition_key: vec![],
                            clustering_key: vec![],
                            partitioner: None,
                            pk_column_specs: vec![],
                        },
                    )]),
                    views: HashMap::new(),
                    user_defined_types: HashMap::new(),
                },
            )
        };

        let keyspaces_after = HashMap::from([
            spec_to_ks_tuple(TABLE_1),
            spec_to_ks_tuple(TABLE_2),
            spec_to_ks_tuple(TABLE_3),
        ]);

        pre.perform_maintenance(
            &keyspaces_after,
            &HashSet::new(),
            &HashMap::new(),
            &HashMap::new(),
        );

        assert_eq!(pre, expected_after);
    }

    /// Builds a tablet whose payload lists `replica_ids` in order -- the first being the
    /// leader, as TABLETS_ROUTING_V2 does -- resolving every id except those in `unresolvable`.
    fn tablet_with_unresolvable(
        replica_ids: &[Uuid],
        unresolvable: &[Uuid],
        tablet_version: Option<i64>,
    ) -> Tablet {
        let nodes: HashMap<Uuid, Arc<Node>> = replica_ids
            .iter()
            .filter(|id| !unresolvable.contains(id))
            .map(|id| {
                let node = Node::new_for_test(Some(*id), None, Some(DC1.to_string()), None);
                (node.host_id, Arc::new(node))
            })
            .collect();

        let raw = RawTablet::new_for_test(
            0,
            100,
            replica_ids.iter().map(|id| (*id, 0)).collect(),
            tablet_version,
        );

        match Tablet::from_raw_tablet(raw, |uuid| nodes.get(&uuid).cloned()) {
            Ok(tablet) => tablet,
            Err((tablet, _failed)) => tablet,
        }
    }

    /// A follower that cannot be resolved must not cost us the leader: the leader is still
    /// the first replica of the payload, and it resolved.
    #[test]
    fn known_leader_survives_a_follower_failing_to_resolve() {
        let leader = Uuid::from_u64_pair(1, 1);
        let follower = Uuid::from_u64_pair(1, 2);

        let tablet = tablet_with_unresolvable(&[leader, follower], &[follower], Some(7));

        let (node, shard) = tablet
            .known_leader()
            .expect("the leader resolved, so it is known");
        assert_eq!(node.host_id, leader);
        assert_eq!(shard, 0);
    }

    /// If the leader itself is the replica that could not be resolved, the first *remaining*
    /// replica is a follower -- it must not be mistaken for the leader.
    #[test]
    fn known_leader_is_unknown_when_the_leader_fails_to_resolve() {
        let leader = Uuid::from_u64_pair(1, 1);
        let follower = Uuid::from_u64_pair(1, 2);

        let tablet = tablet_with_unresolvable(&[leader, follower], &[leader], Some(7));

        // The follower did resolve, so the replica list is non-empty ...
        assert_eq!(tablet.replicas.all.len(), 1);
        assert_eq!(tablet.replicas.all[0].0.host_id, follower);
        // ... but it is not the leader, so no leader is known.
        assert!(tablet.known_leader().is_none());
    }

    #[test]
    fn known_leader_is_the_first_replica_when_all_resolve() {
        let leader = Uuid::from_u64_pair(1, 1);
        let follower = Uuid::from_u64_pair(1, 2);

        let tablet = tablet_with_unresolvable(&[leader, follower], &[], Some(7));

        let (node, _) = tablet.known_leader().expect("nothing failed to resolve");
        assert_eq!(node.host_id, leader);
    }
}
