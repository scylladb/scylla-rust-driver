use bytes::Bytes;
use scylla_cql::deserialize::value::{DeserializeValue, ListlikeIterator};
use scylla_cql::deserialize::{DeserializationError, FrameSlice, TypeCheckError};
use scylla_cql::frame::response::result::{CollectionType, ColumnType, NativeType, TableSpec};
use thiserror::Error;
use tracing::warn;
use uuid::Uuid;

use crate::cluster::Node;
use crate::routing::{Shard, Token};
use crate::utils::safe_format::IteratorSafeFormatExt;

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
}

type RawTabletPayload<'frame, 'metadata> =
    (i64, i64, ListlikeIterator<'frame, 'metadata, (Uuid, i32)>);

static RAW_TABLETS_CQL_TYPE: LazyLock<ColumnType<'static>> = LazyLock::new(|| {
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

const CUSTOM_PAYLOAD_TABLETS_V1_KEY: &str = "tablets-routing-v1";

impl RawTablet {
    pub(crate) fn from_custom_payload(
        payload: &HashMap<String, Bytes>,
    ) -> Option<Result<RawTablet, TabletParsingError>> {
        let payload = payload.get(CUSTOM_PAYLOAD_TABLETS_V1_KEY)?;

        if let Err(err) =
            <RawTabletPayload as DeserializeValue<'_, '_>>::type_check(RAW_TABLETS_CQL_TYPE.deref())
        {
            return Some(Err(err.into()));
        };

        let (first_token, last_token, replicas): RawTabletPayload =
            match <RawTabletPayload as DeserializeValue<'_, '_>>::deserialize(
                RAW_TABLETS_CQL_TYPE.deref(),
                Some(FrameSlice::new(payload)),
            ) {
                Ok(tuple) => tuple,
                Err(err) => return Some(Err(err.into())),
            };

        let replicas = match replicas
            .map(|res| {
                res.map_err(TabletParsingError::from)
                    .and_then(|(uuid, shard_num)| match shard_num.try_into() {
                        Ok(s) => Ok((uuid, s)),
                        Err(_) => Err(TabletParsingError::ShardNum(shard_num)),
                    })
            })
            .collect::<Result<Vec<(Uuid, Shard)>, TabletParsingError>>()
        {
            Ok(r) => r,
            Err(err) => return Some(Err(err)),
        };

        Some(Ok(RawTablet {
            // +1 because ScyllaDB sends left-open range, so received
            // number is the last token not belonging to this tablet.
            first_token: Token::new(first_token + 1),
            last_token: Token::new(last_token),
            replicas: RawTabletReplicas { replicas },
        }))
    }
}

#[derive(Clone, Debug, Default)]
#[cfg_attr(test, derive(Eq))]
struct TabletReplicas {
    all: Vec<(Arc<Node>, Shard)>,
    per_dc: HashMap<String, Vec<(Arc<Node>, Shard)>>,
}

impl TabletReplicas {
    /// Gets raw replica list (which is an array of (Uuid, Shard)), retrieves
    /// `Node` objects and groups node replicas by DC to make life easier for LBP.
    /// In case of failure this function returns Self, but with the problematic nodes skipped,
    /// and a list of skipped uuids - so that the caller can e.g. do some logging.
    pub(crate) fn from_raw_replicas(
        raw_replicas: &RawTabletReplicas,
        replica_translator: impl Fn(Uuid) -> Option<Arc<Node>>,
    ) -> Result<Self, (Self, Vec<Uuid>)> {
        let mut failed = Vec::new();
        let all: Vec<_> = raw_replicas
            .replicas
            .iter()
            .filter_map(|(replica, shard)| {
                if let Some(r) = replica_translator(*replica) {
                    Some((r, *shard as Shard))
                } else {
                    failed.push(*replica);
                    None
                }
            })
            .collect();

        let mut per_dc: HashMap<String, Vec<(Arc<Node>, Shard)>> = HashMap::new();
        all.iter().for_each(|(replica, shard)| {
            if let Some(dc) = replica.datacenter.as_ref() {
                if let Some(replicas) = per_dc.get_mut(dc) {
                    replicas.push((Arc::clone(replica), *shard));
                } else {
                    per_dc.insert(dc.to_string(), vec![(Arc::clone(replica), *shard)]);
                }
            }
        });

        if failed.is_empty() {
            Ok(Self { all, per_dc })
        } else {
            Err((Self { all, per_dc }, failed))
        }
    }

    #[cfg(test)]
    fn new_for_test(replicas: Vec<Arc<Node>>) -> Self {
        let all = replicas.into_iter().map(|r| (r, 0)).collect::<Vec<_>>();
        let mut per_dc: HashMap<String, Vec<(Arc<Node>, Shard)>> = HashMap::new();
        all.iter().for_each(|(replica, shard)| {
            if let Some(dc) = replica.datacenter.as_ref() {
                if let Some(replicas) = per_dc.get_mut(dc) {
                    replicas.push((Arc::clone(replica), *shard));
                } else {
                    per_dc.insert(dc.to_string(), vec![(Arc::clone(replica), *shard)]);
                }
            }
        });

        Self { all, per_dc }
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

        // Implementations of `TableTablets`, `Tablet` and `TabletReplicas`
        // guarantee that if `all` is the same then `per_dc` must be too.
        // If it isn't then it is a bug.
        // Comparing `TabletReplicas` happens only in tests so we can sacrifice
        // a small bit of performance to verify this assumption.
        assert_eq!(self.per_dc.len(), other.per_dc.len());
        for (self_k, self_v) in self.per_dc.iter() {
            let other_v = other.per_dc.get(self_k).unwrap();
            for ((self_node, self_shard), (other_node, other_shard)) in
                self_v.iter().zip(other_v.iter())
            {
                assert_eq!(self_shard, other_shard);
                assert!(Arc::ptr_eq(self_node, other_node));
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
    /// If any of the replicas failed to resolve to a Node,
    /// then this field will contain the original list of replicas.
    failed: Option<RawTabletReplicas>,
}

impl Tablet {
    // Ignore clippy lints here. Clippy suggests to
    // Box<> `Err` variant, because it's too large. It does not
    // make much sense to do so, looking at the caller of this function.
    // Tablet returned in `Err` variant is used as if no error appeared.
    // The only difference is that we use node ids to emit some debug logs.
    #[expect(clippy::result_large_err)]
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
                failed: None,
            }),
            Err((replicas, failed_replicas)) => Err((
                Self {
                    first_token: raw_tablet.first_token,
                    last_token: raw_tablet.last_token,
                    replicas,
                    failed: Some(raw_tablet.replicas),
                },
                failed_replicas,
            )),
        }
    }

    pub(crate) fn range(&self) -> (Token, Token) {
        (self.first_token, self.last_token)
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
        let mut any_updated = false;
        for (node, _) in self.replicas.all.iter_mut() {
            if let Some(new_node) = recreated_nodes.get(&node.host_id) {
                assert!(!Arc::ptr_eq(new_node, node));
                any_updated = true;
                *node = Arc::clone(new_node);
            }
        }

        if any_updated {
            // Now that we know we have some nodes to update we need to go over
            // per-dc nodes and update them too.
            for (_, dc_nodes) in self.replicas.per_dc.iter_mut() {
                for (node, _) in dc_nodes.iter_mut() {
                    if let Some(new_node) = recreated_nodes.get(&node.host_id) {
                        *node = Arc::clone(new_node);
                    }
                }
            }
        }
    }

    #[cfg(test)]
    fn new_for_test(token: i64, replicas: Vec<Arc<Node>>, failed: Option<Vec<Uuid>>) -> Self {
        Self {
            first_token: Token::new(token),
            last_token: Token::new(token),
            replicas: TabletReplicas::new_for_test(replicas),
            failed: failed.map(|vec| RawTabletReplicas {
                replicas: vec.into_iter().map(|id| (id, 0)).collect::<Vec<_>>(),
            }),
        }
    }
}

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
            .map(|tablet| tablet.replicas.all.as_ref())
    }

    pub(crate) fn dc_replicas_for_token(
        &self,
        token: Token,
        dc: &str,
    ) -> Option<&[(Arc<Node>, Shard)]> {
        self.tablet_for_token(token).map(|tablet| {
            tablet
                .replicas
                .per_dc
                .get(dc)
                .map(|x| x.as_slice())
                .unwrap_or(&[])
        })
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
                    warn!("Nodes ({}) listed as replicas for a tablet {{ks: {}, table: {}, range: [{}. {}]}} are not present in ClusterState.known_peers, \
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
    tablets: hashbrown::HashMap<TableSpec<'static>, TableTablets>,
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
        #[derive(Hash)]
        struct TableSpecQueryKey<'a> {
            table_spec: &'a TableSpec<'a>,
        }

        impl<'key, 'query> hashbrown::Equivalent<TableSpec<'key>> for TableSpecQueryKey<'query> {
            fn equivalent(&self, key: &TableSpec<'key>) -> bool {
                self.table_spec == key
            }
        }

        let query_key = TableSpecQueryKey { table_spec };

        self.tablets.get(&query_key)
    }

    pub(crate) fn add_tablet(&mut self, table_spec: TableSpec<'static>, tablet: Tablet) {
        if tablet.failed.is_some() {
            self.has_unknown_replicas = true;
        }
        self.tablets
            .entry(table_spec)
            .or_insert_with_key(|k| {
                tracing::debug!(
                    "Found new tablets table: {}.{}",
                    k.ks_name(),
                    k.table_name()
                );
                TableTablets::new(k.clone())
            })
            .add_tablet(tablet)
    }

    #[expect(clippy::doc_overindented_list_items)]
    /// This method is supposed to be called when topology is updated.
    /// It goes through tablet info and adjusts it to topology changes, to prevent
    /// a situation where local tablet info and a real one are permanently different.
    /// What is updated:
    /// 1. Info for dropped tables is removed.
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
        table_predicate: &impl Fn(&TableSpec) -> bool,
        removed_nodes: &HashSet<Uuid>,
        all_current_nodes: &HashMap<Uuid, Arc<Node>>,
        recreated_nodes: &HashMap<Uuid, Arc<Node>>,
    ) {
        // First we remove info for all tables that are no longer present.
        self.tablets.retain(|k, _| table_predicate(k));

        if !removed_nodes.is_empty() || !recreated_nodes.is_empty() || self.has_unknown_replicas {
            for (_, table_tablets) in self.tablets.iter_mut() {
                table_tablets.perform_maintenance(
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

    use bytes::Bytes;
    use scylla_cql::frame::response::result::{CollectionType, ColumnType, NativeType, TableSpec};
    use scylla_cql::serialize::CellWriter;
    use scylla_cql::serialize::value::SerializeValue;
    use tracing::debug;
    use uuid::Uuid;

    use crate::cluster::Node;
    use crate::routing::Token;
    use crate::routing::locator::tablets::{
        CUSTOM_PAYLOAD_TABLETS_V1_KEY, RAW_TABLETS_CQL_TYPE, RawTablet, RawTabletReplicas,
        TabletParsingError,
    };
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

        SerializeValue::serialize(&value, &RAW_TABLETS_CQL_TYPE, CellWriter::new(&mut data))
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
                }
            }
        );
    }

    #[test]
    fn raw_replicas_to_replicas_groups_correctly() {
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

        let mut per_dc = HashMap::new();
        per_dc.insert(
            "dc1".to_string(),
            vec![
                (translator(Uuid::from_u64_pair(1, 1)).unwrap(), 1),
                (translator(Uuid::from_u64_pair(1, 6)).unwrap(), 1),
            ],
        );
        per_dc.insert(
            "dc2".to_string(),
            vec![
                (translator(Uuid::from_u64_pair(1, 2)).unwrap(), 1),
                (translator(Uuid::from_u64_pair(1, 4)).unwrap(), 1),
                (translator(Uuid::from_u64_pair(1, 5)).unwrap(), 1),
            ],
        );
        per_dc.insert(
            "dc3".to_string(),
            vec![(translator(Uuid::from_u64_pair(1, 3)).unwrap(), 1)],
        );

        assert_eq!(
            replicas,
            Ok(TabletReplicas {
                all: replicas_uids
                    .iter()
                    .cloned()
                    .map(|replica| (translator(replica).unwrap(), 1))
                    .collect(),
                per_dc
            })
        );
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
    fn maintenance_keyspace_remove_test() {
        const TABLE_1: TableSpec<'static> = TableSpec::borrowed("ks_1", "table_1");
        const TABLE_2: TableSpec<'static> = TableSpec::borrowed("ks_2", "table_2");
        const TABLE_DROP: TableSpec<'static> = TableSpec::borrowed("ks_drop", "table_drop");

        let mut pre = TabletsInfo {
            tablets: hashbrown::HashMap::from([
                (TABLE_1.clone(), TableTablets::new(TABLE_1.clone())),
                (TABLE_DROP.clone(), TableTablets::new(TABLE_DROP.clone())),
                (TABLE_2.clone(), TableTablets::new(TABLE_2.clone())),
            ]),
            has_unknown_replicas: false,
        };

        let expected_after = TabletsInfo {
            tablets: hashbrown::HashMap::from([
                (TABLE_1.clone(), TableTablets::new(TABLE_1.clone())),
                (TABLE_2.clone(), TableTablets::new(TABLE_2.clone())),
            ]),
            has_unknown_replicas: false,
        };

        pre.perform_maintenance(
            &|spec| *spec != TABLE_DROP,
            &HashSet::new(),
            &HashMap::new(),
            &HashMap::new(),
        );

        assert_eq!(pre, expected_after);
    }
}
