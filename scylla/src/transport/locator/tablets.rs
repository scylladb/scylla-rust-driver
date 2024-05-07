#![allow(dead_code)]
use lazy_static::lazy_static;
use scylla_cql::cql_to_rust::FromCqlVal;
use scylla_cql::frame::frame_errors::ParseError;
use scylla_cql::frame::response::result::{deser_cql_value, ColumnType, TableSpec};
use thiserror::Error;
use tracing::warn;
use uuid::Uuid;

use crate::routing::{Shard, Token};
use crate::transport::Node;

use std::collections::HashMap;
use std::sync::Arc;

#[derive(Error, Debug)]
pub(crate) enum TabletParsingError {
    #[error(transparent)]
    Parse(#[from] ParseError),
    #[error("Shard id for tablet is negative: {0}")]
    ShardNum(i32),
}

#[derive(Debug, PartialEq, Eq)]
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

type RawTabletPayload = (i64, i64, Vec<(Uuid, i32)>);

lazy_static! {
    static ref RAW_TABLETS_CQL_TYPE: ColumnType = ColumnType::Tuple(vec![
        ColumnType::BigInt,
        ColumnType::BigInt,
        ColumnType::List(Box::new(ColumnType::Tuple(vec![
            ColumnType::Uuid,
            ColumnType::Int,
        ]))),
    ]);
}

const CUSTOM_PAYLOAD_TABLETS_V1_KEY: &str = "tablets-routing-v1";

impl RawTablet {
    pub(crate) fn from_custom_payload(
        payload: &HashMap<String, Vec<u8>>,
    ) -> Option<Result<RawTablet, TabletParsingError>> {
        let payload = payload.get(CUSTOM_PAYLOAD_TABLETS_V1_KEY)?;
        let cql_value = match deser_cql_value(&RAW_TABLETS_CQL_TYPE, &mut payload.as_slice()) {
            Ok(r) => r,
            Err(e) => return Some(Err(e.into())),
        };

        // This could only fail if the type was wrong, but we do pass correct type
        // to `deser_cql_value`.
        let (first_token, last_token, replicas): RawTabletPayload =
            FromCqlVal::from_cql(cql_value).unwrap();

        let replicas = match replicas
            .into_iter()
            .map(|(uuid, shard_num)| match shard_num.try_into() {
                Ok(s) => Ok((uuid, s)),
                Err(_) => Err(shard_num),
            })
            .collect::<Result<Vec<(Uuid, Shard)>, _>>()
        {
            Ok(r) => r,
            Err(shard_num) => return Some(Err(TabletParsingError::ShardNum(shard_num))),
        };

        Some(Ok(RawTablet {
            // +1 because Scylla sends left-open range, so received
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
    pub(crate) fn from_raw_replicas(
        raw_replicas: &RawTabletReplicas,
        replica_translator: impl Fn(Uuid) -> Option<Arc<Node>>,
    ) -> Self {
        let all: Vec<_> = raw_replicas.replicas
            .iter()
            .filter_map(|(replica, shard)| if let Some(r) = replica_translator(*replica) {
                Some((r, *shard as Shard))
            } else {
                // TODO: Should this be an error? When can this happen?
                warn!("Node {replica} from system.tablets not present in ClusterData.known_peers. Skipping this replica");
                None
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
}

impl Tablet {
    pub(crate) fn from_raw_tablet(
        raw_tablet: &RawTablet,
        replica_translator: impl Fn(Uuid) -> Option<Arc<Node>>,
    ) -> Self {
        Self {
            first_token: raw_tablet.first_token,
            last_token: raw_tablet.last_token,
            replicas: TabletReplicas::from_raw_replicas(&raw_tablet.replicas, replica_translator),
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
}

impl TableTablets {
    fn new(table_spec: TableSpec<'static>) -> Self {
        Self {
            table_spec,
            tablet_list: Default::default(),
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

    #[cfg(test)]
    fn new_for_test() -> Self {
        Self::new(TableSpec::borrowed("test_ks", "test_table"))
    }
}

#[derive(Clone, Debug)]
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
}

impl TabletsInfo {
    pub(crate) fn new() -> Self {
        Self {
            tablets: hashbrown::HashMap::new(),
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

        let table_tablets = self.tablets.get(&query_key);

        table_tablets
    }

    pub(crate) fn add_tablet(&mut self, table_spec: TableSpec<'static>, tablet: Tablet) {
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
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use scylla_cql::frame::response::result::{ColumnType, CqlValue};
    use scylla_cql::types::serialize::{value::SerializeCql, CellWriter};
    use tracing::debug;
    use uuid::Uuid;

    use crate::routing::Token;
    use crate::transport::locator::tablets::{
        RawTablet, RawTabletReplicas, TabletParsingError, CUSTOM_PAYLOAD_TABLETS_V1_KEY,
        RAW_TABLETS_CQL_TYPE,
    };
    use crate::transport::Node;

    use super::{TableTablets, Tablet, TabletReplicas};

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
        let custom_payload =
            HashMap::from([(CUSTOM_PAYLOAD_TABLETS_V1_KEY.to_string(), vec![1, 2, 3])]);
        assert_matches::assert_matches!(
            RawTablet::from_custom_payload(&custom_payload),
            Some(Err(TabletParsingError::Parse(_)))
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
            ColumnType::Ascii,
            ColumnType::BigInt,
            ColumnType::List(Box::new(ColumnType::Tuple(vec![
                ColumnType::Uuid,
                ColumnType::Int,
            ]))),
        ]);

        SerializeCql::serialize(&value, &col_type, CellWriter::new(&mut data)).unwrap();
        debug!("{:?}", data);

        custom_payload.insert(CUSTOM_PAYLOAD_TABLETS_V1_KEY.to_string(), data);

        assert_matches::assert_matches!(
            RawTablet::from_custom_payload(&custom_payload),
            Some(Err(TabletParsingError::Parse(_)))
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

        SerializeCql::serialize(&value, &RAW_TABLETS_CQL_TYPE, CellWriter::new(&mut data)).unwrap();
        tracing::debug!("{:?}", data);

        custom_payload.insert(
            CUSTOM_PAYLOAD_TABLETS_V1_KEY.to_string(),
            // Skipping length because `SerializeCql::serialize` adds length at the
            // start of serialized value while Scylla sends the value without initial
            // length.
            data[4..].to_vec(),
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
            TabletReplicas {
                all: replicas_uids
                    .iter()
                    .cloned()
                    .map(|replica| (translator(replica).unwrap(), 1))
                    .collect(),
                per_dc
            }
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
}
