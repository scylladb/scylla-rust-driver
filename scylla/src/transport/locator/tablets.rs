use itertools::Itertools;
use lazy_static::lazy_static;
use scylla_cql::cql_to_rust::FromCqlVal;
use scylla_cql::frame::frame_errors::ParseError;
use scylla_cql::frame::response::result::{deser_cql_value, ColumnType, TableSpec};
use thiserror::Error;
use tracing::{info, warn};
use uuid::Uuid;

use crate::routing::{Shard, Token};
use crate::transport::Node;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Error, Debug)]
pub(crate) enum TabletParsingError {
    #[error(transparent)]
    Parse(#[from] ParseError),
    #[error("Shard id for tablet exceeds u32 range: {0}")]
    ShardNum(i32),
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct RawTabletReplicas {
    replicas: Vec<(Uuid, Shard)>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RawTablet {
    first_token: Token,
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
        let result: RawTabletPayload = FromCqlVal::from_cql(cql_value).unwrap();

        let replicas = match result
            .2
            .into_iter()
            .map(|(uuid, shard_num)| match shard_num.try_into() {
                Ok(s) => Ok((uuid, s)),
                Err(_) => Err(shard_num),
            })
            .collect::<Result<Vec<(Uuid, u32)>, _>>()
        {
            Ok(r) => r,
            Err(shard_num) => return Some(Err(TabletParsingError::ShardNum(shard_num))),
        };

        Some(Ok(RawTablet {
            // +1 because Scylla sends left-open range, so recevied
            // number is the last token not belonging to this tablet.
            first_token: Token::new(result.0 + 1),
            last_token: Token::new(result.1),
            replicas: RawTabletReplicas { replicas },
        }))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
struct TabletReplicas {
    all: Vec<(Arc<Node>, Shard)>,
    per_dc: HashMap<String, Vec<(Arc<Node>, Shard)>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Tablet {
    first_token: Token,
    last_token: Token,
    replicas: TabletReplicas,
    /// If any of the replicas failed to resolve to a node,
    /// then this field will contain the original list of replicas.
    failed: Option<RawTabletReplicas>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct TableTablets {
    tablet_list: Vec<Tablet>,
    /// In order to make typical tablet maintance faster
    /// we remember if there were any tablets that have unrecognized uuids in replica list.
    /// If there were none, and few other conditions are satisfied, we can skip nearly whole maintanace.
    /// This flag may be falsely true: if we add tablet with unknown replica but later
    /// overwrite it with some other tablet.
    has_unknown_replicas: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct TabletsInfo {
    tablets: HashMap<TableSpec, TableTablets>,
    /// See `has_unknown_replicas` field in `TableTablets`.
    /// The field here will be true if it is true for any TableTablets.
    has_unknown_replicas: bool,
}

impl Tablet {
    pub(crate) fn from_raw_tablet(
        raw_tablet: RawTablet,
        replica_translator: impl Fn(Uuid) -> Option<Arc<Node>>,
    ) -> Self {
        let replicas_result =
            TabletReplicas::from_raw_replicas(&raw_tablet.replicas, replica_translator);
        let (replicas, failed) = match replicas_result {
            Ok(r) => (r, None),
            Err((r, f)) => {
                info!("Nodes ({}) from system.tablets not present in current ClusterData.known_peers. \
                       Skipping this replica until topology refresh", f.iter().format(", "));
                (r, Some(raw_tablet.replicas))
            }
        };
        Self {
            first_token: raw_tablet.first_token,
            last_token: raw_tablet.last_token,
            replicas,
            failed,
        }
    }

    // Returns true if after the operation Tablet replicas are fully resolved
    fn re_resolve_replicas(
        &mut self,
        replica_translator: impl Fn(Uuid) -> Option<Arc<Node>>,
    ) -> bool {
        if let Some(failed) = self.failed.as_ref() {
            match TabletReplicas::from_raw_replicas(failed, replica_translator) {
                Ok(resolved_replicas) => {
                    // We managed to successfully resolve all replicas, all is well.
                    self.replicas = resolved_replicas;
                    self.failed = None;
                    true
                }
                Err((_, failed)) => {
                    warn!("Nodes ({}) listed as replicas for a tablet are not present in ClusterData.known_peers, \
                           despite topology refresh. Removing problematic tablet.", failed.iter().format(", "));
                    false
                }
            }
        } else {
            true
        }
    }

    fn update_stale_nodes(&mut self, recreated_nodes: &HashMap<Uuid, Arc<Node>>) {
        let mut any_updated = false;
        for (node, _) in self.replicas.all.iter_mut() {
            if let Some(new_node) = recreated_nodes.get(&node.host_id) {
                any_updated = true;
                *node = Arc::clone(new_node);
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
    }
}

impl TabletReplicas {
    /// Gets raw replica list (which is an array of (Uuid, Shard)), retrieves
    /// `Node` objects and groups node replicas by DC to make life easier for LBP.
    /// In case of failure this function returns Self, but with problematic nodes skipped,
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
        all.iter().for_each(|(replica, node)| {
            if let Some(dc) = replica.datacenter.as_ref() {
                if let Some(replicas) = per_dc.get_mut(dc) {
                    replicas.push((Arc::clone(replica), *node));
                } else {
                    per_dc.insert(dc.to_string(), vec![(Arc::clone(replica), *node)]);
                }
            }
        });

        if failed.is_empty() {
            Ok(Self { all, per_dc })
        } else {
            Err((Self { all, per_dc }, failed))
        }
    }
}

impl TableTablets {
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

    fn perform_maintanance(
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
                tablet.re_resolve_replicas(|id: Uuid| all_current_nodes.get(&id).cloned())
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
        // Situations where driver does this don't happen often:
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
}

impl TabletsInfo {
    pub(crate) fn new() -> Self {
        Self {
            tablets: HashMap::new(),
            has_unknown_replicas: false,
        }
    }

    pub(crate) fn tablets_for_table<'a>(&'a self, table: &TableSpec) -> Option<&'a TableTablets> {
        let tbl = self.tablets.get(table);

        tbl
    }

    pub(crate) fn add_tablet(&mut self, table: TableSpec, tablet: Tablet) {
        if tablet.failed.is_some() {
            self.has_unknown_replicas = true;
        }
        self.tablets.entry(table).or_default().add_tablet(tablet)
    }

    /// This method is supposed to be called when topology is updated.
    /// It goes trough tablet info and adjusts it to topology changes.
    /// What is updated:
    /// 1. Info for dropped tables is removed.
    /// 2. Tablets where removed node was one of replicas are removed.
    ///    Can be skipped if no nodes were removed.
    /// 3. Tablets with unrecognized uuids in replica list are resolved again.
    ///    If this is unsuccessful again then the tablet is removed.
    ///    This can be skipped if we know we have no such tablets.
    /// 4. Rarely the driver may need to re-create `Node` object for given node.
    ///    Old object is replaced with new in replica lists.
    ///    This can be skipped if there were no re-created `Node` objects.
    ///
    /// In order to not perform unnecessary work during typical schema refresh
    /// we avoid iterating trough tablets at all if steps 2-4 can be skipped.
    ///
    /// * `removed_nodes`: Nodes that previously were present in ClusterData but are not anymore.
    ///                    For any such node we should remove all tablets that have it in replica list.
    ///                    This is because otherwise:
    ///                    1. We would hold on to old `Node` objects, not allowing them to release memory.
    ///                    2. Return removed nodes in LBP
    ///                    3. When new node joins and becomes replica for this tablet, we would
    ///                       not use it - instead we would keep querying a subset of replicas.
    ///
    /// * `all_current_nodes`: Map of all nodes. Required to remap unknown replicas.
    ///                        If we didn't try to remap them and instead just skipped them,
    ///                        then we would only query subset of replicas for the tablet,
    ///                        potentially increasing load on those replicas.
    ///                        The alternative is dropping the tablet immediately, but if there are a lot
    ///                        of requests to range belonging to this tablet, then we would get a
    ///                        lot of unnecessary feedbacks sent. Thus the current solution:
    ///                        skipping unknown replicas and dropping the tablet if we still can't resolve
    ///                        them after schema refresh.
    ///
    /// * `recreated_nodes`: There are some situations (IP change, DC / Rack change) where the driver
    ///                      will create a new `Node` object for some node and drop the old one.
    ///                      Tablet info would still contain old object, so the driver would not use
    ///                      new connections. That means if there were such nodes then we need to go over
    ///                      tablets and replace `Arc<Node>` objects for recreated nodes.

    pub(crate) fn perform_maintanance(
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
                table_tablets.perform_maintanance(
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
    use std::{
        collections::HashMap,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
    };

    use scylla_cql::{
        frame::response::result::{ColumnType, CqlValue},
        types::serialize::{value::SerializeCql, CellWriter},
    };
    use uuid::Uuid;

    use crate::{
        routing::Token,
        transport::{
            locator::tablets::{
                RawTablet, RawTabletReplicas, TabletParsingError, CUSTOM_PAYLOAD_TABLETS_V1_KEY,
                RAW_TABLETS_CQL_TYPE,
            },
            topology::PeerEndpoint,
            Node, NodeAddr,
        },
    };

    use super::{TableTablets, Tablet, TabletReplicas};

    #[test]
    fn test_raw_tablet_deser_empty() {
        let custom_payload = HashMap::new();
        assert!(RawTablet::from_custom_payload(&custom_payload).is_none());
    }

    #[test]
    fn test_raw_tablet_deser_trash() {
        let mut custom_payload = HashMap::new();
        custom_payload.insert(CUSTOM_PAYLOAD_TABLETS_V1_KEY.to_string(), vec![1, 2, 3]);
        assert!(matches!(
            RawTablet::from_custom_payload(&custom_payload),
            Some(Err(TabletParsingError::Parse(_)))
        ));
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
        println!("{:?}", data);

        custom_payload.insert(CUSTOM_PAYLOAD_TABLETS_V1_KEY.to_string(), data);

        assert!(matches!(
            RawTablet::from_custom_payload(&custom_payload),
            Some(Err(TabletParsingError::Parse(_)))
        ));
    }

    #[test]
    fn test_raw_tablet_deser_correct() {
        let mut custom_payload = HashMap::new();
        let mut data = vec![];

        let value = CqlValue::Tuple(vec![
            Some(CqlValue::BigInt(1234)),
            Some(CqlValue::BigInt(2137)),
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
        println!("{:?}", data);

        custom_payload.insert(
            CUSTOM_PAYLOAD_TABLETS_V1_KEY.to_string(),
            data[4..].to_vec(), // skipping length
        );

        let tablet = RawTablet::from_custom_payload(&custom_payload)
            .unwrap()
            .unwrap();

        assert_eq!(
            tablet,
            RawTablet {
                first_token: Token::new(1235),
                last_token: Token::new(2137),
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
        let addr =
            NodeAddr::Translatable(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0));
        let nodes: HashMap<Uuid, PeerEndpoint> = vec![
            PeerEndpoint {
                host_id: Uuid::from_u64_pair(1, 1),
                address: addr,
                datacenter: Some("dc1".to_string()),
                rack: None,
            },
            PeerEndpoint {
                host_id: Uuid::from_u64_pair(1, 2),
                address: addr,
                datacenter: Some("dc2".to_string()),
                rack: None,
            },
            PeerEndpoint {
                host_id: Uuid::from_u64_pair(1, 3),
                address: addr,
                datacenter: Some("dc3".to_string()),
                rack: None,
            },
            PeerEndpoint {
                host_id: Uuid::from_u64_pair(1, 4),
                address: addr,
                datacenter: Some("dc2".to_string()),
                rack: None,
            },
            PeerEndpoint {
                host_id: Uuid::from_u64_pair(1, 5),
                address: addr,
                datacenter: Some("dc2".to_string()),
                rack: None,
            },
            PeerEndpoint {
                host_id: Uuid::from_u64_pair(1, 6),
                address: addr,
                datacenter: Some("dc1".to_string()),
                rack: None,
            },
        ]
        .into_iter()
        .map(|endpoint| (endpoint.host_id, endpoint))
        .collect();

        let translator = |uuid| {
            nodes.get(&uuid).map(|endpoint| {
                Arc::new(Node::new(endpoint.clone(), Default::default(), None, false))
            })
        };

        let replicas_uids = [
            Uuid::from_u64_pair(1, 1),
            Uuid::from_u64_pair(1, 2),
            Uuid::from_u64_pair(1, 3),
            Uuid::from_u64_pair(1, 4),
            Uuid::from_u64_pair(1, 5),
            Uuid::from_u64_pair(1, 6),
        ];

        let raw_replicas = RawTabletReplicas {
            replicas: replicas_uids.iter().cloned().map(|uid| (uid, 1)).collect(),
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
        let tablets: TableTablets = Default::default();
        assert!(tablets.tablet_for_token(Token::new(1)).is_none());
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
        let mut tablets = Default::default();

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
        let mut tablets = Default::default();

        insert_ranges(
            &mut tablets,
            &[
                (-2000000, -1900001),
                (-1900000, -1700001),
                (-1700000, -1),
                (0, 19),
                (20, 10000),
            ],
        );
        verify_ranges(
            &tablets,
            &[
                (-2000000, -1900001),
                (-1900000, -1700001),
                (-1700000, -1),
                (0, 19),
                (20, 10000),
            ],
        );
    }

    #[test]
    fn test_adding_tablet_same() {
        let mut tablets = Default::default();

        insert_ranges(&mut tablets, &[(-2000000, -1800000), (-2000000, -1800000)]);
        verify_ranges(&tablets, &[(-2000000, -1800000)]);
    }

    #[test]
    fn test_adding_tablet_overlapping_one() {
        let mut tablets = Default::default();
        insert_ranges(&mut tablets, &[(-2000000, -1800000)]);

        // Replacing a tablet, overlaps right part of the old one
        insert_ranges(&mut tablets, &[(-1900000, -1700000)]);
        verify_ranges(&tablets, &[(-1900000, -1700000)]);

        // Replacing a tablet, overlaps left part of the old one
        insert_ranges(&mut tablets, &[(-2000000, -1800000)]);
        verify_ranges(&tablets, &[(-2000000, -1800000)]);
    }

    #[test]
    fn test_adding_tablet_fill_hole() {
        let mut tablets = Default::default();

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
        let mut tablets = Default::default();
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
        let mut tablets = Default::default();
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
