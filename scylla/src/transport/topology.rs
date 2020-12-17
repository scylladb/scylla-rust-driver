use super::transport_errors::{InternalDriverError, TokenError, TopologyError, TransportError};

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, RwLock, RwLockReadGuard};

use tokio::sync::{mpsc, oneshot};
use tokio::time;

use crate::frame::response::result;
use crate::routing::*;
use crate::transport::connection::{open_connection, open_named_connection, Connection};
use crate::transport::session::IntoTypedRows;

use thiserror::Error;

const UPDATER_CRASHED: &str = "the topology updater thread has crashed. Need to restart it.";

// Represents a view of the cluster's topology, including the shape of the token ring.
// Assiociated with a TopologyReader which may send update messages.
pub struct Topology {
    // TODO accessing the ring through Arc<RwLock<...>> on each read is not the best for efficiency
    // the ring should probably be cached, and the cache refreshed periodically using the Arc
    // calling refresh() would refresh it synchronously
    ring: Arc<RwLock<Ring>>,
    keyspaces: Arc<RwLock<HashMap<String, Keyspace>>>,

    refresh_tx: mpsc::Sender<RefreshRequest>,
}

struct RefreshRequest {
    ack_tx: oneshot::Sender<Result<(), TransportError>>,
}

// Responsible for periodically updating a Topology.
pub struct TopologyReader {
    // invariant: nonempty
    pool: HashMap<Node, Connection>,

    // invariant:
    //  1. for every key `n` in `tokens`,
    //     for every `t` in tokens[`n`],
    //     `ring.owners` has a `(t, n)` entry
    //  2. for every `(t, n)` entry in `ring.owners`,
    //     `tokens` has an entry with key `n`,
    //     and `tokens[n]` contains `t`.
    //
    // `tokens` also serves as a container for cluster members that we know (using the keys of the map).
    // We assume that every ``normal'' member of the cluster (so not a joining or leaving one) has
    // tokens in the ring. TODO: this might change in the future; in this case we'll need a
    // separate set.
    tokens: HashMap<Node, Vec<Token>>,
    ring: Arc<RwLock<Ring>>,
    keyspaces: Arc<RwLock<HashMap<String, Keyspace>>>,

    refresh_rx: mpsc::Receiver<RefreshRequest>,

    // Global port for the cluster. Assumes every node listens on this
    port: u16,
}

#[derive(Error, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SingleRefreshError {
    #[error("Refresh timed out")]
    Timeout,
    #[error("Connection to node {0:?} broken")]
    BrokenConnection(Node),
}

impl Topology {
    pub fn read_ring(&self) -> Result<RwLockReadGuard<'_, Ring>, TransportError> {
        self.ring.read().map_err(|_| {
            TransportError::InternalDriverError(InternalDriverError::UpdaterError(
                UPDATER_CRASHED.to_string(),
            ))
        })
    }

    #[allow(dead_code)] // For internal use - once used in load balancing the warning will disappear
    pub fn read_keyspaces(
        &self,
    ) -> Result<RwLockReadGuard<'_, HashMap<String, Keyspace>>, InternalDriverError> {
        self.keyspaces
            .read()
            .map_err(|_| InternalDriverError::UpdaterError(UPDATER_CRASHED.to_string()))
    }

    // Performs a synchronous refresh of the topology information.
    // After the returned future is awaited, stored topology is not older
    // than the cluster's topology at the moment `refresh` was called.
    pub async fn refresh(&self) -> Result<(), TransportError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.refresh_tx
            .send(RefreshRequest { ack_tx })
            .await
            .map_err(|_| {
                TransportError::InternalDriverError(InternalDriverError::UpdaterError(
                    UPDATER_CRASHED.to_string(),
                ))
            })?;
        ack_rx.await.map_err(|_| {
            TransportError::InternalDriverError(InternalDriverError::UpdaterError(
                UPDATER_CRASHED.to_string(),
            ))
        })?
    }
}

impl TopologyReader {
    pub async fn new(n: Node) -> Result<(Self, Topology), TransportError> {
        // TODO: use compression? maybe not necessarily, since the communicated objects are
        // small and the communication doesn't happen often?
        let conn = open_named_connection(
            n.addr,
            None,
            None,
            Some("scylla-rust-driver:control-connection".to_string()),
        )
        .await?;

        // TODO: When querying system.local of `n` or system.peers of other nodes, we might find
        // that the address of `n` is different than `n.addr` (`n` might have multiple addresses).
        // We need to deal with that. Since the data structures are filled using data obtained from
        // `system.peers` and `system.local`, we probably need to query `system.local` for the
        // address that `n` itself prefers.

        let toks = query_local_tokens(&conn).await?;
        if toks.is_empty() {
            return Err(TransportError::InternalDriverError(
                InternalDriverError::LocalTokensEmptyOnNodes,
            ));
        }

        let mut owners = BTreeMap::new();
        for &t in &toks {
            owners.insert(t, n);
        }

        let ring = Arc::new(RwLock::new(Ring { owners }));
        let keyspaces = Arc::new(RwLock::new(HashMap::new()));
        let tokens = vec![(n, toks)].into_iter().collect();
        let pool = vec![(n, conn)].into_iter().collect();
        let (refresh_tx, refresh_rx) = mpsc::channel(32);

        Ok((
            TopologyReader {
                pool,
                tokens,
                ring: ring.clone(),
                keyspaces: keyspaces.clone(),
                refresh_rx,
                port: n.addr.port(),
            },
            Topology {
                ring,
                keyspaces,
                refresh_tx,
            },
        ))
    }

    pub async fn run(mut self) {
        use time::Duration;

        let mut sleep_dur = Duration::from_secs(0);
        loop {
            tokio::select! {
                _ = time::sleep(sleep_dur) => {
                    sleep_dur = match self.refresh().await {
                        Ok(()) => Duration::from_secs(10),
                        Err(e) => {
                            // TODO: use appropriate logging mechanism
                            eprintln!("TopologyReader: error on periodic topology refresh: {}. Will retry", e);
                            Duration::from_secs(2)
                        }
                    };
                },
                Some(request) = self.refresh_rx.recv() => {
                    let _ = request.ack_tx.send(self.refresh().await);
                    // If there was a send error, the other end must have closed the channel, i.e.
                    // it wasn't interested in the result anymore. We don't need to do anything.
                    sleep_dur = Duration::from_secs(10)
                }
            }
        }
    }

    async fn refresh(&mut self) -> Result<(), TransportError> {
        use time::{timeout, Duration};

        let mut broken_connections: Vec<Node> = vec![];
        let mut refreshed = false;

        // We have to move self.tokens out of self for the update
        // We want to borrow &self in 3 places at once:
        // self.pool.iter(), refresh_peers, refresh_keyspaces
        // And borrow &mut self.tokens in refresh_peers
        // so moving out self.tokens seems resonable
        let mut self_tokens: HashMap<Node, Vec<Token>> = HashMap::new();
        std::mem::swap(&mut self.tokens, &mut self_tokens);

        for (&node, conn) in self.pool.iter() {
            // TODO: handle broken connections
            // TODO: after short timeout, attempt to refresh on another connection *in parallel*
            // while continuing waiting on the previous connection
            let peers_fut = self.refresh_peers(&mut self_tokens, node, conn);
            let keys_fut = self.refresh_keyspaces(node, conn);

            let (peers_res, keys_res) = tokio::join!(peers_fut, keys_fut);

            if let Err(SingleRefreshError::BrokenConnection(node)) = peers_res {
                broken_connections.push(node);
            } else if let Err(SingleRefreshError::BrokenConnection(node)) = keys_res {
                broken_connections.push(node);
            }

            // TODO: Remove nodes that are not in peers anymore
            // (from pool, tokens, and ring)
            if peers_res.is_ok() && keys_res.is_ok() {
                refreshed = true;
            }
        }

        // Put self.tokens back in the struct
        std::mem::swap(&mut self.tokens, &mut self_tokens);

        for node in broken_connections {
            self.pool.remove(&node);
        }

        // Try opening new connections
        let mut any_new = false;
        for &node in self.tokens.keys() {
            if let Entry::Vacant(entry) = self.pool.entry(node) {
                // TODO: use compression?
                let conn = match timeout(
                    Duration::from_secs(5),
                    open_connection(node.addr, None, None),
                )
                .await
                {
                    Err(_) => {
                        // TODO: logging
                        eprintln!(
                            "TopologyReader: timeout when trying to open connection to {}",
                            node.addr
                        );
                        continue;
                    }
                    Ok(Err(e)) => {
                        // TODO: logging
                        eprintln!(
                            "TopologyReader: failed to open connection to {}: {}",
                            node.addr, e
                        );
                        continue;
                    }
                    Ok(Ok(conn)) => conn,
                };
                eprintln!("TopologyReader: opened new connection to {}", node.addr);
                entry.insert(conn);
                any_new = true;
            }
        }

        if refreshed {
            return Ok(());
        }

        if any_new {
            return Err(TransportError::InternalDriverError(
                InternalDriverError::RefreshingFailedOnEveryConnection(
                    "new connections have been opened".to_string(),
                ),
            ));
        }

        Err(TransportError::InternalDriverError(
            InternalDriverError::RefreshingFailedOnEveryConnection(
                "no new connections could be opened".to_string(),
            ),
        ))
    }

    async fn refresh_peers(
        &self,
        self_tokens: &mut HashMap<Node, Vec<Token>>,
        node: Node,
        conn: &Connection,
    ) -> Result<(), SingleRefreshError> {
        use time::{timeout, Duration};

        let (peer_addrs, _) =
            match timeout(Duration::from_secs(5), query_peers(conn, false, self.port)).await {
                Err(_) => {
                    // A timeout doesn't necessarily mean that the connection is useless.
                    // It may be a temporary network partition between us and the server.
                    return Err(SingleRefreshError::Timeout);
                }
                Ok(Ok(ret)) => ret,
                Ok(Err(e)) => {
                    eprintln!("Error when querying system.peers of {}: {}", node.addr, e);
                    // TODO distinguish between different error types and react accordingly
                    // for now, we just drop the connection on every error.
                    return Err(SingleRefreshError::BrokenConnection(node));
                }
            };

        // We assume that tokens of existing peers don't change.
        // TODO: in the future this assumption might become false
        let need_new_tokens = peer_addrs
            .iter()
            .any(|addr| !self_tokens.contains_key(&Node { addr: *addr }));

        if need_new_tokens {
            let (peer_addrs, peer_toks) =
                match timeout(Duration::from_secs(5), query_peers(conn, true, self.port)).await {
                    Err(_) => return Err(SingleRefreshError::Timeout),
                    Ok(Ok(ret)) => ret,
                    Ok(Err(e)) => {
                        eprintln!("Error when querying system.peers of {}: {}", node.addr, e);
                        // TODO get rid of copy-pasta
                        return Err(SingleRefreshError::BrokenConnection(node));
                    }
                };
            assert_eq!(peer_addrs.len(), peer_toks.len());
            for (addr, toks) in peer_addrs.into_iter().zip(peer_toks.into_iter()) {
                let n = Node { addr };
                let new_ring = {
                    let mut ring = self
                        .ring
                        .read()
                        .unwrap_or_else(|_| panic!("poisoned ring lock")) // TODO: handle this better?
                        .clone();
                    for &t in &toks {
                        ring.owners.insert(t, n);
                    }
                    ring
                };

                let mut ring_ref = self
                    .ring
                    .write()
                    .unwrap_or_else(|_| panic!("poisoned ring lock")); // TODO same as above?
                *ring_ref = new_ring;

                self_tokens.insert(n, toks);
            }
        }
        Ok(())
    }

    async fn refresh_keyspaces(
        &self,
        node: Node,
        conn: &Connection,
    ) -> Result<(), SingleRefreshError> {
        use tokio::time::{timeout, Duration};

        // Read keyspace information from system_schema.keyspaces
        let received_keyspaces: Vec<(String, Keyspace)> =
            match timeout(Duration::from_secs(5), query_keyspaces(conn)).await {
                Err(_) => {
                    // A timeout doesn't necessarily mean that the connection is useless.
                    // It may be a temporary network partition between us and the server.
                    return Err(SingleRefreshError::Timeout);
                }
                Ok(Ok(ret)) => ret,
                Ok(Err(e)) => {
                    eprintln!(
                        "Error when querying system_schema.keyspaces of {}: {}",
                        node.addr, e
                    );
                    // TODO distinguish between different error types and react accordingly
                    // for now, we just drop the connection on every error.
                    return Err(SingleRefreshError::BrokenConnection(node));
                }
            };

        // Add received keyspaces to self.keyspaces
        let locked_keyspaces: &mut HashMap<String, Keyspace> = &mut self
            .keyspaces
            .write()
            .unwrap_or_else(|_| panic!("poisoned keyspaces lock")); // TODO: handle this better?;

        for (keyspace_name, keyspace) in received_keyspaces {
            locked_keyspaces.insert(keyspace_name, keyspace);
        }

        Ok(())
    }
}

async fn query_local_tokens(c: &Connection) -> Result<Vec<Token>, TransportError> {
    unwrap_tokens(
        c.query_single_page("SELECT tokens FROM system.local", &[])
            .await?
            .ok_or(TopologyError::LocalExpectedRowResults)?
            .into_iter()
            .next()
            .ok_or(TopologyError::LocalQueryResultEmpty)?
            .columns
            .into_iter()
            .next()
            .ok_or(TopologyError::LocalTokensNoColumnsReturned)?,
    )
}

async fn query_peers(
    c: &Connection,
    get_tokens: bool,
    port: u16,
) -> Result<(Vec<SocketAddr>, Vec<Vec<Token>>), TransportError> {
    // TODO: do we want `peer`, `preferred_ip`, or `rpc_address`? Which one is `external`?
    let rows = c
        .query_single_page(
            format!(
                "SELECT {} FROM system.peers",
                if get_tokens { "peer, tokens" } else { "peer" }
            ),
            &[],
        )
        .await?
        .ok_or(TopologyError::PeersRowError)?;

    let mut addrs = Vec::with_capacity(rows.len());
    let mut tokens = if get_tokens {
        vec![]
    } else {
        Vec::with_capacity(rows.len())
    };
    for row in rows {
        let mut col_iter = row.columns.into_iter();

        let peer = col_iter
            .next()
            .ok_or(TopologyError::PeersColumnError)?
            .ok_or(TopologyError::PeersValueTypeError)?
            .as_inet()
            .ok_or(TopologyError::PeersValueTypeError)?;
        addrs.push(SocketAddr::new(peer, port));

        if get_tokens {
            let toks = unwrap_tokens(col_iter.next().ok_or(TopologyError::PeersColumnError)?)?;
            tokens.push(toks);
        }
    }
    Ok((addrs, tokens))
}

fn unwrap_tokens(tokens_val_opt: Option<result::CQLValue>) -> Result<Vec<Token>, TransportError> {
    let tokens_val = tokens_val_opt.ok_or(TokenError::NullTokens)?;
    let tokens = tokens_val.as_set().ok_or(TokenError::SetValue)?;

    let mut ret = Vec::with_capacity(tokens.len());
    for tok_val in tokens {
        let tok = tok_val.as_text().ok_or(TokenError::NonTextValue)?;
        ret.push(Token::from_str(tok).map_err(|_| TokenError::ParseFailure(tok.to_string()))?);
    }
    Ok(ret)
}

async fn query_keyspaces(conn: &Connection) -> Result<Vec<(String, Keyspace)>, TransportError> {
    let rows = match conn
        .query_single_page(
            "select keyspace_name, toJson(replication) from system_schema.keyspaces",
            &[],
        )
        .await?
    {
        Some(rows) => rows,
        None => {
            return Err(TopologyError::BadKeyspacesQuery(
                "No rows returned!, server response should be Rows".to_string(),
            )
            .into());
        }
    };

    let mut result = Vec::with_capacity(rows.len());

    for row in rows.into_typed::<(String, String)>() {
        let (keyspace_name, keyspace_json_text) = row.map_err(|e| {
            TopologyError::BadKeyspacesQuery(format!("Returned columns have wrong types: {}", e))
        })?;

        let strategy_map: HashMap<String, String> = json_to_string_map(&keyspace_json_text)?;

        let strategy: Strategy = strategy_from_string_map(strategy_map)?;

        result.push((keyspace_name, Keyspace { strategy }));
    }

    Ok(result)
}

fn json_to_string_map(json_text: &str) -> Result<HashMap<String, String>, TopologyError> {
    use serde_json::Value;

    let json: Value = serde_json::from_str(json_text).map_err(|e| {
        TopologyError::BadKeyspacesQuery(format!(
            "Couldn't parse received keyspace data as json: {}",
            e
        ))
    })?;

    let object_map = match json {
        Value::Object(map) => map,
        _ => {
            return Err(TopologyError::BadKeyspacesQuery(
                "Couldn't convert json to map<string,string> - value is not an object".to_string(),
            ))
        }
    };

    let mut result = HashMap::with_capacity(object_map.len());

    for (key, val) in object_map.into_iter() {
        match val {
            Value::String(string) => result.insert(key, string),
            _ => {
                return Err(TopologyError::BadKeyspacesQuery(
                    "Couldn't convert json to map<string,string> - value is not a string"
                        .to_string(),
                ))
            }
        };
    }

    Ok(result)
}

fn strategy_from_string_map(
    mut strategy_map: HashMap<String, String>,
) -> Result<Strategy, TopologyError> {
    let strategy_name: String = strategy_map.remove("class").ok_or_else(|| {
        TopologyError::BadKeyspacesQuery("No 'class' in keyspace data!".to_string())
    })?;

    let mut get_replication_factor = || -> Result<Option<usize>, TopologyError> {
        strategy_map
            .remove("replication_factor")
            .map(|rep_factor_str| usize::from_str(&rep_factor_str)) // Parse rep_factor_str as usize
            .transpose() // Change Option<Result> to Result<Option>
            .map_err(|e| {
                TopologyError::BadKeyspacesQuery(format!(
                    "Couldn't parse replication_factor string as usize! Error: {}",
                    e
                ))
            })
    };

    let strategy: Strategy = match strategy_name.as_str() {
        "org.apache.cassandra.locator.SimpleStrategy" => {
            let replication_factor: usize = get_replication_factor()?.ok_or_else(|| {
                TopologyError::BadKeyspacesQuery(
                    "No replication_factor in SimpleStrategy data!".to_string(),
                )
            })?;

            Strategy::SimpleStrategy { replication_factor }
        }
        "org.apache.cassandra.locator.LocalStrategy" => {
            let replication_factor: Option<usize> = get_replication_factor()?;

            Strategy::LocalStrategy { replication_factor }
        }
        "org.apache.cassandra.locator.NetworkTopologyStrategy" => {
            let mut datacenter_repfactors: HashMap<String, usize> =
                HashMap::with_capacity(strategy_map.len());

            for (datacenter, rep_factor_str) in strategy_map.drain() {
                let rep_factor: usize = match usize::from_str(&rep_factor_str) {
                    Ok(number) => number,
                    Err(_) => continue, // There might be other things in the map, we care only about rep_factors
                };

                datacenter_repfactors.insert(datacenter, rep_factor);
            }

            Strategy::NetworkTopologyStrategy {
                datacenter_repfactors,
            }
        }
        _ => Strategy::Other {
            name: strategy_name,
            data: strategy_map,
        },
    };

    Ok(strategy)
}
