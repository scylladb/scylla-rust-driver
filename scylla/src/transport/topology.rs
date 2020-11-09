use anyhow::Result;

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, RwLock, RwLockReadGuard};

use tokio::sync::{mpsc, oneshot};
use tokio::time;

use crate::frame::response::result;
use crate::routing::*;
use crate::transport::connection::{open_connection, Connection};

const UPDATER_CRASHED: &str = "the topology updater thread has crashed. Need to restart it.";

// Represents a view of the cluster's topology, including the shape of the token ring.
// Assiociated with a TopologyReader which may send update messages.
pub struct Topology {
    // TODO accessing the ring through Arc<RwLock<...>> on each read is not the best for efficiency
    // the ring should probably be cached, and the cache refreshed periodically using the Arc
    // calling refresh() would refresh it synchronously
    ring: Arc<RwLock<Ring>>,

    refresh_tx: mpsc::Sender<RefreshRequest>,
}

struct RefreshRequest {
    ack_tx: oneshot::Sender<Result<()>>,
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

    refresh_rx: mpsc::Receiver<RefreshRequest>,

    // Global port for the cluster. Assumes every node listens on this
    port: u16,
}

impl Topology {
    pub fn read_ring(&self) -> Result<RwLockReadGuard<'_, Ring>> {
        self.ring.read().map_err(|_| anyhow!(UPDATER_CRASHED))
    }

    // Performs a synchronous refresh of the topology information.
    // After the returned future is awaited, stored topology is not older
    // than the cluster's topology at the moment `refresh` was called.
    pub async fn refresh(&self) -> Result<()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.refresh_tx
            .send(RefreshRequest { ack_tx })
            .await
            .map_err(|_| anyhow!(UPDATER_CRASHED))?;
        ack_rx.await.map_err(|_| anyhow!(UPDATER_CRASHED))?
    }
}

impl TopologyReader {
    pub async fn new(n: Node) -> Result<(Self, Topology)> {
        // TODO: use compression? maybe not necessarily, since the communicated objects are
        // small and the communication doesn't happen often?
        let conn = open_connection(n.addr, None, None).await?;

        // TODO: When querying system.local of `n` or system.peers of other nodes, we might find
        // that the address of `n` is different than `n.addr` (`n` might have multiple addresses).
        // We need to deal with that. Since the data structures are filled using data obtained from
        // `system.peers` and `system.local`, we probably need to query `system.local` for the
        // address that `n` itself prefers.

        let toks = query_local_tokens(&conn).await?;
        if toks.is_empty() {
            return Err(anyhow!("system.local tokens empty on the node"));
        }

        let mut owners = BTreeMap::new();
        for &t in &toks {
            owners.insert(t, n);
        }

        let ring = Arc::new(RwLock::new(Ring { owners }));
        let tokens = vec![(n, toks)].into_iter().collect();
        let pool = vec![(n, conn)].into_iter().collect();
        let (refresh_tx, refresh_rx) = mpsc::channel(32);

        Ok((
            TopologyReader {
                pool,
                tokens,
                ring: ring.clone(),
                refresh_rx,
                port: n.addr.port(),
            },
            Topology { ring, refresh_tx },
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

    async fn refresh(&mut self) -> Result<()> {
        use time::{timeout, Duration};

        let mut broken_connections = vec![];
        let mut refreshed = false;
        for (&node, conn) in self.pool.iter() {
            // TODO: handle broken connections
            // TODO: after short timeout, attempt to refresh on another connection *in parallel*
            // while continuing waiting on the previous connection
            let (peer_addrs, _) =
                match timeout(Duration::from_secs(5), query_peers(conn, false, self.port)).await {
                    Err(_) => {
                        // A timeout doesn't necessarily mean that the connection is useless.
                        // It may be a temporary network partition between us and the server.
                        continue;
                    }
                    Ok(Ok(ret)) => ret,
                    Ok(Err(e)) => {
                        eprintln!("Error when querying system.peers of {}: {}", node.addr, e);
                        // TODO distinguish between different error types and react accordingly
                        // for now, we just drop the connection on every error.
                        broken_connections.push(node);
                        continue;
                    }
                };

            // We assume that tokens of existing peers don't change.
            // TODO: in the future this assumption might become false
            let need_new_tokens = peer_addrs
                .iter()
                .any(|addr| !self.tokens.contains_key(&Node { addr: *addr }));

            if need_new_tokens {
                let (peer_addrs, peer_toks) =
                    match timeout(Duration::from_secs(5), query_peers(conn, true, self.port)).await
                    {
                        Err(_) => continue,
                        Ok(Ok(ret)) => ret,
                        Ok(Err(e)) => {
                            eprintln!("Error when querying system.peers of {}: {}", node.addr, e);
                            // TODO get rid of copy-pasta
                            broken_connections.push(node);
                            continue;
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

                    self.tokens.insert(n, toks);
                }
            }

            // TODO: Remove nodes that are not in peers anymore
            // (from pool, tokens, and ring)

            refreshed = true;
        }

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
            return Err(anyhow!(
                "refreshing failed on every connection, but new connections have been opened"
            ));
        }

        Err(anyhow!(
            "refreshing failed on every connection; no new connections could be opened"
        ))
    }
}

async fn query_local_tokens(c: &Connection) -> Result<Vec<Token>> {
    unwrap_tokens(
        c.query_single_page("SELECT tokens FROM system.local", &[])
            .await?
            .ok_or_else(|| anyhow!("Expected rows result when querying system.local"))?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("system.local query result empty"))?
            .columns
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("system.local `tokens` query returned no columns"))?,
    )
}

async fn query_peers(
    c: &Connection,
    get_tokens: bool,
    port: u16,
) -> Result<(Vec<SocketAddr>, Vec<Vec<Token>>)> {
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
        .ok_or_else(|| anyhow!("Expected rows result when querying system.peers"))?;

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
            .ok_or_else(|| anyhow!("system.peers `peer` query returned no columns"))?
            .ok_or_else(|| anyhow!("system.peers `peer` query returned null"))?
            .as_inet()
            .ok_or_else(|| anyhow!("system.peers `peer` query returned wrong value type"))?;
        addrs.push(SocketAddr::new(peer, port));

        if get_tokens {
            let toks = unwrap_tokens(
                col_iter
                    .next()
                    .ok_or_else(|| anyhow!("system.peers `tokens` query returned no columns"))?,
            )?;
            tokens.push(toks);
        }
    }
    Ok((addrs, tokens))
}

fn unwrap_tokens(tokens_val_opt: Option<result::CQLValue>) -> Result<Vec<Token>> {
    let tokens_val = tokens_val_opt.ok_or_else(|| anyhow!("tokens value null"))?;
    let tokens = tokens_val
        .as_set()
        .ok_or_else(|| anyhow!("tokens value type is not a set"))?;

    let mut ret = Vec::with_capacity(tokens.len());
    for tok_val in tokens {
        let tok = tok_val
            .as_text()
            .ok_or_else(|| anyhow!("tokens set contained a non-text value"))?;
        ret.push(
            Token::from_str(tok).map_err(|e| anyhow!("failed to parse token {}: {}", tok, e))?,
        );
    }
    Ok(ret)
}
