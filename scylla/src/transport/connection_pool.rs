use crate::routing::{Shard, ShardCount, Sharder, Token};
use crate::transport::errors::QueryError;
use crate::transport::{
    connection,
    connection::{Connection, ConnectionConfig, ErrorReceiver, VerifiedKeyspaceName},
};

use arc_swap::ArcSwap;
use futures::{
    future::{FusedFuture, RemoteHandle},
    stream::FuturesUnordered,
    Future, FutureExt, StreamExt,
};
use rand::Rng;
use std::convert::TryInto;
use std::io::ErrorKind;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, trace, warn};

/// The target size of a per-node connection pool.
pub enum PoolSize {
    /// Indicates that the pool should establish given number of connections to the node.
    ///
    /// In case of Scylla nodes, this number will be rounded up to a multiplicity of the shard count
    /// and then divided across shards. For example, with 4 shards and `PerHost(5)` pool size, the
    /// pool size will be rounded up to 8 and divided across shards, so there will be 2 connections
    /// per shard.
    PerHost(NonZeroUsize),

    /// Indicates that the pool should establish given number of connections to each shard on the node.
    ///
    /// Cassandra nodes will be treated as if they have only one shard.
    ///
    /// The recommended setting for Scylla is one connection per shard - `PerShard(1)`.
    PerShard(NonZeroUsize),
}

impl Default for PoolSize {
    fn default() -> Self {
        PoolSize::PerShard(NonZeroUsize::new(1).unwrap())
    }
}

enum MaybePoolConnections {
    // The pool is empty and is waiting to be refilled
    Pending,

    // The pool has some connections which are usable (or will be removed soon)
    Ready(PoolConnections),
}

#[derive(Clone)]
enum PoolConnections {
    NotSharded(Vec<Arc<Connection>>),
    Sharded {
        sharder: Sharder,
        connections: Vec<Vec<Arc<Connection>>>,
    },
}

pub struct NodeConnectionPool {
    conns: Arc<ArcSwap<MaybePoolConnections>>,
    use_keyspace_request_sender: mpsc::Sender<UseKeyspaceRequest>,
    _refiller_handle: RemoteHandle<()>,
}

impl NodeConnectionPool {
    pub async fn new(
        address: IpAddr,
        port: u16,
        connection_config: ConnectionConfig,
        refill_goal: PoolSize,
        current_keyspace: Option<VerifiedKeyspaceName>,
    ) -> Self {
        let (use_keyspace_request_sender, use_keyspace_request_receiver) = mpsc::channel(1);

        let mut refiller = PoolRefiller::new(
            address,
            port,
            connection_config,
            refill_goal,
            current_keyspace,
            use_keyspace_request_receiver,
        );

        // Fill the pool for the first time here
        refiller.fill().await;

        let conns = refiller.get_shared_connections();
        let (fut, handle) = refiller.run().remote_handle();
        tokio::spawn(fut);

        Self {
            conns,
            use_keyspace_request_sender,
            _refiller_handle: handle,
        }
    }

    pub fn connection_for_token(&self, token: Token) -> Result<Arc<Connection>, QueryError> {
        self.with_connections(|pool_conns| match pool_conns {
            PoolConnections::NotSharded(conns) => {
                Self::choose_random_connection_from_slice(conns).unwrap()
            }
            PoolConnections::Sharded {
                sharder,
                connections,
            } => {
                let shard: u16 = sharder
                    .shard_of(token)
                    .try_into()
                    .expect("Shard number doesn't fit in u16");
                Self::connection_for_shard(shard, sharder.nr_shards, connections.as_slice())
            }
        })
    }

    pub fn random_connection(&self) -> Result<Arc<Connection>, QueryError> {
        self.with_connections(|pool_conns| match pool_conns {
            PoolConnections::NotSharded(conns) => {
                Self::choose_random_connection_from_slice(conns).unwrap()
            }
            PoolConnections::Sharded {
                sharder,
                connections,
            } => {
                let shard: u16 = rand::thread_rng().gen_range(0..sharder.nr_shards.get());
                Self::connection_for_shard(shard, sharder.nr_shards, connections.as_slice())
            }
        })
    }

    // Tries to get a connection to given shard, if it's broken returns any working connection
    fn connection_for_shard(
        shard: u16,
        nr_shards: ShardCount,
        shard_conns: &[Vec<Arc<Connection>>],
    ) -> Arc<Connection> {
        // Try getting the desired connection
        if let Some(conn) = Self::choose_random_connection_from_slice(&shard_conns[shard as usize])
        {
            return conn;
        }

        // If this fails try getting any other in random order
        let mut shards_to_try: Vec<u16> =
            (shard..nr_shards.get()).chain(0..shard).skip(1).collect();

        while !shards_to_try.is_empty() {
            let idx = rand::thread_rng().gen_range(0..shards_to_try.len());
            let shard = shards_to_try.swap_remove(idx);

            if let Some(conn) =
                Self::choose_random_connection_from_slice(&shard_conns[shard as usize])
            {
                return conn;
            }
        }

        unreachable!("could not find any connection in supposedly non-empty pool")
    }

    pub async fn use_keyspace(
        &self,
        keyspace_name: VerifiedKeyspaceName,
    ) -> Result<(), QueryError> {
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

        self.use_keyspace_request_sender
            .send(UseKeyspaceRequest {
                keyspace_name,
                response_sender,
            })
            .await
            .expect("Bug in ConnectionKeeper::use_keyspace sending");
        // Other end of this channel is in the Refiller, can't be dropped while we have &self to _refiller_handle

        response_receiver.await.unwrap() // NodePoolRefiller always responds
    }

    pub fn get_working_connections(&self) -> Result<Vec<Arc<Connection>>, QueryError> {
        self.with_connections(|pool_conns| match pool_conns {
            PoolConnections::NotSharded(conns) => conns.clone(),
            PoolConnections::Sharded { connections, .. } => {
                connections.iter().flatten().cloned().collect()
            }
        })
    }

    fn choose_random_connection_from_slice(v: &[Arc<Connection>]) -> Option<Arc<Connection>> {
        if v.is_empty() {
            None
        } else if v.len() == 1 {
            Some(v[0].clone())
        } else {
            let idx = rand::thread_rng().gen_range(0..v.len());
            Some(v[idx].clone())
        }
    }

    fn with_connections<T>(&self, f: impl FnOnce(&PoolConnections) -> T) -> Result<T, QueryError> {
        let conns = self.conns.load_full();
        match &*conns {
            MaybePoolConnections::Pending => Err(QueryError::IoError(Arc::new(
                std::io::Error::new(ErrorKind::Other, "No connections in the pool"),
            ))),
            MaybePoolConnections::Ready(pool_connections) => Ok(f(pool_connections)),
        }
    }
}

const EXCESS_CONNECTION_BOUND_PER_SHARD_MULTIPLIER: usize = 10;

const MIN_FILL_BACKOFF: Duration = Duration::from_millis(50);
const MAX_FILL_BACKOFF: Duration = Duration::from_secs(10);
const FILL_BACKOFF_MULTIPLIER: u32 = 2;

struct PoolRefiller {
    // Following information identify the pool and do not change
    address: IpAddr,
    regular_port: u16,
    connection_config: ConnectionConfig,
    goal: PoolSize,
    can_open_shard_aware_port_connections: bool,

    // Following fields are updated with information from OPTIONS
    shard_aware_port: Option<u16>,
    sharder: Option<Sharder>,

    // `shared_conns` is updated only after `conns` change
    shared_conns: Arc<ArcSwap<MaybePoolConnections>>,
    conns: Vec<Vec<Arc<Connection>>>,

    // Receives information about breaking connections
    connection_errors:
        FuturesUnordered<Pin<Box<dyn Future<Output = BrokenConnectionEvent> + Send + 'static>>>,

    // When connecting, Scylla always assigns the shard which handles the least
    // number of connections. If there are some non-shard-aware clients
    // connected to the same node, they might cause the shard distribution
    // to be heavily biased and Scylla will be very reluctant to assign some shards.
    //
    // In order to combat this, if the pool is not full and we get a connection
    // for a shard which was already filled, we keep those additional connections
    // in order to affect how Scylla assigns shards. A similar method is used
    // in Scylla's forks of the java and gocql drivers.
    //
    // The number of those connections is bounded by the number of shards multiplied
    // by a constant factor, and are all closed when they exceed this number.
    excess_connections: Vec<Arc<Connection>>,

    current_keyspace: Option<VerifiedKeyspaceName>,
    use_keyspace_request_receiver: Option<mpsc::Receiver<UseKeyspaceRequest>>,
}

#[derive(Debug)]
struct UseKeyspaceRequest {
    keyspace_name: VerifiedKeyspaceName,
    response_sender: tokio::sync::oneshot::Sender<Result<(), QueryError>>,
}

struct FillResult {
    is_full: bool,
    had_error: bool,
}

impl PoolRefiller {
    pub fn new(
        address: IpAddr,
        port: u16,
        connection_config: ConnectionConfig,
        refill_goal: PoolSize,
        current_keyspace: Option<VerifiedKeyspaceName>,
        use_keyspace_request_receiver: mpsc::Receiver<UseKeyspaceRequest>,
    ) -> Self {
        // At the beginning, we assume the node does not have any shards
        // and assume that the node is a Cassandra node
        // TODO: To speed the connection process up, we could take information
        // about shards from another node, and assume that the new node will
        // have the same amount of shards
        let conns = vec![Vec::new()];
        let shared_conns = Arc::new(ArcSwap::new(Arc::new(MaybePoolConnections::Pending)));

        Self {
            address,
            regular_port: port,
            connection_config,
            goal: refill_goal,
            can_open_shard_aware_port_connections: true,

            shard_aware_port: None,
            sharder: None,

            shared_conns,
            conns,

            connection_errors: FuturesUnordered::new(),

            excess_connections: Vec::new(),

            current_keyspace,
            use_keyspace_request_receiver: Some(use_keyspace_request_receiver),
        }
    }

    pub fn get_shared_connections(&self) -> Arc<ArcSwap<MaybePoolConnections>> {
        self.shared_conns.clone()
    }

    pub async fn run(mut self) {
        debug!("[{}] Started asynchronous pool worker", self.address);

        // The pool will be attempted to be filled before the worker is run,
        // so it might be full already. However, calling full() on a full pool
        // does nothing so it does not hurt to do it again. We do it without
        // any delay - this immediate retry happens only once in pool's lifetime.
        let next_refill_time = tokio::time::sleep(Duration::from_secs(0)).fuse();
        tokio::pin!(next_refill_time);

        let mut refill_backoff = MIN_FILL_BACKOFF;

        let use_keyspace_request_receiver = self.use_keyspace_request_receiver.take().unwrap();
        let mut use_keyspace_request_stream =
            ReceiverStream::new(use_keyspace_request_receiver).fuse();

        loop {
            futures::select! {
                _ = &mut next_refill_time => {
                    let result = self.fill().await;

                    if result.is_full {
                        debug!("[{}] Pool is full, clearing {} excess connections", self.address, self.excess_connections.len());
                        refill_backoff = MIN_FILL_BACKOFF;
                        self.excess_connections.clear();
                    } else {
                        if !result.had_error {
                            refill_backoff = MIN_FILL_BACKOFF;
                            debug!("[{}] Encountered no errors during last refill, but the pool is not full yet", self.address);
                        } else {
                            debug!("[{}] Encountered errors during last refill", self.address);
                        }
                        debug!("[{}] Scheduling next refill in {} ms", self.address, refill_backoff.as_millis());
                        next_refill_time.set(tokio::time::sleep(refill_backoff).fuse());
                        if result.had_error {
                            refill_backoff = std::cmp::min(MAX_FILL_BACKOFF, refill_backoff * FILL_BACKOFF_MULTIPLIER);
                        }
                    }
                }

                evt = self.connection_errors.select_next_some() => {
                    debug!("[{}] Got error for connection {:p}: {:?}", self.address, Arc::as_ptr(&evt.connection), evt.error);

                    // Remove the connection from our local pool
                    self.remove_connection(evt.connection);

                    // Update the shared pool immediately
                    self.update_shared_conns();

                    // Reschedule filling, if it wasn't done already
                    if next_refill_time.is_terminated() {
                        debug!("[{}] Scheduling next refill in {} ms", self.address, refill_backoff.as_millis());
                        next_refill_time.set(tokio::time::sleep(refill_backoff).fuse());
                    }
                }

                req = use_keyspace_request_stream.next() => {
                    match req {
                        None => {
                            // The keyspace request channel is dropped.
                            // This means that the corresponding pool is dropped.
                            // We can stop here.
                            trace!("[{}] Keyspace request channel dropped, stopping asynchronous pool worker", self.address);
                            return;
                        }
                        Some(req) => {
                            debug!("[{}] Requested keyspace change request: {}", self.address, req.keyspace_name.as_str());
                            let res = self.use_keyspace(&req.keyspace_name).await;
                            match req.response_sender.send(res) {
                                Ok(()) => debug!("[{}] Successfully changed current keyspace", self.address),
                                Err(err) => warn!("[{}] Failed to change keyspace: {:?}", self.address, err),
                            }
                        }
                    }
                }
            }
        }
    }

    async fn fill(&mut self) -> FillResult {
        // We treat "no shards" as "one shard"
        let shard_count = self.conns.len();

        // Calculate the desired connection count
        let target_count = match self.goal {
            PoolSize::PerHost(target) => target.get(),
            PoolSize::PerShard(target) => target.get() * shard_count,
        };

        // The goal is to distribute connections evenly across all shards.
        // The target connection count might not be divisible by the number
        // of shards, in such case we just round the number up.
        let per_shard_target = (target_count + shard_count - 1) / shard_count;

        // Adjust the target count if it was not even
        let target_count = per_shard_target * shard_count;

        let mut opened_connection_futs = FuturesUnordered::new();

        match (self.sharder.as_ref(), self.shard_aware_port) {
            (Some(sharder), Some(shard_aware_port))
                if self.can_open_shard_aware_port_connections =>
            {
                // Try to fill up each shard up to `per_shard_target` connections
                for (shard_id, shard_conns) in self.conns.iter().enumerate() {
                    trace!(
                        "[{}] Will open {} connections to shard {}",
                        self.address,
                        per_shard_target.saturating_sub(shard_conns.len()),
                        shard_id,
                    );
                    let sinfo = ShardAwareConnectionInfo {
                        shard: shard_id as Shard,
                        sharder: sharder.clone(),
                        port: shard_aware_port,
                    };
                    for _ in shard_conns.len()..per_shard_target {
                        opened_connection_futs.push(open_connection(
                            self.address,
                            self.regular_port,
                            self.connection_config.clone(),
                            self.current_keyspace.clone(),
                            Some(sinfo.clone()),
                        ));
                    }
                }
            }
            _ => {
                // Calculate how many more connections we need to open in order
                // to achieve the target connection count.
                //
                // When connecting to Scylla through non-shard-aware port,
                // Scylla alone will choose shards for us. We hope that
                // they will distribute across shards in the way we want,
                // but we have no guarantee, so we might have to retry
                // connecting later.
                let total_opened_connections: usize = self.conns.iter().map(Vec::len).sum();

                trace!(
                    "[{}] Will open {} non-shard-aware connections",
                    self.address,
                    target_count.saturating_sub(total_opened_connections),
                );

                for _ in total_opened_connections..target_count {
                    opened_connection_futs.push(open_connection(
                        self.address,
                        self.regular_port,
                        self.connection_config.clone(),
                        self.current_keyspace.clone(),
                        None,
                    ));
                }
            }
        }

        // Wait until all connections are ready
        let mut had_error = false;
        while let Some(evt) = opened_connection_futs.next().await {
            match evt.result {
                Err(err) => {
                    // If we failed to connect to a shard-aware port,
                    // fall back to the non-shard-aware port.
                    // Don't set `had_error` here; the shard-aware port
                    // might be unreachable, but the regular port might be
                    // reachable. If we set `had_error` here, it would cause
                    // the backoff to increase on each refill. With the
                    // non-shard aware port, multiple refills are sometimes
                    // necessary, so increasing the backoff would delay
                    // filling the pool even if the non-shard-aware port works
                    // and does not cause any errors.
                    if evt.requested_shard.is_some() {
                        debug!(
                            "[{}] Failed to open connection to the shard-aware port: {:?}, will retry with regular port",
                            self.address,
                            err,
                        );
                        opened_connection_futs.push(open_connection(
                            self.address,
                            self.regular_port,
                            self.connection_config.clone(),
                            self.current_keyspace.clone(),
                            None,
                        ));
                    } else {
                        // Encountered an error while connecting to the non-shard-aware
                        // port. Set the `had_error` flag so that the next refill
                        // will be delayed more than this one.
                        had_error = true;
                        debug!(
                            "[{}] Failed to open connection to the non-shard-aware port: {:?}",
                            self.address, err,
                        );
                    }
                }
                Ok((connection, error_receiver)) => {
                    // Update sharding and optionally reshard
                    let sharder = connection
                        .get_shard_info()
                        .as_ref()
                        .map(|s| s.get_sharder());
                    self.maybe_reshard(sharder);

                    // Update the shard-aware port
                    if self.shard_aware_port != connection.get_shard_aware_port() {
                        debug!(
                            "[{}] Updating shard aware port: {:?}",
                            self.address,
                            connection.get_shard_aware_port(),
                        );
                        self.shard_aware_port = connection.get_shard_aware_port();
                    }

                    let shard_id = connection
                        .get_shard_info()
                        .as_ref()
                        .map_or(0, |s| s.shard as usize);
                    let conn = Arc::new(connection);

                    if self.conns[shard_id].len() < per_shard_target {
                        // Don't complain and just put the connection to the pool.
                        // If this was a shard-aware port connection which missed
                        // the right shard, we still want to accept it
                        // because it fills our pool.
                        trace!(
                            "[{}] Adding connection {:p} to shard {} pool, now is {}/{}",
                            self.address,
                            Arc::as_ptr(&conn),
                            shard_id,
                            self.conns[shard_id].len() + 1,
                            per_shard_target,
                        );

                        self.conns[shard_id].push(conn.clone());
                        self.connection_errors
                            .push(wait_for_error(conn, error_receiver).boxed());
                    } else if evt.requested_shard.is_some() {
                        // This indicates that some shard-aware connections
                        // missed the target shard (probably due to NAT).
                        // Because we don't know how address translation
                        // works here, it's better to leave the task
                        // of choosing the shard to Scylla. We will retry
                        // immediately with a non-shard-aware port here.
                        warn!(
                            "[{}] Excess shard-aware port connection for shard {}; will retry with non-shard-aware port",
                            self.address,
                            shard_id,
                        );

                        opened_connection_futs.push(open_connection(
                            self.address,
                            self.regular_port,
                            self.connection_config.clone(),
                            self.current_keyspace.clone(),
                            None,
                        ));
                    } else {
                        // We got unlucky and Scylla didn't distribute
                        // shards across connections evenly.
                        // We will retry in the next iteration,
                        // for now put it into the excess connection
                        // pool.

                        let excess_connection_limit =
                            EXCESS_CONNECTION_BOUND_PER_SHARD_MULTIPLIER * target_count;
                        if self.excess_connections.len() > excess_connection_limit {
                            // Remove excess connections if they go over the limit
                            debug!(
                                "[{}] Excess connection pool reached limit of {} connections - clearing",
                                self.address,
                                excess_connection_limit,
                            );
                            self.excess_connections.clear();
                        }

                        trace!(
                            "[{}] Storing excess connection {:p} for shard {}",
                            self.address,
                            Arc::as_ptr(&conn),
                            shard_id,
                        );

                        self.excess_connections.push(conn.clone());
                        self.connection_errors
                            .push(wait_for_error(conn, error_receiver).boxed());
                    }
                }
            }
        }

        // We update here instead of doing it immediately after every connect.
        // The reasoning behind that is that lots of requests might be waiting
        // for the pool to be updated, and if we updated it after the first
        // connection, then we could overwhelm a single shard.
        //
        // By postponing the update, we trade off availability for robustness.
        self.update_shared_conns();

        let is_full = self.conns.iter().all(|v| v.len() == per_shard_target);
        FillResult { is_full, had_error }
    }

    fn maybe_reshard(&mut self, new_sharder: Option<Sharder>) {
        if self.sharder == new_sharder {
            return;
        }

        debug!(
            "[{}] New sharder: {:?}, clearing all connections",
            self.address, new_sharder,
        );

        self.sharder = new_sharder.clone();

        // If the sharder has changed, we can throw away all previous connections.
        // All connections to the same live node will have the same sharder,
        // so the old ones will become dead very soon anyway.
        self.conns.clear();

        let shard_count = new_sharder.map_or(1, |s| s.nr_shards.get() as usize);
        self.conns.resize_with(shard_count, Vec::new);

        self.excess_connections.clear();
    }

    fn update_shared_conns(&mut self) {
        let new_conns = if !self.has_connections() {
            Arc::new(MaybePoolConnections::Pending)
        } else {
            let new_conns = if let Some(sharder) = self.sharder.as_ref() {
                debug_assert_eq!(self.conns.len(), sharder.nr_shards.get() as usize);
                PoolConnections::Sharded {
                    sharder: sharder.clone(),
                    connections: self.conns.clone(),
                }
            } else {
                debug_assert_eq!(self.conns.len(), 1);
                PoolConnections::NotSharded(self.conns[0].clone())
            };
            Arc::new(MaybePoolConnections::Ready(new_conns))
        };

        // Make the connection list available
        self.shared_conns.store(new_conns);
    }

    fn remove_connection(&mut self, connection: Arc<Connection>) {
        let ptr = Arc::as_ptr(&connection);

        let maybe_remove_in_vec = |v: &mut Vec<Arc<Connection>>| -> bool {
            let maybe_idx = v
                .iter()
                .enumerate()
                .find(|(_, other_conn)| Arc::ptr_eq(&connection, other_conn))
                .map(|(idx, _)| idx);
            match maybe_idx {
                Some(idx) => {
                    v.swap_remove(idx);
                    true
                }
                None => false,
            }
        };

        // First, look it up in the shard bucket
        // We might have resharded, so the bucket might not exist anymore
        let shard_id = connection
            .get_shard_info()
            .as_ref()
            .map_or(0, |s| s.shard as usize);
        if shard_id < self.conns.len() && maybe_remove_in_vec(&mut self.conns[shard_id]) {
            trace!(
                "[{}] Connection {:p} removed from shard {} pool",
                self.address,
                ptr,
                shard_id
            );
            return;
        }

        // If we didn't find it, it might sit in the excess_connections bucket
        if maybe_remove_in_vec(&mut self.excess_connections) {
            trace!(
                "[{}] Connection {:p} removed from excess connection pool",
                self.address,
                ptr,
            );
            return;
        }

        trace!(
            "[{}] Connection {:p} was already removed",
            self.address,
            ptr,
        );
    }

    async fn use_keyspace(
        &mut self,
        keyspace_name: &VerifiedKeyspaceName,
    ) -> Result<(), QueryError> {
        self.current_keyspace = Some(keyspace_name.clone());

        if !self.has_connections() {
            return Ok(());
        }

        let mut use_keyspace_futures = Vec::new();

        for shard_conns in self.conns.iter_mut() {
            for conn in shard_conns.iter_mut() {
                let fut = conn.use_keyspace(keyspace_name);
                use_keyspace_futures.push(fut);
            }
        }

        let use_keyspace_results: Vec<Result<(), QueryError>> =
            futures::future::join_all(use_keyspace_futures).await;

        // If there was at least one Ok and the rest were IoErrors we can return Ok
        // keyspace name is correct and will be used on broken connection on the next reconnect

        // If there were only IoErrors then return IoError
        // If there was an error different than IoError return this error - something is wrong

        let mut was_ok: bool = false;
        let mut io_error: Option<Arc<std::io::Error>> = None;

        for result in use_keyspace_results {
            match result {
                Ok(()) => was_ok = true,
                Err(err) => match err {
                    QueryError::IoError(io_err) => io_error = Some(io_err),
                    _ => return Err(err),
                },
            }
        }

        if was_ok {
            return Ok(());
        }

        // We can unwrap io_error because use_keyspace_futures must be nonempty
        Err(QueryError::IoError(io_error.unwrap()))
    }

    fn has_connections(&self) -> bool {
        self.conns.iter().any(|v| !v.is_empty())
    }
}

struct BrokenConnectionEvent {
    connection: Arc<Connection>,
    error: QueryError,
}

async fn wait_for_error(
    connection: Arc<Connection>,
    error_receiver: ErrorReceiver,
) -> BrokenConnectionEvent {
    BrokenConnectionEvent {
        connection,
        error: error_receiver.await.unwrap_or_else(|_| {
            QueryError::IoError(Arc::new(std::io::Error::new(
                ErrorKind::Other,
                "Connection broken",
            )))
        }),
    }
}

#[derive(Clone)]
struct ShardAwareConnectionInfo {
    shard: Shard,
    sharder: Sharder,
    port: u16,
}

struct OpenedConnectionEvent {
    result: Result<(Connection, ErrorReceiver), QueryError>,
    requested_shard: Option<Shard>,
}

// The reason we use the same function for both shard-aware port and
// non-shard-aware port connections is that we want to have the same future
// type returned in both cases. This allows us to put them together
// FuturesUnordered structure.
async fn open_connection(
    address: IpAddr,
    regular_port: u16,
    connection_config: ConnectionConfig,
    keyspace_name: Option<VerifiedKeyspaceName>,
    shard_aware_info: Option<ShardAwareConnectionInfo>,
) -> OpenedConnectionEvent {
    let evt = if let Some(info) = shard_aware_info {
        let shard_aware_address = (address, info.port).into();
        let result = open_connection_to_shard_aware_port(
            shard_aware_address,
            info.shard,
            info.sharder,
            &connection_config,
        )
        .await;

        OpenedConnectionEvent {
            result,
            requested_shard: Some(info.shard),
        }
    } else {
        let non_shard_aware_address = (address, regular_port).into();
        let result =
            connection::open_connection(non_shard_aware_address, None, connection_config).await;

        OpenedConnectionEvent {
            result,
            requested_shard: None,
        }
    };

    // Use keyspace
    if let (Ok((connection, _)), Some(keyspace_name)) = (&evt.result, keyspace_name) {
        if let Err(err) = connection.use_keyspace(&keyspace_name).await {
            warn!(
                "[{}] Failed set keyspace for new connection: {:?}",
                address, err
            );
        };
    }

    evt
}

async fn open_connection_to_shard_aware_port(
    address: SocketAddr,
    shard: Shard,
    sharder: Sharder,
    connection_config: &ConnectionConfig,
) -> Result<(Connection, ErrorReceiver), QueryError> {
    // Create iterator over all possible source ports for this shard
    let source_port_iter = sharder.iter_source_ports_for_shard(shard);

    for port in source_port_iter {
        let connect_result =
            connection::open_connection(address, Some(port), connection_config.clone()).await;

        match connect_result {
            Err(err) if err.is_address_unavailable_for_use() => continue, // If we can't use this port, try the next one
            result => return result,
        }
    }

    // Tried all source ports for that shard, give up
    Err(QueryError::IoError(Arc::new(std::io::Error::new(
        std::io::ErrorKind::AddrInUse,
        "Could not find free source port for shard",
    ))))
}

#[cfg(test)]
mod tests {
    use super::open_connection_to_shard_aware_port;
    use crate::routing::{ShardCount, Sharder};
    use crate::transport::connection::ConnectionConfig;
    use std::net::{SocketAddr, ToSocketAddrs};

    // Open many connections to a node
    // Port collision should occur
    // If they are not handled this test will most likely fail
    #[tokio::test]
    async fn many_connections() {
        let connections_number = 512;

        let connect_address: SocketAddr = std::env::var("SCYLLA_URI")
            .unwrap_or_else(|_| "127.0.0.1:9042".to_string())
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();

        let connection_config = ConnectionConfig {
            compression: None,
            tcp_nodelay: true,
            #[cfg(feature = "ssl")]
            ssl_context: None,
            ..Default::default()
        };

        // This does not have to be the real sharder,
        // the test is only about port collisions, not connecting
        // to the right shard
        let sharder = Sharder::new(ShardCount::new(3).unwrap(), 12);

        // Open the connections
        let mut conns = Vec::new();

        for _ in 0..connections_number {
            conns.push(open_connection_to_shard_aware_port(
                connect_address,
                0,
                sharder.clone(),
                &connection_config,
            ));
        }

        let joined = futures::future::join_all(conns).await;

        // Check that each connection managed to connect successfully
        for res in joined {
            res.unwrap();
        }
    }
}
