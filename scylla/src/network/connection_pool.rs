use super::connection::{
    open_connection, open_connection_to_shard_aware_port, Connection, ConnectionConfig,
    ErrorReceiver, HostConnectionConfig, VerifiedKeyspaceName,
};

use crate::errors::{
    BrokenConnectionErrorKind, ConnectionError, ConnectionPoolError, UseKeyspaceError,
};
use crate::routing::{Shard, ShardCount, Sharder};

use crate::cluster::metadata::{PeerEndpoint, UntranslatedEndpoint};

#[cfg(feature = "metrics")]
use crate::observability::metrics::Metrics;

use crate::cluster::NodeAddr;
use crate::utils::safe_format::IteratorSafeFormatExt;

use arc_swap::ArcSwap;
use futures::{future::RemoteHandle, stream::FuturesUnordered, Future, FutureExt, StreamExt};
use rand::Rng;
use std::convert::TryInto;
use std::num::NonZeroUsize;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::pin::Pin;
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

use tokio::sync::{broadcast, mpsc, Notify};
use tracing::{debug, error, trace, warn};

/// The target size of a per-node connection pool.
#[derive(Debug, Clone, Copy)]
pub enum PoolSize {
    /// Indicates that the pool should establish given number of connections to the node.
    ///
    /// If this option is used with a ScyllaDB cluster, it is not guaranteed that connections will be
    /// distributed evenly across shards. Use this option if you cannot use the shard-aware port
    /// and you suffer from the "connection storm" problems.
    PerHost(NonZeroUsize),

    /// Indicates that the pool should establish given number of connections to each shard on the node.
    ///
    /// Cassandra nodes will be treated as if they have only one shard.
    ///
    /// The recommended setting for ScyllaDB is one connection per shard - `PerShard(1)`.
    PerShard(NonZeroUsize),
}

impl Default for PoolSize {
    fn default() -> Self {
        PoolSize::PerShard(NonZeroUsize::new(1).unwrap())
    }
}

#[derive(Clone)]
pub(crate) struct PoolConfig {
    pub(crate) connection_config: ConnectionConfig,
    pub(crate) pool_size: PoolSize,
    pub(crate) can_use_shard_aware_port: bool,
}

#[cfg(test)]
impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            connection_config: Default::default(),
            pool_size: Default::default(),
            can_use_shard_aware_port: true,
        }
    }
}

impl PoolConfig {
    fn to_host_pool_config(&self, endpoint: &UntranslatedEndpoint) -> HostPoolConfig {
        HostPoolConfig {
            connection_config: self.connection_config.to_host_connection_config(endpoint),
            pool_size: self.pool_size,
            can_use_shard_aware_port: self.can_use_shard_aware_port,
        }
    }
}

#[derive(Clone)]
struct HostPoolConfig {
    pub(crate) connection_config: HostConnectionConfig,
    pub(crate) pool_size: PoolSize,
    pub(crate) can_use_shard_aware_port: bool,
}

#[cfg(test)]
impl Default for HostPoolConfig {
    fn default() -> Self {
        Self {
            connection_config: Default::default(),
            pool_size: Default::default(),
            can_use_shard_aware_port: true,
        }
    }
}

enum MaybePoolConnections {
    // The pool is being filled for the first time
    Initializing,

    // The pool is empty because either initial filling failed or all connections
    // became broken; will be asynchronously refilled. Contains an error
    // from the last connection attempt.
    Broken(ConnectionError),

    // The pool has some connections which are usable (or will be removed soon)
    Ready(PoolConnections),
}

impl std::fmt::Debug for MaybePoolConnections {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MaybePoolConnections::Initializing => write!(f, "Initializing"),
            MaybePoolConnections::Broken(err) => write!(f, "Broken({err:?})"),
            MaybePoolConnections::Ready(conns) => write!(f, "{conns:?}"),
        }
    }
}

#[derive(Clone)]
enum PoolConnections {
    NotSharded(Vec<Arc<Connection>>),
    Sharded {
        sharder: Sharder,
        connections: Vec<Vec<Arc<Connection>>>,
    },
}

struct ConnectionVectorWrapper<'a>(&'a Vec<Arc<Connection>>);
impl std::fmt::Debug for ConnectionVectorWrapper<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|conn| conn.get_connect_address()))
            .finish()
    }
}

struct ShardedConnectionVectorWrapper<'a>(&'a Vec<Vec<Arc<Connection>>>);
impl std::fmt::Debug for ShardedConnectionVectorWrapper<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(
                self.0
                    .iter()
                    .enumerate()
                    .map(|(shard_no, conn_vec)| (shard_no, ConnectionVectorWrapper(conn_vec))),
            )
            .finish()
    }
}

impl std::fmt::Debug for PoolConnections {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PoolConnections::NotSharded(conns) => {
                write!(f, "non-sharded: {:?}", ConnectionVectorWrapper(conns))
            }
            PoolConnections::Sharded {
                sharder,
                connections,
            } => write!(
                f,
                "sharded(nr_shards:{}, msb_ignore_bits:{}): {:?}",
                sharder.nr_shards,
                sharder.msb_ignore,
                ShardedConnectionVectorWrapper(connections)
            ),
        }
    }
}

#[derive(Clone)]
pub(crate) struct NodeConnectionPool {
    conns: Arc<ArcSwap<MaybePoolConnections>>,
    use_keyspace_request_sender: mpsc::Sender<UseKeyspaceRequest>,
    _refiller_handle: Arc<RemoteHandle<()>>,
    pool_updated_notify: Arc<Notify>,
    endpoint: Arc<RwLock<UntranslatedEndpoint>>,
}

impl std::fmt::Debug for NodeConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeConnectionPool")
            .field("conns", &self.conns)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

// These implementations are a temporary solution to the following problem:
// `QueryResult` used to implement `(Ref)UnwindSafe`, but then we wanted it to store a reference to `Node`.
// This, transitively, made it store `NodeConnectionPool`, which did not implement those traits.
// Thus, they would no longer be auto-implemented for `QueryResult`, breaking the public API.
// Not to introduce an API breakage in a minor release, we decided to manually hint that `NodeConnectionPool`
// is indeed unwind-safe. Even if our we are wrong and the hint is misleading, the documentation of those
// traits, not being `unsafe` traits, considers them merely guidelines, not strong guarantees.
impl UnwindSafe for NodeConnectionPool {}
impl RefUnwindSafe for NodeConnectionPool {}

impl NodeConnectionPool {
    pub(crate) fn new(
        endpoint: UntranslatedEndpoint,
        pool_config: &PoolConfig,
        current_keyspace: Option<VerifiedKeyspaceName>,
        pool_empty_notifier: broadcast::Sender<()>,
        #[cfg(feature = "metrics")] metrics: Arc<Metrics>,
    ) -> Self {
        let (use_keyspace_request_sender, use_keyspace_request_receiver) = mpsc::channel(1);
        let pool_updated_notify = Arc::new(Notify::new());

        let host_pool_config = pool_config.to_host_pool_config(&endpoint);

        let arced_endpoint = Arc::new(RwLock::new(endpoint));

        let refiller = PoolRefiller::new(
            arced_endpoint.clone(),
            host_pool_config,
            current_keyspace,
            pool_updated_notify.clone(),
            pool_empty_notifier,
            #[cfg(feature = "metrics")]
            metrics,
        );

        let conns = refiller.get_shared_connections();
        let (fut, refiller_handle) = refiller.run(use_keyspace_request_receiver).remote_handle();
        tokio::spawn(fut);

        Self {
            conns,
            use_keyspace_request_sender,
            _refiller_handle: Arc::new(refiller_handle),
            pool_updated_notify,
            endpoint: arced_endpoint,
        }
    }

    pub(crate) fn is_connected(&self) -> bool {
        let maybe_conns = self.conns.load();
        match maybe_conns.as_ref() {
            MaybePoolConnections::Initializing => false,
            MaybePoolConnections::Broken(_) => false,
            // Here we use the assumption that _pool_connections is always non-empty.
            MaybePoolConnections::Ready(_pool_connections) => true,
        }
    }

    pub(crate) fn update_endpoint(&self, new_endpoint: PeerEndpoint) {
        *self.endpoint.write().unwrap() = UntranslatedEndpoint::Peer(new_endpoint);
    }

    pub(crate) fn sharder(&self) -> Option<Sharder> {
        self.with_connections(|pool_conns| match pool_conns {
            PoolConnections::NotSharded(_) => None,
            PoolConnections::Sharded { sharder, .. } => Some(sharder.clone()),
        })
        .unwrap_or(None)
    }

    pub(crate) fn connection_for_shard(
        &self,
        shard: Shard,
    ) -> Result<Arc<Connection>, ConnectionPoolError> {
        trace!(shard = shard, "Selecting connection for shard");
        self.with_connections(|pool_conns| match pool_conns {
            PoolConnections::NotSharded(conns) => {
                Self::choose_random_connection_from_slice(conns).unwrap()
            }
            PoolConnections::Sharded {
                connections,
                sharder
            } => {
                let shard = shard
                    .try_into()
                    // It's safer to use 0 rather that panic here, as shards are returned by `LoadBalancingPolicy`
                    // now, which can be implemented by a user in an arbitrary way.
                    .unwrap_or_else(|_| {
                        error!("The provided shard number: {} does not fit u16! Using 0 as the shard number. Check your LoadBalancingPolicy implementation.", shard);
                        0
                    });
                Self::connection_for_shard_helper(shard, sharder.nr_shards, connections.as_slice())
            }
        })
    }

    pub(crate) fn random_connection(&self) -> Result<Arc<Connection>, ConnectionPoolError> {
        trace!("Selecting random connection");
        self.with_connections(|pool_conns| match pool_conns {
            PoolConnections::NotSharded(conns) => {
                Self::choose_random_connection_from_slice(conns).unwrap()
            }
            PoolConnections::Sharded {
                sharder,
                connections,
            } => {
                let shard: u16 = rand::rng().random_range(0..sharder.nr_shards.get());
                Self::connection_for_shard_helper(shard, sharder.nr_shards, connections.as_slice())
            }
        })
    }

    // Tries to get a connection to given shard, if it's broken returns any working connection
    fn connection_for_shard_helper(
        shard: u16,
        nr_shards: ShardCount,
        shard_conns: &[Vec<Arc<Connection>>],
    ) -> Arc<Connection> {
        // Try getting the desired connection
        if let Some(conn) = shard_conns
            .get(shard as usize)
            .or_else(|| {
                warn!(
                    shard = shard,
                    "Requested shard is out of bounds.\
                    This is most probably a bug in custom LoadBalancingPolicy implementation!\
                    Targeting a random/arbitrary shard."
                );
                None
            })
            .and_then(|shard_conns| Self::choose_random_connection_from_slice(shard_conns))
        {
            trace!(shard = shard, "Found connection for the target shard");
            return conn;
        }

        // If this fails try getting any other in random order
        let mut shards_to_try: Vec<u16> = (0..shard).chain(shard + 1..nr_shards.get()).collect();

        let orig_shard = shard;
        while !shards_to_try.is_empty() {
            let idx = rand::rng().random_range(0..shards_to_try.len());
            let shard = shards_to_try.swap_remove(idx);

            if let Some(conn) =
                Self::choose_random_connection_from_slice(&shard_conns[shard as usize])
            {
                trace!(
                    orig_shard = orig_shard,
                    shard = shard,
                    "Choosing connection for a different shard"
                );
                return conn;
            }
        }

        unreachable!("could not find any connection in supposedly non-empty pool")
    }

    pub(crate) async fn use_keyspace(
        &self,
        keyspace_name: VerifiedKeyspaceName,
    ) -> Result<(), UseKeyspaceError> {
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

        self.use_keyspace_request_sender
            .send(UseKeyspaceRequest {
                keyspace_name,
                response_sender,
            })
            .await
            .expect("Bug in NodeConnectionPool::use_keyspace sending");
        // Other end of this channel is in the PoolRefiller, can't be dropped while we have &self to _refiller_handle

        response_receiver.await.unwrap() // PoolRefiller always responds
    }

    // Waits until the pool becomes initialized.
    // The pool is considered initialized either if the first connection has been
    // established or after first filling ends, whichever comes first.
    pub(crate) async fn wait_until_initialized(&self) {
        // First, register for the notification
        // so that we don't miss it
        let notified = self.pool_updated_notify.notified();

        if let MaybePoolConnections::Initializing = **self.conns.load() {
            // If the pool is not initialized yet, wait until we get a notification
            notified.await;
        }
    }

    pub(crate) fn get_working_connections(
        &self,
    ) -> Result<Vec<Arc<Connection>>, ConnectionPoolError> {
        self.with_connections(|pool_conns| match pool_conns {
            PoolConnections::NotSharded(conns) => conns.clone(),
            PoolConnections::Sharded { connections, .. } => {
                connections.iter().flatten().cloned().collect()
            }
        })
    }

    fn choose_random_connection_from_slice(v: &[Arc<Connection>]) -> Option<Arc<Connection>> {
        trace!(
            connections = tracing::field::display(
                v.iter()
                    .map(|conn| conn.get_connect_address())
                    .safe_format(", ")
            ),
            "Available"
        );
        if v.is_empty() {
            None
        } else if v.len() == 1 {
            Some(v[0].clone())
        } else {
            let idx = rand::rng().random_range(0..v.len());
            Some(v[idx].clone())
        }
    }

    fn with_connections<T>(
        &self,
        f: impl FnOnce(&PoolConnections) -> T,
    ) -> Result<T, ConnectionPoolError> {
        let conns = self.conns.load_full();
        match &*conns {
            MaybePoolConnections::Ready(pool_connections) => Ok(f(pool_connections)),
            MaybePoolConnections::Broken(err) => Err(ConnectionPoolError::Broken {
                last_connection_error: err.clone(),
            }),
            MaybePoolConnections::Initializing => Err(ConnectionPoolError::Initializing),
        }
    }
}

const EXCESS_CONNECTION_BOUND_PER_SHARD_MULTIPLIER: usize = 10;

// TODO: Make it configurable through a policy (issue #184)
const MIN_FILL_BACKOFF: Duration = Duration::from_millis(50);
const MAX_FILL_BACKOFF: Duration = Duration::from_secs(10);
const FILL_BACKOFF_MULTIPLIER: u32 = 2;

// A simple exponential strategy for pool fill backoffs.
struct RefillDelayStrategy {
    current_delay: Duration,
}

impl RefillDelayStrategy {
    fn new() -> Self {
        Self {
            current_delay: MIN_FILL_BACKOFF,
        }
    }

    fn get_delay(&self) -> Duration {
        self.current_delay
    }

    fn on_successful_fill(&mut self) {
        self.current_delay = MIN_FILL_BACKOFF;
    }

    fn on_fill_error(&mut self) {
        self.current_delay = std::cmp::min(
            MAX_FILL_BACKOFF,
            self.current_delay * FILL_BACKOFF_MULTIPLIER,
        );
    }
}

struct PoolRefiller {
    // Following information identify the pool and do not change
    pool_config: HostPoolConfig,

    // Following information is subject to updates on topology refresh
    endpoint: Arc<RwLock<UntranslatedEndpoint>>,

    // Following fields are updated with information from OPTIONS
    shard_aware_port: Option<u16>,
    sharder: Option<Sharder>,

    // `shared_conns` is updated only after `conns` change
    shared_conns: Arc<ArcSwap<MaybePoolConnections>>,
    conns: Vec<Vec<Arc<Connection>>>,

    // Set to true if there was an error since the last refill,
    // set to false when refilling starts.
    had_error_since_last_refill: bool,

    refill_delay_strategy: RefillDelayStrategy,

    // Receives information about connections becoming ready, i.e. newly connected
    // or after its keyspace was correctly set.
    // TODO: This should probably be a channel
    ready_connections:
        FuturesUnordered<Pin<Box<dyn Future<Output = OpenedConnectionEvent> + Send + 'static>>>,

    // Receives information about breaking connections
    connection_errors:
        FuturesUnordered<Pin<Box<dyn Future<Output = BrokenConnectionEvent> + Send + 'static>>>,

    // When connecting, ScyllaDB always assigns the shard which handles the least
    // number of connections. If there are some non-shard-aware clients
    // connected to the same node, they might cause the shard distribution
    // to be heavily biased and ScyllaDB will be very reluctant to assign some shards.
    //
    // In order to combat this, if the pool is not full and we get a connection
    // for a shard which was already filled, we keep those additional connections
    // in order to affect how ScyllaDB assigns shards. A similar method is used
    // in ScyllaDB's forks of the java and gocql drivers.
    //
    // The number of those connections is bounded by the number of shards multiplied
    // by a constant factor, and are all closed when they exceed this number.
    excess_connections: Vec<Arc<Connection>>,

    current_keyspace: Option<VerifiedKeyspaceName>,

    // Signaled when the connection pool is updated
    pool_updated_notify: Arc<Notify>,

    // Signaled when the connection pool becomes empty
    pool_empty_notifier: broadcast::Sender<()>,

    #[cfg(feature = "metrics")]
    metrics: Arc<Metrics>,
}

#[derive(Debug)]
struct UseKeyspaceRequest {
    keyspace_name: VerifiedKeyspaceName,
    response_sender: tokio::sync::oneshot::Sender<Result<(), UseKeyspaceError>>,
}

impl PoolRefiller {
    pub(crate) fn new(
        endpoint: Arc<RwLock<UntranslatedEndpoint>>,
        pool_config: HostPoolConfig,
        current_keyspace: Option<VerifiedKeyspaceName>,
        pool_updated_notify: Arc<Notify>,
        pool_empty_notifier: broadcast::Sender<()>,
        #[cfg(feature = "metrics")] metrics: Arc<Metrics>,
    ) -> Self {
        // At the beginning, we assume the node does not have any shards
        // and assume that the node is a Cassandra node
        let conns = vec![Vec::new()];
        let shared_conns = Arc::new(ArcSwap::new(Arc::new(MaybePoolConnections::Initializing)));

        Self {
            endpoint,
            pool_config,

            shard_aware_port: None,
            sharder: None,

            shared_conns,
            conns,

            had_error_since_last_refill: false,
            refill_delay_strategy: RefillDelayStrategy::new(),

            ready_connections: FuturesUnordered::new(),
            connection_errors: FuturesUnordered::new(),

            excess_connections: Vec::new(),

            current_keyspace,

            pool_updated_notify,
            pool_empty_notifier,

            #[cfg(feature = "metrics")]
            metrics,
        }
    }

    fn endpoint_description(&self) -> NodeAddr {
        self.endpoint.read().unwrap().address()
    }

    pub(crate) fn get_shared_connections(&self) -> Arc<ArcSwap<MaybePoolConnections>> {
        self.shared_conns.clone()
    }

    // The main loop of the pool refiller
    pub(crate) async fn run(
        mut self,
        mut use_keyspace_request_receiver: mpsc::Receiver<UseKeyspaceRequest>,
    ) {
        debug!(
            "[{}] Started asynchronous pool worker",
            self.endpoint_description()
        );

        let mut next_refill_time = tokio::time::Instant::now();
        let mut refill_scheduled = true;

        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(next_refill_time), if refill_scheduled => {
                    self.had_error_since_last_refill = false;
                    self.start_filling();
                    refill_scheduled = false;
                }

                evt = self.ready_connections.select_next_some(), if !self.ready_connections.is_empty() => {
                    self.handle_ready_connection(evt);

                    if self.is_full() {
                        debug!(
                            "[{}] Pool is full, clearing {} excess connections",
                            self.endpoint_description(),
                            self.excess_connections.len()
                        );
                        self.excess_connections.clear();
                    }
                }

                evt = self.connection_errors.select_next_some(), if !self.connection_errors.is_empty() => {
                    if let Some(conn) = evt.connection.upgrade() {
                        debug!("[{}] Got error for connection {:p}: {:?}", self.endpoint_description(), Arc::as_ptr(&conn), evt.error);
                        self.remove_connection(conn, evt.error);
                    }
                }

                req = use_keyspace_request_receiver.recv() => {
                    if let Some(req) = req {
                        debug!("[{}] Requested keyspace change: {}", self.endpoint_description(), req.keyspace_name.as_str());
                        self.use_keyspace(req.keyspace_name, req.response_sender);
                    } else {
                        // The keyspace request channel is dropped.
                        // This means that the corresponding pool is dropped.
                        // We can stop here.
                        trace!("[{}] Keyspace request channel dropped, stopping asynchronous pool worker", self.endpoint_description());
                        return;
                    }
                }
            }
            trace!(
                pool_state = ?ShardedConnectionVectorWrapper(&self.conns)
            );

            // Schedule refilling here
            if !refill_scheduled && self.need_filling() {
                if self.had_error_since_last_refill {
                    self.refill_delay_strategy.on_fill_error();
                } else {
                    self.refill_delay_strategy.on_successful_fill();
                }
                let delay = self.refill_delay_strategy.get_delay();
                debug!(
                    "[{}] Scheduling next refill in {} ms",
                    self.endpoint_description(),
                    delay.as_millis(),
                );

                next_refill_time = tokio::time::Instant::now() + delay;
                refill_scheduled = true;
            }
        }
    }

    fn is_filling(&self) -> bool {
        !self.ready_connections.is_empty()
    }

    fn is_full(&self) -> bool {
        match self.pool_config.pool_size {
            PoolSize::PerHost(target) => self.active_connection_count() >= target.get(),
            PoolSize::PerShard(target) => {
                self.conns.iter().all(|conns| conns.len() >= target.get())
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.conns.iter().all(|conns| conns.is_empty())
    }

    fn need_filling(&self) -> bool {
        !self.is_filling() && !self.is_full()
    }

    fn can_use_shard_aware_port(&self) -> bool {
        self.sharder.is_some()
            && self.shard_aware_port.is_some()
            && self.pool_config.can_use_shard_aware_port
    }

    // Begins opening a number of connections in order to fill the connection pool.
    // Futures which open the connections are pushed to the `ready_connections`
    // FuturesUnordered structure, and their results are processed in the main loop.
    fn start_filling(&mut self) {
        if self.is_empty() {
            // If the pool is empty, it might mean that the node is not alive.
            // It is more likely than not that the next connection attempt will
            // fail, so there is no use in opening more than one connection now.
            trace!(
                "[{}] Will open the first connection to the node",
                self.endpoint_description()
            );
            self.start_opening_connection(None);
            return;
        }

        if self.can_use_shard_aware_port() {
            // Only use the shard-aware port if we have a PerShard strategy
            if let PoolSize::PerShard(target) = self.pool_config.pool_size {
                // Try to fill up each shard up to `target` connections
                for (shard_id, shard_conns) in self.conns.iter().enumerate() {
                    let to_open_count = target.get().saturating_sub(shard_conns.len());
                    if to_open_count == 0 {
                        continue;
                    }
                    trace!(
                        "[{}] Will open {} connections to shard {}",
                        self.endpoint_description(),
                        to_open_count,
                        shard_id,
                    );
                    for _ in 0..to_open_count {
                        self.start_opening_connection(Some(shard_id as Shard));
                    }
                }
                return;
            }
        }
        // Calculate how many more connections we need to open in order
        // to achieve the target connection count.
        let to_open_count = match self.pool_config.pool_size {
            PoolSize::PerHost(target) => {
                target.get().saturating_sub(self.active_connection_count())
            }
            PoolSize::PerShard(target) => self
                .conns
                .iter()
                .map(|conns| target.get().saturating_sub(conns.len()))
                .sum::<usize>(),
        };
        // When connecting to ScyllaDB through non-shard-aware port,
        // ScyllaDB alone will choose shards for us. We hope that
        // they will distribute across shards in the way we want,
        // but we have no guarantee, so we might have to retry
        // connecting later.
        trace!(
            "[{}] Will open {} non-shard-aware connections",
            self.endpoint_description(),
            to_open_count,
        );
        for _ in 0..to_open_count {
            self.start_opening_connection(None);
        }
    }

    // Handles a newly opened connection and decides what to do with it.
    fn handle_ready_connection(&mut self, evt: OpenedConnectionEvent) {
        match evt.result {
            Err(err) => {
                if evt.requested_shard.is_some() {
                    // If we failed to connect to a shard-aware port,
                    // fall back to the non-shard-aware port.
                    // Don't set `had_error_since_last_refill` here;
                    // the shard-aware port might be unreachable, but
                    // the regular port might be reachable. If we set
                    // `had_error_since_last_refill` here, it would cause
                    // the backoff to increase on each refill. With
                    // the non-shard aware port, multiple refills are sometimes
                    // necessary, so increasing the backoff would delay
                    // filling the pool even if the non-shard-aware port works
                    // and does not cause any errors.
                    debug!(
                        "[{}] Failed to open connection to the shard-aware port: {:?}, will retry with regular port",
                        self.endpoint_description(),
                        err,
                    );
                    self.start_opening_connection(None);
                } else {
                    // Encountered an error while connecting to the non-shard-aware
                    // port. Set the `had_error_since_last_refill` flag so that
                    // the next refill will be delayed more than this one.
                    self.had_error_since_last_refill = true;
                    debug!(
                        "[{}] Failed to open connection to the non-shard-aware port: {:?}",
                        self.endpoint_description(),
                        err,
                    );

                    // If all connection attempts in this fill attempt failed
                    // and the pool is empty, report this error.
                    if !self.is_filling() && self.is_empty() {
                        self.update_shared_conns(Some(err));
                    }
                }
            }
            Ok((connection, error_receiver)) => {
                // Update sharding and optionally reshard
                let shard_info = connection.get_shard_info().as_ref();
                let sharder = shard_info.map(|s| s.get_sharder());
                let shard_id = shard_info.map_or(0, |s| s.shard as usize);
                self.maybe_reshard(sharder);

                // Update the shard-aware port
                if self.shard_aware_port != connection.get_shard_aware_port() {
                    debug!(
                        "[{}] Updating shard aware port: {:?}",
                        self.endpoint_description(),
                        connection.get_shard_aware_port(),
                    );
                    self.shard_aware_port = connection.get_shard_aware_port();
                }

                // Before the connection can be put to the pool, we need
                // to make sure that it uses appropriate keyspace
                if let Some(keyspace) = &self.current_keyspace {
                    if evt.keyspace_name.as_ref() != Some(keyspace) {
                        // Asynchronously start setting keyspace for this
                        // connection. It will be received on the ready
                        // connections channel and will travel through
                        // this logic again, to be finally put into
                        // the conns.
                        self.start_setting_keyspace_for_connection(
                            connection,
                            error_receiver,
                            evt.requested_shard,
                        );
                        return;
                    }
                }

                // Decide if the connection can be accepted, according to
                // the pool filling strategy
                let can_be_accepted = match self.pool_config.pool_size {
                    PoolSize::PerHost(target) => self.active_connection_count() < target.get(),
                    PoolSize::PerShard(target) => self.conns[shard_id].len() < target.get(),
                };

                if can_be_accepted {
                    // Don't complain and just put the connection to the pool.
                    // If this was a shard-aware port connection which missed
                    // the right shard, we still want to accept it
                    // because it fills our pool.
                    let conn = Arc::new(connection);
                    trace!(
                        "[{}] Adding connection {:p} to shard {} pool, now there are {} for the shard, total {}",
                        self.endpoint_description(),
                        Arc::as_ptr(&conn),
                        shard_id,
                        self.conns[shard_id].len() + 1,
                        self.active_connection_count() + 1,
                    );

                    self.connection_errors
                        .push(wait_for_error(Arc::downgrade(&conn), error_receiver).boxed());
                    self.conns[shard_id].push(conn);

                    self.update_shared_conns(None);
                } else if evt.requested_shard.is_some() {
                    // This indicates that some shard-aware connections
                    // missed the target shard (probably due to NAT).
                    // Because we don't know how address translation
                    // works here, it's better to leave the task
                    // of choosing the shard to Scylla. We will retry
                    // immediately with a non-shard-aware port here.
                    debug!(
                        "[{}] Excess shard-aware port connection for shard {}; will retry with non-shard-aware port",
                        self.endpoint_description(),
                        shard_id,
                    );

                    self.start_opening_connection(None);
                } else {
                    // We got unlucky and ScyllaDB didn't distribute
                    // shards across connections evenly.
                    // We will retry in the next iteration,
                    // for now put it into the excess connection
                    // pool.
                    let conn = Arc::new(connection);
                    trace!(
                        "[{}] Storing excess connection {:p} for shard {}",
                        self.endpoint_description(),
                        Arc::as_ptr(&conn),
                        shard_id,
                    );

                    self.connection_errors
                        .push(wait_for_error(Arc::downgrade(&conn), error_receiver).boxed());
                    self.excess_connections.push(conn);

                    let excess_connection_limit = self.excess_connection_limit();
                    if self.excess_connections.len() > excess_connection_limit {
                        debug!(
                            "[{}] Excess connection pool exceeded limit of {} connections - clearing",
                            self.endpoint_description(),
                            excess_connection_limit,
                        );
                        self.excess_connections.clear();
                    }
                }
            }
        }
    }

    // Starts opening a new connection in the background. The result of connecting
    // will be available on `ready_connections`. If the shard is specified and
    // the shard aware port is available, it will attempt to connect directly
    // to the shard using the port.
    fn start_opening_connection(&self, shard: Option<Shard>) {
        let cfg = self.pool_config.connection_config.clone();
        let mut endpoint = self.endpoint.read().unwrap().clone();

        #[cfg(feature = "metrics")]
        let count_in_metrics = {
            let metrics = Arc::clone(&self.metrics);
            move |connect_result: &Result<_, ConnectionError>| {
                if connect_result.is_ok() {
                    metrics.inc_total_connections();
                } else if let Err(ConnectionError::ConnectTimeout) = &connect_result {
                    metrics.inc_connection_timeouts();
                }
            }
        };

        let fut = match (self.sharder.clone(), self.shard_aware_port, shard) {
            (Some(sharder), Some(port), Some(shard)) => async move {
                let shard_aware_endpoint = {
                    endpoint.set_port(port);
                    endpoint
                };
                let result = open_connection_to_shard_aware_port(
                    &shard_aware_endpoint,
                    shard,
                    sharder.clone(),
                    &cfg,
                )
                .await;

                #[cfg(feature = "metrics")]
                count_in_metrics(&result);

                OpenedConnectionEvent {
                    result,
                    requested_shard: Some(shard),
                    keyspace_name: None,
                }
            }
            .boxed(),
            _ => async move {
                let non_shard_aware_endpoint = endpoint;
                let result = open_connection(&non_shard_aware_endpoint, None, &cfg).await;

                #[cfg(feature = "metrics")]
                count_in_metrics(&result);

                OpenedConnectionEvent {
                    result,
                    requested_shard: None,
                    keyspace_name: None,
                }
            }
            .boxed(),
        };
        self.ready_connections.push(fut);
    }

    fn maybe_reshard(&mut self, new_sharder: Option<Sharder>) {
        if self.sharder == new_sharder {
            return;
        }

        debug!(
            "[{}] New sharder: {:?}, clearing all connections",
            self.endpoint_description(),
            new_sharder,
        );

        self.sharder.clone_from(&new_sharder);

        // If the sharder has changed, we can throw away all previous connections.
        // All connections to the same live node will have the same sharder,
        // so the old ones will become dead very soon anyway.
        self.conns.clear();

        let shard_count = new_sharder.map_or(1, |s| s.nr_shards.get() as usize);
        self.conns.resize_with(shard_count, Vec::new);

        self.excess_connections.clear();
    }

    // Updates `shared_conns` based on `conns`.
    // `last_error` must not be `None` if there is a possibility of the pool
    // being empty.
    fn update_shared_conns(&mut self, last_error: Option<ConnectionError>) {
        let new_conns = if self.is_empty() {
            Arc::new(MaybePoolConnections::Broken(last_error.unwrap()))
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

        // Notify potential waiters
        self.pool_updated_notify.notify_waiters();
    }

    // Removes given connection from the pool. It looks both into active
    // connections and excess connections.
    fn remove_connection(&mut self, connection: Arc<Connection>, last_error: ConnectionError) {
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
                    #[cfg(feature = "metrics")]
                    self.metrics.dec_total_connections();
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
                "[{}] Connection {:p} removed from shard {} pool, now there is {} for the shard, total {}",
                self.endpoint_description(),
                ptr,
                shard_id,
                self.conns[shard_id].len(),
                self.active_connection_count(),
            );
            if self.is_empty() {
                let _ = self.pool_empty_notifier.send(());
            }
            self.update_shared_conns(Some(last_error));
            return;
        }

        // If we didn't find it, it might sit in the excess_connections bucket
        if maybe_remove_in_vec(&mut self.excess_connections) {
            trace!(
                "[{}] Connection {:p} removed from excess connection pool",
                self.endpoint_description(),
                ptr,
            );
            return;
        }

        trace!(
            "[{}] Connection {:p} was already removed",
            self.endpoint_description(),
            ptr,
        );
    }

    // Sets current keyspace for available connections.
    // Connections which are being currently opened and future connections
    // will have this keyspace set when they appear on `ready_connections`.
    // Sends response to the `response_sender` when all current connections
    // have their keyspace set.
    fn use_keyspace(
        &mut self,
        keyspace_name: VerifiedKeyspaceName,
        response_sender: tokio::sync::oneshot::Sender<Result<(), UseKeyspaceError>>,
    ) {
        self.current_keyspace = Some(keyspace_name.clone());

        let mut conns = self.conns.clone();
        let address = self.endpoint.read().unwrap().address();
        let connect_timeout = self.pool_config.connection_config.connect_timeout;

        let fut = async move {
            let mut use_keyspace_futures = Vec::new();

            for shard_conns in conns.iter_mut() {
                for conn in shard_conns.iter_mut() {
                    let fut = conn.use_keyspace(&keyspace_name);
                    use_keyspace_futures.push(fut);
                }
            }

            if use_keyspace_futures.is_empty() {
                return Ok(());
            }

            let use_keyspace_results: Vec<Result<(), UseKeyspaceError>> = tokio::time::timeout(
                connect_timeout,
                futures::future::join_all(use_keyspace_futures),
            )
            .await
            // FIXME: We could probably make USE KEYSPACE request timeout configurable in the future.
            .map_err(|_| UseKeyspaceError::RequestTimeout(connect_timeout))?;

            crate::cluster::use_keyspace_result(use_keyspace_results.into_iter())
        };

        tokio::task::spawn(async move {
            let res = fut.await;
            match &res {
                Ok(()) => debug!("[{}] Successfully changed current keyspace", address),
                Err(err) => warn!("[{}] Failed to change keyspace: {:?}", address, err),
            }
            let _ = response_sender.send(res);
        });
    }

    // Requires the keyspace to be set
    // Requires that the event is for a successful connection
    fn start_setting_keyspace_for_connection(
        &mut self,
        connection: Connection,
        error_receiver: ErrorReceiver,
        requested_shard: Option<Shard>,
    ) {
        // TODO: There should be a timeout for this

        let keyspace_name = self.current_keyspace.as_ref().cloned().unwrap();
        self.ready_connections.push(
            async move {
                let result = connection.use_keyspace(&keyspace_name).await;
                if let Err(err) = result {
                    warn!(
                        "[{}] Failed to set keyspace for new connection: {}",
                        connection.get_connect_address().ip(),
                        err,
                    );
                }
                OpenedConnectionEvent {
                    result: Ok((connection, error_receiver)),
                    requested_shard,
                    keyspace_name: Some(keyspace_name),
                }
            }
            .boxed(),
        );
    }

    fn active_connection_count(&self) -> usize {
        self.conns.iter().map(Vec::len).sum::<usize>()
    }

    fn excess_connection_limit(&self) -> usize {
        match self.pool_config.pool_size {
            PoolSize::PerShard(_) => {
                EXCESS_CONNECTION_BOUND_PER_SHARD_MULTIPLIER
                    * self
                        .sharder
                        .as_ref()
                        .map_or(1, |s| s.nr_shards.get() as usize)
            }

            // In PerHost mode we do not need to keep excess connections
            PoolSize::PerHost(_) => 0,
        }
    }
}

struct BrokenConnectionEvent {
    connection: Weak<Connection>,
    error: ConnectionError,
}

async fn wait_for_error(
    connection: Weak<Connection>,
    error_receiver: ErrorReceiver,
) -> BrokenConnectionEvent {
    BrokenConnectionEvent {
        connection,
        error: error_receiver.await.unwrap_or_else(|_| {
            ConnectionError::BrokenConnection(BrokenConnectionErrorKind::ChannelError.into())
        }),
    }
}

struct OpenedConnectionEvent {
    result: Result<(Connection, ErrorReceiver), ConnectionError>,
    requested_shard: Option<Shard>,
    keyspace_name: Option<VerifiedKeyspaceName>,
}

#[cfg(test)]
mod tests {
    use super::super::connection::{open_connection_to_shard_aware_port, HostConnectionConfig};
    use crate::cluster::metadata::UntranslatedEndpoint;
    use crate::cluster::node::ResolvedContactPoint;
    use crate::routing::{ShardCount, Sharder};
    use crate::test_utils::setup_tracing;
    use std::net::{SocketAddr, ToSocketAddrs};

    // Open many connections to a node
    // Port collision should occur
    // If they are not handled this test will most likely fail
    #[tokio::test]
    #[cfg_attr(scylla_cloud_tests, ignore)]
    async fn many_connections() {
        setup_tracing();
        let connections_number = 512;

        let connect_address: SocketAddr = std::env::var("SCYLLA_URI")
            .unwrap_or_else(|_| "127.0.0.1:9042".to_string())
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();

        let connection_config = HostConnectionConfig {
            compression: None,
            tcp_nodelay: true,
            tls_config: None,
            ..Default::default()
        };

        // This does not have to be the real sharder,
        // the test is only about port collisions, not connecting
        // to the right shard
        let sharder = Sharder::new(ShardCount::new(3).unwrap(), 12);

        let endpoint = UntranslatedEndpoint::ContactPoint(ResolvedContactPoint {
            address: connect_address,
            datacenter: None,
        });

        // Open the connections
        let conns = (0..connections_number).map(|_| {
            open_connection_to_shard_aware_port(&endpoint, 0, sharder.clone(), &connection_config)
        });

        let joined = futures::future::join_all(conns).await;

        // Check that each connection managed to connect successfully
        for res in joined {
            res.unwrap();
        }
    }
}
