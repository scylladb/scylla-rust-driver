use super::connection::{
    Connection, ConnectionConfig, ErrorReceiver, HostConnectionConfig, VerifiedKeyspaceName,
    open_connection, open_connection_to_shard_aware_port,
};

use crate::errors::{
    BrokenConnectionErrorKind, ConnectionError, ConnectionPoolError, UseKeyspaceError,
};
#[cfg(test)]
use crate::policies::reconnect::ExponentialReconnectPolicy;
use crate::policies::reconnect::{ReconnectPolicy, ReconnectPolicySession};
use crate::routing::{Shard, ShardCount, Sharder};

use crate::cluster::metadata::{PeerEndpoint, UntranslatedEndpoint};

use crate::observability::metrics::Metrics;

use crate::cluster::NodeAddr;
use crate::utils::safe_format::IteratorSafeFormatExt;

use arc_swap::ArcSwap;
use futures::{Future, FutureExt, StreamExt, future::RemoteHandle, stream::FuturesUnordered};
use rand::Rng;
use std::convert::TryInto;
use std::num::NonZeroUsize;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::pin::Pin;
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;
use uuid::Uuid;

use tokio::sync::{Notify, mpsc};
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
    pub(crate) reconnect_policy: Arc<dyn ReconnectPolicy>,
}

#[cfg(test)]
impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            connection_config: Default::default(),
            pool_size: Default::default(),
            can_use_shard_aware_port: true,
            reconnect_policy: Arc::new(ExponentialReconnectPolicy::new()),
        }
    }
}

impl PoolConfig {
    fn to_host_pool_config(
        &self,
        endpoint: &UntranslatedEndpoint,
    ) -> (HostPoolConfig, Box<dyn ReconnectPolicySession>) {
        let host_reconnect_policy = self.reconnect_policy.new_session();
        let host_pool_config = HostPoolConfig {
            connection_config: self.connection_config.to_host_connection_config(endpoint),
            pool_size: self.pool_size,
            can_use_shard_aware_port: self.can_use_shard_aware_port,
        };
        (host_pool_config, host_reconnect_policy)
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
    /// Signaled to make the pool refiller retry immediately, resetting
    /// its backoff. Used when client routes change makes previously
    /// untranslatable addresses translatable.
    refill_now_notify: Arc<Notify>,
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
        connectivity_events_sender: Option<(Uuid, mpsc::UnboundedSender<ConnectivityChangeEvent>)>,
        current_keyspace: Option<VerifiedKeyspaceName>,
        pool_empty_notifier: mpsc::Sender<()>,
        metrics: Metrics,
    ) -> Self {
        let (use_keyspace_request_sender, use_keyspace_request_receiver) = mpsc::channel(1);
        let pool_updated_notify = Arc::new(Notify::new());
        let refill_now_notify = Arc::new(Notify::new());

        let (host_pool_config, host_reconnect_policy) = pool_config.to_host_pool_config(&endpoint);

        let arced_endpoint = Arc::new(RwLock::new(endpoint));

        let refiller = PoolRefiller::new(
            arced_endpoint.clone(),
            host_pool_config,
            connectivity_events_sender,
            current_keyspace,
            pool_updated_notify.clone(),
            refill_now_notify.clone(),
            pool_empty_notifier,
            metrics,
            host_reconnect_policy,
        );

        let conns = refiller.get_shared_connections();
        let (fut, refiller_handle) = refiller.run(use_keyspace_request_receiver).remote_handle();
        tokio::spawn(fut);

        Self {
            conns,
            use_keyspace_request_sender,
            _refiller_handle: Arc::new(refiller_handle),
            pool_updated_notify,
            refill_now_notify,
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

    /// Signals the pool refiller to retry immediately, resetting its backoff.
    ///
    /// Used when client routes are updated: previously untranslatable addresses
    /// may now be translatable, so the pool should retry without waiting for
    /// the exponential backoff timer.
    pub(crate) fn trigger_immediate_refill(&self) {
        self.refill_now_notify.notify_one();
    }

    /// Makes every connection currently in the pool issue a keepalive (`OPTIONS`)
    /// request immediately, instead of waiting for the next keepalive interval tick.
    ///
    /// Used upon a `STATUS_CHANGE DOWN` event for this node: the node is supposedly down,
    /// so its connections are likely defunct. The keepalives are expected to fail, which
    /// closes those connections; the pool then reports 0 connections and the node stops
    /// being targeted by the load balancing policy. If they succeed, the node is likely
    /// still alive (a stale event) and keeps being targeted.
    ///
    /// The `Result` is deliberately discarded: if the pool is `Initializing` or `Broken`,
    /// there are no connections to probe, which is exactly the case where the driver does
    /// not see the node as connected and the hint is pointless anyway.
    pub(crate) fn trigger_immediate_keepalive(&self) {
        let _ = self.with_connections(|pool_conns| match pool_conns {
            PoolConnections::NotSharded(conns) => {
                for conn in conns {
                    conn.trigger_keepalive();
                }
            }
            PoolConnections::Sharded { connections, .. } => {
                for conn in connections.iter().flatten() {
                    conn.trigger_keepalive();
                }
            }
        });
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
        // We may attempt the original shard again, but its not a problem.
        // An iteration of the loop below should be cheap for a shard with
        // no connections.
        let mut shards_to_try: Vec<u16> = (0..nr_shards.get()).collect();

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

/// For how long the driver refrains from using advanced shard awareness towards a node
/// after discovering that it does not work for that node.
///
/// Long enough not to waste connections on an attempt that is nearly certain to fail,
/// short enough for the driver to recover on its own if the network setup changes.
const ADVANCED_SHARD_AWARENESS_BLOCK_DURATION: Duration = Duration::from_secs(300);

struct PoolRefiller {
    // Following information identify the pool and do not change
    pool_config: HostPoolConfig,

    /// If set, used to send connectivity change events about node with given host_id.
    connectivity_events_sender: Option<(Uuid, mpsc::UnboundedSender<ConnectivityChangeEvent>)>,

    // Following information is subject to updates on topology refresh
    endpoint: Arc<RwLock<UntranslatedEndpoint>>,

    // Following fields are updated with information from OPTIONS
    shard_aware_port: Option<u16>,
    sharder: Option<Sharder>,

    // If set, advanced shard awareness (choosing the connection's source port so that ScyllaDB
    // assigns the desired shard to it) is not attempted until this instant.
    //
    // It is set upon discovering that a connection opened with advanced shard awareness landed
    // on a different shard than the requested one, which means that something in between
    // (most likely a NAT) rewrites our source ports. Further attempts would then have virtually
    // no chance of hitting the requested shard, so they would only waste connections.
    advanced_shard_awareness_blocked_until: Option<tokio::time::Instant>,

    // `shared_conns` is updated only after `conns` change
    shared_conns: Arc<ArcSwap<MaybePoolConnections>>,
    conns: Vec<Vec<Arc<Connection>>>,

    // Set to true if there was an error since the last refill,
    // set to false when refilling starts.
    had_error_since_last_refill: bool,

    refill_delay_strategy: Box<dyn ReconnectPolicySession>,

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

    // Signaled to make the refiller retry immediately with reset backoff
    refill_now_notify: Arc<Notify>,

    // Signaled when the connection pool becomes empty
    pool_empty_notifier: mpsc::Sender<()>,

    metrics: Metrics,
}

#[derive(Debug)]
struct UseKeyspaceRequest {
    keyspace_name: VerifiedKeyspaceName,
    response_sender: tokio::sync::oneshot::Sender<Result<(), UseKeyspaceError>>,
}

impl PoolRefiller {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        endpoint: Arc<RwLock<UntranslatedEndpoint>>,
        pool_config: HostPoolConfig,
        connectivity_events_sender: Option<(Uuid, mpsc::UnboundedSender<ConnectivityChangeEvent>)>,
        current_keyspace: Option<VerifiedKeyspaceName>,
        pool_updated_notify: Arc<Notify>,
        refill_now_notify: Arc<Notify>,
        pool_empty_notifier: mpsc::Sender<()>,
        metrics: Metrics,
        reconnect_policy: Box<dyn ReconnectPolicySession>,
    ) -> Self {
        // At the beginning, we assume the node does not have any shards
        // and assume that the node is a Cassandra node
        let conns = vec![Vec::new()];
        let shared_conns = Arc::new(ArcSwap::new(Arc::new(MaybePoolConnections::Initializing)));

        Self {
            endpoint,
            pool_config,
            connectivity_events_sender,

            shard_aware_port: None,
            sharder: None,
            advanced_shard_awareness_blocked_until: None,

            shared_conns,
            conns,

            had_error_since_last_refill: false,
            refill_delay_strategy: reconnect_policy,

            ready_connections: FuturesUnordered::new(),
            connection_errors: FuturesUnordered::new(),

            excess_connections: Vec::new(),

            current_keyspace,

            pool_updated_notify,
            refill_now_notify,
            pool_empty_notifier,

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

        struct ScheduledRefill {
            when: tokio::time::Instant,
        }

        let mut scheduled_refill = Some(ScheduledRefill {
            when: tokio::time::Instant::now(),
        });

        loop {
            tokio::select! {
                // Note that some default value must be passed to avoid `unwrap()` here; the guard ensures that `scheduled_refill` is `Some`
                // when the sleep future is polled, but it does not ensure that `scheduled_refill` is `Some` when the sleep future
                // is created. With `unwrap()`, we'd get a panic here.
                // The future created with the default value will not be polled anyway, because the guard prevents that.
                //
                // `tokio::select!`'s documentation:
                // > Additionally, each branch may include an optional if precondition. If the precondition returns false, then the branch is disabled.
                // > **The provided <async expression> is still evaluated** but the resulting future is never polled.
                _ = tokio::time::sleep_until(scheduled_refill.as_ref().map_or(tokio::time::Instant::now(), |r| r.when)), if scheduled_refill.is_some() => {
                    self.had_error_since_last_refill = false;
                    self.start_filling();
                    scheduled_refill = None;
                }

                evt = self.ready_connections.select_next_some(), if !self.ready_connections.is_empty() => {
                    self.handle_ready_connection(evt);

                    if self.is_full() {
                        debug!(
                            "[{}] Pool is full, clearing {} excess connections",
                            self.endpoint_description(),
                            self.excess_connections.len()
                        );
                        self.decrement_total_connections(
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

                _ = self.refill_now_notify.notified() => {
                    debug!(
                        "[{}] Immediate refill requested, resetting backoff",
                        self.endpoint_description()
                    );
                    // This resets the backoff, so the reconnect policy will go back to the initial delay after this.
                    self.refill_delay_strategy.on_successful_fill();
                    // This is best-effort reset. Note that a refill may be undergoing, and if a new error is encountered
                    // during that refill, this will be set to true again.
                    self.had_error_since_last_refill = false;

                    // Reschedule the refill with the possibly shorter delay.
                    // Rationale: we may have already scheduled a refill with a longer delay, but that's not a problem -
                    // we unschedule that refill here and then the block below will schedule a new refill with the correct delay.
                    scheduled_refill = None;
                }
            }
            trace!(
                pool_state = ?ShardedConnectionVectorWrapper(&self.conns)
            );

            // Schedule refilling here
            if scheduled_refill.is_none() && self.need_filling() {
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

                scheduled_refill = Some(ScheduledRefill {
                    when: tokio::time::Instant::now() + delay,
                });
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
            && !self.is_advanced_shard_awareness_blocked()
    }

    fn is_advanced_shard_awareness_blocked(&self) -> bool {
        self.advanced_shard_awareness_blocked_until
            .is_some_and(|until| tokio::time::Instant::now() < until)
    }

    /// Stops using advanced shard awareness towards this node for
    /// [`ADVANCED_SHARD_AWARENESS_BLOCK_DURATION`], after it turned out not to work.
    fn block_advanced_shard_awareness(&mut self) {
        // Nothing to log or update if a block is already in effect - the duration is constant,
        // so re-arming it could only postpone the retry indefinitely under a stream of mismatches.
        if self.is_advanced_shard_awareness_blocked() {
            return;
        }

        warn!(
            "[{}] A connection opened with advanced shard awareness landed on an unrequested shard, \
             which suggests that the source port is rewritten (e.g. by a NAT) on the way to the node. \
             Not using advanced shard awareness for this node for the next {} s. \
             Requests will still be routed to the correct shards, but the connection pool \
             will be filled less efficiently. If you intentionally want to not use shard aware port, \
             use `disallow_shard_aware_port` config option. Consider if it's possible to repair your network \
             configuration to prevent source port rewriting.",
            self.endpoint_description(),
            ADVANCED_SHARD_AWARENESS_BLOCK_DURATION.as_secs(),
        );
        self.advanced_shard_awareness_blocked_until =
            Some(tokio::time::Instant::now() + ADVANCED_SHARD_AWARENESS_BLOCK_DURATION);
    }

    // Begins opening a number of connections in order to fill the connection pool.
    // Futures which open the connections are pushed to the `ready_connections`
    // FuturesUnordered structure, and their results are processed in the main loop.
    fn start_filling(&mut self) {
        let endpoint = self.endpoint_description();

        if self.is_empty() {
            // If the pool is empty, it might mean that the node is not alive.
            // It is more likely than not that the next connection attempt will
            // fail, so there is no use in opening more than one connection now.
            trace!("[{}] Will open the first connection to the node", endpoint,);
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
                        endpoint, to_open_count, shard_id,
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
            endpoint, to_open_count,
        );
        for _ in 0..to_open_count {
            self.start_opening_connection(None);
        }
    }

    // Handles a newly opened connection and decides what to do with it.
    fn handle_ready_connection(&mut self, evt: OpenedConnectionEvent) {
        let endpoint = self.endpoint_description();
        match evt.result {
            Err(ConnectionSetupError::Connection(err)) => {
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
                        endpoint, err,
                    );
                    self.start_opening_connection(None);
                } else {
                    // Encountered an error while connecting to the non-shard-aware
                    // port. Set the `had_error_since_last_refill` flag so that
                    // the next refill will be delayed more than this one.
                    self.had_error_since_last_refill = true;
                    debug!(
                        "[{}] Failed to open connection to the non-shard-aware port: {:?}",
                        endpoint, err,
                    );

                    // If all connection attempts in this fill attempt failed
                    // and the pool is empty, report this error.
                    if !self.is_filling() && self.is_empty() {
                        self.update_shared_conns(Some(err));
                    }
                }
            }
            Err(ConnectionSetupError::Keyspace(err)) => {
                self.metrics.dec_total_connections();
                self.had_error_since_last_refill = true;
                debug!(
                    "[{}] Failed to set keyspace for new connection: {}",
                    endpoint, err,
                );

                if !self.is_filling() && self.is_empty() {
                    self.update_shared_conns(Some(err.into()));
                }
            }
            Ok((connection, error_receiver)) => {
                // Before the connection can be put to the pool, we need
                // to make sure that it uses appropriate keyspace
                if let Some(keyspace) = &self.current_keyspace
                    && evt.keyspace_name.as_ref() != Some(keyspace)
                {
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

                // Update sharding and optionally reshard
                let shard_info = connection.get_shard_info().as_ref();
                let sharder = shard_info.map(|s| s.get_sharder());
                let shard_id = shard_info.map_or(0, |s| s.shard as usize);

                // If the connection was opened with advanced shard awareness - i.e. from a source
                // port picked so that ScyllaDB assigns `requested_shard` to the connection - but
                // ScyllaDB assigned another shard, then advanced shard awareness does not work
                // towards this node, so stop attempting it for a while.
                // The port is only meaningful for the shard count it was computed with, so a
                // mismatch counts only if the node still uses that very sharder; otherwise the
                // node resharded while the attempt was in flight and that alone explains it.
                if let Some(requested) = evt.requested_shard.as_ref()
                    && Some(&requested.sharder) == sharder.as_ref()
                    && requested.shard != shard_id as Shard
                {
                    self.block_advanced_shard_awareness();
                }

                self.maybe_reshard(sharder);

                // Update the shard-aware port
                if self.shard_aware_port != connection.get_shard_aware_port() {
                    debug!(
                        "[{}] Updating shard aware port: {:?}",
                        endpoint,
                        connection.get_shard_aware_port(),
                    );
                    self.shard_aware_port = connection.get_shard_aware_port();
                }

                let active_connection_count = self.active_connection_count();
                // `ShardInfo::new` rejects out-of-range shard IDs; without shard info
                // we use shard 0. `maybe_reshard` has sized `self.conns` accordingly.
                let shard_conns = &mut self.conns[shard_id];

                // Decide if the connection can be accepted, according to
                // the pool filling strategy
                let can_be_accepted = match self.pool_config.pool_size {
                    PoolSize::PerHost(target) => active_connection_count < target.get(),
                    PoolSize::PerShard(target) => shard_conns.len() < target.get(),
                };

                if can_be_accepted {
                    // Don't complain and just put the connection to the pool.
                    // If this was a shard-aware port connection which missed
                    // the right shard, we still want to accept it
                    // because it fills our pool.
                    let conn = Arc::new(connection);
                    trace!(
                        "[{}] Adding connection {:p} to shard {} pool, now there are {} for the shard, total {}",
                        endpoint,
                        Arc::as_ptr(&conn),
                        shard_id,
                        shard_conns.len() + 1,
                        active_connection_count + 1,
                    );

                    self.connection_errors
                        .push(wait_for_error(Arc::downgrade(&conn), error_receiver).boxed());
                    shard_conns.push(conn);

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
                        endpoint, shard_id,
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
                        endpoint,
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
                            endpoint, excess_connection_limit,
                        );
                        self.decrement_total_connections(self.excess_connections.len());
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

        let count_in_metrics = {
            let metrics = self.metrics.clone();
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

                count_in_metrics(&result);

                OpenedConnectionEvent {
                    result: result.map_err(ConnectionSetupError::Connection),
                    requested_shard: Some(RequestedShard { shard, sharder }),
                    keyspace_name: None,
                }
            }
            .boxed(),
            _ => async move {
                let non_shard_aware_endpoint = endpoint;
                let result = open_connection(&non_shard_aware_endpoint, None, &cfg).await;

                count_in_metrics(&result);

                OpenedConnectionEvent {
                    result: result.map_err(ConnectionSetupError::Connection),
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
        self.decrement_total_connections(
            self.active_connection_count() + self.excess_connections.len(),
        );
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

        // Make the connection list available.
        let old_conns = self.shared_conns.swap(new_conns);

        // Notify potential waiters.
        self.pool_updated_notify.notify_waiters();

        // Emit transition events.
        self.emit_events(old_conns.as_ref());
    }

    /// Emits connectivity change events if the pool transitioned
    /// between empty and non-empty states,
    /// provided that connectivity notifier is configured.
    fn emit_events(&self, old_conns: &MaybePoolConnections) {
        let Some((host_id, ref connectivity_notifier)) = self.connectivity_events_sender else {
            // No notifier configured, nothing to do.
            return;
        };

        // This is used to notify the ClusterWorker about host reachability
        // in case of non-control connection pool.

        let maybe_event = match (old_conns, !self.is_empty()) {
            (MaybePoolConnections::Initializing, true)
            | (MaybePoolConnections::Broken(_), true) => {
                // There was no connectivity before, now there are some connections.
                Some(ConnectivityChangeEvent::Established { host_id })
            }
            (MaybePoolConnections::Ready(_), false) => {
                // There was connectivity before, now there are no connections.
                Some(ConnectivityChangeEvent::Lost { host_id })
            }
            (MaybePoolConnections::Broken(_), false) => {
                // Already broken, no transition.
                None
            }
            (MaybePoolConnections::Ready(_), true) => {
                // Already ready, no transition.
                None
            }
            (MaybePoolConnections::Initializing, false) => {
                // Initially we optimistically assumed the node was alive,
                // now we have a hint that it is not.
                Some(ConnectivityChangeEvent::Lost { host_id })
            }
        };

        let Some(event) = maybe_event else {
            // No transition, nothing to do.
            return;
        };
        let endpoint = self.endpoint_description();
        match event {
            ConnectivityChangeEvent::Established { .. } => {
                debug!(
                    "[{} - {}] Connection pool is no longer empty, notifying listeners",
                    host_id, endpoint,
                );
            }
            ConnectivityChangeEvent::Lost { .. } => {
                debug!(
                    "[{} - {}] Connection pool is now empty, notifying listeners",
                    host_id, endpoint,
                );
            }
        }

        // Ignore failure to send. If there are no listeners, it's fine.
        let _ = connectivity_notifier.send(event);
    }

    // Removes given connection from the pool. It looks both into active
    // connections and excess connections.
    fn remove_connection(&mut self, connection: Arc<Connection>, last_error: ConnectionError) {
        let ptr = Arc::as_ptr(&connection);

        let endpoint = self.endpoint_description();

        let maybe_remove_in_vec = |v: &mut Vec<Arc<Connection>>| -> bool {
            let maybe_idx = v
                .iter()
                .enumerate()
                .find(|(_, other_conn)| Arc::ptr_eq(&connection, other_conn))
                .map(|(idx, _)| idx);
            match maybe_idx {
                Some(idx) => {
                    v.swap_remove(idx);
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
                endpoint,
                ptr,
                shard_id,
                self.conns[shard_id].len(),
                self.active_connection_count(),
            );

            if self.is_empty() {
                // This is used to notify the ClusterWorker that the control connection has died.
                // `try_send()` is OK here because if the channel is full, the notification is already pending.
                let _ = self.pool_empty_notifier.try_send(());
            }

            self.update_shared_conns(Some(last_error));
            return;
        }

        // If we didn't find it, it might sit in the excess_connections bucket
        if maybe_remove_in_vec(&mut self.excess_connections) {
            trace!(
                "[{}] Connection {:p} removed from excess connection pool",
                endpoint, ptr,
            );
            return;
        }

        trace!("[{}] Connection {:p} was already removed", endpoint, ptr,);
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
        requested_shard: Option<RequestedShard>,
    ) {
        // TODO: There should be a timeout for this

        let keyspace_name = self.current_keyspace.as_ref().cloned().unwrap();
        self.ready_connections.push(
            async move {
                let result = connection
                    .use_keyspace(&keyspace_name)
                    .await
                    .map(|()| (connection, error_receiver))
                    .map_err(ConnectionSetupError::Keyspace);
                OpenedConnectionEvent {
                    result,
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

    fn decrement_total_connections(&self, count: usize) {
        for _ in 0..count {
            self.metrics.dec_total_connections();
        }
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

/// The shard that a connection attempt targeted using advanced shard awareness, together with
/// the sharder that its source port was computed with.
///
/// The sharder is remembered because the node may reshard while the attempt is in flight: the
/// resulting shard mismatch is then explained by the stale shard count rather than by the source
/// port not surviving the way to the node, and must not be blamed on advanced shard awareness.
/// The pool's current sharder cannot answer that question - it may have already adopted the new
/// one from an earlier attempt of the same, now obsolete, generation.
struct RequestedShard {
    shard: Shard,
    sharder: Sharder,
}

struct OpenedConnectionEvent {
    result: Result<(Connection, ErrorReceiver), ConnectionSetupError>,
    requested_shard: Option<RequestedShard>,
    keyspace_name: Option<VerifiedKeyspaceName>,
}

enum ConnectionSetupError {
    Connection(ConnectionError),
    Keyspace(UseKeyspaceError),
}

/// Signals that connectivity to a node has changed.
#[derive(Debug)]
pub(crate) enum ConnectivityChangeEvent {
    /// A new connection to the node was established, while there were no working connections.
    Established { host_id: Uuid },

    /// The last working connection to the node was lost.
    Lost { host_id: Uuid },
}
impl ConnectivityChangeEvent {
    /// Returns the host ID associated with this event.
    pub(crate) fn host_id(&self) -> Uuid {
        match *self {
            ConnectivityChangeEvent::Established { host_id }
            | ConnectivityChangeEvent::Lost { host_id } => host_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::connection::{
        HostConnectionConfig, VerifiedKeyspaceName, open_connection,
        open_connection_to_shard_aware_port,
    };
    use super::{
        ADVANCED_SHARD_AWARENESS_BLOCK_DURATION, ConnectionSetupError, HostPoolConfig,
        MaybePoolConnections, OpenedConnectionEvent, PoolConnections, PoolRefiller, RequestedShard,
    };
    use crate::cluster::metadata::UntranslatedEndpoint;
    use crate::cluster::node::ResolvedContactPoint;
    use crate::errors::{ConnectionError, UseKeyspaceError};
    use crate::frame::request::options;
    use crate::network::TcpSocketOptions;
    use crate::observability::metrics::Metrics;
    use crate::policies::reconnect::{ExponentialReconnectPolicy, ReconnectPolicy as _};
    use crate::routing::{Shard, ShardCount, ShardInfo, Sharder};
    use crate::test_utils::setup_tracing;
    use bytes::Bytes;
    use futures::{FutureExt, StreamExt};
    use scylla_proxy::{
        Condition, Node, Proxy, ProxyError, Reaction as _, RequestFrame, RequestOpcode,
        RequestReaction, RequestRule, ResponseFrame, ResponseOpcode, RunningProxy, WorkerError,
    };
    use std::collections::HashMap;
    use std::net::{SocketAddr, ToSocketAddrs};
    use std::sync::{Arc, RwLock};
    use std::time::Duration;
    use tokio::sync::{Notify, mpsc};

    #[test]
    fn keyspace_setup_failure_triggers_refill() {
        let endpoint = Arc::new(RwLock::new(UntranslatedEndpoint::ContactPoint(
            ResolvedContactPoint {
                address: "127.0.0.1:9042".parse().unwrap(),
            },
        )));
        let metrics = Metrics::new();
        let (pool_empty_notifier, _pool_empty_receiver) = mpsc::channel(1);
        let pool_updated_notify = Arc::new(Notify::new());
        let mut pool_updated = Box::pin(pool_updated_notify.notified());
        assert!(!pool_updated.as_mut().enable());
        let mut refiller = PoolRefiller::new(
            endpoint,
            HostPoolConfig::default(),
            None,
            None,
            pool_updated_notify.clone(),
            Arc::new(Notify::new()),
            pool_empty_notifier,
            metrics.clone(),
            ExponentialReconnectPolicy::new().new_session(),
        );

        metrics.inc_total_connections();
        refiller.handle_ready_connection(OpenedConnectionEvent {
            result: Err(ConnectionSetupError::Keyspace(
                UseKeyspaceError::RequestTimeout(Duration::from_secs(1)),
            )),
            requested_shard: Some(RequestedShard {
                shard: 0,
                sharder: Sharder::new(ShardCount::new(1).unwrap(), 12),
            }),
            keyspace_name: None,
        });

        assert!(refiller.had_error_since_last_refill);
        assert!(refiller.ready_connections.is_empty());
        assert!(refiller.is_empty());
        assert!(refiller.need_filling());
        let shared = refiller.shared_conns.load_full();
        assert!(matches!(
            shared.as_ref(),
            MaybePoolConnections::Broken(ConnectionError::UseKeyspaceError(
                UseKeyspaceError::RequestTimeout(duration)
            )) if *duration == Duration::from_secs(1)
        ));
        assert!(pool_updated.now_or_never().is_some());
        #[cfg(feature = "metrics")]
        assert_eq!(metrics.get_total_connections(), 0);
    }

    #[tokio::test]
    async fn keyspace_setup_failure_preserves_pool_state() {
        let proxy_addr = SocketAddr::new(scylla_proxy::get_exclusive_local_address(), 9042);
        let make_rules = |shard_info: Option<ShardInfo>, keyspace_succeeds: bool| {
            let query_reaction = if keyspace_succeeds {
                RequestReaction::forge_response(Arc::new(move |frame: RequestFrame| {
                    ResponseFrame {
                        params: frame.params.for_response(),
                        opcode: ResponseOpcode::Result,
                        body: Bytes::from_static(b"\0\0\0\x03\0\x08keyspace"),
                    }
                }))
            } else {
                RequestReaction::forge().server_error()
            };
            vec![
                RequestRule(
                    Condition::RequestOpcode(RequestOpcode::Options),
                    RequestReaction::forge_response(Arc::new(move |frame: RequestFrame| {
                        ResponseFrame::forged_supported(frame.params, &{
                            let mut options = HashMap::new();
                            if let Some(shard_info) = shard_info.as_ref() {
                                shard_info.add_to_options(&mut options);
                            }
                            options
                        })
                        .unwrap()
                    })),
                ),
                RequestRule(
                    Condition::RequestOpcode(RequestOpcode::Startup),
                    RequestReaction::forge_response(Arc::new(move |frame: RequestFrame| {
                        ResponseFrame::forged_ready(frame.params)
                    })),
                ),
                RequestRule(
                    Condition::RequestOpcode(RequestOpcode::Query),
                    query_reaction,
                ),
            ]
        };
        let mut proxy = Proxy::builder()
            .with_node(
                Node::builder()
                    .proxy_address(proxy_addr)
                    .request_rules(make_rules(None, false))
                    .build_dry_mode(),
            )
            .build()
            .run()
            .await
            .unwrap();
        let endpoint = UntranslatedEndpoint::ContactPoint(ResolvedContactPoint {
            address: proxy_addr,
        });
        let connection_config = HostConnectionConfig::default();
        let (initial_connection, initial_error_receiver) =
            open_connection(&endpoint, None, &connection_config)
                .await
                .unwrap();
        let metrics = Metrics::new();
        let (pool_empty_notifier, _pool_empty_receiver) = mpsc::channel(1);
        let mut refiller = PoolRefiller::new(
            Arc::new(RwLock::new(endpoint.clone())),
            HostPoolConfig::default(),
            None,
            None,
            Arc::new(Notify::new()),
            Arc::new(Notify::new()),
            pool_empty_notifier,
            metrics.clone(),
            ExponentialReconnectPolicy::new().new_session(),
        );

        metrics.inc_total_connections();
        refiller.handle_ready_connection(OpenedConnectionEvent {
            result: Ok((initial_connection, initial_error_receiver)),
            requested_shard: None,
            keyspace_name: None,
        });
        assert_eq!(refiller.active_connection_count(), 1);
        #[cfg(feature = "metrics")]
        assert_eq!(metrics.get_total_connections(), 1);
        let shared = refiller.shared_conns.load_full();
        assert!(matches!(
            shared.as_ref(),
            MaybePoolConnections::Ready(PoolConnections::NotSharded(connections))
                if connections.len() == 1
        ));

        refiller.current_keyspace =
            Some(VerifiedKeyspaceName::new("keyspace".to_owned(), false).unwrap());
        let shard_info = ShardInfo::new(0, ShardCount::new(2).unwrap(), 12).unwrap();
        proxy.running_nodes[0]
            .change_request_rules(Some(make_rules(Some(shard_info.clone()), false)));
        let (new_connection, new_error_receiver) =
            open_connection(&endpoint, None, &connection_config)
                .await
                .unwrap();

        metrics.inc_total_connections();
        refiller.handle_ready_connection(OpenedConnectionEvent {
            result: Ok((new_connection, new_error_receiver)),
            requested_shard: None,
            keyspace_name: None,
        });
        assert_eq!(refiller.active_connection_count(), 1);
        assert!(refiller.sharder.is_none());

        let event = refiller.ready_connections.next().await.unwrap();
        assert!(matches!(
            &event.result,
            Err(ConnectionSetupError::Keyspace(_))
        ));
        refiller.handle_ready_connection(event);

        assert_eq!(refiller.active_connection_count(), 1);
        assert!(refiller.sharder.is_none());
        #[cfg(feature = "metrics")]
        assert_eq!(metrics.get_total_connections(), 1);
        let shared = refiller.shared_conns.load_full();
        assert!(matches!(
            shared.as_ref(),
            MaybePoolConnections::Ready(PoolConnections::NotSharded(connections))
                if connections.len() == 1
        ));

        proxy.running_nodes[0].change_request_rules(Some(make_rules(Some(shard_info), true)));
        let (successful_connection, successful_error_receiver) =
            open_connection(&endpoint, None, &connection_config)
                .await
                .unwrap();

        metrics.inc_total_connections();
        refiller.handle_ready_connection(OpenedConnectionEvent {
            result: Ok((successful_connection, successful_error_receiver)),
            requested_shard: None,
            keyspace_name: None,
        });
        assert_eq!(refiller.active_connection_count(), 1);
        assert!(refiller.sharder.is_none());

        let event = refiller.ready_connections.next().await.unwrap();
        match &event.result {
            Err(ConnectionSetupError::Connection(err)) => panic!("connection setup failed: {err}"),
            Err(ConnectionSetupError::Keyspace(err)) => panic!("keyspace setup failed: {err}"),
            Ok(_) => {}
        }
        refiller.handle_ready_connection(event);

        assert_eq!(refiller.active_connection_count(), 1);
        #[cfg(feature = "metrics")]
        assert_eq!(metrics.get_total_connections(), 1);
        assert_eq!(refiller.sharder.as_ref().unwrap().nr_shards.get(), 2);
        let shared = refiller.shared_conns.load_full();
        assert!(matches!(
            shared.as_ref(),
            MaybePoolConnections::Ready(PoolConnections::Sharded {
                sharder,
                connections,
            }) if sharder.nr_shards.get() == 2
                && connections.iter().map(Vec::len).sum::<usize>() == 1
        ));

        match proxy.finish().await {
            Ok(()) | Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => {}
            Err(err) => panic!("{err}"),
        }
        drop(refiller);
    }

    async fn test_many_connections_with_config(connection_config: HostConnectionConfig) {
        let connections_number = 400;

        let connect_address: SocketAddr = std::env::var("SCYLLA_URI")
            .unwrap_or_else(|_| "172.42.0.2:9042".to_string())
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();

        // This does not have to be the real sharder,
        // the test is only about port collisions, not connecting
        // to the right shard
        let sharder = Sharder::new(ShardCount::new(3).unwrap(), 12);

        let endpoint = UntranslatedEndpoint::ContactPoint(ResolvedContactPoint {
            address: connect_address,
        });

        // Open the connections
        let conns = (0..connections_number).map(|_| {
            open_connection_to_shard_aware_port(&endpoint, 0, sharder.clone(), &connection_config)
        });

        let _joined = futures::future::try_join_all(conns).await.unwrap();
    }

    // Open many connections to a node
    // Port collision should occur
    // If they are not handled this test will most likely fail
    #[tokio::test]
    async fn many_connections() {
        setup_tracing();

        test_many_connections_with_config(HostConnectionConfig {
            compression: None,
            tcp_socket_options: TcpSocketOptions {
                nodelay: true,
                ..Default::default()
            },
            tls_config: None,
            ..Default::default()
        })
        .await;

        test_many_connections_with_config(HostConnectionConfig {
            compression: None,
            tcp_socket_options: TcpSocketOptions {
                nodelay: true,
                reuse_address: Some(true),
                ..Default::default()
            },
            tls_config: None,
            ..Default::default()
        })
        .await;
    }

    fn mock_pool_refiller() -> PoolRefiller {
        let endpoint = Arc::new(RwLock::new(UntranslatedEndpoint::ContactPoint(
            ResolvedContactPoint {
                address: SocketAddr::from(([127, 0, 0, 1], 9042)),
            },
        )));
        // The receiver is dropped right away; the refiller only does a best-effort `try_send()`.
        let (pool_empty_notifier, _) = mpsc::channel(1);

        PoolRefiller::new(
            endpoint,
            HostPoolConfig::default(),
            None,
            None,
            Arc::new(Notify::new()),
            Arc::new(Notify::new()),
            pool_empty_notifier,
            Metrics::new(),
            ExponentialReconnectPolicy::default().new_session(),
        )
    }

    /// Once the driver discovers that connections opened with advanced shard awareness land on
    /// shards other than the requested ones, it must stop attempting it - but only temporarily,
    /// so that it recovers on its own if the network setup changes.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn advanced_shard_awareness_is_blocked_temporarily() {
        setup_tracing();

        let mut refiller = mock_pool_refiller();
        // Pretend that OPTIONS revealed a sharded node with a shard-aware port.
        refiller.sharder = Some(Sharder::new(ShardCount::new(4).unwrap(), 12));
        refiller.shard_aware_port = Some(19042);
        assert!(refiller.can_use_shard_aware_port());

        refiller.block_advanced_shard_awareness();
        assert!(!refiller.can_use_shard_aware_port());

        tokio::time::advance(ADVANCED_SHARD_AWARENESS_BLOCK_DURATION - Duration::from_secs(1))
            .await;
        assert!(!refiller.can_use_shard_aware_port());

        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(refiller.can_use_shard_aware_port());
    }

    /// The shard-aware port that the simulated node advertises, so that the pool is willing to
    /// use advanced shard awareness towards it in the first place.
    const SIMULATED_SHARD_AWARE_PORT: u16 = 19042;

    /// Starts a dry-mode proxy pretending to be a ScyllaDB node with the given sharding info.
    ///
    /// The assigned shard is something only the node can report (in SUPPORTED), so simulating the
    /// node is what makes shard mismatches reproducible without a real cluster - and independent
    /// of the cluster's shard count.
    async fn start_simulated_node(shard_info: ShardInfo) -> (RunningProxy, UntranslatedEndpoint) {
        let proxy_addr = SocketAddr::new(scylla_proxy::get_exclusive_local_address(), 9042);
        let rules = vec![
            // OPTIONS -> SUPPORTED, advertising the sharding info of the simulated node.
            RequestRule(
                Condition::RequestOpcode(RequestOpcode::Options),
                RequestReaction::forge_response(Arc::new(move |frame: RequestFrame| {
                    ResponseFrame::forged_supported(frame.params, &{
                        let mut options = HashMap::new();
                        shard_info.add_to_options(&mut options);
                        options.insert(
                            options::SCYLLA_SHARD_AWARE_PORT.to_owned(),
                            vec![SIMULATED_SHARD_AWARE_PORT.to_string()],
                        );
                        options
                    })
                    .unwrap()
                })),
            ),
            // STARTUP -> READY, so that the handshake completes.
            RequestRule(
                Condition::RequestOpcode(RequestOpcode::Startup),
                RequestReaction::forge_response(Arc::new(|frame: RequestFrame| {
                    ResponseFrame::forged_ready(frame.params)
                })),
            ),
        ];
        let proxy = Proxy::builder()
            .with_node(
                Node::builder()
                    .proxy_address(proxy_addr)
                    .request_rules(rules)
                    .build_dry_mode(),
            )
            .build()
            .run()
            .await
            .unwrap();

        let endpoint = UntranslatedEndpoint::ContactPoint(ResolvedContactPoint {
            address: proxy_addr,
        });
        (proxy, endpoint)
    }

    #[cfg(feature = "metrics")]
    async fn unrequested_connection_event(
        endpoint: &UntranslatedEndpoint,
    ) -> OpenedConnectionEvent {
        let result = open_connection(endpoint, None, &HostConnectionConfig::default()).await;
        OpenedConnectionEvent {
            result: Ok(result.unwrap()),
            requested_shard: None,
            keyspace_name: None,
        }
    }

    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn resharding_discards_old_connections_and_balances_metrics() {
        setup_tracing();

        let initial_shard_info = ShardInfo {
            shard: 0,
            nr_shards: ShardCount::new(1).unwrap(),
            msb_ignore: 12,
        };
        let (mut proxy, endpoint) = start_simulated_node(initial_shard_info).await;
        proxy.running_nodes[0].change_request_rules(Some(vec![
            RequestRule(
                Condition::RequestOpcode(RequestOpcode::Options),
                RequestReaction::forge_response(Arc::new(|frame: RequestFrame| {
                    ResponseFrame::forged_supported(frame.params, &HashMap::new()).unwrap()
                })),
            ),
            RequestRule(
                Condition::RequestOpcode(RequestOpcode::Startup),
                RequestReaction::forge_response(Arc::new(|frame: RequestFrame| {
                    ResponseFrame::forged_ready(frame.params)
                })),
            ),
        ]));
        let mut refiller = mock_pool_refiller();
        let metrics = refiller.metrics.clone();

        metrics.inc_total_connections();
        refiller.handle_ready_connection(unrequested_connection_event(&endpoint).await);
        assert!(refiller.sharder.is_none());

        let new_shard_info = ShardInfo {
            shard: 1,
            nr_shards: ShardCount::new(2).unwrap(),
            msb_ignore: 12,
        };
        let new_sharder = new_shard_info.get_sharder();
        proxy.running_nodes[0].change_request_rules(Some(vec![
            RequestRule(
                Condition::RequestOpcode(RequestOpcode::Options),
                RequestReaction::forge_response(Arc::new(move |frame: RequestFrame| {
                    ResponseFrame::forged_supported(frame.params, &{
                        let mut options = HashMap::new();
                        new_shard_info.add_to_options(&mut options);
                        options.insert(
                            options::SCYLLA_SHARD_AWARE_PORT.to_owned(),
                            vec![SIMULATED_SHARD_AWARE_PORT.to_string()],
                        );
                        options
                    })
                    .unwrap()
                })),
            ),
            RequestRule(
                Condition::RequestOpcode(RequestOpcode::Startup),
                RequestReaction::forge_response(Arc::new(|frame: RequestFrame| {
                    ResponseFrame::forged_ready(frame.params)
                })),
            ),
        ]));

        metrics.inc_total_connections();
        refiller.handle_ready_connection(unrequested_connection_event(&endpoint).await);

        assert_eq!(refiller.sharder.as_ref(), Some(&new_sharder));
        assert_eq!(refiller.active_connection_count(), 1);
        assert!(refiller.conns[0].is_empty());
        assert_eq!(refiller.conns[1].len(), 1);
        assert!(refiller.excess_connections.is_empty());
        assert_eq!(metrics.get_total_connections(), 1);

        drop(refiller);
        let _ = proxy.finish().await;
    }

    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn excess_connection_limit_clears_connections_and_balances_metrics() {
        setup_tracing();

        let shard_info = ShardInfo {
            shard: 0,
            nr_shards: ShardCount::new(1).unwrap(),
            msb_ignore: 12,
        };
        let (proxy, endpoint) = start_simulated_node(shard_info).await;
        let mut refiller = mock_pool_refiller();
        let metrics = refiller.metrics.clone();
        let connection_count = refiller.excess_connection_limit() + 2;

        for _ in 0..connection_count {
            metrics.inc_total_connections();
            refiller.handle_ready_connection(unrequested_connection_event(&endpoint).await);
        }

        assert_eq!(refiller.active_connection_count(), 1);
        assert!(refiller.excess_connections.is_empty());
        assert_eq!(metrics.get_total_connections(), 1);

        drop(refiller);
        let _ = proxy.finish().await;
    }

    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn full_pool_clears_excess_connections_and_balances_metrics() {
        setup_tracing();

        let shard_info = ShardInfo {
            shard: 0,
            nr_shards: ShardCount::new(1).unwrap(),
            msb_ignore: 12,
        };
        let (proxy, endpoint) = start_simulated_node(shard_info).await;
        let mut refiller = mock_pool_refiller();
        let metrics = refiller.metrics.clone();

        for _ in 0..2 {
            metrics.inc_total_connections();
            refiller.handle_ready_connection(unrequested_connection_event(&endpoint).await);
        }

        assert_eq!(refiller.active_connection_count(), 1);
        assert_eq!(refiller.excess_connections.len(), 1);
        assert_eq!(metrics.get_total_connections(), 2);

        metrics.inc_total_connections();
        let event = unrequested_connection_event(&endpoint).await;
        refiller
            .ready_connections
            .push(futures::future::ready(event).boxed());
        let (use_keyspace_sender, use_keyspace_receiver) = mpsc::channel(1);
        let worker = tokio::spawn(refiller.run(use_keyspace_receiver));

        tokio::time::timeout(Duration::from_secs(5), async {
            while metrics.get_total_connections() != 1 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();

        drop(use_keyspace_sender);
        worker.await.unwrap();
        assert_eq!(metrics.get_total_connections(), 1);
        let _ = proxy.finish().await;
    }

    /// Opens a connection to the simulated node and wraps it in the event that the pool would
    /// receive had the connection been opened by an advanced shard awareness attempt targeting
    /// `shard` with a source port computed using `sharder`.
    async fn shard_aware_attempt_result(
        endpoint: &UntranslatedEndpoint,
        shard: Shard,
        sharder: &Sharder,
    ) -> OpenedConnectionEvent {
        let result = open_connection(endpoint, None, &HostConnectionConfig::default()).await;
        OpenedConnectionEvent {
            result: Ok(result.unwrap()),
            requested_shard: Some(RequestedShard {
                shard,
                sharder: sharder.clone(),
            }),
            keyspace_name: None,
        }
    }

    /// A refiller in the state it would be in after learning the node's sharding.
    fn refiller_aware_of(sharder: &Sharder) -> PoolRefiller {
        let mut refiller = mock_pool_refiller();
        refiller.conns = vec![Vec::new(); sharder.nr_shards.get() as usize];
        refiller.sharder = Some(sharder.clone());
        refiller.shard_aware_port = Some(SIMULATED_SHARD_AWARE_PORT);
        refiller
    }

    /// Verifies the detection itself: a connection that reports a shard other than the requested
    /// one must put advanced shard awareness on hold, while a matching one must not.
    #[tokio::test]
    async fn shard_mismatch_blocks_advanced_shard_awareness() {
        setup_tracing();

        // The simulated node always reports shard 0.
        let shard_info = ShardInfo {
            shard: 0,
            nr_shards: ShardCount::new(4).unwrap(),
            msb_ignore: 12,
        };
        let sharder = shard_info.get_sharder();
        let (proxy, endpoint) = start_simulated_node(shard_info).await;

        let mut refiller = refiller_aware_of(&sharder);

        // A connection that landed on the requested shard proves nothing about the network setup.
        refiller.handle_ready_connection(shard_aware_attempt_result(&endpoint, 0, &sharder).await);
        assert!(refiller.can_use_shard_aware_port());

        // A connection that landed elsewhere means that our source port did not reach the node
        // intact, so advanced shard awareness must be put on hold.
        refiller.handle_ready_connection(shard_aware_attempt_result(&endpoint, 1, &sharder).await);
        assert!(!refiller.can_use_shard_aware_port());

        // The refiller holds the connections, so drop it before the proxy stops serving them.
        drop(refiller);
        let _ = proxy.finish().await;
    }

    /// A node that reshards invalidates the source ports of the attempts that are still in flight:
    /// they were computed for a shard count the node no longer uses, so the shard they land on has
    /// nothing to do with whether the source port survived the way to the node. Such a mismatch
    /// must not block advanced shard awareness - and that must hold for every attempt of the
    /// obsolete generation, not just for the first one to arrive, even though that first one
    /// already makes the pool adopt the new sharder.
    #[tokio::test]
    async fn resharding_does_not_block_advanced_shard_awareness() {
        setup_tracing();

        // The node used to have 4 shards; it now reports 2 shards and always shard 0.
        let stale_sharder = Sharder::new(ShardCount::new(4).unwrap(), 12);
        let shard_info = ShardInfo {
            shard: 0,
            nr_shards: ShardCount::new(2).unwrap(),
            msb_ignore: 12,
        };
        let sharder = shard_info.get_sharder();
        let (proxy, endpoint) = start_simulated_node(shard_info).await;

        // The pool still believes in the old topology, as do the attempts it started.
        let mut refiller = refiller_aware_of(&stale_sharder);

        // First attempt of the obsolete generation: the mismatch is explained by the reshard,
        // which the pool learns about from this very connection.
        refiller.handle_ready_connection(
            shard_aware_attempt_result(&endpoint, 1, &stale_sharder).await,
        );
        assert_eq!(refiller.sharder.as_ref(), Some(&sharder));
        assert!(refiller.can_use_shard_aware_port());

        // Second attempt of the same generation: the pool has already adopted the new sharder,
        // but this attempt's source port still comes from the old one, so the mismatch is still
        // no evidence against advanced shard awareness.
        refiller.handle_ready_connection(
            shard_aware_attempt_result(&endpoint, 1, &stale_sharder).await,
        );
        assert!(refiller.can_use_shard_aware_port());

        // An attempt whose source port was computed with the sharder the node actually uses is
        // evidence, though - and must block.
        refiller.handle_ready_connection(shard_aware_attempt_result(&endpoint, 1, &sharder).await);
        assert!(!refiller.can_use_shard_aware_port());

        drop(refiller);
        let _ = proxy.finish().await;
    }
}
