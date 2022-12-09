use uuid::Uuid;

/// Node represents a cluster node along with it's data and connections
use crate::routing::{Sharder, Token};
use crate::transport::connection::Connection;
use crate::transport::connection::VerifiedKeyspaceName;
use crate::transport::connection_pool::{NodeConnectionPool, PoolConfig};
use crate::transport::errors::QueryError;

use std::{
    hash::{Hash, Hasher},
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy)]
pub struct TimestampedAverage {
    pub timestamp: Instant,
    pub average: Duration,
    pub num_measures: usize,
}

impl TimestampedAverage {
    pub(crate) fn compute_next(previous: Option<Self>, last_latency: Duration) -> Option<Self> {
        let now = Instant::now();
        match previous {
            prev if last_latency.is_zero() => prev,
            None => Some(Self {
                num_measures: 1,
                average: last_latency,
                timestamp: now,
            }),
            Some(prev_avg) => Some({
                let delay = (now - prev_avg.timestamp).as_secs_f64();
                let prev_weight = (delay + 1.).ln() / delay;
                let last_latency_nanos = last_latency.as_nanos() as f64;
                let prev_avg_nanos = prev_avg.average.as_nanos() as f64;
                let average = Duration::from_nanos(
                    ((1. - prev_weight) * last_latency_nanos + prev_weight * prev_avg_nanos).round()
                        as u64,
                );
                Self {
                    num_measures: prev_avg.num_measures + 1,
                    timestamp: now,
                    average,
                }
            }),
        }
    }
}

/// Node represents a cluster node along with it's data and connections
#[derive(Debug)]
pub struct Node {
    pub host_id: Uuid,
    pub address: SocketAddr,
    pub datacenter: Option<String>,
    pub rack: Option<String>,

    pub average_latency: RwLock<Option<TimestampedAverage>>,

    // If the node is filtered out by the host filter, this will be None
    pool: Option<NodeConnectionPool>,

    down_marker: AtomicBool,
}

impl Node {
    /// Creates new node which starts connecting in the background
    /// # Arguments
    ///
    /// `address` - address to connect to
    /// `compression` - preferred compression to use
    /// `datacenter` - optional datacenter name
    /// `rack` - optional rack name
    pub(crate) fn new(
        host_id: Uuid,
        address: SocketAddr,
        pool_config: PoolConfig,
        datacenter: Option<String>,
        rack: Option<String>,
        keyspace_name: Option<VerifiedKeyspaceName>,
        enabled: bool,
    ) -> Self {
        let pool = enabled.then(|| {
            NodeConnectionPool::new(address.ip(), address.port(), pool_config, keyspace_name)
        });

        Node {
            host_id,
            address,
            datacenter,
            rack,
            pool,
            down_marker: false.into(),
            average_latency: RwLock::new(None),
        }
    }

    pub fn sharder(&self) -> Option<Sharder> {
        self.pool.as_ref()?.sharder()
    }

    /// Get connection which should be used to connect using given token
    /// If this connection is broken get any random connection to this Node
    pub(crate) async fn connection_for_token(
        &self,
        token: Token,
    ) -> Result<Arc<Connection>, QueryError> {
        self.get_pool()?.connection_for_token(token)
    }

    /// Get random connection
    pub(crate) async fn random_connection(&self) -> Result<Arc<Connection>, QueryError> {
        self.get_pool()?.random_connection()
    }

    pub fn is_down(&self) -> bool {
        self.down_marker.load(Ordering::Relaxed)
    }

    /// Returns a boolean which indicates whether this node was is enabled.
    /// Only enabled nodes will have connections open. For disabled nodes,
    /// no connections will be opened.
    pub fn is_enabled(&self) -> bool {
        self.pool.is_some()
    }

    pub(crate) fn change_down_marker(&self, is_down: bool) {
        self.down_marker.store(is_down, Ordering::Relaxed);
    }

    pub(crate) async fn use_keyspace(
        &self,
        keyspace_name: VerifiedKeyspaceName,
    ) -> Result<(), QueryError> {
        if let Some(pool) = &self.pool {
            pool.use_keyspace(keyspace_name).await?;
        }
        Ok(())
    }

    pub(crate) fn get_working_connections(&self) -> Result<Vec<Arc<Connection>>, QueryError> {
        self.get_pool()?.get_working_connections()
    }

    pub(crate) async fn wait_until_pool_initialized(&self) {
        if let Some(pool) = &self.pool {
            pool.wait_until_initialized().await;
        }
    }

    fn get_pool(&self) -> Result<&NodeConnectionPool, QueryError> {
        self.pool.as_ref().ok_or_else(|| {
            QueryError::IoError(Arc::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "No connections in the pool: the node has been disabled \
                by the host filter",
            )))
        })
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address
    }
}

impl Eq for Node {}

impl Hash for Node {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.address.hash(state);
    }
}
