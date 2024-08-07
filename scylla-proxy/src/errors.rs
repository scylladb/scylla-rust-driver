use std::net::SocketAddr;

use scylla_cql::frame::frame_errors::{FrameHeaderParseError, LowLevelDeserializationError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DoorkeeperError {
    #[error("Listen on {0} failed with {1}")]
    Listen(SocketAddr, std::io::Error),
    #[error("Connection attempt from driver to proxy {0} failed with {1}")]
    DriverConnectionAttempt(SocketAddr, std::io::Error),
    #[error("Connection to node {0} failed with {1}")]
    NodeConnectionAttempt(SocketAddr, std::io::Error),
    #[error("Could not create TCP socket: {0}")]
    SocketCreate(std::io::Error),
    #[error("Could not bind socket to ephemeral port: {0}")]
    SocketBind(std::io::Error),
    #[error("Shard-aware connection failed, because no more possible ports are left")]
    NoMorePorts,
    #[error("Could not send Options frame for obtaining shards number: {0}")]
    ObtainingShardNumber(std::io::Error),
    #[error("Could not send read Supported frame for obtaining shards number: {0}")]
    ObtainingShardNumberFrame(FrameHeaderParseError),
    #[error("Could not read Supported options: {0}")]
    ObtainingShardNumberParseOptions(LowLevelDeserializationError),
    #[error("ShardInfo parameters missing")]
    ObtainingShardNumberNoShardInfo,
    #[error("Could not parse shard number: {0}")]
    ObtainingShardNumberParseShardNumber(std::num::ParseIntError),
    #[error("0 as number of shards!")]
    ObtainingShardNumberGotZero,
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("Driver {0} disconnected")]
    DriverDisconnected(SocketAddr),
    #[error("Node {0} disconnected")]
    NodeDisconnected(SocketAddr),
}

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("Doorkeeper failed: {0}")]
    Doorkeeper(DoorkeeperError),
    #[error("Worker failed: {0}")]
    Worker(WorkerError),
    #[error("Could not await proxy finish: {0}")]
    AwaitFinishFailure(String),
    #[error("All error reporting channels have already been closed")]
    SanityCheckFailure,
}

impl From<WorkerError> for ProxyError {
    fn from(err: WorkerError) -> Self {
        ProxyError::Worker(err)
    }
}

impl From<DoorkeeperError> for ProxyError {
    fn from(err: DoorkeeperError) -> Self {
        ProxyError::Doorkeeper(err)
    }
}
