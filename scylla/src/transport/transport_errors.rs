use crate::frame::frame_errors;
use crate::frame::frame_errors::ParseError;
use crate::frame::response::cql_to_rust::FromRowError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TokenError {
    #[error("tokens value null")]
    NullTokens,
    #[error("tokens value type is not a set")]
    SetValue,
    #[error("tokens set contained a non-text value")]
    NonTextValue,
    #[error("failed to parse token {0}")]
    ParseFailure(String),
}

#[derive(Error, Debug)]
pub enum TopologyError {
    #[error("system.peers query returned no columns")]
    PeersColumnError,
    #[error("system.peers `peer` query returned wrong value type or null")]
    PeersValueTypeError,
    #[error("Expected rows result when querying system.peers")]
    PeersRowError,

    #[error("Expected rows result when querying system.local")]
    LocalExpectedRowResults,
    #[error("system.local query result empty")]
    LocalQueryResultEmpty,
    #[error("system.local `tokens` query returned no columns")]
    LocalTokensNoColumnsReturned,
}

#[derive(Error, Debug)]
pub enum InternalDriverError {
    #[error("Unexpected response!")]
    UnexpectedResponse,
    #[error("refreshing failed on every connection, {0}")]
    RefreshingFailedOnEveryConnections(String),
    #[error("system.local tokens empty on the node")]
    LocalTokensEmptyOnNodes,
    #[error("Pool Lock Poisoned: {0}")]
    PoolLockPoisoned(String),
    #[error("Updater Crashed: {0}")]
    UpdaterError(String),
    #[error("fatal error, broken invariant: no connections available")]
    FatalConnectionError,
}

#[derive(Error, Debug)]
pub enum PrepareError {
    #[error("Internal Driver Error")]
    InternalDriverError(#[from] InternalDriverError),
}

#[derive(Error, Debug)]
pub enum TransportError {
    #[error("Task queue closed")]
    TaskQueueClosed,
    #[error("Request dropped!")]
    DroppedRequest,
    #[error("failed to resolve {0}")]
    FailedToResolveAddress(String),
    #[error("Reprepared statement unexpectedly changed its id")]
    RepreparedStatmentIDChanged,
    #[error("Length of provided values ({0}) must be equal to number of batch statements ({1})")]
    ValueLenMismatch(usize, usize),
    #[error("Response is an error message: code: {0} message: {1}")]
    ErrorMsg(i32, String),

    #[error("Internal Driver Error")]
    InternalDriverError(#[from] InternalDriverError),

    #[error(transparent)]
    PrepareError(#[from] PrepareError),
    #[error(transparent)]
    ParseError(#[from] ParseError),
    #[error(transparent)]
    ToplogyError(#[from] TopologyError),
    #[error(transparent)]
    TokenError(#[from] TokenError),
    #[error(transparent)]
    StdIOError(#[from] std::io::Error),
    #[error(transparent)]
    FrameError(#[from] frame_errors::FrameError),
    #[error(transparent)]
    TokioSyncOneshotError(#[from] tokio::sync::oneshot::error::RecvError),
    #[error(transparent)]
    FromRowError(#[from] FromRowError),
}
