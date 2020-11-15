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
pub enum ConnectionError {
    #[error("Task queue closed")]
    TaskQueueClosed,
    #[error("Request dropped!")]
    DroppedRequest,
    #[error("Unexpected frame!")]
    UnexpectedFrame,
    #[error("Unexpected response!")]
    UnexpectedResponse,
    #[error("refreshing failed on every connection, {0}")]
    RefreshingFailedOnEveryConnections(String),
    #[error("failed to resolve {0}")]
    FailedToResolveAddress(String),
    #[error("system.local tokens empty on the node")]
    LocalTokensEmptyOnNodes,
    #[error("Reprepared statement unexpectedly changed its id")]
    RepreparedStatmentIDChanged,
    #[error("Pool Lock Poisoned: {0}")]
    PoolLockPoisoned(String),
    #[error("Upadter Crashed: {0}")]
    UpdaterError(String),
    #[error("fatal error, broken invariant: no connections available")]
    FatalConnectionError,
    #[error("Length of provided values ({0}) must be equal to number of batch statements ({1})")]
    ValueLenMismatch(usize, usize),
    #[error("Response is an error message: code: {0} message: {1}")]
    ErrorMsg(i32, String),

    #[error("Request error")]
    ParseError(#[from] ParseError),
    #[error("Topology error")]
    ToplogyError(#[from] TopologyError),
    #[error("Token error")]
    TokenError(#[from] TokenError),
    #[error("std io error encountered while connecting")]
    StdIOError(#[from] std::io::Error),
    #[error("Frame mod error encountered while connecting")]
    FrameError(#[from] frame_errors::FrameError),
    #[error("Tokio sync oneshot error recieved")]
    TokioSyncOneshotError(#[from] tokio::sync::oneshot::error::RecvError),
    #[error("From Row Error")]
    FromRowError(#[from] FromRowError),
}
