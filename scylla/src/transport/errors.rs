//! This module contains various errors which can be returned by [`Session`](crate::transport::session::Session).

// Re-export DbError type and types that it depends on
// so they can be found in `scylla::errors`.
pub use scylla_cql::frame::response::error::{DbError, OperationType, WriteType};

use std::{
    error::Error,
    io::ErrorKind,
    net::{AddrParseError, IpAddr, SocketAddr},
    num::ParseIntError,
    sync::Arc,
};

use scylla_cql::{
    frame::{
        frame_errors::{
            CqlAuthChallengeParseError, CqlAuthSuccessParseError, CqlAuthenticateParseError,
            CqlErrorParseError, CqlEventParseError, CqlRequestSerializationError,
            CqlResponseParseError, CqlResultParseError, CqlSupportedParseError,
            FrameBodyExtensionsParseError, FrameHeaderParseError,
        },
        request::CqlRequestKind,
        response::CqlResponseKind,
        value::SerializeValuesError,
    },
    types::{
        deserialize::{DeserializationError, TypeCheckError},
        serialize::SerializationError,
    },
};

use thiserror::Error;

use crate::{authentication::AuthError, frame::response};

use super::iterator::NextRowError;
#[allow(deprecated)]
use super::legacy_query_result::IntoLegacyQueryResultError;
use super::query_result::{IntoRowsResultError, SingleRowError};

/// Error that occurred during query execution
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
#[allow(deprecated)]
pub enum QueryError {
    /// Database sent a response containing some error with a message
    #[error("Database returned an error: {0}, Error message: {1}")]
    DbError(DbError, String),

    /// Caller passed an invalid query
    #[error(transparent)]
    BadQuery(#[from] BadQuery),

    /// Failed to serialize CQL request.
    #[error("Failed to serialize CQL request: {0}")]
    CqlRequestSerialization(#[from] CqlRequestSerializationError),

    /// Failed to deserialize frame body extensions.
    #[error(transparent)]
    BodyExtensionsParseError(#[from] FrameBodyExtensionsParseError),

    /// Load balancing policy returned an empty plan.
    #[error(
        "Load balancing policy returned an empty plan.\
        First thing to investigate should be the logic of custom LBP implementation.\
        If you think that your LBP implementation is correct, or you make use of `DefaultPolicy`,\
        then this is most probably a driver bug!"
    )]
    EmptyPlan,

    /// Received a RESULT server response, but failed to deserialize it.
    #[error(transparent)]
    CqlResultParseError(#[from] CqlResultParseError),

    /// Received an ERROR server response, but failed to deserialize it.
    #[error("Failed to deserialize ERROR response: {0}")]
    CqlErrorParseError(#[from] CqlErrorParseError),

    /// A metadata error occurred during schema agreement.
    #[error("Cluster metadata fetch error occurred during automatic schema agreement: {0}")]
    MetadataError(#[from] MetadataError),

    /// Selected node's connection pool is in invalid state.
    #[error("No connections in the pool: {0}")]
    ConnectionPoolError(#[from] ConnectionPoolError),

    /// Protocol error.
    #[error("Protocol error: {0}")]
    ProtocolError(#[from] ProtocolError),

    /// Timeout error has occurred, function didn't complete in time.
    #[error("Timeout Error")]
    TimeoutError,

    /// A connection has been broken during query execution.
    #[error(transparent)]
    BrokenConnection(#[from] BrokenConnectionError),

    /// Driver was unable to allocate a stream id to execute a query on.
    #[error("Unable to allocate stream id")]
    UnableToAllocStreamId,

    /// Client timeout occurred before any response arrived
    #[error("Request timeout: {0}")]
    RequestTimeout(String),

    // TODO: This should not belong here, but it requires changes to error types
    // returned in async iterator API. This should be handled in separate PR.
    // The reason this needs to be included is that topology.rs makes use of iter API and returns QueryError.
    // Once iter API is adjusted, we can then adjust errors returned by topology module (e.g. refactor MetadataError and not include it in QueryError).
    /// An error occurred during async iteration over rows of result.
    #[error("An error occurred during async iteration over rows of result: {0}")]
    NextRowError(#[from] NextRowError),

    /// Failed to convert [`QueryResult`][crate::transport::query_result::QueryResult]
    /// into [`LegacyQueryResult`][crate::transport::legacy_query_result::LegacyQueryResult].
    #[deprecated(
        since = "0.15.1",
        note = "Legacy deserialization API is inefficient and is going to be removed soon"
    )]
    #[allow(deprecated)]
    #[error("Failed to convert `QueryResult` into `LegacyQueryResult`: {0}")]
    IntoLegacyQueryResultError(#[from] IntoLegacyQueryResultError),
}

impl From<SerializeValuesError> for QueryError {
    fn from(serialized_err: SerializeValuesError) -> QueryError {
        QueryError::BadQuery(BadQuery::SerializeValuesError(serialized_err))
    }
}

impl From<SerializationError> for QueryError {
    fn from(serialized_err: SerializationError) -> QueryError {
        QueryError::BadQuery(BadQuery::SerializationError(serialized_err))
    }
}

impl From<tokio::time::error::Elapsed> for QueryError {
    fn from(timer_error: tokio::time::error::Elapsed) -> QueryError {
        QueryError::RequestTimeout(format!("{}", timer_error))
    }
}

impl From<UserRequestError> for QueryError {
    fn from(value: UserRequestError) -> Self {
        match value {
            UserRequestError::CqlRequestSerialization(e) => e.into(),
            UserRequestError::DbError(err, msg) => QueryError::DbError(err, msg),
            UserRequestError::CqlResultParseError(e) => e.into(),
            UserRequestError::CqlErrorParseError(e) => e.into(),
            UserRequestError::BrokenConnectionError(e) => e.into(),
            UserRequestError::UnexpectedResponse(response) => {
                ProtocolError::UnexpectedResponse(response).into()
            }
            UserRequestError::BodyExtensionsParseError(e) => e.into(),
            UserRequestError::UnableToAllocStreamId => QueryError::UnableToAllocStreamId,
            UserRequestError::RepreparedIdChanged {
                statement,
                expected_id,
                reprepared_id,
            } => ProtocolError::RepreparedIdChanged {
                statement,
                expected_id,
                reprepared_id,
            }
            .into(),
        }
    }
}

impl From<QueryError> for NewSessionError {
    fn from(query_error: QueryError) -> NewSessionError {
        match query_error {
            QueryError::DbError(e, msg) => NewSessionError::DbError(e, msg),
            QueryError::BadQuery(e) => NewSessionError::BadQuery(e),
            QueryError::CqlRequestSerialization(e) => NewSessionError::CqlRequestSerialization(e),
            QueryError::CqlResultParseError(e) => NewSessionError::CqlResultParseError(e),
            QueryError::CqlErrorParseError(e) => NewSessionError::CqlErrorParseError(e),
            QueryError::BodyExtensionsParseError(e) => NewSessionError::BodyExtensionsParseError(e),
            QueryError::EmptyPlan => NewSessionError::EmptyPlan,
            QueryError::MetadataError(e) => NewSessionError::MetadataError(e),
            QueryError::ConnectionPoolError(e) => NewSessionError::ConnectionPoolError(e),
            QueryError::ProtocolError(e) => NewSessionError::ProtocolError(e),
            QueryError::TimeoutError => NewSessionError::TimeoutError,
            QueryError::BrokenConnection(e) => NewSessionError::BrokenConnection(e),
            QueryError::UnableToAllocStreamId => NewSessionError::UnableToAllocStreamId,
            QueryError::RequestTimeout(msg) => NewSessionError::RequestTimeout(msg),
            #[allow(deprecated)]
            QueryError::IntoLegacyQueryResultError(e) => {
                NewSessionError::IntoLegacyQueryResultError(e)
            }
            QueryError::NextRowError(e) => NewSessionError::NextRowError(e),
        }
    }
}

impl From<BadKeyspaceName> for QueryError {
    fn from(keyspace_err: BadKeyspaceName) -> QueryError {
        QueryError::BadQuery(BadQuery::BadKeyspaceName(keyspace_err))
    }
}

impl From<response::Error> for QueryError {
    fn from(error: response::Error) -> QueryError {
        QueryError::DbError(error.error, error.reason)
    }
}

/// Error that occurred during session creation
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
#[allow(deprecated)]
pub enum NewSessionError {
    /// Failed to resolve hostname passed in Session creation
    #[error("Couldn't resolve any hostname: {0:?}")]
    FailedToResolveAnyHostname(Vec<String>),

    /// List of known nodes passed to Session constructor is empty
    /// There needs to be at least one node to connect to
    #[error("Empty known nodes list")]
    EmptyKnownNodesList,

    /// Database sent a response containing some error with a message
    #[error("Database returned an error: {0}, Error message: {1}")]
    DbError(DbError, String),

    /// Caller passed an invalid query
    #[error(transparent)]
    BadQuery(#[from] BadQuery),

    /// Failed to serialize CQL request.
    #[error("Failed to serialize CQL request: {0}")]
    CqlRequestSerialization(#[from] CqlRequestSerializationError),

    /// Load balancing policy returned an empty plan.
    #[error(
        "Load balancing policy returned an empty plan.\
        First thing to investigate should be the logic of custom LBP implementation.\
        If you think that your LBP implementation is correct, or you make use of `DefaultPolicy`,\
        then this is most probably a driver bug!"
    )]
    EmptyPlan,

    /// Failed to deserialize frame body extensions.
    #[error(transparent)]
    BodyExtensionsParseError(#[from] FrameBodyExtensionsParseError),

    /// Failed to perform initial cluster metadata fetch.
    #[error("Failed to perform initial cluster metadata fetch: {0}")]
    MetadataError(#[from] MetadataError),

    /// Received a RESULT server response, but failed to deserialize it.
    #[error(transparent)]
    CqlResultParseError(#[from] CqlResultParseError),

    /// Received an ERROR server response, but failed to deserialize it.
    #[error("Failed to deserialize ERROR response: {0}")]
    CqlErrorParseError(#[from] CqlErrorParseError),

    /// Selected node's connection pool is in invalid state.
    #[error("No connections in the pool: {0}")]
    ConnectionPoolError(#[from] ConnectionPoolError),

    /// Protocol error.
    #[error("Protocol error: {0}")]
    ProtocolError(#[from] ProtocolError),

    /// Timeout error has occurred, couldn't connect to node in time.
    #[error("Timeout Error")]
    TimeoutError,

    /// A connection has been broken during query execution.
    #[error(transparent)]
    BrokenConnection(#[from] BrokenConnectionError),

    /// Driver was unable to allocate a stream id to execute a query on.
    #[error("Unable to allocate stream id")]
    UnableToAllocStreamId,

    /// Client timeout occurred before a response arrived for some query
    /// during `Session` creation.
    #[error("Client timeout: {0}")]
    RequestTimeout(String),

    // TODO: This should not belong here, but it requires changes to error types
    // returned in async iterator API. This should be handled in separate PR.
    // The reason this needs to be included is that topology.rs makes use of iter API and returns QueryError.
    // Once iter API is adjusted, we can then adjust errors returned by topology module (e.g. refactor MetadataError and not include it in QueryError).
    /// An error occurred during async iteration over rows of result.
    #[error("An error occurred during async iteration over rows of result: {0}")]
    NextRowError(#[from] NextRowError),

    /// Failed to convert [`QueryResult`][crate::transport::query_result::QueryResult]
    /// into [`LegacyQueryResult`][crate::transport::legacy_query_result::LegacyQueryResult].
    #[deprecated(
        since = "0.15.1",
        note = "Legacy deserialization API is inefficient and is going to be removed soon"
    )]
    #[allow(deprecated)]
    #[error("Failed to convert `QueryResult` into `LegacyQueryResult`: {0}")]
    IntoLegacyQueryResultError(#[from] IntoLegacyQueryResultError),
}

/// A protocol error.
///
/// It indicates an inconsistency between CQL protocol
/// and server's behavior.
/// In some cases, it could also represent a misuse
/// of internal driver API - a driver bug.
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum ProtocolError {
    /// Received an unexpected response when RESULT or ERROR was expected.
    #[error(
        "Received unexpected response from the server: {0}. Expected RESULT or ERROR response."
    )]
    UnexpectedResponse(CqlResponseKind),

    /// Prepared statement id mismatch.
    #[error(
        "Prepared statement id mismatch between multiple connections - all result ids should be equal."
    )]
    PreparedStatementIdsMismatch,

    /// Prepared statement id changed after repreparation.
    #[error(
        "Prepared statement id changed after repreparation; md5 sum (computed from the query string) should stay the same;\
        Statement: \"{statement}\"; expected id: {expected_id:?}; reprepared id: {reprepared_id:?}"
    )]
    RepreparedIdChanged {
        statement: String,
        expected_id: Vec<u8>,
        reprepared_id: Vec<u8>,
    },

    /// USE KEYSPACE protocol error.
    #[error("USE KEYSPACE protocol error: {0}")]
    UseKeyspace(#[from] UseKeyspaceProtocolError),

    /// A protocol error appeared during schema version fetch.
    #[error("Schema version fetch protocol error: {0}")]
    SchemaVersionFetch(#[from] SchemaVersionFetchError),

    /// A result with nonfinished paging state received for unpaged query.
    #[error("Unpaged query returned a non-empty paging state! This is a driver-side or server-side bug.")]
    NonfinishedPagingState,

    /// Failed to parse CQL type.
    #[error("Failed to parse a CQL type '{typ}', at position {position}: {reason}")]
    InvalidCqlType {
        typ: String,
        position: usize,
        reason: String,
    },

    /// Unable extract a partition key based on prepared statement's metadata.
    #[error("Unable extract a partition key based on prepared statement's metadata")]
    PartitionKeyExtraction,

    /// A protocol error occurred during tracing info fetch.
    #[error("Tracing info fetch protocol error: {0}")]
    Tracing(#[from] TracingProtocolError),

    /// Driver tried to reprepare a statement in the batch, but the reprepared
    /// statement's id is not included in the batch.
    #[error("Reprepared statement's id does not exist in the batch.")]
    RepreparedIdMissingInBatch,
}

/// A protocol error that occurred during `USE KEYSPACE <>` request.
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum UseKeyspaceProtocolError {
    #[error("Keyspace name mismtach; expected: {expected_keyspace_name_lowercase}, received: {result_keyspace_name_lowercase}")]
    KeyspaceNameMismatch {
        expected_keyspace_name_lowercase: String,
        result_keyspace_name_lowercase: String,
    },
    #[error("Received unexpected response: {0}. Expected RESULT:Set_keyspace")]
    UnexpectedResponse(CqlResponseKind),
}

/// A protocol error that occurred during schema version fetch.
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum SchemaVersionFetchError {
    /// Failed to convert schema version query result into rows result.
    #[error("Failed to convert schema version query result into rows result: {0}")]
    TracesEventsIntoRowsResultError(IntoRowsResultError),

    /// Failed to deserialize a single row from schema version query response.
    #[error(transparent)]
    SingleRowError(SingleRowError),
}

/// A protocol error that occurred during tracing info fetch.
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum TracingProtocolError {
    /// Failed to convert result of system_traces.session query to rows result.
    #[error("Failed to convert result of system_traces.session query to rows result")]
    TracesSessionIntoRowsResultError(IntoRowsResultError),

    /// system_traces.session has invalid column type.
    #[error("system_traces.session has invalid column type: {0}")]
    TracesSessionInvalidColumnType(TypeCheckError),

    /// Response to system_traces.session failed to deserialize.
    #[error("Response to system_traces.session failed to deserialize: {0}")]
    TracesSessionDeserializationFailed(DeserializationError),

    /// Failed to convert result of system_traces.events query to rows result.
    #[error("Failed to convert result of system_traces.events query to rows result")]
    TracesEventsIntoRowsResultError(IntoRowsResultError),

    /// system_traces.events has invalid column type.
    #[error("system_traces.events has invalid column type: {0}")]
    TracesEventsInvalidColumnType(TypeCheckError),

    /// Response to system_traces.events failed to deserialize.
    #[error("Response to system_traces.events failed to deserialize: {0}")]
    TracesEventsDeserializationFailed(DeserializationError),

    /// All tracing queries returned an empty result.
    #[error(
        "All tracing queries returned an empty result, \
        maybe the trace information didn't propagate yet. \
        Consider configuring Session with \
        a longer fetch interval (tracing_info_fetch_interval)"
    )]
    EmptyResults,
}

/// An error that occurred during cluster metadata fetch.
///
/// An error can occur during metadata fetch of:
/// - peers
/// - keyspaces
/// - UDTs
/// - tables
/// - views
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum MetadataError {
    /// Bad peers metadata.
    #[error("Bad peers metadata: {0}")]
    Peers(#[from] PeersMetadataError),

    /// Bad keyspaces metadata.
    #[error("Bad keyspaces metadata: {0}")]
    Keyspaces(#[from] KeyspacesMetadataError),

    /// Bad UDTs metadata.
    #[error("Bad UDTs metadata: {0}")]
    Udts(#[from] UdtMetadataError),

    /// Bad tables metadata.
    #[error("Bad tables metadata: {0}")]
    Tables(#[from] TablesMetadataError),

    /// Bad views metadata.
    #[error("Bad views metadata: {0}")]
    Views(#[from] ViewsMetadataError),
}

/// An error that occurred during peers metadata fetch.
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum PeersMetadataError {
    /// system.peers has invalid column type.
    #[error("system.peers has invalid column type: {0}")]
    SystemPeersInvalidColumnType(TypeCheckError),

    /// system.local has invalid column type.
    #[error("system.local has invalid column type: {0}")]
    SystemLocalInvalidColumnType(TypeCheckError),

    /// Empty peers list returned during peers metadata fetch.
    #[error("Peers list is empty")]
    EmptyPeers,

    /// All peers have empty token lists.
    #[error("All peers have empty token lists")]
    EmptyTokenLists,
}

/// An error that occurred during keyspaces metadata fetch.
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum KeyspacesMetadataError {
    /// system_schema.keyspaces has invalid column type.
    #[error("system_schema.keyspaces has invalid column type: {0}")]
    SchemaKeyspacesInvalidColumnType(TypeCheckError),

    /// Bad keyspace replication strategy.
    #[error("Bad keyspace <{keyspace}> replication strategy: {error}")]
    Strategy {
        keyspace: String,
        error: KeyspaceStrategyError,
    },
}

/// An error that occurred during specific keyspace's metadata fetch.
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum KeyspaceStrategyError {
    /// Keyspace strategy map missing a `class` field.
    #[error("keyspace strategy definition is missing a 'class' field")]
    MissingClassForStrategyDefinition,

    /// Missing replication factor for SimpleStrategy.
    #[error("Missing replication factor field for SimpleStrategy")]
    MissingReplicationFactorForSimpleStrategy,

    /// Replication factor could not be parsed as unsigned integer.
    #[error("Failed to parse a replication factor as unsigned integer: {0}")]
    ReplicationFactorParseError(ParseIntError),

    /// Received an unexpected NTS option.
    /// Driver expects only 'class' and replication factor per dc ('dc': rf)
    #[error("Unexpected NetworkTopologyStrategy option: '{key}': '{value}'")]
    UnexpectedNetworkTopologyStrategyOption { key: String, value: String },
}

/// An error that occurred during UDTs metadata fetch.
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum UdtMetadataError {
    /// system_schema.types has invalid column type.
    #[error("system_schema.types has invalid column type: {0}")]
    SchemaTypesInvalidColumnType(TypeCheckError),

    /// Circular UDT dependency detected.
    #[error("Detected circular dependency between user defined types - toposort is impossible!")]
    CircularTypeDependency,
}

/// An error that occurred during tables metadata fetch.
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum TablesMetadataError {
    /// system_schema.tables has invalid column type.
    #[error("system_schema.tables has invalid column type: {0}")]
    SchemaTablesInvalidColumnType(TypeCheckError),

    /// system_schema.columns has invalid column type.
    #[error("system_schema.columns has invalid column type: {0}")]
    SchemaColumnsInvalidColumnType(TypeCheckError),

    /// Unknown column kind.
    #[error("Unknown column kind '{column_kind}' for {keyspace_name}.{table_name}.{column_name}")]
    UnknownColumnKind {
        keyspace_name: String,
        table_name: String,
        column_name: String,
        column_kind: String,
    },
}

/// An error that occurred during views metadata fetch.
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum ViewsMetadataError {
    /// system_schema.views has invalid column type.
    #[error("system_schema.views has invalid column type: {0}")]
    SchemaViewsInvalidColumnType(TypeCheckError),
}

/// Error caused by caller creating an invalid query
#[derive(Error, Debug, Clone)]
#[error("Invalid query passed to Session")]
#[non_exhaustive]
pub enum BadQuery {
    /// Failed to serialize values passed to a query - values too big
    #[error("Serializing values failed: {0} ")]
    SerializeValuesError(#[from] SerializeValuesError),

    #[error("Serializing values failed: {0} ")]
    SerializationError(#[from] SerializationError),

    /// Serialized values are too long to compute partition key
    #[error("Serialized values are too long to compute partition key! Length: {0}, Max allowed length: {1}")]
    ValuesTooLongForKey(usize, usize),

    /// Passed invalid keyspace name to use
    #[error("Passed invalid keyspace name to use: {0}")]
    BadKeyspaceName(#[from] BadKeyspaceName),

    /// Too many queries in the batch statement
    #[error("Number of Queries in Batch Statement supplied is {0} which has exceeded the max value of 65,535")]
    TooManyQueriesInBatchStatement(usize),

    /// Other reasons of bad query
    #[error("{0}")]
    Other(String),
}

/// Invalid keyspace name given to `Session::use_keyspace()`
#[derive(Debug, Error, Clone)]
#[non_exhaustive]
pub enum BadKeyspaceName {
    /// Keyspace name is empty
    #[error("Keyspace name is empty")]
    Empty,

    /// Keyspace name too long, must be up to 48 characters
    #[error("Keyspace name too long, must be up to 48 characters, found {1} characters. Bad keyspace name: '{0}'")]
    TooLong(String, usize),

    /// Illegal character - only alphanumeric and underscores allowed.
    #[error("Illegal character found: '{1}', only alphanumeric and underscores allowed. Bad keyspace name: '{0}'")]
    IllegalCharacter(String, char),
}

/// An error that occurred when selecting a node connection
/// to perform a request on.
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum ConnectionPoolError {
    /// A connection pool is broken. Includes an error of a last connection.
    #[error("The pool is broken; Last connection failed with: {last_connection_error}")]
    Broken {
        last_connection_error: ConnectionError,
    },

    /// A connection pool is still being initialized.
    #[error("Pool is still being initialized")]
    Initializing,

    /// A corresponding node was disabled by a host filter.
    #[error("The node has been disabled by a host filter")]
    NodeDisabledByHostFilter,
}

/// An error that appeared on a connection level.
/// It indicated that connection can no longer be used
/// and should be dropped.
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum ConnectionError {
    /// Provided connect timeout elapsed.
    #[error("Connect timeout elapsed")]
    ConnectTimeout,

    /// Input/Output error occurred.
    #[error(transparent)]
    IoError(Arc<std::io::Error>),

    /// Driver was unable to find a free source port for given shard.
    #[error("Could not find free source port for shard {0}")]
    NoSourcePortForShard(u32),

    /// Failed to translate an address before establishing a connection.
    #[error("Address translation failed: {0}")]
    TranslationError(#[from] TranslationError),

    /// A connection has been broken after being established.
    #[error(transparent)]
    BrokenConnection(#[from] BrokenConnectionError),

    /// A request required to initialize a connection failed.
    #[error(transparent)]
    ConnectionSetupRequestError(#[from] ConnectionSetupRequestError),
}

impl From<std::io::Error> for ConnectionError {
    fn from(value: std::io::Error) -> Self {
        ConnectionError::IoError(Arc::new(value))
    }
}

impl ConnectionError {
    /// Checks if this error indicates that a chosen source port/address cannot be bound.
    /// This is caused by one of the following:
    /// - The source address is already used by another socket,
    /// - The source address is reserved and the process does not have sufficient privileges to use it.
    pub fn is_address_unavailable_for_use(&self) -> bool {
        if let ConnectionError::IoError(io_error) = self {
            match io_error.kind() {
                ErrorKind::AddrInUse | ErrorKind::PermissionDenied => return true,
                _ => {}
            }
        }

        false
    }
}

/// Error caused by failed address translation done before establishing connection
#[non_exhaustive]
#[derive(Debug, Clone, Error)]
pub enum TranslationError {
    /// Driver failed to find a translation rule for a provided address.
    #[error("No rule for address {0}")]
    NoRuleForAddress(SocketAddr),

    /// A translation rule for a provided address was found, but the translated address was invalid.
    #[error("Failed to parse translated address: {translated_addr_str}, reason: {reason}")]
    InvalidAddressInRule {
        translated_addr_str: &'static str,
        reason: AddrParseError,
    },
}

/// An error that occurred during connection setup request execution.
/// It indicates that request needed to initiate a connection failed.
#[derive(Error, Debug, Clone)]
#[error("Failed to perform a connection setup request. Request: {request_kind}, reason: {error}")]
pub struct ConnectionSetupRequestError {
    request_kind: CqlRequestKind,
    error: ConnectionSetupRequestErrorKind,
}

#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum ConnectionSetupRequestErrorKind {
    /// Failed to serialize CQL request.
    #[error("Failed to serialize CQL request: {0}")]
    CqlRequestSerialization(#[from] CqlRequestSerializationError),

    /// Failed to deserialize frame body extensions.
    #[error(transparent)]
    BodyExtensionsParseError(#[from] FrameBodyExtensionsParseError),

    /// Driver was unable to allocate a stream id to execute a setup request on.
    #[error("Unable to allocate stream id")]
    UnableToAllocStreamId,

    /// A connection was broken during setup request execution.
    #[error(transparent)]
    BrokenConnection(#[from] BrokenConnectionError),

    /// Received a server error in response to connection setup request.
    #[error("Database returned an error: {0}, Error message: {1}")]
    DbError(DbError, String),

    /// Received an unexpected response from the server.
    #[error("Received unexpected response from the server: {0}")]
    UnexpectedResponse(CqlResponseKind),

    /// Received a response to OPTIONS request, but failed to deserialize its body.
    #[error("Failed to deserialize SUPPORTED response: {0}")]
    CqlSupportedParseError(#[from] CqlSupportedParseError),

    /// Received an AUTHENTICATE response, but failed to deserialize its body.
    #[error("Failed to deserialize AUTHENTICATE response: {0}")]
    CqlAuthenticateParseError(#[from] CqlAuthenticateParseError),

    /// Received an AUTH_SUCCESS response, but failed to deserialize its body.
    #[error("Failed to deserialize AUTH_SUCCESS response: {0}")]
    CqlAuthSuccessParseError(#[from] CqlAuthSuccessParseError),

    /// Received an AUTH_CHALLENGE response, but failed to deserialize its body.
    #[error("Failed to deserialize AUTH_CHALLENGE response: {0}")]
    CqlAuthChallengeParseError(#[from] CqlAuthChallengeParseError),

    /// Received server ERROR response, but failed to deserialize its body.
    #[error("Failed to deserialize ERROR response: {0}")]
    CqlErrorParseError(#[from] CqlErrorParseError),

    /// An error returned by [`AuthenticatorProvider::start_authentication_session`](crate::authentication::AuthenticatorProvider::start_authentication_session).
    #[error("Failed to start client's auth session: {0}")]
    StartAuthSessionError(AuthError),

    /// An error returned by [`AuthenticatorSession::evaluate_challenge`](crate::authentication::AuthenticatorSession::evaluate_challenge).
    #[error("Failed to evaluate auth challenge on client side: {0}")]
    AuthChallengeEvaluationError(AuthError),

    /// An error returned by [`AuthenticatorSession::success`](crate::authentication::AuthenticatorSession::success).
    #[error("Failed to finish auth challenge on client side: {0}")]
    AuthFinishError(AuthError),

    /// User did not provide authentication while the cluster requires it.
    /// See [`SessionBuilder::user`](crate::transport::session_builder::SessionBuilder::user)
    /// and/or [`SessionBuilder::authenticator_provider`](crate::transport::session_builder::SessionBuilder::authenticator_provider).
    #[error("Authentication is required. You can use SessionBuilder::user(\"user\", \"pass\") to provide credentials or SessionBuilder::authenticator_provider to provide custom authenticator")]
    MissingAuthentication,
}

impl ConnectionSetupRequestError {
    pub(crate) fn new(
        request_kind: CqlRequestKind,
        error: ConnectionSetupRequestErrorKind,
    ) -> Self {
        ConnectionSetupRequestError {
            request_kind,
            error,
        }
    }

    pub fn get_error(&self) -> &ConnectionSetupRequestErrorKind {
        &self.error
    }
}

/// An error indicating that a connection was broken.
/// Possible error reasons:
/// - keepalive query errors - driver failed to sent a keepalive query, or the query timed out
/// - received a frame with unexpected stream id
/// - failed to handle a server event (message received on stream -1)
/// - some low-level IO errors - e.g. driver failed to write data via socket
#[derive(Error, Debug, Clone)]
#[error("Connection broken, reason: {0}")]
pub struct BrokenConnectionError(Arc<dyn Error + Sync + Send>);

impl BrokenConnectionError {
    /// Retrieve an error reason by downcasting to specific type.
    pub fn downcast_ref<T: Error + 'static>(&self) -> Option<&T> {
        self.0.downcast_ref()
    }
}

/// A reason why connection was broken.
///
/// See [`BrokenConnectionError::downcast_ref()`].
/// You can retrieve the actual type by downcasting `Arc<dyn Error>`.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum BrokenConnectionErrorKind {
    /// Driver sent a keepalive request to the database, but the request timed out.
    #[error("Timed out while waiting for response to keepalive request on connection to node {0}")]
    KeepaliveTimeout(IpAddr),

    /// Driver sent a keepalive request to the database, but request execution failed.
    #[error("Failed to execute keepalive query: {0}")]
    KeepaliveQueryError(RequestError),

    /// Failed to deserialize response frame header.
    #[error("Failed to deserialize frame: {0}")]
    FrameHeaderParseError(FrameHeaderParseError),

    /// Failed to handle a CQL event (server response received on stream -1).
    #[error("Failed to handle server event: {0}")]
    CqlEventHandlingError(#[from] CqlEventHandlingError),

    /// Received a server frame with unexpected stream id.
    #[error("Received a server frame with unexpected stream id: {0}")]
    UnexpectedStreamId(i16),

    /// IO error - server failed to write data to the socket.
    #[error("Failed to write data: {0}")]
    WriteError(std::io::Error),

    /// Maximum number of orphaned streams exceeded.
    #[error("Too many orphaned stream ids: {0}")]
    TooManyOrphanedStreamIds(u16),

    /// Failed to send data via tokio channel. This implies
    /// that connection was probably already broken for some other reason.
    #[error(
        "Failed to send/receive data needed to perform a request via tokio channel.
        It implies that other half of the channel has been dropped.
        The connection was already broken for some other reason."
    )]
    ChannelError,
}

impl From<BrokenConnectionErrorKind> for BrokenConnectionError {
    fn from(value: BrokenConnectionErrorKind) -> Self {
        BrokenConnectionError(Arc::new(value))
    }
}

/// Failed to handle a CQL event received on a stream -1.
/// Possible error kinds are:
/// - failed to deserialize response's frame header
/// - failed to deserialize CQL event response
/// - received invalid server response
/// - failed to send an event info via channel (connection is probably broken)
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum CqlEventHandlingError {
    /// Received an EVENT server response, but failed to deserialize it.
    #[error("Failed to deserialize EVENT response: {0}")]
    CqlEventParseError(#[from] CqlEventParseError),

    /// Received an unexpected response on stream -1.
    #[error("Received unexpected server response on stream -1: {0}. Expected EVENT response")]
    UnexpectedResponse(CqlResponseKind),

    /// Failed to deserialize body extensions of frame received on stream -1.
    #[error("Failed to deserialize a header of frame received on stream -1: {0}")]
    BodyExtensionParseError(#[from] FrameBodyExtensionsParseError),

    /// Driver failed to send event data between the internal tasks.
    /// It implies that connection was broken for some reason.
    #[error("Failed to send event info via channel. The channel is probably closed, which is caused by connection being broken")]
    SendError,
}

/// An error type that occurred when executing one of:
/// - QUERY
/// - PREPARE
/// - EXECUTE
/// - BATCH
///
/// requests.
#[derive(Error, Debug)]
pub(crate) enum UserRequestError {
    #[error("Failed to serialize CQL request: {0}")]
    CqlRequestSerialization(#[from] CqlRequestSerializationError),
    #[error("Database returned an error: {0}, Error message: {1}")]
    DbError(DbError, String),
    #[error(transparent)]
    CqlResultParseError(#[from] CqlResultParseError),
    #[error("Failed to deserialize ERROR response: {0}")]
    CqlErrorParseError(#[from] CqlErrorParseError),
    #[error(
        "Received unexpected response from the server: {0}. Expected RESULT or ERROR response."
    )]
    UnexpectedResponse(CqlResponseKind),
    #[error(transparent)]
    BrokenConnectionError(#[from] BrokenConnectionError),
    #[error(transparent)]
    BodyExtensionsParseError(#[from] FrameBodyExtensionsParseError),
    #[error("Unable to allocate stream id")]
    UnableToAllocStreamId,
    #[error(
        "Prepared statement id changed after repreparation; md5 sum (computed from the query string) should stay the same;\
        Statement: \"{statement}\"; expected id: {expected_id:?}; reprepared id: {reprepared_id:?}"
    )]
    RepreparedIdChanged {
        statement: String,
        expected_id: Vec<u8>,
        reprepared_id: Vec<u8>,
    },
}

impl From<response::error::Error> for UserRequestError {
    fn from(value: response::error::Error) -> Self {
        UserRequestError::DbError(value.error, value.reason)
    }
}

impl From<RequestError> for UserRequestError {
    fn from(value: RequestError) -> Self {
        match value {
            RequestError::CqlRequestSerialization(e) => e.into(),
            RequestError::BodyExtensionsParseError(e) => e.into(),
            RequestError::CqlResponseParseError(e) => match e {
                // Only possible responses are RESULT and ERROR. If we failed parsing
                // other response, treat it as unexpected response.
                CqlResponseParseError::CqlErrorParseError(e) => e.into(),
                CqlResponseParseError::CqlResultParseError(e) => e.into(),
                _ => UserRequestError::UnexpectedResponse(e.to_response_kind()),
            },
            RequestError::BrokenConnection(e) => e.into(),
            RequestError::UnableToAllocStreamId => UserRequestError::UnableToAllocStreamId,
        }
    }
}

/// An error that occurred when performing a request.
///
/// Possible error kinds:
/// - Connection is broken
/// - Response's frame header deserialization error
/// - CQL response (frame body) deserialization error
/// - Driver was unable to allocate a stream id for a request
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum RequestError {
    /// Failed to serialize CQL request.
    #[error("Failed to serialize CQL request: {0}")]
    CqlRequestSerialization(#[from] CqlRequestSerializationError),

    /// Failed to deserialize frame body extensions.
    #[error(transparent)]
    BodyExtensionsParseError(#[from] FrameBodyExtensionsParseError),

    /// Failed to deserialize a CQL response (frame body).
    #[error(transparent)]
    CqlResponseParseError(#[from] CqlResponseParseError),

    /// A connection was broken during request execution.
    #[error(transparent)]
    BrokenConnection(#[from] BrokenConnectionError),

    /// Driver was unable to allocate a stream id to execute a request on.
    #[error("Unable to allocate a stream id")]
    UnableToAllocStreamId,
}

impl From<ResponseParseError> for RequestError {
    fn from(value: ResponseParseError) -> Self {
        match value {
            ResponseParseError::BodyExtensionsParseError(e) => e.into(),
            ResponseParseError::CqlResponseParseError(e) => e.into(),
        }
    }
}

/// An error type returned from `Connection::parse_response`.
/// This is driver's internal type.
#[derive(Error, Debug)]
pub(crate) enum ResponseParseError {
    #[error(transparent)]
    BodyExtensionsParseError(#[from] FrameBodyExtensionsParseError),
    #[error(transparent)]
    CqlResponseParseError(#[from] CqlResponseParseError),
}

#[cfg(test)]
mod tests {
    use scylla_cql::Consistency;

    use crate::transport::errors::{DbError, QueryError, WriteType};

    #[test]
    fn write_type_from_str() {
        let test_cases: [(&str, WriteType); 9] = [
            ("SIMPLE", WriteType::Simple),
            ("BATCH", WriteType::Batch),
            ("UNLOGGED_BATCH", WriteType::UnloggedBatch),
            ("COUNTER", WriteType::Counter),
            ("BATCH_LOG", WriteType::BatchLog),
            ("CAS", WriteType::Cas),
            ("VIEW", WriteType::View),
            ("CDC", WriteType::Cdc),
            ("SOMEOTHER", WriteType::Other("SOMEOTHER".to_string())),
        ];

        for (write_type_str, expected_write_type) in &test_cases {
            let write_type = WriteType::from(*write_type_str);
            assert_eq!(write_type, *expected_write_type);
        }
    }

    // A test to check that displaying DbError and QueryError::DbError works as expected
    // - displays error description
    // - displays error parameters
    // - displays error message
    // - indented multiline strings don't cause whitespace gaps
    #[test]
    fn dberror_full_info() {
        // Test that DbError::Unavailable is displayed correctly
        let db_error = DbError::Unavailable {
            consistency: Consistency::Three,
            required: 3,
            alive: 2,
        };

        let db_error_displayed: String = format!("{}", db_error);

        let mut expected_dberr_msg =
            "Not enough nodes are alive to satisfy required consistency level ".to_string();
        expected_dberr_msg += "(consistency: Three, required: 3, alive: 2)";

        assert_eq!(db_error_displayed, expected_dberr_msg);

        // Test that QueryError::DbError::(DbError::Unavailable) is displayed correctly
        let query_error =
            QueryError::DbError(db_error, "a message about unavailable error".to_string());
        let query_error_displayed: String = format!("{}", query_error);

        let mut expected_querr_msg = "Database returned an error: ".to_string();
        expected_querr_msg += &expected_dberr_msg;
        expected_querr_msg += ", Error message: a message about unavailable error";

        assert_eq!(query_error_displayed, expected_querr_msg);
    }
}
