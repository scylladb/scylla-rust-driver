//! CQL protocol-level response types.

pub mod error;
pub mod result;

/// Possible CQL responses received from the server
// Why is it distinct from [ResponseOpcode]?
// TODO(2.0): merge this with `ResponseOpcode`.
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
pub enum CqlResponseKind {
    /// Indicates an error processing a request.
    Error,

    /// Indicates that the server is ready to process queries. This message will be
    /// sent by the server either after a STARTUP message if no authentication is
    /// required (if authentication is required, the server indicates readiness by
    /// sending a AUTH_RESPONSE message).
    Ready,

    ///  Indicates that the server requires authentication, and which authentication
    /// mechanism to use.
    ///
    /// The authentication is SASL based and thus consists of a number of server
    /// challenges (AUTH_CHALLENGE) followed by client responses (AUTH_RESPONSE).
    /// The initial exchange is however bootstrapped by an initial client response.
    /// The details of that exchange (including how many challenge-response pairs
    /// are required) are specific to the authenticator in use. The exchange ends
    /// when the server sends an AUTH_SUCCESS message or an ERROR message.
    ///
    /// This message will be sent following a STARTUP message if authentication is
    /// required and must be answered by a AUTH_RESPONSE message from the client.
    Authenticate,

    /// Indicates which startup options are supported by the server. This message
    /// comes as a response to an OPTIONS message.
    Supported,

    /// The result to a query (QUERY, PREPARE, EXECUTE or BATCH messages).
    /// It has multiple kinds:
    /// - Void: for results carrying no information.
    /// - Rows: for results to select queries, returning a set of rows.
    /// - Set_keyspace: the result to a `USE` statement.
    /// - Prepared: result to a PREPARE message.
    /// - Schema_change: the result to a schema altering statement.
    Result,

    /// An event pushed by the server. A client will only receive events for the
    /// types it has REGISTER-ed to. The valid event types are:
    /// - "TOPOLOGY_CHANGE": events related to change in the cluster topology.
    ///   Currently, events are sent when new nodes are added to the cluster, and
    ///   when nodes are removed.
    /// - "STATUS_CHANGE": events related to change of node status. Currently,
    ///   up/down events are sent.
    /// - "SCHEMA_CHANGE": events related to schema change.
    ///   The type of changed involved may be one of "CREATED", "UPDATED" or
    ///   "DROPPED".
    Event,

    /// A server authentication challenge (see AUTH_RESPONSE for more details).
    /// Clients are expected to answer the server challenge with an AUTH_RESPONSE
    /// message.
    AuthChallenge,

    /// Indicates the success of the authentication phase.
    AuthSuccess,
}

impl std::fmt::Display for CqlResponseKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind_str = match self {
            CqlResponseKind::Error => "ERROR",
            CqlResponseKind::Ready => "READY",
            CqlResponseKind::Authenticate => "AUTHENTICATE",
            CqlResponseKind::Supported => "SUPPORTED",
            CqlResponseKind::Result => "RESULT",
            CqlResponseKind::Event => "EVENT",
            CqlResponseKind::AuthChallenge => "AUTH_CHALLENGE",
            CqlResponseKind::AuthSuccess => "AUTH_SUCCESS",
        };
        f.write_str(kind_str)
    }
}
