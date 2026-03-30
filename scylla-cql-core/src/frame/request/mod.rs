pub mod query;

/// Possible requests sent by the client.
// Why is it distinct from [RequestOpcode]?
// TODO(2.0): merge this with `RequestOpcode`.
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
pub enum CqlRequestKind {
    /// Initialize the connection. The server will respond by either a READY message
    /// (in which case the connection is ready for queries) or an AUTHENTICATE message
    /// (in which case credentials will need to be provided using AUTH_RESPONSE).
    ///
    /// This must be the first message of the connection, except for OPTIONS that can
    /// be sent before to find out the options supported by the server. Once the
    /// connection has been initialized, a client should not send any more STARTUP
    /// messages.
    Startup,

    /// Answers a server authentication challenge.
    ///
    /// Authentication in the protocol is SASL based. The server sends authentication
    /// challenges (a bytes token) to which the client answers with this message. Those
    /// exchanges continue until the server accepts the authentication by sending a
    /// AUTH_SUCCESS message after a client AUTH_RESPONSE. Note that the exchange
    /// begins with the client sending an initial AUTH_RESPONSE in response to a
    /// server AUTHENTICATE request.
    ///
    /// The response to a AUTH_RESPONSE is either a follow-up AUTH_CHALLENGE message,
    /// an AUTH_SUCCESS message or an ERROR message.
    AuthResponse,

    /// Asks the server to return which STARTUP options are supported. The server
    /// will respond with a SUPPORTED message.
    Options,

    /// Performs a CQL query, i.e., executes an unprepared statement.
    /// The server will respond to a QUERY message with a RESULT message, the content
    /// of which depends on the query.
    Query,

    /// Prepares a query for later execution (through EXECUTE).
    /// The server will respond with a RESULT::Prepared message.
    Prepare,

    /// Executes a prepared query.
    /// The response from the server will be a RESULT message.
    Execute,

    /// Allows executing a list of queries (prepared or not) as a batch (note that
    /// only DML statements are accepted in a batch).
    /// The server will respond with a RESULT message.
    Batch,

    /// Register this connection to receive some types of events.
    /// The response to a REGISTER message will be a READY message.
    Register,
}

impl std::fmt::Display for CqlRequestKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind_str = match self {
            CqlRequestKind::Startup => "STARTUP",
            CqlRequestKind::AuthResponse => "AUTH_RESPONSE",
            CqlRequestKind::Options => "OPTIONS",
            CqlRequestKind::Query => "QUERY",
            CqlRequestKind::Prepare => "PREPARE",
            CqlRequestKind::Execute => "EXECUTE",
            CqlRequestKind::Batch => "BATCH",
            CqlRequestKind::Register => "REGISTER",
        };

        f.write_str(kind_str)
    }
}
