use bytes::Bytes;
use futures::{future::RemoteHandle, FutureExt};
use scylla_cql::errors::TranslationError;
use scylla_cql::frame::types::SerialConsistency;
use tokio::io::{split, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tracing::{debug, error, trace, warn, Instrument};
use uuid::Uuid;

#[cfg(feature = "ssl")]
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
#[cfg(feature = "ssl")]
use tokio_openssl::SslStream;

#[cfg(feature = "ssl")]
pub(crate) use ssl_config::SslConfig;

use crate::authentication::AuthenticatorProvider;
use scylla_cql::frame::response::authenticate::Authenticate;
use std::collections::{BTreeSet, HashMap};
use std::convert::TryFrom;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::{
    cmp::Ordering,
    net::{Ipv4Addr, Ipv6Addr},
};

use super::errors::{BadKeyspaceName, DbError, QueryError};
use super::iterator::RowIterator;
use super::session::AddressTranslator;
use super::topology::{PeerEndpoint, UntranslatedEndpoint, UntranslatedPeer};
use super::NodeAddr;
#[cfg(feature = "cloud")]
use crate::cloud::CloudConfig;

use crate::batch::{Batch, BatchStatement};
use crate::frame::protocol_features::ProtocolFeatures;
use crate::frame::{
    self,
    request::{self, batch, execute, query, register, Request},
    response::{event::Event, result, NonErrorResponse, Response, ResponseOpcode},
    server_event_type::EventType,
    value::{BatchValues, ValueList},
    FrameParams, SerializedRequest,
};
use crate::otel_span;
use crate::query::Query;
use crate::routing::ShardInfo;
use crate::statement::prepared_statement::PreparedStatement;
use crate::statement::Consistency;
use crate::transport::session::IntoTypedRows;
use crate::transport::Compression;

// Existing code imports scylla::transport::connection::QueryResult because it used to be located in this file.
// Reexport QueryResult to avoid breaking the existing code.
use crate::utils::{RecordError, SpanExt};
pub use crate::QueryResult;

// Queries for schema agreement
const LOCAL_VERSION: &str = "SELECT schema_version FROM system.local WHERE key='local'";

// FIXME: Make this constants configurable
// The term "orphan" refers to stream ids, that were allocated for a {request, response} that no
// one is waiting anymore (due to cancellation of `Connection::send_request`). Old orphan refers to
// a stream id, that is orphaned for a long time. This long time is defined below
// (`OLD_AGE_ORPHAN_THRESHOLD`). Connection, that has a big number (`OLD_ORPHAN_COUNT_THRESHOLD`)
// of old orphans is shut down (and created again by a connection management layer).
const OLD_ORPHAN_COUNT_THRESHOLD: usize = 1024;
const OLD_AGE_ORPHAN_THRESHOLD: std::time::Duration = std::time::Duration::from_secs(1);

pub struct Connection {
    submit_channel: mpsc::Sender<Task>,
    _worker_handle: RemoteHandle<()>,

    connect_address: SocketAddr,
    config: ConnectionConfig,
    features: ConnectionFeatures,

    // Each request send by `Connection::send_request` needs a unique request id.
    // This field is a monotonic generator of such ids.
    request_id_generator: AtomicU64,
    // If a `Connection::send_request` is cancelled, it sends notification
    // about orphaning via the sender below.
    // Also, this sender is unbounded, because only unbounded channels support
    // pushing values in a synchronous way (without an `.await`), which is
    // needed for pushing values in `Drop` implementations.
    orphan_notification_sender: mpsc::UnboundedSender<RequestId>,
}

#[derive(Default)]
pub(crate) struct ConnectionFeatures {
    shard_info: Option<ShardInfo>,
    shard_aware_port: Option<u16>,
    protocol_features: ProtocolFeatures,
}

type RequestId = u64;

struct ResponseHandler {
    response_sender: oneshot::Sender<Result<TaskResponse, QueryError>>,
    request_id: RequestId,
}

// Used to notify `Connection::orphaner` about `Connection::send_request`
// future being dropped before receiving response.
struct OrphanhoodNotifier<'a> {
    enabled: bool,
    request_id: RequestId,
    notification_sender: &'a mpsc::UnboundedSender<RequestId>,
}

impl<'a> OrphanhoodNotifier<'a> {
    fn new(
        request_id: RequestId,
        notification_sender: &'a mpsc::UnboundedSender<RequestId>,
    ) -> Self {
        Self {
            enabled: true,
            request_id,
            notification_sender,
        }
    }

    fn disable(mut self) {
        self.enabled = false;
    }
}

impl<'a> Drop for OrphanhoodNotifier<'a> {
    fn drop(&mut self) {
        if self.enabled {
            let _ = self.notification_sender.send(self.request_id);
        }
    }
}

struct Task {
    serialized_request: SerializedRequest,
    response_handler: ResponseHandler,
}

struct TaskResponse {
    params: FrameParams,
    opcode: ResponseOpcode,
    body: Bytes,
}

pub struct QueryResponse {
    pub response: Response,
    pub tracing_id: Option<Uuid>,
    pub warnings: Vec<String>,
}

// A QueryResponse in which response can not be Response::Error
pub struct NonErrorQueryResponse {
    pub response: NonErrorResponse,
    pub tracing_id: Option<Uuid>,
    pub warnings: Vec<String>,
}

impl QueryResponse {
    pub fn into_non_error_query_response(self) -> Result<NonErrorQueryResponse, QueryError> {
        Ok(NonErrorQueryResponse {
            response: self.response.into_non_error_response()?,
            tracing_id: self.tracing_id,
            warnings: self.warnings,
        })
    }

    pub fn into_query_result(self) -> Result<QueryResult, QueryError> {
        self.into_non_error_query_response()?.into_query_result()
    }
}

impl NonErrorQueryResponse {
    pub fn as_set_keyspace(&self) -> Option<&result::SetKeyspace> {
        match &self.response {
            NonErrorResponse::Result(result::Result::SetKeyspace(sk)) => Some(sk),
            _ => None,
        }
    }

    pub fn as_schema_change(&self) -> Option<&result::SchemaChange> {
        match &self.response {
            NonErrorResponse::Result(result::Result::SchemaChange(sc)) => Some(sc),
            _ => None,
        }
    }

    pub fn into_query_result(self) -> Result<QueryResult, QueryError> {
        let (rows, paging_state, col_specs) = match self.response {
            NonErrorResponse::Result(result::Result::Rows(rs)) => (
                Some(rs.rows),
                rs.metadata.paging_state,
                rs.metadata.col_specs,
            ),
            NonErrorResponse::Result(_) => (None, None, vec![]),
            _ => {
                return Err(QueryError::ProtocolError(
                    "Unexpected server response, expected Result or Error",
                ))
            }
        };

        Ok(QueryResult {
            rows,
            warnings: self.warnings,
            tracing_id: self.tracing_id,
            paging_state,
            col_specs,
        })
    }
}
#[cfg(feature = "ssl")]
mod ssl_config {
    use openssl::{
        error::ErrorStack,
        ssl::{Ssl, SslContext},
    };
    #[cfg(feature = "cloud")]
    use uuid::Uuid;

    /// This struct encapsulates all Ssl-regarding configuration and helps pass it tidily through the code.
    //
    // There are 3 possible options for SslConfig, whose behaviour is somewhat subtle.
    // Option 1: No ssl configuration. Then it is None everytime.
    // Option 2: User-provided global SslContext. Then, a SslConfig is created upon Session creation
    // and henceforth stored in the ConnectionConfig.
    // Option 3: Serverless Cloud. The Option<SslConfig> remains None in ConnectionConfig until it reaches
    // NodeConnectionPool::new(). Inside that function, the field is mutated to contain SslConfig specific
    // for the particular node. (The SslConfig must be different, because SNIs differ for different nodes.)
    // Thenceforth, all connections to that node share the same SslConfig.
    #[derive(Clone)]
    pub struct SslConfig {
        context: SslContext,
        #[cfg(feature = "cloud")]
        sni: Option<String>,
    }

    impl SslConfig {
        // Used in case when the user provided their own SslContext to be used in all connections.
        pub fn new_with_global_context(context: SslContext) -> Self {
            Self {
                context,
                #[cfg(feature = "cloud")]
                sni: None,
            }
        }

        // Used in case of Serverless Cloud connections.
        #[cfg(feature = "cloud")]
        pub(crate) fn new_for_sni(
            context: SslContext,
            domain_name: &str,
            host_id: Option<Uuid>,
        ) -> Self {
            Self {
                context,
                #[cfg(feature = "cloud")]
                sni: Some(if let Some(host_id) = host_id {
                    format!("{}.{}", host_id, domain_name)
                } else {
                    domain_name.into()
                }),
            }
        }

        // Produces a new Ssl object that is able to wrap a TCP stream.
        pub(crate) fn new_ssl(&self) -> Result<Ssl, ErrorStack> {
            #[allow(unused_mut)]
            let mut ssl = Ssl::new(&self.context)?;
            #[cfg(feature = "cloud")]
            if let Some(sni) = self.sni.as_ref() {
                ssl.set_hostname(sni)?;
            }
            Ok(ssl)
        }
    }
}

#[derive(Clone)]
pub struct ConnectionConfig {
    pub compression: Option<Compression>,
    pub tcp_nodelay: bool,
    #[cfg(feature = "ssl")]
    pub ssl_config: Option<SslConfig>,
    pub connect_timeout: std::time::Duration,
    // should be Some only in control connections,
    pub event_sender: Option<mpsc::Sender<Event>>,
    pub default_consistency: Consistency,
    #[cfg(feature = "cloud")]
    pub(crate) cloud_config: Option<Arc<CloudConfig>>,
    pub authenticator: Option<Arc<dyn AuthenticatorProvider>>,
    pub address_translator: Option<Arc<dyn AddressTranslator>>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            compression: None,
            tcp_nodelay: true,
            event_sender: None,
            #[cfg(feature = "ssl")]
            ssl_config: None,
            connect_timeout: std::time::Duration::from_secs(5),
            default_consistency: Default::default(),
            authenticator: None,
            address_translator: None,
            #[cfg(feature = "cloud")]
            cloud_config: None,
        }
    }
}

impl ConnectionConfig {
    #[cfg(feature = "ssl")]
    pub fn is_ssl(&self) -> bool {
        #[cfg(feature = "cloud")]
        if self.cloud_config.is_some() {
            return true;
        }
        self.ssl_config.is_some()
    }

    #[cfg(not(feature = "ssl"))]
    pub fn is_ssl(&self) -> bool {
        false
    }
}

// Used to listen for fatal error in connection
pub type ErrorReceiver = tokio::sync::oneshot::Receiver<QueryError>;

impl Connection {
    // Returns new connection and ErrorReceiver which can be used to wait for a fatal error
    pub async fn new(
        addr: SocketAddr,
        source_port: Option<u16>,
        config: ConnectionConfig,
    ) -> Result<(Self, ErrorReceiver), QueryError> {
        let stream_connector = match source_port {
            Some(p) => {
                tokio::time::timeout(config.connect_timeout, connect_with_source_port(addr, p))
                    .await
            }
            None => tokio::time::timeout(config.connect_timeout, TcpStream::connect(addr)).await,
        };
        let stream = match stream_connector {
            Ok(stream) => stream?,
            Err(_) => {
                return Err(QueryError::TimeoutError);
            }
        };
        stream.set_nodelay(config.tcp_nodelay)?;

        // TODO: What should be the size of the channel?
        let (sender, receiver) = mpsc::channel(1024);
        let (error_sender, error_receiver) = tokio::sync::oneshot::channel();
        // Unbounded because it allows for synchronous pushes
        let (orphan_notification_sender, orphan_notification_receiver) = mpsc::unbounded_channel();

        let _worker_handle = Self::run_router(
            config.clone(),
            stream,
            receiver,
            error_sender,
            orphan_notification_receiver,
        )
        .await?;

        let connection = Connection {
            submit_channel: sender,
            _worker_handle,
            config,
            features: Default::default(),
            connect_address: addr,
            request_id_generator: AtomicU64::new(0),
            orphan_notification_sender,
        };

        Ok((connection, error_receiver))
    }

    pub async fn startup(&self, options: HashMap<String, String>) -> Result<Response, QueryError> {
        Ok(self
            .send_request(&request::Startup { options }, false, false)
            .await?
            .response)
    }

    pub async fn get_options(&self) -> Result<Response, QueryError> {
        Ok(self
            .send_request(&request::Options {}, false, false)
            .await?
            .response)
    }

    pub async fn prepare(&self, query: &Query) -> Result<PreparedStatement, QueryError> {
        let span = otel_span!(
            "Prepare attempt",
            net.peer = display(self.connect_address),
            db.scylladb.shard_id = self
                .features
                .shard_info
                .as_ref()
                .map(|shard_info| shard_info.shard),
        );
        let query_response = self
            .send_request(
                &request::Prepare {
                    query: &query.contents,
                },
                true,
                query.config.tracing,
            )
            .instrument(span.clone())
            .await
            .record_error(&span)?;

        let mut prepared_statement = match query_response.response {
            Response::Error(err) => {
                let query_err = err.into();
                span.record_error(&query_err);
                return Err(query_err);
            }
            Response::Result(result::Result::Prepared(p)) => PreparedStatement::new(
                p.id,
                self.features
                    .protocol_features
                    .prepared_flags_contain_lwt_mark(p.prepared_metadata.flags as u32),
                p.prepared_metadata,
                query.contents.clone(),
                query.get_page_size(),
                query.config.clone(),
            ),
            _ => {
                return Err(QueryError::ProtocolError(
                    "PREPARE: Unexpected server response",
                ))
            }
        };

        if let Some(tracing_id) = query_response.tracing_id {
            prepared_statement.prepare_tracing_ids.push(tracing_id);
        }
        Ok(prepared_statement)
    }

    async fn reprepare(&self, previous_prepared: &PreparedStatement) -> Result<(), QueryError> {
        let reprepare_query: Query = previous_prepared.get_statement().into();
        let span = otel_span!(
            "Repreparation",
            db.scylladb.statement_type = "prepare_request",
            db.scylladb.statement = reprepare_query.contents,
            db.scylladb.prepared_id = format_args!("{:X}", previous_prepared.get_id()),
        );
        let reprepared = self
            .prepare(&reprepare_query)
            .instrument(span.clone())
            .await
            .record_error(&span)?;
        // Reprepared statement should keep its id - it's the md5 sum
        // of statement contents
        if reprepared.get_id() != previous_prepared.get_id() {
            Err(QueryError::ProtocolError(
                "Prepared statement Id changed, md5 sum should stay the same",
            ))
        } else {
            Ok(())
        }
    }

    pub async fn authenticate_response(
        &self,
        response: Option<Vec<u8>>,
    ) -> Result<QueryResponse, QueryError> {
        self.send_request(&request::AuthResponse { response }, false, false)
            .await
    }

    pub async fn query_single_page(
        &self,
        query: impl Into<Query>,
        values: impl ValueList,
    ) -> Result<QueryResult, QueryError> {
        let query: Query = query.into();

        // This method is used only for driver internal queries, so no need to consult execution profile here.
        let consistency = query
            .config
            .determine_consistency(self.config.default_consistency);
        let serial_consistency = query.config.serial_consistency;

        self.query_single_page_with_consistency(
            query,
            &values,
            consistency,
            serial_consistency.flatten(),
        )
        .await
    }

    pub async fn query_single_page_with_consistency(
        &self,
        query: impl Into<Query>,
        values: impl ValueList,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
    ) -> Result<QueryResult, QueryError> {
        let query: Query = query.into();
        self.query_with_consistency(&query, &values, consistency, serial_consistency, None)
            .await?
            .into_query_result()
    }

    pub async fn query(
        &self,
        query: &Query,
        values: impl ValueList,
        paging_state: Option<Bytes>,
    ) -> Result<QueryResponse, QueryError> {
        // This method is used only for driver internal queries, so no need to consult execution profile here.
        self.query_with_consistency(
            query,
            values,
            query
                .config
                .determine_consistency(self.config.default_consistency),
            query.config.serial_consistency.flatten(),
            paging_state,
        )
        .await
    }

    pub async fn query_with_consistency(
        &self,
        query: &Query,
        values: impl ValueList,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
        paging_state: Option<Bytes>,
    ) -> Result<QueryResponse, QueryError> {
        let serialized_values = values.serialized()?;

        let query_frame = query::Query {
            contents: &query.contents,
            parameters: query::QueryParameters {
                consistency,
                serial_consistency,
                values: &serialized_values,
                page_size: query.get_page_size(),
                paging_state,
                timestamp: query.get_timestamp(),
            },
        };

        self.send_request(&query_frame, true, query.config.tracing)
            .await
    }

    pub async fn execute_with_consistency(
        &self,
        prepared_statement: &PreparedStatement,
        values: impl ValueList,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
        paging_state: Option<Bytes>,
    ) -> Result<QueryResponse, QueryError> {
        let serialized_values = values.serialized()?;

        let execute_frame = execute::Execute {
            id: prepared_statement.get_id().to_owned(),
            parameters: query::QueryParameters {
                consistency,
                serial_consistency,
                values: &serialized_values,
                page_size: prepared_statement.get_page_size(),
                timestamp: prepared_statement.get_timestamp(),
                paging_state,
            },
        };

        let query_response = self
            .send_request(&execute_frame, true, prepared_statement.config.tracing)
            .await?;

        match &query_response.response {
            Response::Error(frame::response::Error {
                error: DbError::Unprepared { statement_id },
                ..
            }) => {
                debug!("Connection::execute: Got DbError::Unprepared - repreparing statement with id {:?}", statement_id);
                // Repreparation of a statement is needed
                self.reprepare(prepared_statement).await?;
                self.send_request(&execute_frame, true, prepared_statement.config.tracing)
                    .await
            }
            _ => Ok(query_response),
        }
    }

    /// Executes a query and fetches its results over multiple pages, using
    /// the asynchronous iterator interface.
    pub(crate) async fn query_iter(
        self: Arc<Self>,
        query: Query,
        values: impl ValueList,
    ) -> Result<RowIterator, QueryError> {
        let serialized_values = values.serialized()?.into_owned();

        let consistency = query
            .config
            .determine_consistency(self.config.default_consistency);
        let serial_consistency = query.config.serial_consistency.flatten();

        RowIterator::new_for_connection_query_iter(
            query,
            self,
            serialized_values,
            consistency,
            serial_consistency,
        )
        .await
    }

    #[allow(dead_code)]
    pub async fn batch(
        &self,
        batch: &Batch,
        values: impl BatchValues,
    ) -> Result<QueryResult, QueryError> {
        self.batch_with_consistency(
            batch,
            values,
            batch
                .config
                .determine_consistency(self.config.default_consistency),
            batch.config.serial_consistency.flatten(),
        )
        .await
    }

    pub async fn batch_with_consistency(
        &self,
        batch: &Batch,
        values: impl BatchValues,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
    ) -> Result<QueryResult, QueryError> {
        let statements_iter = batch.statements.iter().map(|s| match s {
            BatchStatement::Query(q) => batch::BatchStatement::Query { text: &q.contents },
            BatchStatement::PreparedStatement(s) => {
                batch::BatchStatement::Prepared { id: s.get_id() }
            }
        });

        let batch_frame = batch::Batch {
            statements_count: statements_iter.len(),
            statements: statements_iter,
            values,
            batch_type: batch.get_type(),
            consistency,
            serial_consistency,
            timestamp: batch.get_timestamp(),
        };

        loop {
            let query_response = self
                .send_request(&batch_frame, true, batch.config.tracing)
                .await?;

            return match query_response.response {
                Response::Error(err) => match err.error {
                    DbError::Unprepared { statement_id } => {
                        debug!("Connection::batch: got DbError::Unprepared - repreparing statement with id {:?}", statement_id);
                        let prepared_statement = batch.statements.iter().find_map(|s| match s {
                            BatchStatement::PreparedStatement(s) if *s.get_id() == statement_id => {
                                Some(s)
                            }
                            _ => None,
                        });
                        if let Some(p) = prepared_statement {
                            self.reprepare(p).await?;
                            continue;
                        } else {
                            return Err(QueryError::ProtocolError(
                                "The server returned a prepared statement Id that did not exist in the batch",
                            ));
                        }
                    }
                    _ => Err(err.into()),
                },
                Response::Result(_) => Ok(query_response.into_query_result()?),
                _ => Err(QueryError::ProtocolError(
                    "BATCH: Unexpected server response",
                )),
            };
        }
    }

    pub async fn use_keyspace(
        &self,
        keyspace_name: &VerifiedKeyspaceName,
    ) -> Result<(), QueryError> {
        // Trying to pass keyspace_name as bound value doesn't work
        // We have to send "USE " + keyspace_name
        let query: Query = match keyspace_name.is_case_sensitive {
            true => format!("USE \"{}\"", keyspace_name.as_str()).into(),
            false => format!("USE {}", keyspace_name.as_str()).into(),
        };

        let query_response = self.query(&query, (), None).await?;

        match query_response.response {
            Response::Result(result::Result::SetKeyspace(set_keyspace)) => {
                if set_keyspace.keyspace_name.to_lowercase()
                    != keyspace_name.as_str().to_lowercase()
                {
                    return Err(QueryError::ProtocolError(
                        "USE <keyspace_name> returned response with different keyspace name",
                    ));
                }

                Ok(())
            }
            Response::Error(err) => Err(err.into()),
            _ => Err(QueryError::ProtocolError(
                "USE <keyspace_name> returned unexpected response",
            )),
        }
    }

    async fn register(
        &self,
        event_types_to_register_for: Vec<EventType>,
    ) -> Result<(), QueryError> {
        let register_frame = register::Register {
            event_types_to_register_for,
        };

        match self
            .send_request(&register_frame, true, false)
            .await?
            .response
        {
            Response::Ready => Ok(()),
            Response::Error(err) => Err(err.into()),
            _ => Err(QueryError::ProtocolError(
                "Unexpected response to REGISTER message",
            )),
        }
    }

    pub async fn fetch_schema_version(&self) -> Result<Uuid, QueryError> {
        let (version_id,): (Uuid,) = self
            .query_single_page(LOCAL_VERSION, &[])
            .await?
            .rows
            .ok_or(QueryError::ProtocolError("Version query returned not rows"))?
            .into_typed::<(Uuid,)>()
            .next()
            .ok_or(QueryError::ProtocolError("Admin table returned empty rows"))?
            .map_err(|_| QueryError::ProtocolError("Row is not uuid type as it should be"))?;
        Ok(version_id)
    }

    fn allocate_request_id(&self) -> RequestId {
        self.request_id_generator
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    async fn send_request<R: Request>(
        &self,
        request: &R,
        compress: bool,
        tracing: bool,
    ) -> Result<QueryResponse, QueryError> {
        let compression = if compress {
            self.config.compression
        } else {
            None
        };
        let serialized_request = SerializedRequest::make(request, compression, tracing)?;
        let request_id = self.allocate_request_id();

        let (response_sender, receiver) = oneshot::channel();
        let response_handler = ResponseHandler {
            response_sender,
            request_id,
        };

        // Dropping `notifier` (before calling `notifier.disable()`) will send a notification to
        // `Connection::router`. This notification is then used to mark a `stream_id` associated
        // with this request as orphaned and free associated resources.
        let notifier = OrphanhoodNotifier::new(request_id, &self.orphan_notification_sender);

        self.submit_channel
            .send(Task {
                serialized_request,
                response_handler,
            })
            .await
            .map_err(|_| {
                QueryError::IoError(Arc::new(std::io::Error::new(
                    ErrorKind::Other,
                    "Connection broken",
                )))
            })?;

        let task_response = receiver.await.map_err(|_| {
            QueryError::IoError(Arc::new(std::io::Error::new(
                ErrorKind::Other,
                "Connection broken",
            )))
        })?;

        // Response was successfully received, so it's time to disable
        // notification about orphaning.
        notifier.disable();

        Self::parse_response(
            task_response?,
            self.config.compression,
            &self.features.protocol_features,
        )
    }

    fn parse_response(
        task_response: TaskResponse,
        compression: Option<Compression>,
        features: &ProtocolFeatures,
    ) -> Result<QueryResponse, QueryError> {
        let body_with_ext = frame::parse_response_body_extensions(
            task_response.params.flags,
            compression,
            task_response.body,
        )?;

        for warn_description in &body_with_ext.warnings {
            warn!(
                warning = warn_description.as_str(),
                "Response from the database contains a warning",
            );
        }

        let response =
            Response::deserialize(features, task_response.opcode, &mut &*body_with_ext.body)?;

        Ok(QueryResponse {
            response,
            warnings: body_with_ext.warnings,
            tracing_id: body_with_ext.trace_id,
        })
    }

    async fn run_router(
        config: ConnectionConfig,
        stream: TcpStream,
        receiver: mpsc::Receiver<Task>,
        error_sender: tokio::sync::oneshot::Sender<QueryError>,
        orphan_notification_receiver: mpsc::UnboundedReceiver<RequestId>,
    ) -> Result<RemoteHandle<()>, std::io::Error> {
        #[cfg(feature = "ssl")]
        if let Some(ssl_config) = &config.ssl_config {
            let ssl = ssl_config.new_ssl()?;
            let mut stream = SslStream::new(ssl, stream)?;
            let _pin = Pin::new(&mut stream).connect().await;

            let (task, handle) = Self::router(
                config,
                stream,
                receiver,
                error_sender,
                orphan_notification_receiver,
            )
            .remote_handle();
            tokio::task::spawn(task);
            return Ok(handle);
        }

        let (task, handle) = Self::router(
            config,
            stream,
            receiver,
            error_sender,
            orphan_notification_receiver,
        )
        .remote_handle();
        tokio::task::spawn(task);
        Ok(handle)
    }

    async fn router(
        config: ConnectionConfig,
        stream: (impl AsyncRead + AsyncWrite),
        receiver: mpsc::Receiver<Task>,
        error_sender: tokio::sync::oneshot::Sender<QueryError>,
        orphan_notification_receiver: mpsc::UnboundedReceiver<RequestId>,
    ) {
        let (read_half, write_half) = split(stream);
        // Why are we using a mutex here?
        //
        // The handler_map is supposed to be shared between reader and writer
        // futures, which will be run on the same task. The mutex should not
        // be normally required here, but Rust complains if we try to use
        // a RefCell instead of Mutex (the whole future becomes !Sync and we
        // cannot use it in tasks).
        //
        // Notice that this lock will have no contention, because reader
        // and writer futures are run on the same fiber, and both of them
        // are carefully written in such a way that they do not hold the lock
        // across .await points. Therefore, it should not be too expensive.
        let handler_map = StdMutex::new(ResponseHandlerMap::new());

        let r = Self::reader(
            BufReader::with_capacity(8192, read_half),
            &handler_map,
            config,
        );
        let w = Self::writer(
            BufWriter::with_capacity(8192, write_half),
            &handler_map,
            receiver,
        );
        let o = Self::orphaner(&handler_map, orphan_notification_receiver);

        let result = futures::try_join!(r, w, o);

        let error: QueryError = match result {
            Ok(_) => return, // Connection was dropped, we can return
            Err(err) => err,
        };

        // Respond to all pending requests with the error
        let response_handlers: HashMap<i16, ResponseHandler> =
            handler_map.into_inner().unwrap().into_handlers();

        for (_, handler) in response_handlers {
            // Ignore sending error, request was dropped
            let _ = handler.response_sender.send(Err(error.clone()));
        }

        // If someone is listening for connection errors notify them
        let _ = error_sender.send(error);
    }

    async fn reader(
        mut read_half: (impl AsyncRead + Unpin),
        handler_map: &StdMutex<ResponseHandlerMap>,
        config: ConnectionConfig,
    ) -> Result<(), QueryError> {
        loop {
            let (params, opcode, body) = frame::read_response_frame(&mut read_half).await?;
            let response = TaskResponse {
                params,
                opcode,
                body,
            };

            match params.stream.cmp(&-1) {
                Ordering::Less => {
                    // The spec reserves negative-numbered streams for server-generated
                    // events. As of writing this driver, there are no other negative
                    // streams used apart from -1, so ignore it.
                    continue;
                }
                Ordering::Equal => {
                    if let Some(event_sender) = config.event_sender.as_ref() {
                        Self::handle_event(response, config.compression, event_sender).await?;
                    }
                    continue;
                }
                _ => {}
            }

            let handler_lookup_res = {
                // We are guaranteed here that handler_map will not be locked
                // by anybody else, so we can do try_lock().unwrap()
                let mut handler_map_guard = handler_map.try_lock().unwrap();
                handler_map_guard.lookup(params.stream)
            };

            use HandlerLookupResult::*;
            match handler_lookup_res {
                Handler(handler) => {
                    // Don't care if sending of the response fails. This must
                    // mean that the receiver side was impatient and is not
                    // waiting for the result anymore.
                    let _ = handler.response_sender.send(Ok(response));
                }
                Missing => {
                    // Unsolicited frame. This should not happen and indicates
                    // a bug either in the driver, or in the database
                    debug!(
                        "Received response with unexpected StreamId {}",
                        params.stream
                    );
                    return Err(QueryError::ProtocolError(
                        "Received response with unexpected StreamId",
                    ));
                }
                Orphaned => {
                    // Do nothing, handler was freed because this stream_id has
                    // been marked as orphaned
                }
            }
        }
    }

    fn alloc_stream_id(
        handler_map: &StdMutex<ResponseHandlerMap>,
        response_handler: ResponseHandler,
    ) -> Option<i16> {
        // We are guaranteed here that handler_map will not be locked
        // by anybody else, so we can do try_lock().unwrap()
        let mut handler_map_guard = handler_map.try_lock().unwrap();
        match handler_map_guard.allocate(response_handler) {
            Ok(stream_id) => Some(stream_id),
            Err(response_handler) => {
                error!("Could not allocate stream id");
                let _ = response_handler
                    .response_sender
                    .send(Err(QueryError::UnableToAllocStreamId));
                None
            }
        }
    }

    async fn writer(
        mut write_half: (impl AsyncWrite + Unpin),
        handler_map: &StdMutex<ResponseHandlerMap>,
        mut task_receiver: mpsc::Receiver<Task>,
    ) -> Result<(), QueryError> {
        // When the Connection object is dropped, the sender half
        // of the channel will be dropped, this task will return an error
        // and the whole worker will be stopped
        while let Some(mut task) = task_receiver.recv().await {
            let mut num_requests = 0;
            let mut total_sent = 0;
            while let Some(stream_id) = Self::alloc_stream_id(handler_map, task.response_handler) {
                let mut req = task.serialized_request;
                req.set_stream(stream_id);
                let req_data: &[u8] = req.get_data();
                total_sent += req_data.len();
                num_requests += 1;
                write_half.write_all(req_data).await?;
                task = match task_receiver.try_recv() {
                    Ok(t) => t,
                    Err(_) => {
                        // Yielding was empirically tested to inject a 1-300µs delay,
                        // much better than tokio::time::sleep's 1ms granularity.
                        // Also, yielding in a busy system let's the queue catch up with new items.
                        tokio::task::yield_now().await;
                        match task_receiver.try_recv() {
                            Ok(t) => t,
                            Err(_) => break,
                        }
                    }
                }
            }
            trace!("Sending {} requests; {} bytes", num_requests, total_sent);
            write_half.flush().await?;
        }

        Ok(())
    }

    // This task receives notifications from `OrphanhoodNotifier`s and tries to
    // mark streams as orphaned. It also checks count of old orphans periodically.
    // After an ald orphan threshold is reached, `orphaner` returns an error
    // causing the connection to break.
    async fn orphaner(
        handler_map: &StdMutex<ResponseHandlerMap>,
        mut orphan_receiver: mpsc::UnboundedReceiver<RequestId>,
    ) -> Result<(), QueryError> {
        let mut interval = tokio::time::interval(OLD_AGE_ORPHAN_THRESHOLD);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // We are guaranteed here that handler_map will not be locked
                    // by anybody else, so we can do try_lock().unwrap()
                    let handler_map_guard = handler_map.try_lock().unwrap();
                    let old_orphan_count = handler_map_guard.old_orphans_count();
                    if old_orphan_count > OLD_ORPHAN_COUNT_THRESHOLD {
                        warn!(
                            "Too many old orphaned stream ids: {}",
                            old_orphan_count,
                        );
                        return Err(QueryError::TooManyOrphanedStreamIds(old_orphan_count as u16))
                    }
                }
                Some(request_id) = orphan_receiver.recv() => {
                    trace!(
                        "Trying to orphan stream id associated with request_id = {}",
                        request_id,
                    );
                    let mut handler_map_guard = handler_map.try_lock().unwrap(); // Same as above
                    handler_map_guard.orphan(request_id);
                }
                else => { break }
            }
        }

        Ok(())
    }

    async fn handle_event(
        task_response: TaskResponse,
        compression: Option<Compression>,
        event_sender: &mpsc::Sender<Event>,
    ) -> Result<(), QueryError> {
        // Protocol features are negotiated during connection handshake.
        // However, the router is already created and sent to a different tokio
        // task before the handshake begins, therefore it's hard to cleanly
        // update the protocol features in the router at this point.
        // Making it possible would require restructuring the handshake process,
        // or passing the negotiated features via a channel/mutex/etc.
        // Fortunately, events do not need information about protocol features
        // to be serialized (yet), therefore I'm leaving this problem for
        // future implementors.
        let features = ProtocolFeatures::default(); // TODO: Use the right features

        let response = Self::parse_response(task_response, compression, &features)?.response;
        let event = match response {
            Response::Event(e) => e,
            _ => {
                warn!("Expected to receive Event response, got {:?}", response);
                return Ok(());
            }
        };

        event_sender.send(event).await.map_err(|_| {
            QueryError::IoError(Arc::new(std::io::Error::new(
                ErrorKind::Other,
                "Connection broken",
            )))
        })
    }

    pub fn get_shard_info(&self) -> &Option<ShardInfo> {
        &self.features.shard_info
    }

    pub fn get_shard_aware_port(&self) -> Option<u16> {
        self.features.shard_aware_port
    }

    fn set_features(&mut self, features: ConnectionFeatures) {
        self.features = features;
    }

    pub fn get_connect_address(&self) -> SocketAddr {
        self.connect_address
    }
}

async fn maybe_translated_addr(
    endpoint: UntranslatedEndpoint,
    address_translator: Option<&dyn AddressTranslator>,
) -> Result<SocketAddr, TranslationError> {
    match endpoint {
        UntranslatedEndpoint::ContactPoint(addr) => Ok(addr.address),
        UntranslatedEndpoint::Peer(PeerEndpoint {
            host_id,
            address,
            datacenter,
            rack,
        }) => match address {
            NodeAddr::Translatable(addr) => {
                // In this case, addr is subject to AddressTranslator.
                if let Some(translator) = address_translator {
                    let res = translator
                        .translate_address(&UntranslatedPeer {
                            host_id,
                            untranslated_address: addr,
                            datacenter,
                            rack,
                        })
                        .await;
                    if let Err(ref err) = res {
                        error!("Address translation failed for addr {}: {}", addr, err);
                    }
                    res
                } else {
                    Ok(addr)
                }
            }
            NodeAddr::Untranslatable(addr) => {
                // In this case, addr is considered to be translated, as it is the control connection's address.
                Ok(addr)
            }
        },
    }
}

pub async fn open_connection(
    endpoint: UntranslatedEndpoint,
    source_port: Option<u16>,
    config: ConnectionConfig,
) -> Result<(Connection, ErrorReceiver), QueryError> {
    let addr = maybe_translated_addr(endpoint, config.address_translator.as_deref()).await?;
    open_named_connection(
        addr,
        source_port,
        config,
        Some("scylla-rust-driver".to_string()),
        option_env!("CARGO_PKG_VERSION").map(|v| v.to_string()),
    )
    .await
}

pub async fn open_named_connection(
    addr: SocketAddr,
    source_port: Option<u16>,
    config: ConnectionConfig,
    driver_name: Option<String>,
    driver_version: Option<String>,
) -> Result<(Connection, ErrorReceiver), QueryError> {
    // TODO: shouldn't all this logic be in Connection::new?
    let (mut connection, error_receiver) =
        Connection::new(addr, source_port, config.clone()).await?;

    let options_result = connection.get_options().await?;

    let shard_aware_port_key = match config.is_ssl() {
        true => "SCYLLA_SHARD_AWARE_PORT_SSL",
        false => "SCYLLA_SHARD_AWARE_PORT",
    };

    let mut supported = match options_result {
        Response::Supported(supported) => supported,
        _ => {
            return Err(QueryError::ProtocolError(
                "Wrong response to OPTIONS message was received",
            ));
        }
    };

    let shard_info = ShardInfo::try_from(&supported.options).ok();
    let supported_compression = supported.options.remove("COMPRESSION").unwrap_or_default();
    let shard_aware_port = supported
        .options
        .remove(shard_aware_port_key)
        .unwrap_or_default()
        .into_iter()
        .next()
        .and_then(|p| p.parse::<u16>().ok());

    let protocol_features = ProtocolFeatures::parse_from_supported(&supported.options);

    let mut options = HashMap::new();
    protocol_features.add_startup_options(&mut options);

    let features = ConnectionFeatures {
        shard_info,
        shard_aware_port,
        protocol_features,
    };
    connection.set_features(features);

    options.insert("CQL_VERSION".to_string(), "4.0.0".to_string()); // FIXME: hardcoded values
    if let Some(name) = driver_name {
        options.insert("DRIVER_NAME".to_string(), name);
    }
    if let Some(version) = driver_version {
        options.insert("DRIVER_VERSION".to_string(), version);
    }
    if let Some(compression) = &config.compression {
        let compression_str = compression.to_string();
        if supported_compression.iter().any(|c| c == &compression_str) {
            // Compression is reported to be supported by the server,
            // request it from the server
            options.insert("COMPRESSION".to_string(), compression.to_string());
        } else {
            // Fall back to no compression
            connection.config.compression = None;
        }
    }
    let result = connection.startup(options).await?;
    match result {
        Response::Ready => {}
        Response::Authenticate(authenticate) => {
            perform_authenticate(&mut connection, &authenticate).await?;
        }
        _ => {
            return Err(QueryError::ProtocolError(
                "Unexpected response to STARTUP message",
            ))
        }
    }

    if connection.config.event_sender.is_some() {
        let all_event_types = vec![
            EventType::TopologyChange,
            EventType::StatusChange,
            EventType::SchemaChange,
        ];
        connection.register(all_event_types).await?;
    }

    Ok((connection, error_receiver))
}

async fn perform_authenticate(
    connection: &mut Connection,
    authenticate: &Authenticate,
) -> Result<(), QueryError> {
    let authenticator = &authenticate.authenticator_name as &str;

    match connection.config.authenticator {
        Some(ref authenticator_provider) => {
            let (mut response, mut auth_session) = authenticator_provider
                .start_authentication_session(authenticator)
                .await
                .map_err(QueryError::InvalidMessage)?;

            loop {
                match connection
                    .authenticate_response(response)
                    .await?.response
                {
                    Response::AuthChallenge(challenge) => {
                        response = auth_session
                            .evaluate_challenge(
                                challenge.authenticate_message.as_deref(),
                            )
                            .await
                            .map_err(QueryError::InvalidMessage)?;
                    }
                    Response::AuthSuccess(success) => {
                        auth_session
                            .success(success.success_message.as_deref())
                            .await
                            .map_err(QueryError::InvalidMessage)?;
                        break;
                    }
                    Response::Error(err) => {
                        return Err(err.into());
                    }
                    _ => {
                        return Err(QueryError::ProtocolError(
                            "Unexpected response to Authenticate Response message",
                        ))
                    }
                }
            }
        },
        None => return Err(QueryError::InvalidMessage(
            "Authentication is required. You can use SessionBuilder::user(\"user\", \"pass\") to provide credentials \
                    or SessionBuilder::authenticator_provider to provide custom authenticator".to_string(),
        )),
    }

    Ok(())
}

async fn connect_with_source_port(
    addr: SocketAddr,
    source_port: u16,
) -> Result<TcpStream, std::io::Error> {
    match addr {
        SocketAddr::V4(_) => {
            let socket = TcpSocket::new_v4()?;
            socket.bind(SocketAddr::new(
                Ipv4Addr::new(0, 0, 0, 0).into(),
                source_port,
            ))?;
            Ok(socket.connect(addr).await?)
        }
        SocketAddr::V6(_) => {
            let socket = TcpSocket::new_v6()?;
            socket.bind(SocketAddr::new(
                Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0).into(),
                source_port,
            ))?;
            Ok(socket.connect(addr).await?)
        }
    }
}

struct OrphanageTracker {
    orphans: HashMap<i16, Instant>,
    by_orphaning_times: BTreeSet<(Instant, i16)>,
}

impl OrphanageTracker {
    pub fn new() -> Self {
        Self {
            orphans: HashMap::new(),
            by_orphaning_times: BTreeSet::new(),
        }
    }

    pub fn insert(&mut self, stream_id: i16) {
        let now = Instant::now();
        self.orphans.insert(stream_id, now);
        self.by_orphaning_times.insert((now, stream_id));
    }

    pub fn remove(&mut self, stream_id: i16) {
        if let Some(time) = self.orphans.remove(&stream_id) {
            self.by_orphaning_times.remove(&(time, stream_id));
        }
    }

    pub fn contains(&self, stream_id: i16) -> bool {
        self.orphans.contains_key(&stream_id)
    }

    pub fn orphans_older_than(&self, age: std::time::Duration) -> usize {
        let minimal_age = Instant::now() - age;
        self.by_orphaning_times
            .range(..(minimal_age, i16::MAX))
            .count() // This has linear time complexity, but in terms of
                     // the number of old orphans. Healthy connection - one
                     // that does not have old orphaned stream ids, will
                     // calculate this function quickly.
    }
}

struct ResponseHandlerMap {
    stream_set: StreamIdSet,
    handlers: HashMap<i16, ResponseHandler>,

    request_to_stream: HashMap<RequestId, i16>,
    orphanage_tracker: OrphanageTracker,
}

enum HandlerLookupResult {
    Orphaned,
    Handler(ResponseHandler),
    Missing,
}

impl ResponseHandlerMap {
    pub fn new() -> Self {
        Self {
            stream_set: StreamIdSet::new(),
            handlers: HashMap::new(),
            request_to_stream: HashMap::new(),
            orphanage_tracker: OrphanageTracker::new(),
        }
    }

    pub fn allocate(&mut self, response_handler: ResponseHandler) -> Result<i16, ResponseHandler> {
        if let Some(stream_id) = self.stream_set.allocate() {
            self.request_to_stream
                .insert(response_handler.request_id, stream_id);
            let prev_handler = self.handlers.insert(stream_id, response_handler);
            assert!(prev_handler.is_none());

            Ok(stream_id)
        } else {
            Err(response_handler)
        }
    }

    // Orphan stream_id (associated with this request_id) by moving it to
    // `orphanage_tracker`, and freeing its handler
    pub fn orphan(&mut self, request_id: RequestId) {
        if let Some(stream_id) = self.request_to_stream.get(&request_id) {
            debug!(
                "Orphaning stream_id = {} associated with request_id = {}",
                stream_id, request_id
            );
            self.orphanage_tracker.insert(*stream_id);
            self.handlers.remove(stream_id);
            self.request_to_stream.remove(&request_id);
        }
    }

    pub fn old_orphans_count(&self) -> usize {
        self.orphanage_tracker
            .orphans_older_than(OLD_AGE_ORPHAN_THRESHOLD)
    }

    pub fn lookup(&mut self, stream_id: i16) -> HandlerLookupResult {
        self.stream_set.free(stream_id);

        if self.orphanage_tracker.contains(stream_id) {
            self.orphanage_tracker.remove(stream_id);
            // This `stream_id` had been orphaned, so its handler got removed.
            // This is a valid state (as opposed to missing handler)
            return HandlerLookupResult::Orphaned;
        }

        if let Some(handler) = self.handlers.remove(&stream_id) {
            // A mapping `request_id` -> `stream_id` must be removed, to
            // prevent marking this `stream_id` as orphaned by some late
            // orphan notification.
            self.request_to_stream.remove(&handler.request_id);

            HandlerLookupResult::Handler(handler)
        } else {
            HandlerLookupResult::Missing
        }
    }

    // Retrieves the map of handlers, used after connection breaks
    // and we have to respond to all of them with an error
    pub fn into_handlers(self) -> HashMap<i16, ResponseHandler> {
        self.handlers
    }
}

struct StreamIdSet {
    used_bitmap: Box<[u64]>,
}

impl StreamIdSet {
    pub fn new() -> Self {
        const BITMAP_SIZE: usize = (std::i16::MAX as usize + 1) / 64;
        Self {
            used_bitmap: vec![0; BITMAP_SIZE].into_boxed_slice(),
        }
    }

    pub fn allocate(&mut self) -> Option<i16> {
        for (block_id, block) in self.used_bitmap.iter_mut().enumerate() {
            if *block != !0 {
                let off = block.trailing_ones();
                *block |= 1u64 << off;
                let stream_id = off as i16 + block_id as i16 * 64;
                return Some(stream_id);
            }
        }
        None
    }

    pub fn free(&mut self, stream_id: i16) {
        let block_id = stream_id as usize / 64;
        let off = stream_id as usize % 64;
        self.used_bitmap[block_id] &= !(1 << off);
    }
}

/// This type can only hold a valid keyspace name
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct VerifiedKeyspaceName {
    name: Arc<String>,
    pub is_case_sensitive: bool,
}

impl VerifiedKeyspaceName {
    pub fn new(keyspace_name: String, case_sensitive: bool) -> Result<Self, BadKeyspaceName> {
        Self::verify_keyspace_name_is_valid(&keyspace_name)?;

        Ok(VerifiedKeyspaceName {
            name: Arc::new(keyspace_name),
            is_case_sensitive: case_sensitive,
        })
    }

    pub fn as_str(&self) -> &str {
        self.name.as_str()
    }

    // "Keyspace names can have up to 48 alphanumeric characters and contain underscores;
    // only letters and numbers are supported as the first character."
    // https://docs.datastax.com/en/cql-oss/3.3/cql/cql_reference/cqlCreateKeyspace.html
    // Despite that cassandra accepts underscore as first character so we do too
    // https://github.com/scylladb/scylla/blob/62551b3bd382c7c47371eb3fc38173bd0cfed44d/test/cql-pytest/test_keyspace.py#L58
    // https://github.com/scylladb/scylla/blob/718976e794790253c4b24e2c78208e11f24e7502/cql3/statements/create_keyspace_statement.cc#L75
    fn verify_keyspace_name_is_valid(keyspace_name: &str) -> Result<(), BadKeyspaceName> {
        if keyspace_name.is_empty() {
            return Err(BadKeyspaceName::Empty);
        }

        // Verify that length <= 48
        let keyspace_name_len: usize = keyspace_name.chars().count(); // Only ascii allowed so it's equal to .len()
        if keyspace_name_len > 48 {
            return Err(BadKeyspaceName::TooLong(
                keyspace_name.to_string(),
                keyspace_name_len,
            ));
        }

        // Verify all chars are alphanumeric or underscore
        for character in keyspace_name.chars() {
            match character {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '_' => {}
                _ => {
                    return Err(BadKeyspaceName::IllegalCharacter(
                        keyspace_name.to_string(),
                        character,
                    ))
                }
            };
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use scylla_cql::frame::protocol_features::{
        LWT_OPTIMIZATION_META_BIT_MASK_KEY, SCYLLA_LWT_ADD_METADATA_MARK_EXTENSION,
    };
    use scylla_cql::frame::types;
    use scylla_proxy::{
        Condition, Node, Proxy, Reaction, RequestFrame, RequestOpcode, RequestReaction,
        RequestRule, ResponseFrame,
    };

    use tokio::select;
    use tokio::sync::mpsc;

    use super::ConnectionConfig;
    use crate::query::Query;
    use crate::transport::cluster::ContactPoint;
    use crate::transport::connection::open_connection;
    use crate::transport::topology::UntranslatedEndpoint;
    use crate::utils::test_utils::unique_keyspace_name;
    use crate::SessionBuilder;
    use futures::{StreamExt, TryStreamExt};
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::Arc;

    // Just like resolve_hostname in session.rs
    async fn resolve_hostname(hostname: &str) -> SocketAddr {
        match tokio::net::lookup_host(hostname).await {
            Ok(mut addrs) => addrs.next().unwrap(),
            Err(_) => {
                tokio::net::lookup_host((hostname, 9042)) // Port might not be specified, try default
                    .await
                    .unwrap()
                    .next()
                    .unwrap()
            }
        }
    }

    /// Tests for Connection::query_iter
    /// 1. SELECT from an empty table.
    /// 2. Create table and insert ints 0..100.
    ///    Then use query_iter with page_size set to 7 to select all 100 rows.
    /// 3. INSERT query_iter should work and not return any rows.
    #[tokio::test]
    async fn connection_query_iter_test() {
        let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        let addr: SocketAddr = resolve_hostname(&uri).await;

        let (connection, _) = super::open_connection(
            UntranslatedEndpoint::ContactPoint(ContactPoint {
                address: addr,
                datacenter: None,
            }),
            None,
            ConnectionConfig::default(),
        )
        .await
        .unwrap();
        let connection = Arc::new(connection);

        let ks = unique_keyspace_name();

        {
            // Preparation phase
            let session = SessionBuilder::new()
                .known_node_addr(addr)
                .build()
                .await
                .unwrap();
            session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks.clone()), &[]).await.unwrap();
            session.use_keyspace(ks.clone(), false).await.unwrap();
            session
                .query("DROP TABLE IF EXISTS connection_query_iter_tab", &[])
                .await
                .unwrap();
            session
                .query(
                    "CREATE TABLE IF NOT EXISTS connection_query_iter_tab (p int primary key)",
                    &[],
                )
                .await
                .unwrap();
        }

        connection
            .use_keyspace(&super::VerifiedKeyspaceName::new(ks, false).unwrap())
            .await
            .unwrap();

        // 1. SELECT from an empty table returns query result where rows are Some(Vec::new())
        let select_query = Query::new("SELECT p FROM connection_query_iter_tab").with_page_size(7);
        let empty_res = connection
            .clone()
            .query_iter(select_query.clone(), &[])
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(empty_res.is_empty());

        // 2. Insert 100 and select using query_iter with page_size 7
        let values: Vec<i32> = (0..100).collect();
        let mut insert_futures = Vec::new();
        let insert_query =
            Query::new("INSERT INTO connection_query_iter_tab (p) VALUES (?)").with_page_size(7);
        for v in &values {
            insert_futures.push(connection.query_single_page(insert_query.clone(), (v,)));
        }

        futures::future::try_join_all(insert_futures).await.unwrap();

        let mut results: Vec<i32> = connection
            .clone()
            .query_iter(select_query.clone(), &[])
            .await
            .unwrap()
            .into_typed::<(i32,)>()
            .map(|ret| ret.unwrap().0)
            .collect::<Vec<_>>()
            .await;
        results.sort_unstable(); // Clippy recommended to use sort_unstable instead of sort()
        assert_eq!(results, values);

        // 3. INSERT query_iter should work and not return any rows.
        let insert_res1 = connection
            .query_iter(insert_query, (0,))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(insert_res1.is_empty());
    }

    #[tokio::test]
    async fn test_lwt_optimisation_mark_negotiation() {
        const MASK: &str = "2137";

        let lwt_optimisation_entry = format!("{}={}", LWT_OPTIMIZATION_META_BIT_MASK_KEY, MASK);

        let proxy_addr = SocketAddr::new(scylla_proxy::get_exclusive_local_address(), 9042);

        let config = ConnectionConfig::default();

        let (startup_tx, mut startup_rx) = mpsc::unbounded_channel();

        let options_without_lwt_optimisation_support = HashMap::<String, Vec<String>>::new();
        let options_with_lwt_optimisation_support = [(
            SCYLLA_LWT_ADD_METADATA_MARK_EXTENSION.into(),
            vec![lwt_optimisation_entry.clone()],
        )]
        .into_iter()
        .collect::<HashMap<String, Vec<String>>>();

        let make_rules = |options| {
            vec![
                RequestRule(
                    Condition::RequestOpcode(RequestOpcode::Options),
                    RequestReaction::forge_response(Arc::new(move |frame: RequestFrame| {
                        ResponseFrame::forged_supported(frame.params, &options).unwrap()
                    })),
                ),
                RequestRule(
                    Condition::RequestOpcode(RequestOpcode::Startup),
                    RequestReaction::drop_frame().with_feedback_when_performed(startup_tx.clone()),
                ),
            ]
        };

        let mut proxy = Proxy::builder()
            .with_node(
                Node::builder()
                    .proxy_address(proxy_addr)
                    .request_rules(make_rules(options_without_lwt_optimisation_support))
                    .build_dry_mode(),
            )
            .build()
            .run()
            .await
            .unwrap();

        // We must interrupt the driver's full connection opening, because our proxy does not interact further after Startup.
        let startup_without_lwt_optimisation = select! {
            _ = open_connection(UntranslatedEndpoint::ContactPoint(ContactPoint{address: proxy_addr, datacenter: None}), None, config.clone()) => unreachable!(),
            startup = startup_rx.recv() => startup.unwrap(),
        };

        proxy.running_nodes[0]
            .change_request_rules(Some(make_rules(options_with_lwt_optimisation_support)));

        let startup_with_lwt_optimisation = select! {
            _ = open_connection(UntranslatedEndpoint::ContactPoint(ContactPoint{address: proxy_addr, datacenter: None}), None, config.clone()) => unreachable!(),
            startup = startup_rx.recv() => startup.unwrap(),
        };

        let _ = proxy.finish().await;

        let chosen_options =
            types::read_string_map(&mut &*startup_without_lwt_optimisation.body).unwrap();
        assert!(!chosen_options.contains_key(SCYLLA_LWT_ADD_METADATA_MARK_EXTENSION));

        let chosen_options =
            types::read_string_map(&mut &startup_with_lwt_optimisation.body[..]).unwrap();
        assert!(chosen_options.contains_key(SCYLLA_LWT_ADD_METADATA_MARK_EXTENSION));
        assert_eq!(
            chosen_options
                .get(SCYLLA_LWT_ADD_METADATA_MARK_EXTENSION)
                .unwrap(),
            &lwt_optimisation_entry
        )
    }
}
