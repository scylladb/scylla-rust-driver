use bytes::Bytes;
use futures::{future::RemoteHandle, FutureExt};
use tokio::io::{split, AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, warn};
use uuid::Uuid;

#[cfg(feature = "ssl")]
use openssl::ssl::{Ssl, SslContext};
#[cfg(feature = "ssl")]
use std::pin::Pin;
#[cfg(feature = "ssl")]
use tokio_openssl::SslStream;

use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::{
    cmp::Ordering,
    net::{Ipv4Addr, Ipv6Addr},
};

use super::errors::{BadKeyspaceName, BadQuery, DbError, QueryError};

use crate::batch::{Batch, BatchStatement};
use crate::frame::{
    self,
    request::{self, batch, execute, query, register, Request},
    response::{event::Event, result, Response, ResponseOpcode},
    server_event_type::EventType,
    value::{BatchValues, ValueList},
    FrameParams, SerializedRequest,
};
use crate::query::Query;
use crate::routing::ShardInfo;
use crate::statement::prepared_statement::PreparedStatement;
use crate::transport::session::IntoTypedRows;
use crate::transport::Authenticator;
use crate::transport::Authenticator::{
    AllowAllAuthenticator, CassandraAllowAllAuthenticator, CassandraPasswordAuthenticator,
    PasswordAuthenticator, ScyllaTransitionalAuthenticator,
};
use crate::transport::Compression;

// Queries for schema agreement
const LOCAL_VERSION: &str = "SELECT schema_version FROM system.local WHERE key='local'";

pub struct Connection {
    submit_channel: mpsc::Sender<Task>,
    _worker_handle: RemoteHandle<()>,
    connect_address: SocketAddr,
    source_port: u16,
    shard_info: Option<ShardInfo>,
    config: ConnectionConfig,
    is_shard_aware: bool,
}

type ResponseHandler = oneshot::Sender<Result<TaskResponse, QueryError>>;

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

/// Result of a single query  
/// Contains all rows returned by the database and some more information
#[derive(Default, Debug)]
pub struct QueryResult {
    /// Rows returned by the database
    pub rows: Option<Vec<result::Row>>,
    /// Warnings returned by the database
    pub warnings: Vec<String>,
    /// CQL Tracing uuid - can only be Some if tracing is enabled for this query
    pub tracing_id: Option<Uuid>,
    /// Paging state returned from the server
    pub paging_state: Option<Bytes>,
}

/// Result of Session::batch(). Contains no rows, only some useful information.
pub struct BatchResult {
    /// Warnings returned by the database
    pub warnings: Vec<String>,
    /// CQL Tracing uuid - can only be Some if tracing is enabled for this batch
    pub tracing_id: Option<Uuid>,
}

impl QueryResponse {
    pub fn into_query_result(self) -> Result<QueryResult, QueryError> {
        let (rows, paging_state) = match self.response {
            Response::Error(err) => return Err(err.into()),
            Response::Result(result::Result::Rows(rs)) => (Some(rs.rows), rs.metadata.paging_state),
            Response::Result(_) => (None, None),
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
        })
    }
}

#[derive(Clone)]
pub struct ConnectionConfig {
    pub compression: Option<Compression>,
    pub tcp_nodelay: bool,
    #[cfg(feature = "ssl")]
    pub ssl_context: Option<SslContext>,
    pub auth_username: Option<String>,
    pub auth_password: Option<String>,
    pub connect_timeout: std::time::Duration,
    // should be Some only in control connections,
    pub event_sender: Option<mpsc::Sender<Event>>,
    /*
    These configuration options will be added in the future:

    pub tcp_keepalive: bool,

    pub load_balancing: Option<String>,
    pub retry_policy: Option<String>,

    pub default_consistency: Option<String>,
    */
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            compression: None,
            tcp_nodelay: true,
            event_sender: None,
            #[cfg(feature = "ssl")]
            ssl_context: None,
            auth_username: None,
            auth_password: None,
            connect_timeout: std::time::Duration::from_secs(5),
        }
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
        let source_port = stream.local_addr()?.port();
        stream.set_nodelay(config.tcp_nodelay)?;

        // TODO: What should be the size of the channel?
        let (sender, receiver) = mpsc::channel(128);

        let (error_sender, error_receiver) = tokio::sync::oneshot::channel();

        let _worker_handle =
            Self::run_router(config.clone(), stream, receiver, error_sender).await?;

        let connection = Connection {
            submit_channel: sender,
            _worker_handle,
            source_port,
            connect_address: addr,
            shard_info: None,
            config,
            is_shard_aware: false,
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
        let query_response = self
            .send_request(
                &request::Prepare {
                    query: &query.get_contents(),
                },
                true,
                query.config.tracing,
            )
            .await?;

        let mut prepared_statement = match query_response.response {
            Response::Error(err) => return Err(err.into()),
            Response::Result(result::Result::Prepared(p)) => PreparedStatement::new(
                p.id,
                p.prepared_metadata,
                query.get_contents().to_owned(),
                query.get_page_size(),
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

    pub async fn authenticate_response(
        &self,
        username: Option<String>,
        password: Option<String>,
        authenticator: Authenticator,
    ) -> Result<QueryResponse, QueryError> {
        self.send_request(
            &request::AuthResponse {
                username,
                password,
                authenticator,
            },
            false,
            false,
        )
        .await
    }

    pub async fn query_single_page(
        &self,
        query: impl Into<Query>,
        values: impl ValueList,
    ) -> Result<QueryResult, QueryError> {
        let query: Query = query.into();
        self.query(&query, &values, None).await?.into_query_result()
    }

    pub async fn query_single_page_by_ref(
        &self,
        query: &Query,
        values: &impl ValueList,
        paging_state: Option<Bytes>,
    ) -> Result<QueryResult, QueryError> {
        self.query(query, values, paging_state)
            .await?
            .into_query_result()
    }

    pub async fn query(
        &self,
        query: &Query,
        values: impl ValueList,
        paging_state: Option<Bytes>,
    ) -> Result<QueryResponse, QueryError> {
        let serialized_values = values.serialized()?;

        let query_frame = query::Query {
            contents: query.get_contents().to_owned(),
            parameters: query::QueryParameters {
                consistency: query.get_consistency(),
                serial_consistency: query.get_serial_consistency(),
                values: &serialized_values,
                page_size: query.get_page_size(),
                paging_state,
            },
        };

        self.send_request(&query_frame, true, query.config.tracing)
            .await
    }

    pub async fn execute_single_page(
        &self,
        prepared_statement: &PreparedStatement,
        values: impl ValueList,
        paging_state: Option<Bytes>,
    ) -> Result<QueryResult, QueryError> {
        self.execute(prepared_statement, values, paging_state)
            .await?
            .into_query_result()
    }

    pub async fn execute(
        &self,
        prepared_statement: &PreparedStatement,
        values: impl ValueList,
        paging_state: Option<Bytes>,
    ) -> Result<QueryResponse, QueryError> {
        let serialized_values = values.serialized()?;

        let execute_frame = execute::Execute {
            id: prepared_statement.get_id().to_owned(),
            parameters: query::QueryParameters {
                consistency: prepared_statement.get_consistency(),
                serial_consistency: prepared_statement.get_serial_consistency(),
                values: &serialized_values,
                page_size: prepared_statement.get_page_size(),
                paging_state,
            },
        };

        let query_response = self
            .send_request(&execute_frame, true, prepared_statement.config.tracing)
            .await?;

        if let Response::Error(err) = &query_response.response {
            if err.error == DbError::Unprepared {
                // Repreparation of a statement is needed
                let reprepare_query: Query = prepared_statement.get_statement().into();
                let reprepared = self.prepare(&reprepare_query).await?;
                // Reprepared statement should keep its id - it's the md5 sum
                // of statement contents
                if reprepared.get_id() != prepared_statement.get_id() {
                    return Err(QueryError::ProtocolError(
                        "Prepared statement Id changed, md5 sum should stay the same",
                    ));
                }

                return self
                    .send_request(&execute_frame, true, prepared_statement.config.tracing)
                    .await;
            }
        }

        Ok(query_response)
    }

    pub async fn batch(
        &self,
        batch: &Batch,
        values: impl BatchValues,
    ) -> Result<BatchResult, QueryError> {
        let statements_count = batch.get_statements().len();
        if statements_count != values.len() {
            return Err(QueryError::BadQuery(BadQuery::ValueLenMismatch(
                values.len(),
                statements_count,
            )));
        }

        let statements_iter = batch.get_statements().iter().map(|s| match s {
            BatchStatement::Query(q) => batch::BatchStatement::Query {
                text: q.get_contents(),
            },
            BatchStatement::PreparedStatement(s) => {
                batch::BatchStatement::Prepared { id: s.get_id() }
            }
        });

        let batch_frame = batch::Batch {
            statements: statements_iter,
            statements_count,
            values,
            batch_type: batch.get_type(),
            consistency: batch.get_consistency(),
            serial_consistency: batch.get_serial_consistency(),
        };

        let query_response = self
            .send_request(&batch_frame, true, batch.config.tracing)
            .await?;

        match query_response.response {
            Response::Error(err) => Err(err.into()),
            Response::Result(_) => Ok(BatchResult {
                warnings: query_response.warnings,
                tracing_id: query_response.tracing_id,
            }),
            _ => Err(QueryError::ProtocolError(
                "BATCH: Unexpected server response",
            )),
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

        let (sender, receiver) = oneshot::channel();

        self.submit_channel
            .send(Task {
                serialized_request,
                response_handler: sender,
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
        })??;

        Self::parse_response(task_response, self.config.compression)
    }

    fn parse_response(
        task_response: TaskResponse,
        compression: Option<Compression>,
    ) -> Result<QueryResponse, QueryError> {
        let body_with_ext = frame::parse_response_body_extensions(
            task_response.params.flags,
            compression,
            task_response.body,
        )?;

        for warn_description in &body_with_ext.warnings {
            warn!(warning = warn_description.as_str());
        }

        let response = Response::deserialize(task_response.opcode, &mut &*body_with_ext.body)?;

        Ok(QueryResponse {
            response,
            warnings: body_with_ext.warnings,
            tracing_id: body_with_ext.trace_id,
        })
    }

    #[cfg(feature = "ssl")]
    async fn run_router(
        config: ConnectionConfig,
        stream: TcpStream,
        receiver: mpsc::Receiver<Task>,
        error_sender: tokio::sync::oneshot::Sender<QueryError>,
    ) -> Result<RemoteHandle<()>, std::io::Error> {
        let res = match config.ssl_context {
            Some(ref context) => {
                let ssl = Ssl::new(context)?;
                let mut stream = SslStream::new(ssl, stream)?;
                let _pin = Pin::new(&mut stream).connect().await;
                Self::run_router_spawner(stream, receiver, error_sender, config)
            }
            None => Self::run_router_spawner(stream, receiver, error_sender, config),
        };
        Ok(res)
    }

    #[cfg(not(feature = "ssl"))]
    async fn run_router(
        config: ConnectionConfig,
        stream: TcpStream,
        receiver: mpsc::Receiver<Task>,
        error_sender: tokio::sync::oneshot::Sender<QueryError>,
    ) -> Result<RemoteHandle<()>, std::io::Error> {
        Ok(Self::run_router_spawner(
            stream,
            receiver,
            error_sender,
            config,
        ))
    }

    fn run_router_spawner(
        stream: (impl AsyncRead + AsyncWrite + Send + 'static),
        receiver: mpsc::Receiver<Task>,
        error_sender: tokio::sync::oneshot::Sender<QueryError>,
        config: ConnectionConfig,
    ) -> RemoteHandle<()> {
        let (task, handle) = Self::router(stream, receiver, error_sender, config).remote_handle();
        tokio::task::spawn(task);
        handle
    }

    async fn router(
        stream: (impl AsyncRead + AsyncWrite),
        receiver: mpsc::Receiver<Task>,
        error_sender: tokio::sync::oneshot::Sender<QueryError>,
        config: ConnectionConfig,
    ) {
        let (read_half, write_half) = split(stream);
        // Why are using a mutex here?
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

        let r = Self::reader(read_half, &handler_map, config);
        let w = Self::writer(write_half, &handler_map, receiver);

        let result = futures::try_join!(r, w);

        let error: QueryError = match result {
            Ok(_) => return, // Connection was dropped, we can return
            Err(err) => err,
        };

        // Respond to all pending requests with the error
        let response_handlers: HashMap<i16, ResponseHandler> =
            handler_map.into_inner().unwrap().into_handlers();

        for (_, handler) in response_handlers {
            // Ignore sending error, request was dropped
            let _ = handler.send(Err(error.clone()));
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

            let handler = {
                // We are guaranteed here that handler_map will not be locked
                // by anybody else, so we can do try_lock().unwrap()
                let mut lock = handler_map.try_lock().unwrap();
                lock.take(params.stream)
            };

            if let Some(handler) = handler {
                // Don't care if sending of the response fails. This must
                // mean that the receiver side was impatient and is not
                // waiting for the result anymore.
                let _ = handler.send(Ok(response));
            } else {
                // Unsolicited frame. This should not happen and indicates
                // a bug either in the driver, or in the database
                return Err(QueryError::ProtocolError(
                    "Received reponse with unexpected StreamId",
                ));
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
        while let Some(task) = task_receiver.recv().await {
            let stream_id = {
                // We are guaranteed here that handler_map will not be locked
                // by anybody else, so we can do try_lock().unwrap()
                let mut lock = handler_map.try_lock().unwrap();

                if let Some(stream_id) = lock.allocate(task.response_handler) {
                    stream_id
                } else {
                    // TODO: Handle this error better, for now we drop this
                    // request and return an error to the receiver
                    error!("Could not allocate stream id");
                    continue;
                }
            };

            let mut req = task.serialized_request;
            req.set_stream(stream_id);
            write_half.write_all(req.get_data()).await?;
        }

        Ok(())
    }

    async fn handle_event(
        task_response: TaskResponse,
        compression: Option<Compression>,
        event_sender: &mpsc::Sender<Event>,
    ) -> Result<(), QueryError> {
        let response = Self::parse_response(task_response, compression)?.response;
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
        &self.shard_info
    }

    /// Are we connected to Scylla's shard aware port?
    // TODO: couple this with shard_info?
    pub fn get_is_shard_aware(&self) -> bool {
        self.is_shard_aware
    }

    pub fn get_source_port(&self) -> u16 {
        self.source_port
    }

    fn set_shard_info(&mut self, shard_info: Option<ShardInfo>) {
        self.shard_info = shard_info
    }

    fn set_is_shard_aware(&mut self, is_shard_aware: bool) {
        self.is_shard_aware = is_shard_aware;
    }

    pub fn get_connect_address(&self) -> SocketAddr {
        self.connect_address
    }
}

pub async fn open_connection(
    addr: SocketAddr,
    source_port: Option<u16>,
    config: ConnectionConfig,
) -> Result<(Connection, ErrorReceiver), QueryError> {
    open_named_connection(
        addr,
        source_port,
        config,
        Some("scylla-rust-driver".to_string()),
    )
    .await
}

pub async fn open_named_connection(
    addr: SocketAddr,
    source_port: Option<u16>,
    config: ConnectionConfig,
    driver_name: Option<String>,
) -> Result<(Connection, ErrorReceiver), QueryError> {
    // TODO: shouldn't all this logic be in Connection::new?
    let (mut connection, error_receiver) =
        Connection::new(addr, source_port, config.clone()).await?;

    let options_result = connection.get_options().await?;

    let (shard_info, supported_compression, shard_aware_port) = match options_result {
        Response::Supported(mut supported) => {
            let shard_info = ShardInfo::try_from(&supported.options).ok();
            let supported_compression = supported
                .options
                .remove("COMPRESSION")
                .unwrap_or_else(Vec::new);
            let shard_aware_port = supported
                .options
                .remove("SCYLLA_SHARD_AWARE_PORT")
                .unwrap_or_else(Vec::new)
                .into_iter()
                .next()
                .and_then(|p| p.parse::<u16>().ok());
            (shard_info, supported_compression, shard_aware_port)
        }
        _ => (None, Vec::new(), None),
    };
    connection.set_shard_info(shard_info);
    connection.set_is_shard_aware(Some(addr.port()) == shard_aware_port);

    let mut options = HashMap::new();
    options.insert("CQL_VERSION".to_string(), "4.0.0".to_string()); // FIXME: hardcoded values
    if let Some(name) = driver_name {
        options.insert("DRIVER_NAME".to_string(), name);
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
            let authenticator: Authenticator = match &authenticate.authenticator_name as &str {
                "AllowAllAuthenticator" => AllowAllAuthenticator,
                "PasswordAuthenticator" => PasswordAuthenticator,
                "org.apache.cassandra.auth.PasswordAuthenticator" => CassandraPasswordAuthenticator,
                "org.apache.cassandra.auth.AllowAllAuthenticator" => CassandraAllowAllAuthenticator,
                "com.scylladb.auth.TransitionalAuthenticator" => ScyllaTransitionalAuthenticator,
                _ => unimplemented!(
                    "Authenticator not supported, {}",
                    authenticate.authenticator_name
                ),
            };

            let username = connection.config.auth_username.to_owned();
            let password = connection.config.auth_password.to_owned();

            let auth_result = connection
                .authenticate_response(username, password, authenticator)
                .await?;
            match auth_result.response {
                Response::AuthChallenge(authenticate_challenge) => {
                    let challenge_message = authenticate_challenge.authenticate_message;
                    unimplemented!(
                        "Auth Challenge not implemented yet, {:?}",
                        challenge_message
                    )
                }
                Response::AuthSuccess(_authenticate_success) => {
                    // OK, continue
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

struct ResponseHandlerMap {
    stream_set: StreamIdSet,
    handlers: HashMap<i16, ResponseHandler>,
}

impl ResponseHandlerMap {
    pub fn new() -> Self {
        Self {
            stream_set: StreamIdSet::new(),
            handlers: HashMap::new(),
        }
    }

    pub fn allocate(&mut self, response_handler: ResponseHandler) -> Option<i16> {
        let stream_id = self.stream_set.allocate()?;
        let prev_handler = self.handlers.insert(stream_id, response_handler);
        assert!(prev_handler.is_none());
        Some(stream_id)
    }

    pub fn take(&mut self, stream_id: i16) -> Option<ResponseHandler> {
        self.stream_set.free(stream_id);
        self.handlers.remove(&stream_id)
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

    // "Keyspace names can have up to 48 alpha-numeric characters and contain underscores;
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

        // Verify all chars are alpha-numeric or underscore
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
