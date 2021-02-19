use bytes::Bytes;
use futures::{future::RemoteHandle, FutureExt};
use tokio::io::{split, AsyncRead, AsyncWrite};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::{mpsc, oneshot};

use tokio_rustls::{rustls::ClientConfig, webpki::DNSNameRef, TlsConnector};

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

use super::errors::{BadKeyspaceName, BadQuery, QueryError};

use crate::batch::{Batch, BatchStatement};
use crate::frame::{
    self,
    request::{self, batch, execute, query, Request, RequestOpcode},
    response::{result, Response, ResponseOpcode},
    value::{BatchValues, ValueList},
    FrameParams, RequestBodyWithExtensions,
};
use crate::query::Query;
use crate::routing::ShardInfo;
use crate::statement::prepared_statement::PreparedStatement;
use crate::transport::Compression;

pub struct Connection {
    submit_channel: mpsc::Sender<Task>,
    _worker_handle: RemoteHandle<()>,
    connect_address: SocketAddr,
    source_port: u16,
    shard_info: Option<ShardInfo>,
    config: ConnectionConfig,
    is_shard_aware: bool,
}

type ResponseHandler = oneshot::Sender<TaskResponse>;

struct Task {
    request_flags: u8,
    request_opcode: RequestOpcode,
    request_body: Bytes,
    response_handler: ResponseHandler,
}

struct TaskResponse {
    params: FrameParams,
    opcode: ResponseOpcode,
    body: Bytes,
}

#[derive(Clone)]
pub struct ConnectionConfig {
    pub compression: Option<Compression>,
    pub tcp_nodelay: bool,
    pub use_tls: bool,
    /*
    These configuration options will be added in the future:

    pub auth_username: Option<String>,
    pub auth_password: Option<String>,

    pub tls_certificate_path: Option<String>,

    pub tcp_keepalive: bool,

    pub load_balancing: Option<String>,
    pub retry_policy: Option<String>,

    pub default_consistency: Option<String>,
    */
}

impl Connection {
    pub async fn new(
        addr: SocketAddr,
        source_port: Option<u16>,
        config: ConnectionConfig,
    ) -> Result<Self, std::io::Error> {
        let stream = match source_port {
            Some(p) => connect_with_source_port(addr, p).await?,
            None => TcpStream::connect(addr).await?,
        };
        let source_port = stream.local_addr()?.port();
        stream.set_nodelay(config.tcp_nodelay)?;

        // TODO: What should be the size of the channel?
        let (sender, receiver) = mpsc::channel(128);

        let _worker_handle = match config.use_tls {
            true => {
                let mut tls_config = ClientConfig::new();
                tls_config
                    .root_store
                    .add_server_trust_anchors(&webpki_roots::TLS_SERVER_ROOTS);
                let connector = TlsConnector::from(Arc::new(tls_config));
                let domain = addr.to_string();
                let domain = DNSNameRef::try_from_ascii_str(&domain).map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid dnsname")
                })?;
                Self::run_router(connector.connect(domain, stream).await?, receiver)
            }
            false => Self::run_router(stream, receiver),
        };

        Ok(Self {
            submit_channel: sender,
            _worker_handle,
            source_port,
            connect_address: addr,
            shard_info: None,
            config,
            is_shard_aware: false,
        })
    }

    pub async fn startup(&self, options: HashMap<String, String>) -> Result<Response, QueryError> {
        self.send_request(&request::Startup { options }, false)
            .await
    }

    pub async fn get_options(&self) -> Result<Response, QueryError> {
        self.send_request(&request::Options {}, false).await
    }

    pub async fn prepare(&self, query: &str) -> Result<PreparedStatement, QueryError> {
        let result = self.send_request(&request::Prepare { query }, true).await?;
        match result {
            Response::Error(err) => Err(err.into()),
            Response::Result(result::Result::Prepared(p)) => Ok(PreparedStatement::new(
                p.id,
                p.prepared_metadata,
                query.to_owned(),
            )),
            _ => Err(QueryError::ProtocolError(
                "PREPARE: Unexpected server response",
            )),
        }
    }

    pub async fn query_single_page(
        &self,
        query: impl Into<Query>,
        values: impl ValueList,
    ) -> Result<Option<Vec<result::Row>>, QueryError> {
        let result = self.query(&query.into(), values, None).await?;
        match result {
            Response::Error(err) => Err(err.into()),
            Response::Result(result::Result::Rows(rs)) => Ok(Some(rs.rows)),
            Response::Result(_) => Ok(None),
            _ => Err(QueryError::ProtocolError(
                "QUERY: Unexpected server response",
            )),
        }
    }

    pub async fn query(
        &self,
        query: &Query,
        values: impl ValueList,
        paging_state: Option<Bytes>,
    ) -> Result<Response, QueryError> {
        let serialized_values = values.serialized()?;

        let query_frame = query::Query {
            contents: query.get_contents().to_owned(),
            parameters: query::QueryParameters {
                consistency: query.get_consistency(),
                values: &serialized_values,
                page_size: query.get_page_size(),
                paging_state,
            },
        };

        self.send_request(&query_frame, true).await
    }

    pub async fn execute(
        &self,
        prepared_statement: &PreparedStatement,
        values: impl ValueList,
        paging_state: Option<Bytes>,
    ) -> Result<Response, QueryError> {
        let serialized_values = values.serialized()?;

        let execute_frame = execute::Execute {
            id: prepared_statement.get_id().to_owned(),
            parameters: query::QueryParameters {
                consistency: prepared_statement.get_consistency(),
                values: &serialized_values,
                page_size: prepared_statement.get_page_size(),
                paging_state,
            },
        };

        self.send_request(&execute_frame, true).await
    }

    pub async fn batch(
        &self,
        batch: &Batch,
        values: impl BatchValues,
    ) -> Result<Response, QueryError> {
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
        };

        self.send_request(&batch_frame, true).await
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

        match query_response {
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

    // TODO: Return the response associated with that frame
    async fn send_request<R: Request>(
        &self,
        request: &R,
        compress: bool,
    ) -> Result<Response, QueryError> {
        let body = request.to_bytes()?;
        let compression = if compress {
            self.config.compression
        } else {
            None
        };
        let body_with_ext = RequestBodyWithExtensions { body };

        let (flags, raw_request) =
            frame::prepare_request_body_with_extensions(body_with_ext, compression)?;

        let (sender, receiver) = oneshot::channel();

        self.submit_channel
            .send(Task {
                request_flags: flags,
                request_opcode: R::OPCODE,
                request_body: raw_request,
                response_handler: sender,
            })
            .await
            .map_err(|_| {
                QueryError::IOError(Arc::new(std::io::Error::new(
                    ErrorKind::Other,
                    "Connection broken",
                )))
            })?;

        let task_response = receiver.await.map_err(|_| {
            QueryError::IOError(Arc::new(std::io::Error::new(
                ErrorKind::Other,
                "Connection broken",
            )))
        })?;
        let body_with_ext = frame::parse_response_body_extensions(
            task_response.params.flags,
            self.config.compression,
            task_response.body,
        )?;

        // TODO: Do something more sensible with warnings
        // For now, just print them to stderr
        for warning in body_with_ext.warnings {
            eprintln!("Warning: {}", warning);
        }

        let response = Response::deserialize(task_response.opcode, &mut &*body_with_ext.body)?;

        Ok(response)
    }

    fn run_router(
        stream: (impl AsyncRead + AsyncWrite + Send + 'static),
        receiver: mpsc::Receiver<Task>,
    ) -> RemoteHandle<()> {
        let (task, handle) = Self::router(stream, receiver).remote_handle();
        tokio::task::spawn(task);
        handle
    }

    async fn router(stream: (impl AsyncRead + AsyncWrite), receiver: mpsc::Receiver<Task>) {
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

        let r = Self::reader(read_half, &handler_map);
        let w = Self::writer(write_half, &handler_map, receiver);

        // TODO: What to do with this error?
        let _ = futures::try_join!(r, w);
    }

    async fn reader(
        mut read_half: (impl AsyncRead + Unpin),
        handler_map: &StdMutex<ResponseHandlerMap>,
    ) -> Result<(), QueryError> {
        loop {
            let (params, opcode, body) = frame::read_response_frame(&mut read_half).await?;

            match params.stream.cmp(&-1) {
                Ordering::Less => {
                    // The spec reserves negative-numbered streams for server-generated
                    // events. As of writing this driver, there are no other negative
                    // streams used apart from -1, so ignore it.
                    continue;
                }
                Ordering::Equal => {
                    // TODO: Server events
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
                let _ = handler.send(TaskResponse {
                    params,
                    opcode,
                    body,
                });
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
                    continue;
                }
            };

            let params = frame::FrameParams {
                stream: stream_id,
                flags: task.request_flags,
                ..Default::default()
            };

            frame::write_request_frame(
                &mut write_half,
                params,
                task.request_opcode,
                task.request_body,
            )
            .await?;
        }

        Err(std::io::Error::new(ErrorKind::Other, "connection broken").into())
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
) -> Result<Connection, QueryError> {
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
) -> Result<Connection, QueryError> {
    // TODO: shouldn't all this logic be in Connection::new?
    let mut connection = Connection::new(addr, source_port, config.clone()).await?;

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
        Response::Authenticate => unimplemented!("Authentication is not yet implemented"),
        _ => {
            return Err(QueryError::ProtocolError(
                "Unexpected response to STARTUP message",
            ))
        }
    }

    Ok(connection)
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
    stream_set: StreamIDSet,
    handlers: HashMap<i16, ResponseHandler>,
}

impl ResponseHandlerMap {
    pub fn new() -> Self {
        Self {
            stream_set: StreamIDSet::new(),
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
}

struct StreamIDSet {
    used_bitmap: Box<[u64]>,
}

impl StreamIDSet {
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
