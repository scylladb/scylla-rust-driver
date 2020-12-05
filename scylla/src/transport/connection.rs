use bytes::Bytes;
use futures::{future::RemoteHandle, FutureExt};
use tokio::net::{tcp, TcpSocket, TcpStream};
use tokio::sync::{mpsc, oneshot};

use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Mutex as StdMutex;

use super::transport_errors::{InternalDriverError, PrepareError, TransportError};

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
    source_port: u16,
    shard_info: Option<ShardInfo>,
    compression: Option<Compression>,
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

impl Connection {
    pub async fn new(
        addr: SocketAddr,
        source_port: Option<u16>,
        compression: Option<Compression>,
    ) -> Result<Self, TransportError> {
        let stream = match source_port {
            Some(p) => connect_with_source_port(addr, p).await?,
            None => TcpStream::connect(addr).await?,
        };
        let source_port = stream.local_addr()?.port();

        // TODO: What should be the size of the channel?
        let (sender, receiver) = mpsc::channel(128);

        let (fut, _worker_handle) = Self::router(stream, receiver).remote_handle();
        tokio::task::spawn(fut);

        Ok(Self {
            submit_channel: sender,
            _worker_handle,
            source_port,
            shard_info: None,
            compression,
            is_shard_aware: false,
        })
    }

    pub async fn startup(
        &self,
        options: HashMap<String, String>,
    ) -> Result<Response, TransportError> {
        self.send_request(&request::Startup { options }, false)
            .await
    }

    pub async fn get_options(&self) -> Result<Response, TransportError> {
        self.send_request(&request::Options {}, false).await
    }

    pub async fn prepare(&self, query: &str) -> Result<PreparedStatement, TransportError> {
        let result = self.send_request(&request::Prepare { query }, true).await?;
        match result {
            Response::Error(err) => Err(err.into()),
            Response::Result(result::Result::Prepared(p)) => Ok(PreparedStatement::new(
                p.id,
                p.prepared_metadata,
                query.to_owned(),
            )),
            _ => Err(TransportError::PrepareError(
                PrepareError::InternalDriverError(InternalDriverError::UnexpectedResponse),
            )),
        }
    }

    pub async fn query_single_page(
        &self,
        query: impl Into<Query>,
        values: impl ValueList,
    ) -> Result<Option<Vec<result::Row>>, TransportError> {
        let result = self.query(&query.into(), values, None).await?;
        match result {
            Response::Error(err) => Err(err.into()),
            Response::Result(result::Result::Rows(rs)) => Ok(Some(rs.rows)),
            Response::Result(_) => Ok(None),
            _ => Err(TransportError::PrepareError(
                PrepareError::InternalDriverError(InternalDriverError::UnexpectedResponse),
            )),
        }
    }

    pub async fn query(
        &self,
        query: &Query,
        values: impl ValueList,
        paging_state: Option<Bytes>,
    ) -> Result<Response, TransportError> {
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
    ) -> Result<Response, TransportError> {
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
    ) -> Result<Response, TransportError> {
        let statements_count = batch.get_statements().len();
        if statements_count != values.len() {
            return Err(TransportError::ValueLenMismatch(
                values.len(),
                statements_count,
            ));
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

    // TODO: Return the response associated with that frame
    async fn send_request<R: Request>(
        &self,
        request: &R,
        compress: bool,
    ) -> Result<Response, TransportError> {
        let body = request.to_bytes()?;
        let compression = if compress { self.compression } else { None };
        let body_with_ext = RequestBodyWithExtensions { body };

        let (flags, raw_request) =
            frame::prepare_request_body_with_extensions(body_with_ext, compression);

        let (sender, receiver) = oneshot::channel();

        self.submit_channel
            .send(Task {
                request_flags: flags,
                request_opcode: R::OPCODE,
                request_body: raw_request,
                response_handler: sender,
            })
            .await
            .map_err(|_| TransportError::DroppedRequest)?;

        let task_response = receiver.await?;
        let body_with_ext = frame::parse_response_body_extensions(
            task_response.params.flags,
            self.compression,
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

    async fn router(mut stream: TcpStream, receiver: mpsc::Receiver<Task>) {
        let (read_half, write_half) = stream.split();

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

    async fn reader<'a>(
        mut read_half: tcp::ReadHalf<'a>,
        handler_map: &StdMutex<ResponseHandlerMap>,
    ) -> Result<(), TransportError> {
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
                panic!(format!("Unsolicited frame on stream {}", params.stream));
            }
        }
    }

    async fn writer<'a>(
        mut write_half: tcp::WriteHalf<'a>,
        handler_map: &StdMutex<ResponseHandlerMap>,
        mut task_receiver: mpsc::Receiver<Task>,
    ) -> Result<(), TransportError> {
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

            let mut params = FrameParams::default();
            params.stream = stream_id;
            params.flags = task.request_flags;

            frame::write_request_frame(
                &mut write_half,
                params,
                task.request_opcode,
                task.request_body,
            )
            .await?;
        }

        Err(TransportError::TaskQueueClosed)
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

    fn set_compression(&mut self, compression: Option<Compression>) {
        self.compression = compression;
    }

    fn set_is_shard_aware(&mut self, is_shard_aware: bool) {
        self.is_shard_aware = is_shard_aware;
    }
}

pub async fn open_connection(
    addr: SocketAddr,
    source_port: Option<u16>,
    compression: Option<Compression>,
) -> Result<Connection, TransportError> {
    open_named_connection(
        addr,
        source_port,
        compression,
        Some("scylla-rust-driver".to_string()),
    )
    .await
}

pub async fn open_named_connection(
    addr: SocketAddr,
    source_port: Option<u16>,
    compression: Option<Compression>,
    driver_name: Option<String>,
) -> Result<Connection, TransportError> {
    // TODO: shouldn't all this logic be in Connection::new?
    let mut connection = Connection::new(addr, source_port, compression).await?;

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
    if let Some(compression) = &compression {
        let compression_str = compression.to_string();
        if supported_compression.iter().any(|c| c == &compression_str) {
            // Compression is reported to be supported by the server,
            // request it from the server
            options.insert("COMPRESSION".to_string(), compression.to_string());
        } else {
            // Fall back to no compression
            connection.set_compression(None);
        }
    }
    let result = connection.startup(options).await?;
    match result {
        Response::Ready => {}
        Response::Authenticate => unimplemented!("Authentication is not yet implemented"),
        _ => {
            return Err(TransportError::InternalDriverError(
                InternalDriverError::UnexpectedResponse,
            ))
        }
    }

    Ok(connection)
}

// TODO: if we use the same source port twice in a row, we'll get an "address already in use" error.
// We should try with multiple source ports in a loop.
async fn connect_with_source_port(
    addr: SocketAddr,
    source_port: u16,
) -> Result<TcpStream, TransportError> {
    // TODO: handle ipv6?
    let socket = TcpSocket::new_v4()?;
    let source_addr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
    socket.bind(SocketAddr::new(source_addr, source_port))?;
    Ok(socket.connect(addr).await?)
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
