use anyhow::Result;
use bytes::Bytes;
use futures::{future::RemoteHandle, FutureExt};
use tokio::net::{tcp, TcpStream};
use tokio::sync::{mpsc, oneshot};

use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::sync::Mutex as StdMutex;

use crate::frame::{
    self,
    request::{self, execute, query, Request, RequestOpcode},
    response::{result, Response, ResponseOpcode},
    value::Value,
    FrameParams, RequestBodyWithExtensions,
};
use crate::query::Query;
use crate::routing::ShardInfo;
use crate::statement::prepared_statement::PreparedStatement;
use crate::transport::Compression;

pub struct Connection {
    submit_channel: mpsc::Sender<Task>,
    _worker_handle: RemoteHandle<()>,
    shard_info: Option<ShardInfo>,
    compression: Option<Compression>,
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
    pub async fn new(addr: SocketAddr, compression: Option<Compression>) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;

        // TODO: What should be the size of the channel?
        let (sender, receiver) = mpsc::channel(128);

        let (fut, _worker_handle) = Self::router(stream, receiver).remote_handle();
        tokio::task::spawn(fut);

        Ok(Self {
            submit_channel: sender,
            _worker_handle,
            shard_info: None,
            compression,
        })
    }

    pub async fn startup(&self, options: HashMap<String, String>) -> Result<Response> {
        self.send_request(&request::Startup { options }, false)
            .await
    }

    pub async fn get_options(&self) -> Result<Response> {
        self.send_request(&request::Options {}, false).await
    }

    pub async fn prepare(&self, query: String) -> Result<Response> {
        self.send_request(&request::Prepare { query }, true).await
    }

    pub async fn query_single_page<'a>(
        &self,
        query: impl Into<Query>,
        values: &'a [Value],
    ) -> Result<Option<Vec<result::Row>>> {
        let result = self.query(&query.into(), values, None).await?;
        match result {
            Response::Error(err) => Err(err.into()),
            Response::Result(result::Result::Rows(rs)) => Ok(Some(rs.rows)),
            Response::Result(_) => Ok(None),
            _ => Err(anyhow!("Unexpected frame received")),
        }
    }

    pub async fn query<'a>(
        &self,
        query: &Query,
        values: &'a [Value],
        paging_state: Option<Bytes>,
    ) -> Result<Response> {
        let query_frame = query::Query {
            contents: query.get_contents().to_owned(),
            parameters: query::QueryParameters {
                values,
                page_size: query.get_page_size(),
                paging_state,
                ..Default::default()
            },
        };

        self.send_request(&query_frame, true).await
    }

    pub async fn execute<'a>(
        &self,
        prepared_statement: &PreparedStatement,
        values: &'a [Value],
        paging_state: Option<Bytes>,
    ) -> Result<Response> {
        let execute_frame = execute::Execute {
            id: prepared_statement.get_id().to_owned(),
            parameters: query::QueryParameters {
                values,
                page_size: prepared_statement.get_page_size(),
                paging_state,
                ..Default::default()
            },
        };

        self.send_request(&execute_frame, true).await
    }

    // TODO: Return the response associated with that frame
    async fn send_request<R: Request>(&self, request: &R, compress: bool) -> Result<Response> {
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
            .map_err(|_| anyhow!("Request dropped"))?;

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
    ) -> Result<()> {
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
    ) -> Result<()> {
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

        Err(anyhow!("Task queue closed"))
    }

    pub fn get_shard_info(&self) -> &Option<ShardInfo> {
        &self.shard_info
    }

    pub fn set_shard_info(&mut self, shard_info: Option<ShardInfo>) {
        self.shard_info = shard_info
    }

    pub fn set_compression(&mut self, compression: Option<Compression>) {
        self.compression = compression;
    }
}

pub async fn open_connection(
    addr: SocketAddr,
    compression: Option<Compression>,
) -> Result<Connection> {
    // TODO: shouldn't all this logic be in Connection::new?
    let mut connection = Connection::new(addr, compression).await?;

    let options_result = connection.get_options().await?;

    let (shard_info, supported_compression) = match options_result {
        Response::Supported(mut supported) => {
            let shard_info = ShardInfo::try_from(&supported.options).ok();
            let supported_compression = supported
                .options
                .remove("COMPRESSION")
                .unwrap_or_else(Vec::new);
            (shard_info, supported_compression)
        }
        _ => (None, Vec::new()),
    };
    connection.set_shard_info(shard_info);

    let mut options = HashMap::new();
    options.insert("CQL_VERSION".to_string(), "4.0.0".to_string()); // FIXME: hardcoded values
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
        _ => return Err(anyhow!("Unexpected frame received")),
    }

    Ok(connection)
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
