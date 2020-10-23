use anyhow::Result;
use bytes::Bytes;
use tokio::net::{tcp, TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Mutex as StdMutex;

use crate::frame::{
    self,
    request::{self, execute, query, Request, RequestOpcode},
    response::{Response, ResponseOpcode},
    value::Value,
    FrameParams,
};
use crate::query::Query;
use crate::statement::prepared_statement::PreparedStatement;
use crate::transport::Compression;

pub struct Connection {
    submit_channel: mpsc::Sender<Task>,
    worker_handle: JoinHandle<()>,
}

type ResponseHandler = oneshot::Sender<TaskResponse>;

struct Task {
    request_opcode: RequestOpcode,
    request_body: Bytes,
    compress: bool,
    response_handler: ResponseHandler,
}

struct TaskResponse {
    opcode: ResponseOpcode,
    body: Bytes,
}

impl Connection {
    pub async fn new(addr: impl ToSocketAddrs, compression: Option<Compression>) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;

        // TODO: What should be the size of the channel?
        let (sender, receiver) = mpsc::channel(128);

        let handle = tokio::task::spawn(Self::router(stream, receiver, compression));

        Ok(Self {
            submit_channel: sender,
            worker_handle: handle,
        })
    }

    // It's not required for the worker to stop - it will also stop
    // if connection is dropped without closing - but it allows
    // to wait until the worker is finished.
    pub async fn close(self) {
        // This will cause the worker to eventually stop
        std::mem::drop(self.submit_channel);

        // Wait for the worker to stop
        self.worker_handle.await.unwrap();
    }

    pub async fn startup(&self, options: HashMap<String, String>) -> Result<Response> {
        self.send_request(&request::Startup { options }, false)
            .await
    }

    pub async fn prepare(&self, query: String) -> Result<Response> {
        self.send_request(&request::Prepare { query }, true).await
    }

    pub async fn query<'a>(&self, query: &Query, values: &'a [Value]) -> Result<Response> {
        let query_frame = query::Query {
            contents: query.get_contents().to_owned(),
            parameters: query::QueryParameters {
                consistency: 1, // ONE
                values,
            },
        };

        self.send_request(&query_frame, true).await
    }

    pub async fn execute<'a>(
        &self,
        prepared_statement: &PreparedStatement,
        values: &'a [Value],
    ) -> Result<Response> {
        let execute_frame = execute::Execute {
            id: prepared_statement.get_id().to_owned(),
            parameters: query::QueryParameters {
                consistency: 1, // ONE
                values,
            },
        };

        self.send_request(&execute_frame, true).await
    }

    // TODO: Return the response associated with that frame
    async fn send_request<R: Request>(&self, request: &R, compress: bool) -> Result<Response> {
        let raw_request = request.to_bytes()?;
        let (sender, receiver) = oneshot::channel();
        self.submit_channel
            .send(Task {
                request_opcode: R::OPCODE,
                request_body: raw_request,
                compress,
                response_handler: sender,
            })
            .await
            .map_err(|_| anyhow!("Request dropped"))?;

        let task_response = receiver.await?;
        let response = Response::deserialize(task_response.opcode, &mut &*task_response.body)?;

        Ok(response)
    }

    async fn router(
        mut stream: TcpStream,
        receiver: mpsc::Receiver<Task>,
        compression: Option<Compression>,
    ) {
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

        let r = Self::reader(read_half, &handler_map, compression);
        let w = Self::writer(write_half, &handler_map, receiver, compression);

        // TODO: What to do with this error?
        let _ = futures::try_join!(r, w);
    }

    async fn reader<'a>(
        mut read_half: tcp::ReadHalf<'a>,
        handler_map: &StdMutex<ResponseHandlerMap>,
        compression: Option<Compression>,
    ) -> Result<()> {
        loop {
            let (params, opcode, body) = frame::read_response(&mut read_half, compression).await?;

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
                let _ = handler.send(TaskResponse { opcode, body });
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
        compression: Option<Compression>,
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

            let compression = if task.compress { compression } else { None };

            frame::write_request(
                &mut write_half,
                params,
                task.request_opcode,
                task.request_body,
                compression,
            )
            .await?;
        }

        Err(anyhow!("Task queue closed"))
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
