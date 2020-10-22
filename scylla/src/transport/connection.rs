use anyhow::Result;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::Mutex;

use std::collections::HashMap;

use crate::frame::{
    self,
    request::{self, execute, Request},
    response::Response,
    value::Value,
};
use crate::query::Query;
use crate::statement::prepared_statement::PreparedStatement;
use crate::transport::Compression;

pub struct Connection {
    stream: Mutex<TcpStream>,
    compression: Option<Compression>,
}

impl Connection {
    pub async fn new(addr: impl ToSocketAddrs, compression: Option<Compression>) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            stream: Mutex::new(stream),
            compression,
        })
    }

    pub async fn startup(&self, options: HashMap<String, String>) -> Result<Response> {
        self.send_request(&request::Startup { options }, None).await
    }

    pub async fn prepare(&self, query: String) -> Result<Response> {
        self.send_request(&request::Prepare { query }, self.compression)
            .await
    }

    pub async fn query(&self, query: &Query) -> Result<Response> {
        self.send_request(&request::Query::from(query), self.compression)
            .await
    }

    pub async fn execute(
        &self,
        prepared_statement: &PreparedStatement,
        values: Vec<Value>,
    ) -> Result<Response> {
        let execute_frame = execute::Execute {
            id: prepared_statement.get_id().to_owned(),
            values,
        };
        //TODO: reprepare if execution failed because the statement was evicted from the server
        self.send_request(&execute_frame, self.compression).await
    }

    // TODO: Return the response associated with that frame
    async fn send_request<R: Request>(
        &self,
        request: &R,
        compression: Option<Compression>,
    ) -> Result<Response> {
        let raw_body = request.to_bytes()?;
        let mut locked_stream = self.stream.lock().await;
        frame::write_request(
            &mut *locked_stream,
            Default::default(),
            R::OPCODE,
            raw_body,
            compression,
        )
        .await?;

        let (_, opcode, raw_response) =
            frame::read_response(&mut *locked_stream, self.compression).await?;
        let response = Response::deserialize(opcode, &mut &*raw_response)?;

        Ok(response)
    }
}
