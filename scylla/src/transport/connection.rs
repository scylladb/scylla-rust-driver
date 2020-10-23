use anyhow::Result;
use tokio::io::AsyncWriteExt;
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
    async fn send_request(
        &self,
        request: &impl Request,
        compression: Option<Compression>,
    ) -> Result<Response> {
        let raw_request = frame::serialize_request(Default::default(), request, compression)?;
        let mut locked_stream = self.stream.lock().await;
        locked_stream.write_all(&raw_request).await?;

        let (_, response) = frame::read_response(&mut *locked_stream, self.compression).await?;
        Ok(response)
    }
}
