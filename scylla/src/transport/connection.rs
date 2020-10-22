use anyhow::Result;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::Mutex;

use std::collections::HashMap;

use crate::frame::{
    self,
    request::{self, Request},
    response::Response,
};
use crate::query::Query;

pub struct Connection {
    stream: Mutex<TcpStream>,
}

impl Connection {
    pub async fn new(addr: impl ToSocketAddrs) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            stream: Mutex::new(stream),
        })
    }

    pub async fn startup(&self, options: HashMap<String, String>) -> Result<Response> {
        self.send_request(&request::Startup { options }).await
    }

    pub async fn query(&self, query: &Query) -> Result<Response> {
        self.send_request(&request::Query::from(query)).await
    }

    // TODO: Return the response associated with that frame
    async fn send_request(&self, request: &impl Request) -> Result<Response> {
        let raw_request = frame::serialize_request(Default::default(), request)?;
        let mut locked_stream = self.stream.lock().await;
        locked_stream.write_all(&raw_request).await?;

        let (_, response) = frame::read_response(&mut *locked_stream).await?;
        Ok(response)
    }
}
