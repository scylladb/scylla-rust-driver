use anyhow::Result;
use tokio::net::ToSocketAddrs;
use std::collections::HashMap;

use crate::frame::response::Response;
use crate::query::Query;
use crate::prepared_statement::PreparedStatement;
use crate::transport::connection::Connection;
use crate::transport::Compression;

pub struct Session {
    connection: Connection,
}

impl Session {
    pub async fn connect(addr: impl ToSocketAddrs, compression: Option<Compression>) -> Result<Self> {
        let mut options = HashMap::new();
        if let Some(compression) = &compression {
            let val = match compression {
                Compression::LZ4 => "lz4",
            };
            options.insert("COMPRESSION".to_string(), val.to_string());
        }

        let connection = Connection::new(addr, compression).await?;

        connection.startup(options).await?;

        Ok(Session { connection })
    }

    // TODO: Should return an iterator over results
    pub async fn query(&self, query: impl Into<Query>) -> Result<()> {
        let result = self.connection.query(&query.into()).await?;
        match result {
            Response::Error(err) => {
                return Err(err.into());
            }
            Response::Result(_) => {}
            _ => return Err(anyhow!("Unexpected frame received")),
        }
        Ok(())
    }

    pub async fn prepare(&self, query: String) -> Result<PreparedStatement> {
        let result = self.connection.prepare(query).await?;
        match result {
            Response::Error(err) => {
                Err(err.into())
            }
            Response::Result(_) => {
                //FIXME: actually read the id
                Ok(PreparedStatement::new("stub_id".into()))
            }
            _ => return Err(anyhow!("Unexpected frame received")),
        }
    }
}
