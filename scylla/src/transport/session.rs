use anyhow::Result;
use tokio::net::ToSocketAddrs;
use std::collections::HashMap;

use crate::frame::response::Response;
use crate::frame::response::result;
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
    // actually, if we consider "INSERT" a query, then no.
    // But maybe "INSERT" and "SELECT" should go through different methods,
    // so we expect "SELECT" to always return Vec<result::Row>?
    pub async fn query(&self, query: impl Into<Query>) -> Result<Option<Vec<result::Row>>> {
        let result = self.connection.query(&query.into()).await?;
        match result {
            Response::Error(err) => {
                Err(err.into())
            }
            Response::Result(result::Result::Rows(rs)) => {
                Ok(Some(rs.rows))
            }
            Response::Result(_) => {
                Ok(None)
            }
            _ => Err(anyhow!("Unexpected frame received")),
        }
    }

    pub async fn prepare(&self, query: String) -> Result<PreparedStatement> {
        let result = self.connection.prepare(query.clone()).await?;
        match result {
            Response::Error(err) => {
                Err(err.into())
            }
            Response::Result(result::Result::Prepared(p)) => {
                Ok(PreparedStatement::new(p.id, query))
            }
            _ => return Err(anyhow!("Unexpected frame received")),
        }
    }
}
