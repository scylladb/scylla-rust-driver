use anyhow::Result;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::net::{lookup_host, ToSocketAddrs};

use crate::frame::response::result;
use crate::frame::response::Response;
use crate::frame::value::Value;
use crate::prepared_statement::PreparedStatement;
use crate::query::Query;
use crate::transport::connection::Connection;
use crate::transport::Compression;

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Node {
    // TODO: a potentially node may have multiple addresses, remember them?
    // but we need an Ord instance on Node
    addr: SocketAddr,
}

pub struct Session {
    // invariant: nonempty
    pool: HashMap<Node, Connection>,
}

impl Session {
    pub async fn connect(
        addr: impl ToSocketAddrs + Clone,
        compression: Option<Compression>,
    ) -> Result<Self> {
        let mut options = HashMap::new();
        if let Some(compression) = &compression {
            let val = match compression {
                Compression::LZ4 => "lz4",
            };
            options.insert("COMPRESSION".to_string(), val.to_string());
        }

        let resolved = lookup_host(addr.clone())
            .await?
            .next()
            .map_or(Err(anyhow!("no addresses found")), |a| Ok(a))?;
        let connection = Connection::new(addr, compression).await?;
        connection.startup(options).await?;

        let pool = vec![(Node { addr: resolved }, connection)]
            .into_iter()
            .collect();

        Ok(Session { pool })
    }

    pub async fn close(self) {
        for (_, conn) in self.pool.into_iter() {
            conn.close().await;
        }
    }

    // TODO: Should return an iterator over results
    // actually, if we consider "INSERT" a query, then no.
    // But maybe "INSERT" and "SELECT" should go through different methods,
    // so we expect "SELECT" to always return Vec<result::Row>?
    pub async fn query<'a>(
        &self,
        query: impl Into<Query>,
        values: &'a [Value],
    ) -> Result<Option<Vec<result::Row>>> {
        let result = self.any_connection().query(&query.into(), values).await?;
        match result {
            Response::Error(err) => Err(err.into()),
            Response::Result(result::Result::Rows(rs)) => Ok(Some(rs.rows)),
            Response::Result(_) => Ok(None),
            _ => Err(anyhow!("Unexpected frame received")),
        }
    }

    pub async fn prepare(&self, query: &str) -> Result<PreparedStatement> {
        // FIXME: Prepared statement ids are local to a node, so we must make sure
        // that prepare() sends to all nodes and keeps all ids.
        let result = self.any_connection().prepare(query.to_owned()).await?;
        match result {
            Response::Error(err) => Err(err.into()),
            Response::Result(result::Result::Prepared(p)) => {
                Ok(PreparedStatement::new(p.id, query.to_owned()))
            }
            _ => return Err(anyhow!("Unexpected frame received")),
        }
    }

    pub async fn execute<'a>(
        &self,
        prepared: &mut PreparedStatement,
        values: &'a [Value],
    ) -> Result<Option<Vec<result::Row>>> {
        // FIXME: Prepared statement ids are local to a node, so we must make sure
        // that prepare() sends to all nodes and keeps all ids.
        let connection = self.any_connection();
        let result = connection.execute(prepared, values).await?;
        match result {
            Response::Error(err) => {
                match err.code {
                    9472 => {
                        // Repreparation of a statement is needed
                        let reprepared = self.prepare(prepared.get_statement()).await?;
                        prepared.update_id(reprepared.get_id().to_owned());
                        let result = connection.execute(prepared, values).await?;
                        match result {
                            Response::Error(err) => Err(err.into()),
                            Response::Result(result::Result::Rows(rs)) => Ok(Some(rs.rows)),
                            Response::Result(_) => Ok(None),
                            _ => Err(anyhow!("Unexpected frame received")),
                        }
                    }
                    _ => Err(err.into()),
                }
            }
            Response::Result(result::Result::Rows(rs)) => Ok(Some(rs.rows)),
            Response::Result(_) => Ok(None),
            _ => Err(anyhow!("Unexpected frame received")),
        }
    }

    fn any_connection(&self) -> &Connection {
        self.pool.values().next().unwrap()
    }
}
