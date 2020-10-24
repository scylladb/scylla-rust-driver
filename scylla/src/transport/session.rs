use anyhow::Result;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::net::SocketAddr;
use tokio::net::{lookup_host, ToSocketAddrs};

use crate::frame::response::result;
use crate::frame::response::Response;
use crate::frame::value::Value;
use crate::prepared_statement::PreparedStatement;
use crate::query::Query;
use crate::routing::ShardInfo;
use crate::transport::connection::Connection;
use crate::transport::Compression;

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Node {
    // TODO: a potentially node may have multiple addresses, remember them?
    // but we need an Ord instance on Node
    addr: SocketAddr,
}

pub struct Session {
    // invariant: nonempty
    pool: HashMap<Node, Connection>,
}

/// Represents a CQL session, which can be used to communicate
/// with the database
impl Session {
    /// Estabilishes a CQL session with the database
    /// # Arguments
    ///
    /// * `addr` - address of the server
    /// * `compression` - optional compression settings
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
        let mut connection = Connection::new(addr, compression).await?;

        let options_result = connection.get_options().await?;
        let shard_info = match options_result {
            Response::Supported(supported) => ShardInfo::try_from(&supported.options).ok(),
            _ => None,
        };
        connection.set_shard_info(shard_info);

        let result = connection.startup(options).await?;
        match result {
            Response::Ready => {}
            Response::Authenticate => unimplemented!("Authentication is not yet implemented"),
            _ => return Err(anyhow!("Unexpected frame received")),
        }

        let pool = vec![(Node { addr: resolved }, connection)]
            .into_iter()
            .collect();

        Ok(Session { pool })
    }

    /// Closes a CQL session
    pub async fn close(self) {
        for (_, conn) in self.pool.into_iter() {
            conn.close().await;
        }
    }

    // TODO: Should return an iterator over results
    // actually, if we consider "INSERT" a query, then no.
    // But maybe "INSERT" and "SELECT" should go through different methods,
    // so we expect "SELECT" to always return Vec<result::Row>?
    /// Sends a query to the database and receives a response
    /// # Arguments
    ///
    /// * `query` - query to be performed
    /// * `values` - values bound to the query
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

    /// Prepares a statement on the server side and returns a prepared statement,
    /// which can later be used to perform more efficient queries
    /// # Arguments
    ///
    /// * `query` - query to be prepared
    pub async fn prepare(&self, query: &str) -> Result<PreparedStatement> {
        // FIXME: Prepared statement ids are local to a node, so we must make sure
        // that prepare() sends to all nodes and keeps all ids.
        let result = self.any_connection().prepare(query.to_owned()).await?;
        match result {
            Response::Error(err) => Err(err.into()),
            Response::Result(result::Result::Prepared(p)) => Ok(PreparedStatement::new(
                p.id,
                p.prepared_metadata,
                query.to_owned(),
            )),
            _ => return Err(anyhow!("Unexpected frame received")),
        }
    }

    /// Executes a previously prepared statement
    /// # Arguments
    ///
    /// * `prepared` - a statement prepared with [prepare](crate::transport::session::prepare)
    /// * `values` - values bound to the query
    pub async fn execute<'a>(
        &self,
        prepared: &PreparedStatement,
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
                        // Reprepared statement should keep its id - it's the md5 sum
                        // of statement contents
                        assert!(reprepared.get_id() == prepared.get_id());
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

    /// Returns the connection pool used by this session
    pub fn get_pool(&self) -> &HashMap<Node, Connection> {
        &self.pool
    }
}
