use anyhow::Result;
use futures::{future::RemoteHandle, FutureExt};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::net::ToSocketAddrs;

use crate::frame::response::result;
use crate::frame::response::Response;
use crate::frame::value::Value;
use crate::prepared_statement::PreparedStatement;
use crate::query::Query;
use crate::routing::{murmur3_token, Node, Token};
use crate::transport::connection::{open_connection, Connection};
use crate::transport::iterator::RowIterator;
use crate::transport::topology::{Topology, TopologyReader};
use crate::transport::Compression;

const POOL_LOCK_POISONED: &str =
    "Connection pool lock is poisoned, this session is no longer usable. \
                                  Drop it and create a new one.";

pub struct Session {
    // invariant: nonempty
    // PLEASE DO NOT EXPOSE IT TO THE USER OR PEOPLE WILL DIE
    pool: Arc<RwLock<HashMap<Node, Arc<Connection>>>>,

    // compression options passed to the session when it was created
    compression: Option<Compression>,

    topology: Topology,
    _topology_reader_handle: RemoteHandle<()>,
}

/// Represents a CQL session, which can be used to communicate
/// with the database
impl Session {
    // because it's more convenient
    /// Estabilishes a CQL session with the database
    /// # Arguments
    ///
    /// * `addrs` - address/addresses of the server
    /// * `compression` - optional compression settings
    pub async fn connect(
        addrs: impl ToSocketAddrs + Clone,
        compression: Option<Compression>,
    ) -> Result<Self> {
        let connection = open_connection(addrs, compression).await?;
        let addr = connection.get_addr();
        let node = Node { addr: addr.clone() };

        let (topology_reader, topology) = TopologyReader::new(node).await?;
        let (fut, _topology_reader_handle) = topology_reader.run().remote_handle();
        tokio::task::spawn(fut);

        let pool = Arc::new(RwLock::new(
            vec![(node, Arc::new(connection))].into_iter().collect(),
        ));

        Ok(Session {
            pool,
            compression,
            topology,
            _topology_reader_handle,
        })
    }

    // TODO: Should return an iterator over results
    // actually, if we consider "INSERT" a query, then no.
    // But maybe "INSERT" and "SELECT" should go through different methods,
    // so we expect "SELECT" to always return Vec<result::Row>?
    /// Sends a query to the database and receives a response.
    /// If `query` has paging enabled, this will return only the first page.
    /// # Arguments
    ///
    /// * `query` - query to be performed
    /// * `values` - values bound to the query
    pub async fn query<'a>(
        &self,
        query: impl Into<Query>,
        values: &'a [Value],
    ) -> Result<Option<Vec<result::Row>>> {
        self.any_connection()?
            .query_single_page(query, values)
            .await
    }

    pub fn query_iter(&self, query: impl Into<Query>, values: &[Value]) -> Result<RowIterator> {
        Ok(RowIterator::new_for_query(
            self.any_connection()?,
            query.into(),
            values.to_owned(),
        ))
    }

    /// Prepares a statement on the server side and returns a prepared statement,
    /// which can later be used to perform more efficient queries
    /// # Arguments
    ///#
    pub async fn prepare(&self, query: &str) -> Result<PreparedStatement> {
        // FIXME: Prepared statement ids are local to a node, so we must make sure
        // that prepare() sends to all nodes and keeps all ids.
        let result = self.any_connection()?.prepare(query.to_owned()).await?;
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
        let token = calculate_token(prepared, values);
        let connection = self.pick_connection(token).await?;
        let result = connection.execute(prepared, values, None).await?;
        match result {
            Response::Error(err) => {
                match err.code {
                    9472 => {
                        // Repreparation of a statement is needed
                        let reprepared_result = connection
                            .prepare(prepared.get_statement().to_owned())
                            .await?;
                        // Reprepared statement should keep its id - it's the md5 sum
                        // of statement contents
                        match reprepared_result {
                            Response::Error(err) => return Err(err.into()),
                            Response::Result(result::Result::Prepared(reprepared)) => {
                                if reprepared.id != prepared.get_id() {
                                    return Err(anyhow!(
                                        "Reprepared statement unexpectedly changed its id"
                                    ));
                                }
                            }
                            _ => return Err(anyhow!("Unexpected frame received")),
                        }
                        let result = connection.execute(prepared, values, None).await?;
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

    pub fn execute_iter(
        &self,
        prepared: impl Into<PreparedStatement>,
        values: &[Value],
    ) -> Result<RowIterator> {
        Ok(RowIterator::new_for_prepared_statement(
            self.any_connection()?,
            prepared.into(),
            values.to_owned(),
        ))
    }

    /// Returns all connections that the session has currently opened.
    pub fn get_connections(&self) -> Result<Vec<(Node, Arc<Connection>)>> {
        Ok(self
            .pool
            .read()
            .map_err(|_| anyhow!(POOL_LOCK_POISONED))?
            .iter()
            .map(|(&node, conn)| (node, conn.to_owned()))
            .collect())
    }

    pub async fn refresh_topology(&self) -> Result<()> {
        self.topology.refresh().await
    }

    async fn pick_connection(&self, t: Token) -> Result<Arc<Connection>> {
        // TODO: we try only the owner of the range (vnode) that the token lies in
        // we should calculate the *set* of replicas for this token, using the replication strategy
        // of the table being queried.
        let owner = self.topology.read_ring()?.owner(t);
        if let Some(c) = self
            .pool
            .read()
            .map_err(|_| anyhow!(POOL_LOCK_POISONED))?
            .get(&owner)
        {
            return Ok(c.clone());
        }

        // We don't have a connection for this node yet, create it.
        // TODO: don't do this on the query path, but concurrently in some other fiber?
        // Take any_connection() if there is no connection for this node (yet)?
        let new_conn = open_connection(owner.addr, self.compression).await?;

        Ok(self
            .pool
            .write()
            .map_err(|_| anyhow!(POOL_LOCK_POISONED))?
            .entry(owner)
            // If someone opened a connection to this node faster while we were creating our own
            // and inserted it into the pool, drop our connection and take the existing one.
            .or_insert_with(|| Arc::new(new_conn))
            .to_owned())
    }

    fn any_connection(&self) -> Result<Arc<Connection>> {
        Ok(self
            .pool
            .read()
            // TODO: better error message?
            .map_err(|_| anyhow!(POOL_LOCK_POISONED))?
            .values()
            .next()
            .ok_or_else(|| anyhow!("fatal error, broken invariant: no connections available"))?
            .to_owned())
    }
}

fn calculate_token<'a>(stmt: &PreparedStatement, values: &'a [Value]) -> Token {
    // TODO: take the partitioner of the table that is being queried and calculate the token using
    // that partitioner. The below logic gives correct token only for murmur3partitioner
    murmur3_token(stmt.compute_partition_key(values))
}
