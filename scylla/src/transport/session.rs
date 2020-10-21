use anyhow::Result;
use tokio::net::ToSocketAddrs;

use crate::query::Query;
use crate::transport::connection::Connection;

pub struct Session {
    connection: Connection,
}

impl Session {
    pub async fn connect(addr: impl ToSocketAddrs) -> Result<Self> {
        let connection = Connection::new(addr).await?;

        connection.startup(Default::default()).await?;

        Ok(Session { connection })
    }

    // TODO: Should return an iterator over results
    pub async fn query(&self, query: impl Into<Query>) -> Result<()> {
        self.connection.query(&query.into()).await?;
        Ok(())
    }
}
