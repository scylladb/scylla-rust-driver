/// ConnectionKeeper keeps a Connection to some address and works to keep it open
use crate::transport::errors::QueryError;
use crate::transport::{
    connection,
    connection::{Connection, ConnectionConfig, ErrorReceiver},
};

use futures::{future::RemoteHandle, FutureExt};
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;

/// ConnectionKeeper keeps a Connection to some address and works to keep it open
pub struct ConnectionKeeper {
    conn_state_receiver: tokio::sync::watch::Receiver<ConnectionState>,
    _worker_handle: RemoteHandle<()>,
}

#[derive(Clone)]
pub enum ConnectionState {
    Initializing, // First connect attempt ongoing
    Connected(Arc<Connection>),
    Broken(QueryError),
}

/// Works in the background to keep the connection open
struct ConnectionKeeperWorker {
    address: SocketAddr,
    config: ConnectionConfig,

    conn_state_sender: tokio::sync::watch::Sender<ConnectionState>,
}

impl ConnectionKeeper {
    /// Creates new ConnectionKeeper that starts a connection in the background
    /// # Arguments
    ///
    /// * `address` - IP address to connect to
    /// * `config` - connection configuration to use
    pub fn new(address: SocketAddr, config: ConnectionConfig) -> Self {
        let (conn_state_sender, conn_state_receiver) =
            tokio::sync::watch::channel(ConnectionState::Initializing);

        let worker = ConnectionKeeperWorker {
            address,
            config,
            conn_state_sender,
        };

        let (fut, worker_handle) = worker.work().remote_handle();
        tokio::spawn(fut);

        ConnectionKeeper {
            conn_state_receiver,
            _worker_handle: worker_handle,
        }
    }

    /// Get current connection state, returns immediately
    pub fn connection_state(&self) -> ConnectionState {
        self.conn_state_receiver.borrow().clone()
    }

    pub async fn wait_until_initialized(&self) {
        match &*self.conn_state_receiver.borrow() {
            ConnectionState::Initializing => {}
            _ => return,
        };

        let mut my_receiver = self.conn_state_receiver.clone();

        my_receiver
            .changed()
            .await
            .expect("Bug in ConnectionKeeper::wait_until_initialized");
        // Worker can't stop while we have &self to struct with worker_handle

        // Now state must be != Initializing
        debug_assert!(!matches!(
            &*self.conn_state_receiver.borrow(),
            ConnectionState::Initializing
        ));
    }

    /// Wait for the connection to initialize and get it if succesfylly connected
    pub async fn get_connection(&self) -> Result<Arc<Connection>, QueryError> {
        self.wait_until_initialized().await;

        match self.connection_state() {
            ConnectionState::Connected(conn) => Ok(conn),
            ConnectionState::Broken(e) => Err(e),
            _ => unreachable!(),
        }
    }
}

enum RunConnectionRes {
    // An error occured during the connection
    Error(QueryError),
    // ConnectionKeeper was dropped and channels closed, we should stop
    ShouldStop,
}

impl ConnectionKeeperWorker {
    pub async fn work(mut self) {
        // Reconnect at most every 8 seconds
        let reconnect_cooldown = tokio::time::Duration::from_secs(8);
        let mut last_reconnect_time;

        loop {
            last_reconnect_time = tokio::time::Instant::now();

            // Connect and wait for error
            let current_error: QueryError = match self.run_connection().await {
                RunConnectionRes::Error(e) => e,
                RunConnectionRes::ShouldStop => return,
            };

            // Mark the connection as broken, wait cooldown and reconnect
            if self
                .conn_state_sender
                .send(ConnectionState::Broken(current_error))
                .is_err()
            {
                // ConnectionKeeper was dropped, we should stop
                return;
            }

            let next_reconnect_time = last_reconnect_time
                .checked_add(reconnect_cooldown)
                .unwrap_or_else(tokio::time::Instant::now);

            tokio::time::sleep_until(next_reconnect_time).await;
        }
    }

    // Opens a new connection and waits until some fatal error occurs
    async fn run_connection(&mut self) -> RunConnectionRes {
        // Connect to the node
        let (connection, error_receiver) = match self.open_new_connection().await {
            Ok(opened) => opened,
            Err(e) => return RunConnectionRes::Error(e),
        };

        // Mark connection as Connected
        if self
            .conn_state_sender
            .send(ConnectionState::Connected(connection.clone()))
            .is_err()
        {
            // If the channel was dropped we should stop
            return RunConnectionRes::ShouldStop;
        }

        let connection_closed_error = QueryError::IoError(Arc::new(std::io::Error::new(
            ErrorKind::Other,
            "Connection closed",
        )));

        // Wait for a fatal error to occur
        let connection_error = error_receiver.await;
        let error = connection_error.unwrap_or(connection_closed_error);
        RunConnectionRes::Error(error)
    }

    async fn open_new_connection(&self) -> Result<(Arc<Connection>, ErrorReceiver), QueryError> {
        let (connection, error_receiver) =
            connection::open_connection(self.address, None, self.config.clone()).await?;

        Ok((Arc::new(connection), error_receiver))
    }
}
