/// ConnectionKeeper keeps a Connection to some address and works to keep it open
use crate::routing::ShardInfo;
use crate::transport::errors::QueryError;
use crate::transport::{
    connection,
    connection::{Connection, ConnectionConfig, VerifiedKeyspaceName},
};

use futures::{future::RemoteHandle, FutureExt};
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;

/// ConnectionKeeper keeps a Connection to some address and works to keep it open
pub struct ConnectionKeeper {
    conn_state_receiver: tokio::sync::watch::Receiver<ConnectionState>,
    use_keyspace_channel: tokio::sync::mpsc::Sender<UseKeyspaceRequest>,
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
    shard_info: Option<ShardInfo>,

    shard_info_sender: Option<ShardInfoSender>,
    conn_state_sender: tokio::sync::watch::Sender<ConnectionState>,

    // Channel used to receive use keyspace requests
    use_keyspace_channel: tokio::sync::mpsc::Receiver<UseKeyspaceRequest>,

    // Keyspace send in "USE <keyspace name>" when opening each connection
    used_keyspace: Option<VerifiedKeyspaceName>,
}

pub type ShardInfoSender = Arc<std::sync::Mutex<tokio::sync::watch::Sender<Option<ShardInfo>>>>;

#[derive(Debug)]
struct UseKeyspaceRequest {
    keyspace_name: VerifiedKeyspaceName,
    response_chan: tokio::sync::oneshot::Sender<Result<(), QueryError>>,
}

impl ConnectionKeeper {
    /// Creates new ConnectionKeeper that starts a connection in the background
    /// # Arguments
    ///
    /// * `address` - IP address to connect to
    /// * `compression` - preferred compression method to use
    /// * `shard_info` - ShardInfo to use, will connect to shard number `shard_info.shard`
    /// * `shard_info_sender` - channel to send new ShardInfo after each connection creation
    pub fn new(
        address: SocketAddr,
        config: ConnectionConfig,
        shard_info: Option<ShardInfo>,
        shard_info_sender: Option<ShardInfoSender>,
        keyspace_name: Option<VerifiedKeyspaceName>,
    ) -> Self {
        let (conn_state_sender, conn_state_receiver) =
            tokio::sync::watch::channel(ConnectionState::Initializing);

        let (use_keyspace_sender, use_keyspace_receiver) = tokio::sync::mpsc::channel(1);

        let worker = ConnectionKeeperWorker {
            address,
            config,
            shard_info,
            shard_info_sender,
            conn_state_sender,
            use_keyspace_channel: use_keyspace_receiver,
            used_keyspace: keyspace_name,
        };

        let (fut, worker_handle) = worker.work().remote_handle();
        tokio::spawn(fut);

        ConnectionKeeper {
            conn_state_receiver,
            use_keyspace_channel: use_keyspace_sender,
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

    pub async fn use_keyspace(
        &self,
        keyspace_name: VerifiedKeyspaceName,
    ) -> Result<(), QueryError> {
        let (response_sender, response_receiver) = tokio::sync::oneshot::channel();

        self.use_keyspace_channel
            .send(UseKeyspaceRequest {
                keyspace_name,
                response_chan: response_sender,
            })
            .await
            .expect("Bug in ConnectionKeeper::use_keyspace sending");
        // Other end of this channel is in the Worker, can't be dropped while we have &self to _worker_handle

        response_receiver.await.unwrap() // ConnectionKeeperWorker always responds
    }
}

impl ConnectionKeeperWorker {
    pub async fn work(mut self) {
        // Reconnect at most every 8 seconds
        let reconnect_cooldown = tokio::time::Duration::from_secs(8);
        let mut last_reconnect_time;

        loop {
            last_reconnect_time = tokio::time::Instant::now();

            // Connect and wait for error
            let current_error: QueryError = self.run_connection().await;

            // Mark the connection as broken, wait cooldown and reconnect
            let _ = self
                .conn_state_sender
                .send(ConnectionState::Broken(current_error));

            let next_reconnect_time = last_reconnect_time
                .checked_add(reconnect_cooldown)
                .unwrap_or_else(tokio::time::Instant::now);

            tokio::time::sleep_until(next_reconnect_time).await;
        }
    }

    // Opens a new connection and waits until some fatal error occurs
    async fn run_connection(&mut self) -> QueryError {
        // If shard is specified choose random source port
        let mut source_port: Option<u16> = None;
        if let Some(info) = &self.shard_info {
            source_port = Some(info.draw_source_port_for_shard(info.shard.into()));
        }

        // Connect to the node
        let connect_result =
            connection::open_connection(self.address, source_port, self.config.clone()).await;

        let (connection, mut error_receiver) = match connect_result {
            Ok((connection, error_receiver)) => (Arc::new(connection), error_receiver),
            Err(e) => return e,
        };

        // Mark connection as Connected
        let _ = self
            .conn_state_sender
            .send(ConnectionState::Connected(connection.clone()));

        // Notify about new shard info
        if let Some(sender) = &self.shard_info_sender {
            let new_shard_info: Option<ShardInfo> = connection.get_shard_info().clone();

            // Ignore sending error
            // If no one wants to get shard_info that's OK
            // If lock is poisoned do nothing
            if let Ok(sender_locked) = sender.lock() {
                let _ = sender_locked.send(new_shard_info);
            }
        }

        // Use the specified keyspace
        if let Some(keyspace_name) = &self.used_keyspace {
            let _ = connection.use_keyspace(&keyspace_name).await;
            // Ignore the error, used_keyspace could be set a long time ago and then deleted
            // user gets all errors from session.use_keyspace()
        }

        let connection_closed_error = QueryError::IOError(Arc::new(std::io::Error::new(
            ErrorKind::Other,
            "Connection closed",
        )));

        // Wait for events - a use keyspace request or a fatal error
        loop {
            tokio::select! {
                recv_res = self.use_keyspace_channel.recv() => {
                    match recv_res {
                        Some(request) => {
                            self.used_keyspace = Some(request.keyspace_name.clone());

                            // Send USE KEYSPACE request, send result if channel wasn't closed
                            let res = connection.use_keyspace(&request.keyspace_name).await;
                            let _ = request.response_chan.send(res);
                        },
                        None => return connection_closed_error,
                    }
                },
                connection_error = &mut error_receiver => {
                    return connection_error.unwrap_or(connection_closed_error);
                }
            }
        }
    }
}
