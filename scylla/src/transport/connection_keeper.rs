/// ConnectionKeeper keeps a Connection to some address and works to keep it open
use crate::routing::ShardInfo;
use crate::transport::errors::QueryError;
use crate::transport::{
    connection,
    connection::{Connection, ConnectionConfig, ErrorReceiver, VerifiedKeyspaceName},
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
        let (connection, mut error_receiver) = match self.open_new_connection().await {
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

        let connection_closed_error = QueryError::IoError(Arc::new(std::io::Error::new(
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
                        None => return RunConnectionRes::ShouldStop, // If the channel was dropped we should stop
                    }
                },
                connection_error = &mut error_receiver => {
                    let error = connection_error.unwrap_or(connection_closed_error);
                    return RunConnectionRes::Error(error);
                }
            }
        }
    }

    async fn open_new_connection(&self) -> Result<(Arc<Connection>, ErrorReceiver), QueryError> {
        let (connection, error_receiver) = match &self.shard_info {
            Some(info) => self.open_new_connection_to_shard(info).await?,
            None => connection::open_connection(self.address, None, self.config.clone()).await?,
        };

        Ok((Arc::new(connection), error_receiver))
    }

    async fn open_new_connection_to_shard(
        &self,
        shard_info: &ShardInfo,
    ) -> Result<(Connection, ErrorReceiver), QueryError> {
        // Create iterator over all possible source ports for this shard
        let source_port_iter = shard_info.iter_source_ports_for_shard(shard_info.shard.into());

        for port in source_port_iter {
            let connect_result =
                connection::open_connection(self.address, Some(port), self.config.clone()).await;

            match connect_result {
                Err(err) if err.is_address_unavailable_for_use() => continue, // If we can't use this port, try the next one
                result => return result,
            }
        }

        // Tried all source ports for that shard, give up
        return Err(QueryError::IoError(Arc::new(std::io::Error::new(
            std::io::ErrorKind::AddrInUse,
            "Could not find free source port for shard",
        ))));
    }
}

#[cfg(test)]
mod tests {
    use super::ConnectionKeeper;
    use crate::transport::connection::ConnectionConfig;
    use std::net::{SocketAddr, ToSocketAddrs};

    // Open many connections to a node
    // Port collision should occur
    // If they are not handled this test will most likely fail
    #[tokio::test]
    async fn many_connections() {
        let connections_number = 512;

        let connect_address: SocketAddr = std::env::var("SCYLLA_URI")
            .unwrap_or_else(|_| "127.0.0.1:9042".to_string())
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();

        let connection_config = ConnectionConfig {
            compression: None,
            tcp_nodelay: true,
            #[cfg(feature = "ssl")]
            ssl_context: None,
            ..Default::default()
        };

        // Get shard info from a single connection, all connections will open to this shard
        let conn_keeper =
            ConnectionKeeper::new(connect_address, connection_config.clone(), None, None, None);
        let shard_info = conn_keeper
            .get_connection()
            .await
            .unwrap()
            .get_shard_info()
            .clone();

        // Open the connections
        let mut conn_keepers: Vec<ConnectionKeeper> = Vec::new();

        for _ in 0..connections_number {
            let conn_keeper = ConnectionKeeper::new(
                connect_address,
                connection_config.clone(),
                shard_info.clone(),
                None,
                None,
            );

            conn_keepers.push(conn_keeper);
        }

        // Check that each connection keeper connected succesfully
        for conn_keeper in conn_keepers {
            conn_keeper.get_connection().await.unwrap();
        }
    }
}
