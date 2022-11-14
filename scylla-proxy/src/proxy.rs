use crate::actions::{EvaluationContext, RequestRule, ResponseRule};
use crate::errors::{DoorkeeperError, ProxyError, WorkerError};
use crate::frame::{
    self, read_response_frame, write_frame, FrameOpcode, FrameParams, RequestFrame, ResponseFrame,
};
use crate::RequestOpcode;
use bytes::Bytes;
use scylla_cql::frame::types::read_string_multimap;
use std::collections::HashMap;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{broadcast, mpsc, Mutex};
use tracing::{debug, error, info, trace, warn};

// Used to notify the user that the proxy finished - this happens when all Senders are dropped.
type FinishWaiter = mpsc::Receiver<()>;
type FinishGuard = mpsc::Sender<()>;

// Used to tell all the proxy workers to stop when the user requests that with [RunningProxy::finish()].
type TerminateNotifier = tokio::sync::broadcast::Receiver<()>;
type TerminateSignaler = tokio::sync::broadcast::Sender<()>;

// Used to tell all proxy workers working on same connection to stop when
// a rule being applied has connection drop set.
type ConnectionCloseNotifier = tokio::sync::broadcast::Receiver<()>;
type ConnectionCloseSignaler = tokio::sync::broadcast::Sender<()>;

// Used to gather errors from all proxy workers and propagate them to the proxy user,
// returning the first of them from [RunningProxy::finish()].
type ErrorPropagator = mpsc::UnboundedSender<ProxyError>;
type ErrorSink = mpsc::UnboundedReceiver<ProxyError>;

static HARDCODED_OPTIONS_PARAMS: FrameParams = FrameParams {
    flags: 0,
    version: 0x04,
    stream: 0,
};

/// Specifies proxy's behaviour regarding shard awareness.
#[derive(Clone, Copy, Debug)]
pub enum ShardAwareness {
    /// Acts as if the connection was made to the shard-unaware port.
    Unaware,
    /// The first time the driver attempts to connect to the particular node (through proxy),
    /// the related node is first queried on a temporary connection for its number of shards,
    /// and only then establishes another connection for the driver's real communication with the node.
    QueryNode,
    /// Binds to the port that is the same as the driver's port modulo the provided number of shards.
    FixedNum(u16),
}

impl ShardAwareness {
    pub fn is_aware(&self) -> bool {
        !matches!(self, Self::Unaware)
    }
}

enum NodeType {
    Real {
        real_addr: SocketAddr,
        shard_awareness: ShardAwareness,
        response_rules: Option<Vec<ResponseRule>>,
    },
}

pub struct Node {
    proxy_addr: SocketAddr,
    request_rules: Option<Vec<RequestRule>>,
    node_type: NodeType,
}

impl Node {
    /// Creates an abstract node that is backed by a real Scylla node.
    pub fn new(
        real_addr: SocketAddr,
        proxy_addr: SocketAddr,
        shard_awareness: ShardAwareness,
        request_rules: Option<Vec<RequestRule>>,
        response_rules: Option<Vec<ResponseRule>>,
    ) -> Self {
        Self {
            proxy_addr,
            request_rules,
            node_type: NodeType::Real {
                real_addr,
                shard_awareness,
                response_rules,
            },
        }
    }

    pub fn builder() -> NodeBuilder {
        NodeBuilder {
            real_addr: None,
            proxy_addr: None,
            shard_awareness: None,
            request_rules: None,
            response_rules: None,
        }
    }
}

pub struct NodeBuilder {
    real_addr: Option<SocketAddr>,
    proxy_addr: Option<SocketAddr>,
    shard_awareness: Option<ShardAwareness>,
    request_rules: Option<Vec<RequestRule>>,
    response_rules: Option<Vec<ResponseRule>>,
}

impl NodeBuilder {
    pub fn real_address(mut self, real_addr: SocketAddr) -> Self {
        self.real_addr = Some(real_addr);
        self
    }

    pub fn proxy_address(mut self, proxy_addr: SocketAddr) -> Self {
        self.proxy_addr = Some(proxy_addr);
        self
    }

    pub fn shard_awareness(mut self, shard_awareness: ShardAwareness) -> Self {
        self.shard_awareness = Some(shard_awareness);
        self
    }

    pub fn request_rules(mut self, request_rules: Vec<RequestRule>) -> Self {
        self.request_rules = Some(request_rules);
        self
    }

    pub fn response_rules(mut self, response_rules: Vec<ResponseRule>) -> Self {
        self.response_rules = Some(response_rules);
        self
    }

    /// Creates an abstract node that is backed by a real Scylla node.
    pub fn build(self) -> Node {
        Node {
            proxy_addr: self.proxy_addr.expect("Proxy addr is required!"),
            request_rules: self.request_rules,
            node_type: NodeType::Real {
                real_addr: self.real_addr.expect("Real addr is required!"),
                shard_awareness: self.shard_awareness.expect("Shard awareness is required!"),
                response_rules: self.response_rules,
            },
        }
    }
}

struct InternalNode {
    real_addr: SocketAddr,
    proxy_addr: SocketAddr,
    shard_awareness: ShardAwareness,
    request_rules: Arc<Mutex<Vec<RequestRule>>>,
    response_rules: Arc<Mutex<Vec<ResponseRule>>>,
}

impl From<Node> for InternalNode {
    fn from(node: Node) -> Self {
        match node.node_type {
            NodeType::Real {
                real_addr,
                shard_awareness,
                response_rules,
            } => InternalNode {
                real_addr,
                proxy_addr: node.proxy_addr,
                shard_awareness,
                request_rules: node
                    .request_rules
                    .map(|rules| Arc::new(Mutex::new(rules)))
                    .unwrap_or_default(),
                response_rules: response_rules
                    .map(|rules| Arc::new(Mutex::new(rules)))
                    .unwrap_or_default(),
            },
        }
    }
}

pub struct ProxyBuilder {
    nodes: Vec<Node>,
}

impl ProxyBuilder {
    pub fn with_node(mut self, node: Node) -> ProxyBuilder {
        self.nodes.push(node);
        self
    }

    pub fn build(self) -> Proxy {
        Proxy::new(self.nodes)
    }
}

pub struct Proxy {
    nodes: Vec<InternalNode>,
}

impl Proxy {
    pub fn new(nodes: impl IntoIterator<Item = Node>) -> Self {
        Proxy {
            nodes: nodes.into_iter().map(|node| node.into()).collect(),
        }
    }

    pub fn builder() -> ProxyBuilder {
        ProxyBuilder { nodes: vec![] }
    }

    /// Build a translation map based on provided proxy and node addresses.
    /// The map can be passed to `Session` `address_translator()` to ensure
    /// that the driver contacts the nodes through the proxy (and not directly).
    pub fn translation_map(&self) -> HashMap<SocketAddr, SocketAddr> {
        let mut translation_map = HashMap::new();
        for node in self.nodes.iter() {
            translation_map.insert(node.real_addr, node.proxy_addr);
        }
        translation_map
    }

    /// Runs the [Proxy], i.e. makes it ready for accepting drivers' connections.
    /// Returns a [RunningProxy] handle that can be used to stop the proxy or change the rules.
    pub async fn run(self) -> Result<RunningProxy, DoorkeeperError> {
        let (terminate_signaler, _t) = tokio::sync::broadcast::channel(1);
        let (finish_guard, finish_waiter) = mpsc::channel(1);

        let (error_propagator, error_sink) = mpsc::unbounded_channel();
        let (doorkeepers, running_nodes): (Vec<_>, Vec<RunningNode>) = self
            .nodes
            .into_iter()
            .map(|node| {
                let running = RunningNode {
                    request_rules: node.request_rules.clone(),
                    response_rules: Some(node.response_rules.clone()),
                };
                (
                    Doorkeeper::spawn(
                        node,
                        terminate_signaler.clone(),
                        finish_guard.clone(),
                        error_propagator.clone(),
                    ),
                    running,
                )
            })
            .unzip();

        for doorkeeper in doorkeepers {
            doorkeeper.await?; // await doorkeeper creation, including binding to a socket
        }

        Ok(RunningProxy {
            terminate_signaler,
            finish_waiter,
            running_nodes,
            error_sink,
        })
    }
}

/// A handle that can be used to change the rules regarding the particular node.
pub struct RunningNode {
    request_rules: Arc<Mutex<Vec<RequestRule>>>,
    response_rules: Option<Arc<Mutex<Vec<ResponseRule>>>>,
}

impl RunningNode {
    /// Replaces the previous request rules with the new ones.
    pub async fn change_request_rules(&mut self, rules: Option<Vec<RequestRule>>) {
        *self.request_rules.lock().await = rules.unwrap_or_default();
    }

    /// Replaces the previous response rules with the new ones.
    pub async fn change_response_rules(&mut self, rules: Option<Vec<ResponseRule>>) {
        *self
            .response_rules
            .as_ref()
            .expect("No response rules on a simulated node!")
            .lock()
            .await = rules.unwrap_or_default();
    }
}

/// A handle that can be used to stop the proxy or change the rules.
pub struct RunningProxy {
    terminate_signaler: TerminateSignaler,
    finish_waiter: FinishWaiter,
    pub running_nodes: Vec<RunningNode>,
    error_sink: ErrorSink,
}

impl RunningProxy {
    /// Disables all the rules in the proxy, effectively making it a pass-through-only proxy.
    pub async fn turn_off_rules(&mut self) {
        for (request_rules, response_rules) in self
            .running_nodes
            .iter_mut()
            .map(|node| (&node.request_rules, &node.response_rules))
        {
            request_rules.lock().await.clear();
            if let Some(response_rules) = response_rules {
                response_rules.lock().await.clear();
            }
        }
    }

    /// Attempts to fetch the first error that has occured in proxy since last check.
    /// If no errors occured, returns Ok(()).
    pub fn sanity_check(&mut self) -> Result<(), ProxyError> {
        match self.error_sink.try_recv() {
            Ok(err) => Err(err),
            Err(TryRecvError::Empty) => Ok(()),
            Err(TryRecvError::Disconnected) => {
                // As we haven't awaited finish of all workers yet, there must be a faulty case without proper error handling.
                Err(ProxyError::SanityCheckFailure)
            }
        }
    }

    /// Waits until an error occurs in proxy. If proxy finishes with no errors occured, returns Err(()).
    pub async fn wait_for_error(&mut self) -> Option<ProxyError> {
        self.error_sink.recv().await
    }

    /// Requests termination of all proxy workers and awaits its completion.
    /// Returns the first error that occured in proxy.
    pub async fn finish(mut self) -> Result<(), ProxyError> {
        self.terminate_signaler.send(()).map_err(|err| {
            ProxyError::AwaitFinishFailure(format!(
                "Send error in terminate_signaler: {} (bug!)",
                err
            ))
        })?;
        info!("Sent finish signal to proxy workers.");

        // This to make sure that also workers not-yet-spawned when terminate signal was sent will terminate.
        std::mem::drop(self.terminate_signaler);

        if self.finish_waiter.recv().await.is_some() {
            unreachable!();
        };
        info!("All workers have finished.");

        match self.error_sink.try_recv() {
            Ok(err) => Err(err),
            Err(TryRecvError::Disconnected) => Ok(()),
            Err(TryRecvError::Empty) => {
                // As we have already awaited finish of all workers, there must be a logic bug.
                unreachable!("Worker await logic bug!");
            }
        }
    }
}

/// A worker corresponding to a particular node. It listens in a loop for driver's connections
/// on specified proxy bind address, respects ports regarding advanced shard-awareness (if set),
/// to this end obtaining number of shards from the node (if set), then establishes connection
/// to the node, spawns workers for this connection and continues to listen.
struct Doorkeeper {
    node: InternalNode,
    listener: TcpListener,
    terminate_signaler: TerminateSignaler,
    finish_guard: FinishGuard,
    shards_number: Option<u16>,
    error_propagator: ErrorPropagator,
}

impl Doorkeeper {
    async fn spawn(
        node: InternalNode,
        terminate_signaler: TerminateSignaler,
        finish_guard: FinishGuard,
        error_propagator: ErrorPropagator,
    ) -> Result<(), DoorkeeperError> {
        let listener = TcpListener::bind(node.proxy_addr)
            .await
            .map_err(|err| DoorkeeperError::DriverConnectionAttempt(node.proxy_addr, err))?;

        info!(
            "Spawned a {} doorkeeper for pair real:{} - proxy:{}.",
            if node.shard_awareness.is_aware() {
                "shard-aware"
            } else {
                "shard-unaware"
            },
            node.real_addr,
            node.proxy_addr,
        );
        let doorkeeper = Doorkeeper {
            shards_number: if let InternalNode {
                shard_awareness: ShardAwareness::FixedNum(shards_num),
                ..
            } = node
            {
                Some(shards_num)
            } else {
                None
            },
            node,
            listener,
            terminate_signaler,
            finish_guard,
            error_propagator,
        };
        tokio::task::spawn(doorkeeper.run());
        Ok(())
    }

    async fn run(mut self) {
        let mut own_terminate_notifier = self.terminate_signaler.subscribe();
        let (connection_close_tx, _connection_close_rx) = broadcast::channel::<()>(2);
        let mut connection_no: usize = 0;
        loop {
            tokio::select! {
                res = self.accept_connection(&connection_close_tx, connection_no) => {
                    match res {
                        Ok(()) => connection_no += 1,
                        Err(err) => {
                            error!("Error in doorkeeper with addr {} for node {}: {}", self.node.proxy_addr, self.node.real_addr, err);
                            let _ = self.error_propagator.send(err.into());
                            break;
                        },
                    }
                },
                _terminate = own_terminate_notifier.recv() => break
            }
        }
        debug!(
            "Doorkeeper exits: proxy {}, node {}.",
            self.node.proxy_addr, self.node.real_addr
        );
    }

    async fn spawn_workers(
        &mut self,
        driver_addr: SocketAddr,
        connection_close_tx: &ConnectionCloseSignaler,
        connection_no: usize,
        driver_stream: TcpStream,
        cluster_stream: TcpStream,
    ) {
        let (driver_read, driver_write) = driver_stream.into_split();

        let new_worker = || ProxyWorker {
            terminate_notifier: self.terminate_signaler.subscribe(),
            finish_guard: self.finish_guard.clone(),
            connection_close_notifier: connection_close_tx.subscribe(),
            error_propagator: self.error_propagator.clone(),
            driver_addr,
            real_addr: self.node.real_addr,
            proxy_addr: self.node.proxy_addr,
        };

        let (tx_request, rx_request) = mpsc::unbounded_channel::<RequestFrame>();
        let (tx_response, rx_response) = mpsc::unbounded_channel::<ResponseFrame>();
        let (tx_cluster, rx_cluster) = mpsc::unbounded_channel::<RequestFrame>();
        let (tx_driver, rx_driver) = mpsc::unbounded_channel::<ResponseFrame>();

        tokio::task::spawn(new_worker().receiver_from_driver(driver_read, tx_request));
        tokio::task::spawn(new_worker().sender_to_driver(
            driver_write,
            rx_driver,
            connection_close_tx.subscribe(),
            self.terminate_signaler.subscribe(),
        ));
        tokio::task::spawn(new_worker().request_processor(
            rx_request,
            tx_driver.clone(),
            tx_cluster.clone(),
            connection_no,
            self.node.request_rules.clone(),
            connection_close_tx.clone(),
        ));

        let (cluster_read, cluster_write) = cluster_stream.into_split();
        tokio::task::spawn(new_worker().sender_to_cluster(
            cluster_write,
            rx_cluster,
            connection_close_tx.subscribe(),
            self.terminate_signaler.subscribe(),
        ));
        tokio::task::spawn(new_worker().receiver_from_cluster(cluster_read, tx_response));
        tokio::task::spawn(new_worker().response_processor(
            rx_response,
            tx_driver,
            tx_cluster,
            connection_no,
            self.node.response_rules.clone(),
            connection_close_tx.clone(),
        ));

        debug!(
            "Doorkeeper with addr {} of node {} spawned workers.",
            self.node.proxy_addr, self.node.real_addr
        );
    }

    async fn accept_connection(
        &mut self,
        connection_close_tx: &ConnectionCloseSignaler,
        connection_no: usize,
    ) -> Result<(), DoorkeeperError> {
        let (driver_stream, driver_addr) = self.make_driver_stream(connection_no).await?;
        let cluster_stream = self
            .make_cluster_stream(driver_addr, self.node.real_addr, self.node.shard_awareness)
            .await?;

        self.spawn_workers(
            driver_addr,
            connection_close_tx,
            connection_no,
            driver_stream,
            cluster_stream,
        )
        .await;

        Ok(())
    }

    async fn make_driver_stream(
        &mut self,
        connection_no: usize,
    ) -> Result<(TcpStream, SocketAddr), DoorkeeperError> {
        let (driver_stream, driver_addr) =
            self.listener.accept().await.map_err(|err| {
                DoorkeeperError::DriverConnectionAttempt(self.node.proxy_addr, err)
            })?;
        info!(
            "Connected driver from {} to {}, connection no={}.",
            driver_addr, self.node.proxy_addr, connection_no
        );
        Ok((driver_stream, driver_addr))
    }

    async fn make_cluster_stream(
        &mut self,
        driver_addr: SocketAddr,
        real_addr: SocketAddr,
        shard_awareness: ShardAwareness,
    ) -> Result<TcpStream, DoorkeeperError> {
        let cluster_stream = if shard_awareness.is_aware() {
            let shards = match self.shards_number {
                None => {
                    let temporary_stream = TcpStream::connect(real_addr)
                        .await
                        .map_err(|err| DoorkeeperError::NodeConnectionAttempt(real_addr, err))?;
                    let shards = self
                        .obtain_shards_number(temporary_stream, real_addr)
                        .await?;
                    self.shards_number = Some(shards);

                    shards
                }
                Some(shards) => shards,
            };

            let socket = match self.node.proxy_addr.ip() {
                std::net::IpAddr::V4(_) => TcpSocket::new_v4(),
                std::net::IpAddr::V6(_) => TcpSocket::new_v6(),
            }
            .map_err(DoorkeeperError::SocketCreate)?;

            let shard_preserving_addr = {
                let mut desired_addr =
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), driver_addr.port());
                while socket.bind(desired_addr).is_err() {
                    // in search for a port that translates to the desired shard
                    let next_port = self.next_port_to_same_shard(desired_addr.port());
                    if next_port == driver_addr.port() {
                        return Err(DoorkeeperError::NoMorePorts);
                    }
                    desired_addr.set_port(next_port);
                }
                desired_addr
            };

            socket.connect(real_addr).await.map(|ok| {
                info!(
                    "Connected to the cluster from {} at {}, shard {}.",
                    ok.local_addr().unwrap(),
                    real_addr,
                    shard_preserving_addr.port() % shards
                );
                ok
            })
        } else {
            TcpStream::connect(real_addr).await.map(|ok| {
                info!("Connected to the cluster at {}.", real_addr);
                ok
            })
        }
        .map_err(|err| DoorkeeperError::NodeConnectionAttempt(real_addr, err))?;

        Ok(cluster_stream)
    }

    fn next_port_to_same_shard(&self, port: u16) -> u16 {
        port.wrapping_add(self.shards_number.unwrap())
    }

    async fn obtain_shards_number(
        &self,
        mut connection: TcpStream,
        real_addr: SocketAddr,
    ) -> Result<u16, DoorkeeperError> {
        write_frame(
            HARDCODED_OPTIONS_PARAMS,
            FrameOpcode::Request(RequestOpcode::Options),
            &Bytes::new(),
            &mut connection,
        )
        .await
        .map_err(DoorkeeperError::ObtainingShardNumber)?;

        let supported_frame = read_response_frame(&mut connection)
            .await
            .map_err(DoorkeeperError::ObtainingShardNumberFrame)?;

        let options = read_string_multimap(&mut supported_frame.body.as_ref())
            .map_err(DoorkeeperError::ObtainingShardNumberParseOptions)?;
        let nr_shards_entry = options.get("SCYLLA_NR_SHARDS");
        let shards = match nr_shards_entry
            .and_then(|vec| vec.first())
            .ok_or(DoorkeeperError::ObtainingShardNumberNoShardInfo)?
            .parse::<u16>()
            .map_err(DoorkeeperError::ObtainingShardNumberParseShardNumber)?
        {
            0u16 => Err(DoorkeeperError::ObtainingShardNumberGotZero),
            num => Ok(num),
        }?;
        info!("Obtained shards number on node {}: {}", real_addr, shards);
        Ok(shards)
    }
}

struct ProxyWorker {
    terminate_notifier: TerminateNotifier,
    finish_guard: FinishGuard,
    connection_close_notifier: ConnectionCloseNotifier,
    error_propagator: ErrorPropagator,
    driver_addr: SocketAddr,
    real_addr: SocketAddr,
    proxy_addr: SocketAddr,
}

impl ProxyWorker {
    fn exit(self, duty: &'static str) {
        debug!(
            "Worker exits: [driver: {}, proxy: {}, node: {}]::{}.",
            self.driver_addr, self.proxy_addr, self.real_addr, duty
        );
        std::mem::drop(self.finish_guard);
    }

    async fn run_until_interrupted<F, Fut>(mut self, worker_name: &'static str, f: F)
    where
        F: FnOnce(SocketAddr, SocketAddr, SocketAddr) -> Fut,
        Fut: Future<Output = Result<(), ProxyError>>,
    {
        let fut = f(self.driver_addr, self.proxy_addr, self.real_addr);

        tokio::select! {
            result = fut => {
                if let Err(err) = result {
                    // error_propagator could be a field
                    let _ = self.error_propagator.send(err);
                }
            }
            _ = self.terminate_notifier.recv() => (),
            _ = self.connection_close_notifier.recv() => (),
        }
        self.exit(worker_name);
    }

    async fn receiver_from_driver(
        self,
        mut read_half: (impl AsyncRead + Unpin),
        request_processor_tx: mpsc::UnboundedSender<RequestFrame>,
    ) {
        self.run_until_interrupted(
            "receiver_from_driver",
            |driver_addr, proxy_addr, _real_addr| async move {
                loop {
                    let frame = frame::read_request_frame(&mut read_half)
                        .await
                        .map_err(|err| {
                            warn!("Request reception from {} error: {}", driver_addr, err);
                            WorkerError::DriverDisconnected(driver_addr)
                        })?;

                    debug!(
                        "Intercepted Driver ({}) -> Cluster ({}) frame. opcode: {:?}.",
                        driver_addr, proxy_addr, &frame.opcode
                    );
                    if request_processor_tx.send(frame).is_err() {
                        warn!("request_processor had exited.");
                        return Result::<(), ProxyError>::Ok(());
                    }
                }
            },
        )
        .await
    }

    async fn receiver_from_cluster(
        self,
        mut read_half: (impl AsyncRead + Unpin),
        response_processor_tx: mpsc::UnboundedSender<ResponseFrame>,
    ) {
        self.run_until_interrupted(
            "receiver_from_cluster",
            |driver_addr, _proxy_addr, real_addr| async move {
                loop {
                    let frame =
                        frame::read_response_frame(&mut read_half)
                            .await
                            .map_err(|err| {
                                warn!("Response reception from {} error: {}", real_addr, err);
                                WorkerError::NodeDisconnected(real_addr)
                            })?;

                    debug!(
                        "Intercepted Cluster ({}) -> Driver ({}) frame. opcode: {:?}.",
                        real_addr, driver_addr, &frame.opcode
                    );

                    if response_processor_tx.send(frame).is_err() {
                        warn!("response_processor had exited.");
                        return Ok::<(), ProxyError>(());
                    }
                }
            },
        )
        .await;
    }

    async fn sender_to_driver(
        self,
        mut write_half: (impl AsyncWrite + Unpin),
        mut responses_rx: mpsc::UnboundedReceiver<ResponseFrame>,
        mut connection_close_notifier: ConnectionCloseNotifier,
        mut terminate_notifier: TerminateNotifier,
    ) {
        self.run_until_interrupted(
            "sender_to_driver",
            |driver_addr, proxy_addr, _real_addr| async move {
                loop {
                    let response = match responses_rx.recv().await {
                        Some(response) => response,
                        None => {
                            if terminate_notifier.try_recv().is_err()
                                && connection_close_notifier.try_recv().is_err()
                            {
                                warn!("Response processor had exited");
                            }
                            return Ok(());
                        }
                    };

                    debug!(
                        "Sending Proxy ({}) -> Driver ({}) frame. opcode: {:?}.",
                        proxy_addr, driver_addr, &response.opcode
                    );
                    if response.write(&mut write_half).await.is_err() {
                        if terminate_notifier.try_recv().is_err()
                            && connection_close_notifier.try_recv().is_err()
                        {
                            warn!("Driver dropped connection");
                            return Err(WorkerError::DriverDisconnected(driver_addr).into());
                        }
                        return Ok(());
                    }
                }
            },
        )
        .await;
    }

    async fn sender_to_cluster(
        self,
        mut write_half: (impl AsyncWrite + Unpin),
        mut requests_rx: mpsc::UnboundedReceiver<RequestFrame>,
        mut connection_close_notifier: ConnectionCloseNotifier,
        mut terminate_notifier: TerminateNotifier,
    ) {
        self.run_until_interrupted(
            "sender_to_driver",
            |_driver_addr, proxy_addr, real_addr| async move {
                loop {
                    let request = match requests_rx.recv().await {
                        Some(request) => request,
                        None => {
                            if terminate_notifier.try_recv().is_err()
                                && connection_close_notifier.try_recv().is_err()
                            {
                                warn!("Request processor had exited");
                            }
                            return Ok(());
                        }
                    };

                    debug!(
                        "Sending Proxy ({}) -> Cluster ({}) frame. opcode: {:?}.",
                        proxy_addr, real_addr, &request.opcode
                    );

                    if request.write(&mut write_half).await.is_err() {
                        if terminate_notifier.try_recv().is_err()
                            && connection_close_notifier.try_recv().is_err()
                        {
                            warn!("Node {} dropped connection", real_addr);
                            return Err(WorkerError::NodeDisconnected(real_addr).into());
                        }
                        return Ok(());
                    }
                }
            },
        )
        .await;
    }

    async fn request_processor(
        self,
        mut requests_rx: mpsc::UnboundedReceiver<RequestFrame>,
        driver_tx: mpsc::UnboundedSender<ResponseFrame>,
        cluster_tx: mpsc::UnboundedSender<RequestFrame>,
        connection_no: usize,
        request_rules: Arc<Mutex<Vec<RequestRule>>>,
        connection_close_signaler: ConnectionCloseSignaler,
    ) {
        self.run_until_interrupted("request_processor", |driver_addr, _, real_addr| async move {
            'mainloop: loop {
                match requests_rx.recv().await {
                    Some(request) => {
                        let ctx = EvaluationContext {
                            connection_seq_no: connection_no,
                            opcode: FrameOpcode::Request(request.opcode),
                            frame_body: request.body.clone(),
                        };
                        let mut guard = request_rules.lock().await;
                        '_ruleloop: for (i, request_rule) in guard.iter_mut().enumerate() {
                            if request_rule.0.eval(&ctx) {
                                info!("Applying rule no={} to request ({} -> {}).", i, driver_addr, real_addr);
                                debug!("-> Applied rule: {:?}", request_rule);
                                debug!("-> To request: {:?}", ctx.opcode);
                                trace!("{:?}", request);

                                if let Some(ref tx) = request_rule.1.feedback_channel {
                                    tx.send(request.clone()).unwrap_or_else(|err|
                                        warn!("Could not send received request as feedback: {}", err)
                                    );
                                }

                                let request_rule = request_rule.clone();
                                let to_addressee_action = request_rule.1.to_addressee;
                                let to_sender_action = request_rule.1.to_sender;
                                let drop_connection_action = request_rule.1.drop_connection;

                                let cluster_tx_clone = cluster_tx.clone();
                                let request_clone = request.clone();
                                let pass_action = async move {
                                    if let Some(ref pass_action) = to_addressee_action {
                                        if let Some(time) = pass_action.delay {
                                            tokio::time::sleep(time).await;
                                        }
                                        let passed_frame = match pass_action.msg_processor {
                                            Some(ref processor) => processor(request_clone),
                                            None => request_clone,
                                        };
                                        let _ = cluster_tx_clone.send(passed_frame);
                                    };
                                };

                                let driver_tx_clone = driver_tx.clone();
                                let request_clone = request.clone();
                                let forge_action = async move {
                                    if let Some(ref forge_action) = to_sender_action {
                                        if let Some(time) = forge_action.delay {
                                            tokio::time::sleep(time).await;
                                        }
                                        let forged_frame = {
                                            let processor = forge_action.msg_processor.as_ref()
                                                .expect("Frame processor is required to forge a frame.");
                                            processor(request_clone)
                                        };
                                        let _ = driver_tx_clone.send(forged_frame);
                                    };
                                };

                                let connection_close_signaler_clone =
                                    connection_close_signaler.clone();
                                let drop_action = async move {
                                    if let Some(ref delay) = drop_connection_action {
                                        if let Some(ref time) = delay {
                                            tokio::time::sleep(*time).await;
                                        }
                                        // close connection.
                                        info!(
                                            "Dropping connection between {} and {} (as requested by a proxy rule)!"
                                        , driver_addr, real_addr);
                                        let _ = connection_close_signaler_clone.send(());
                                    }
                                };

                                tokio::task::spawn(async {
                                    futures::join!(pass_action, forge_action, drop_action);
                                });

                                continue 'mainloop; // only one rule can be applied to one frame
                            }
                        }
                        let _ = cluster_tx.send(request); // default action
                    }
                    None => return Ok(()),
                }
            }
        })
        .await;
    }

    async fn response_processor(
        self,
        mut responses_rx: mpsc::UnboundedReceiver<ResponseFrame>,
        driver_tx: mpsc::UnboundedSender<ResponseFrame>,
        cluster_tx: mpsc::UnboundedSender<RequestFrame>,
        connection_no: usize,
        response_rules: Arc<Mutex<Vec<ResponseRule>>>,
        connection_close_signaler: ConnectionCloseSignaler,
    ) {
        self.run_until_interrupted("request_processor", |driver_addr, _, real_addr| async move {
            'mainloop: loop {
                match responses_rx.recv().await {
                    Some(response) => {
                        let ctx = EvaluationContext {
                            connection_seq_no: connection_no,
                            opcode: FrameOpcode::Response(response.opcode),
                            frame_body: response.body.clone(),
                        };
                        let mut guard = response_rules.lock().await;
                        '_ruleloop: for (i, response_rule) in guard.iter_mut().enumerate() {
                            if response_rule.0.eval(&ctx) {
                                info!("Applying rule no={} to request ({} -> {}).", i, real_addr, driver_addr);
                                debug!("-> Applied rule: {:?}", response_rule);
                                debug!("-> To response: {:?}", ctx.opcode);
                                trace!("{:?}", response);

                                if let Some(ref tx) = response_rule.1.feedback_channel {
                                    tx.send(response.clone()).unwrap_or_else(|err| warn!(
                                        "Could not send received response as feedback: {}", err
                                    ));
                                }

                                let response_rule = response_rule.clone();
                                let to_addressee_action = response_rule.1.to_addressee;
                                let to_sender_action = response_rule.1.to_sender;
                                let drop_connection_action = response_rule.1.drop_connection;

                                let response_clone = response.clone();
                                let driver_tx_clone = driver_tx.clone();
                                let pass_action = async move {
                                    if let Some(ref pass_action) = to_addressee_action {
                                        if let Some(time) = pass_action.delay {
                                            tokio::time::sleep(time).await;
                                        }
                                        let passed_frame = match pass_action.msg_processor {
                                            Some(ref processor) => processor(response_clone),
                                            None => response_clone,
                                        };
                                        let _ = driver_tx_clone.send(passed_frame);
                                    };
                                };

                                let response_clone = response.clone();
                                let cluster_tx_clone = cluster_tx.clone();
                                let forge_action = async move {
                                    if let Some(ref forge_action) = to_sender_action {
                                        if let Some(time) = forge_action.delay {
                                            tokio::time::sleep(time).await;
                                        }
                                        let forged_frame = {
                                            let processor = forge_action.msg_processor.as_ref()
                                                .expect("Frame processor is required to forge a frame.");
                                            processor(response_clone)
                                        };
                                        let _ = cluster_tx_clone.send(forged_frame);
                                    };
                                };

                                let connection_close_signaler_clone =
                                    connection_close_signaler.clone();
                                let drop_action = async move {
                                    if let Some(ref delay) = drop_connection_action {
                                        if let Some(ref time) = delay {
                                            tokio::time::sleep(*time).await;
                                        }
                                        // close connection.
                                        info!(
                                            "Dropping connection between {} and {} (as requested by a proxy rule)!"
                                        , driver_addr, real_addr);
                                        let _ = connection_close_signaler_clone.send(());
                                    }
                                };

                                tokio::task::spawn(async {
                                    futures::join!(pass_action, forge_action, drop_action);
                                });

                                continue 'mainloop;
                            }
                        }
                        let _ = driver_tx.send(response); // default action
                    }
                    None => return Ok(()),
                }
            }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::read_request_frame;
    use crate::{Condition, Reaction as _, RequestReaction, ResponseOpcode, ResponseReaction};
    use assert_matches::assert_matches;
    use bytes::{BufMut, BytesMut};
    use futures::future::join;
    use rand::RngCore;
    use scylla_cql::frame::frame_errors::FrameError;
    use scylla_cql::frame::types::write_string_multimap;
    use std::collections::HashMap;
    use std::mem;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicU16, Ordering};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::sync::oneshot;

    // This is for convenient logs from failing tests. Just call it at the beginning of a test.
    #[allow(unused)]
    fn init_logger() {
        let _ = tracing_subscriber::fmt::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .without_time()
            .try_init();
    }

    fn random_body() -> Bytes {
        let body_len = (rand::random::<u32>() % 1000) as usize;
        let mut body = BytesMut::zeroed(body_len);
        rand::thread_rng().fill_bytes(body.as_mut());
        body.freeze()
    }

    async fn respond_with_shards_number(mut conn: TcpStream, shards_num: u16) {
        let RequestFrame {
            params: recvd_params,
            opcode: recvd_opcode,
            body: recvd_body,
        } = read_request_frame(&mut conn).await.unwrap();
        assert_eq!(recvd_params, HARDCODED_OPTIONS_PARAMS);
        assert_eq!(recvd_opcode, RequestOpcode::Options);
        assert_eq!(recvd_body, Bytes::new()); // body should be empty

        let mut body = BytesMut::new();
        let mut sharded_info = HashMap::new();
        sharded_info.insert(
            String::from("SCYLLA_NR_SHARDS"),
            vec![shards_num.to_string()],
        );
        write_string_multimap(&sharded_info, &mut body).unwrap();

        let body = body.freeze();

        write_frame(
            HARDCODED_OPTIONS_PARAMS.for_response(),
            FrameOpcode::Response(ResponseOpcode::Supported),
            &body,
            &mut conn,
        )
        .await
        .unwrap();
    }

    fn next_local_address_with_port(port: u16) -> SocketAddr {
        static ADDRESS_LAST_OCTET: AtomicU16 = AtomicU16::new(42);
        let next_octet = ADDRESS_LAST_OCTET.fetch_add(1, Ordering::Relaxed);
        let next_octet = if next_octet < u8::MAX as u16 {
            next_octet as u8
        } else {
            panic!("Loopback address pool for tests depleted")
        };
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, next_octet)), port)
    }

    async fn identity_proxy_does_not_mutate_frames(shard_awareness: ShardAwareness) {
        let node1_real_addr = next_local_address_with_port(9876);
        let node1_proxy_addr = next_local_address_with_port(9876);
        let proxy = Proxy::new([Node::new(
            node1_real_addr,
            node1_proxy_addr,
            shard_awareness,
            None,
            None,
        )]);
        let running_proxy = proxy.run().await.unwrap();

        let mock_node_listener = TcpListener::bind(node1_real_addr).await.unwrap();

        let params = FrameParams {
            flags: 0,
            version: 0x04,
            stream: 0,
        };
        let opcode = FrameOpcode::Request(RequestOpcode::Options);

        let body = random_body();

        let mock_driver_action = async {
            let mut conn = TcpStream::connect(node1_proxy_addr).await.unwrap();

            write_frame(params, opcode, &body, &mut conn).await.unwrap();
            conn
        };

        let mock_node_action = async {
            if let ShardAwareness::QueryNode = shard_awareness {
                respond_with_shards_number(mock_node_listener.accept().await.unwrap().0, 1).await;
            }
            let (mut conn, _) = mock_node_listener.accept().await.unwrap();
            let RequestFrame {
                params: recvd_params,
                opcode: recvd_opcode,
                body: recvd_body,
            } = read_request_frame(&mut conn).await.unwrap();
            assert_eq!(recvd_params, params);
            assert_eq!(FrameOpcode::Request(recvd_opcode), opcode);
            assert_eq!(recvd_body, body);
            conn
        };

        // we keep the connections open until proxy finishes to let it perform clean exit with no disconnects
        let (_node_conn, _driver_conn) = join(mock_node_action, mock_driver_action).await;
        running_proxy.finish().await.unwrap();
    }

    #[tokio::test]
    #[ntest::timeout(1000)]
    async fn identity_shard_unaware_proxy_does_not_mutate_frames() {
        identity_proxy_does_not_mutate_frames(ShardAwareness::Unaware).await
    }

    #[tokio::test]
    #[ntest::timeout(1000)]
    async fn identity_shard_aware_proxy_does_not_mutate_frames() {
        init_logger();
        identity_proxy_does_not_mutate_frames(ShardAwareness::QueryNode).await
    }

    #[tokio::test]
    #[ntest::timeout(1000)]
    async fn shard_aware_proxy_is_transparent_for_connection_to_shards() {
        async fn test_for_shards_num(shards_num: u16) {
            let node1_real_addr = next_local_address_with_port(9876);
            let node1_proxy_addr = next_local_address_with_port(9876);
            let proxy = Proxy::new([Node::new(
                node1_real_addr,
                node1_proxy_addr,
                ShardAwareness::FixedNum(shards_num),
                None,
                None,
            )]);
            let running_proxy = proxy.run().await.unwrap();

            let mock_node_listener = TcpListener::bind(node1_real_addr).await.unwrap();

            let (driver_addr_tx, driver_addr_rx) = oneshot::channel::<SocketAddr>();

            let mock_driver_action = async {
                let socket = TcpSocket::new_v4().unwrap();
                socket
                    .bind(SocketAddr::from_str("0.0.0.0:0").unwrap())
                    .unwrap();
                let conn = socket.connect(node1_proxy_addr).await.unwrap();
                driver_addr_tx.send(conn.local_addr().unwrap()).unwrap();
                conn
            };

            let mock_node_action = async {
                let (conn, remote_addr) = mock_node_listener.accept().await.unwrap();
                let driver_addr = driver_addr_rx.await.unwrap();
                assert_eq!(
                    driver_addr.port() % shards_num,
                    remote_addr.port() % shards_num
                );
                conn
            };

            // we keep the connections open until proxy finishes to let it perform clean exit with no disconnects
            let (_node_conn, _driver_conn) = join(mock_node_action, mock_driver_action).await;
            running_proxy.finish().await.unwrap();
        }

        for shard_num in 1..6 {
            test_for_shards_num(shard_num).await;
        }
    }

    #[tokio::test]
    #[ntest::timeout(1000)]
    async fn shard_aware_proxy_queries_shards_number() {
        async fn test_for_shards_num(shards_num: u16) {
            for shard_num in 0..shards_num {
                let node1_real_addr = next_local_address_with_port(9876);
                let node1_proxy_addr = next_local_address_with_port(9876);
                let proxy = Proxy::new([Node::new(
                    node1_real_addr,
                    node1_proxy_addr,
                    ShardAwareness::QueryNode,
                    None,
                    None,
                )]);
                let running_proxy = proxy.run().await.unwrap();

                let mock_node_listener = TcpListener::bind(node1_real_addr).await.unwrap();

                let (driver_addr_tx, driver_addr_rx) = oneshot::channel::<SocketAddr>();

                let mock_driver_addr = next_local_address_with_port(shards_num * 1234 + shard_num);
                let mock_driver_action = async {
                    let socket = TcpSocket::new_v4().unwrap();
                    socket
                        .bind(mock_driver_addr)
                        .unwrap_or_else(|_| panic!("driver_addr failed: {}", mock_driver_addr));
                    driver_addr_tx.send(socket.local_addr().unwrap()).unwrap();
                    socket.connect(node1_proxy_addr).await.unwrap()
                };

                let mock_node_action = async {
                    respond_with_shards_number(
                        mock_node_listener.accept().await.unwrap().0,
                        shards_num,
                    )
                    .await;
                    let (conn, remote_addr) = mock_node_listener.accept().await.unwrap();
                    let driver_addr = driver_addr_rx.await.unwrap();
                    assert_eq!(
                        driver_addr.port() % shards_num,
                        remote_addr.port() % shards_num
                    );
                    conn
                };

                let (_node_conn, _driver_conn) = join(mock_node_action, mock_driver_action).await;
                running_proxy.finish().await.unwrap();
            }
        }

        for shard_num in 1..6 {
            test_for_shards_num(shard_num).await;
        }
    }

    #[tokio::test]
    #[ntest::timeout(1000)]
    async fn forger_proxy_forges_response() {
        let node1_real_addr = next_local_address_with_port(9876);
        let node1_proxy_addr = next_local_address_with_port(9876);

        let this_shall_pass = b"This.Shall.Pass.";
        let test_msg = b"Test";

        let proxy = Proxy::new([Node::new(
            node1_real_addr,
            node1_proxy_addr,
            ShardAwareness::Unaware,
            Some(vec![
                RequestRule(
                    Condition::RequestOpcode(RequestOpcode::Register),
                    RequestReaction::forge_response(Arc::new(|RequestFrame { params, .. }| {
                        ResponseFrame {
                            params: params.for_response(),
                            opcode: ResponseOpcode::Event,
                            body: Bytes::from_static(test_msg),
                        }
                    })),
                ),
                RequestRule(
                    Condition::BodyContainsCaseSensitive(Box::new(*this_shall_pass)),
                    RequestReaction::noop(),
                ),
                RequestRule(
                    Condition::True, // only the first matching rule is applied, so "True" covers all remaining cases
                    RequestReaction::forge_response(Arc::new(|RequestFrame { params, .. }| {
                        ResponseFrame {
                            params: params.for_response(),
                            opcode: ResponseOpcode::Ready,
                            body: Bytes::new(),
                        }
                    })),
                ),
            ]),
            None,
        )]);
        let running_proxy = proxy.run().await.unwrap();

        let mock_node_listener = TcpListener::bind(node1_real_addr).await.unwrap();

        let params1 = FrameParams {
            flags: 3,
            version: 0x42,
            stream: 42,
        };
        let opcode1 = FrameOpcode::Request(RequestOpcode::Startup);

        let params2 = FrameParams {
            flags: 4,
            version: 0x04,
            stream: 17,
        };
        let opcode2 = FrameOpcode::Request(RequestOpcode::Register);

        let params3 = FrameParams {
            flags: 8,
            version: 0x04,
            stream: 11,
        };
        let opcode3 = FrameOpcode::Request(RequestOpcode::Execute);

        let body1 = random_body();
        let body2 = random_body();
        let body3 = {
            let mut body = BytesMut::new();
            body.put(&b"uSeLeSs JuNk"[..]);
            body.put(&this_shall_pass[..]);
            body.freeze()
        };

        let mock_driver_action = async {
            let mut conn = TcpStream::connect(node1_proxy_addr).await.unwrap();

            write_frame(params1, opcode1, &body1, &mut conn)
                .await
                .unwrap();
            write_frame(params2, opcode2, &body2, &mut conn)
                .await
                .unwrap();
            write_frame(params3, opcode3, &body3, &mut conn)
                .await
                .unwrap();

            let ResponseFrame {
                params: recvd_params,
                opcode: recvd_opcode,
                body: recvd_body,
            } = read_response_frame(&mut conn).await.unwrap();
            assert_eq!(recvd_params, params1.for_response());
            assert_eq!(recvd_opcode, ResponseOpcode::Ready);
            assert_eq!(recvd_body, Bytes::new());

            let ResponseFrame {
                params: recvd_params,
                opcode: recvd_opcode,
                body: recvd_body,
            } = read_response_frame(&mut conn).await.unwrap();
            assert_eq!(recvd_params, params2.for_response());
            assert_eq!(recvd_opcode, ResponseOpcode::Event);
            assert_eq!(recvd_body, Bytes::from_static(test_msg));

            conn
        };

        let mock_node_action = async {
            let (mut conn, _) = mock_node_listener.accept().await.unwrap();
            let RequestFrame {
                params: recvd_params,
                opcode: recvd_opcode,
                body: recvd_body,
            } = read_request_frame(&mut conn).await.unwrap();
            assert_eq!(recvd_params, params3);
            assert_eq!(FrameOpcode::Request(recvd_opcode), opcode3);
            assert_eq!(recvd_body, body3);

            conn
        };

        let (mut node_conn, mut driver_conn) = join(mock_node_action, mock_driver_action).await;

        running_proxy.finish().await.unwrap();

        assert_matches!(driver_conn.read(&mut [0u8; 1]).await, Ok(0));
        assert_matches!(node_conn.read(&mut [0u8; 1]).await, Ok(0));
    }

    #[tokio::test]
    #[ntest::timeout(1000)]
    async fn ad_hoc_rules_changing() {
        let node1_real_addr = next_local_address_with_port(9876);
        let node1_proxy_addr = next_local_address_with_port(9876);
        let proxy = Proxy::new([Node::new(
            node1_real_addr,
            node1_proxy_addr,
            ShardAwareness::Unaware,
            None,
            None,
        )]);
        let mut running_proxy = proxy.run().await.unwrap();

        let mock_node_listener = TcpListener::bind(node1_real_addr).await.unwrap();

        let params = FrameParams {
            flags: 0,
            version: 0x04,
            stream: 0,
        };
        let opcode = FrameOpcode::Request(RequestOpcode::Options);

        let body = random_body();

        let (mut driver, mut node) = {
            let results = join(
                TcpStream::connect(node1_proxy_addr),
                mock_node_listener.accept(),
            )
            .await;
            (results.0.unwrap(), results.1.unwrap().0)
        };

        async fn request(
            driver: &mut TcpStream,
            node: &mut TcpStream,
            params: FrameParams,
            opcode: FrameOpcode,
            body: &Bytes,
        ) -> Result<RequestFrame, FrameError> {
            let (send_res, recv_res) = join(
                write_frame(params, opcode, &body.clone(), driver),
                read_request_frame(node),
            )
            .await;
            send_res.unwrap();
            recv_res
        }
        {
            // one run still without custom rules
            let RequestFrame {
                params: recvd_params,
                opcode: recvd_opcode,
                body: recvd_body,
            } = request(&mut driver, &mut node, params, opcode, &body)
                .await
                .unwrap();
            assert_eq!(recvd_params, params);
            assert_eq!(FrameOpcode::Request(recvd_opcode), opcode);
            assert_eq!(recvd_body, body);
        }
        running_proxy.running_nodes[0]
            .change_request_rules(Some(vec![RequestRule(
                Condition::True,
                RequestReaction::drop_frame(),
            )]))
            .await;

        {
            // one run with custom rules
            tokio::select! {
                res = request(&mut driver, &mut node, params, opcode, &body) => panic!("Rules did not work: received response {:?}", res),
                _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => (),
            };
        }

        running_proxy.turn_off_rules().await;

        {
            // one run already without custom rules
            let RequestFrame {
                params: recvd_params,
                opcode: recvd_opcode,
                body: recvd_body,
            } = request(&mut driver, &mut node, params, opcode, &body)
                .await
                .unwrap();
            assert_eq!(recvd_params, params);
            assert_eq!(FrameOpcode::Request(recvd_opcode), opcode);
            assert_eq!(recvd_body, body);
        }

        running_proxy.finish().await.unwrap();
    }

    #[tokio::test]
    #[ntest::timeout(2000)]
    async fn limited_times_condition_expires() {
        const FAILING_TRIES: usize = 4;
        const PASSING_TRIES: usize = 5;

        let node1_real_addr = next_local_address_with_port(9876);
        let node1_proxy_addr = next_local_address_with_port(9876);
        let proxy = Proxy::new([Node::new(
            node1_real_addr,
            node1_proxy_addr,
            ShardAwareness::Unaware,
            Some(vec![
                RequestRule(
                    // this will be always fired after first PASSING_TRIES + FAILING_TRIES
                    Condition::not(Condition::TrueForLimitedTimes(
                        FAILING_TRIES + PASSING_TRIES,
                    )),
                    RequestReaction::drop_frame(),
                ),
                RequestRule(
                    // this will be fired for PASSING_TRIES after first FAILING_TRIES
                    Condition::not(Condition::TrueForLimitedTimes(FAILING_TRIES)),
                    RequestReaction::noop(),
                ),
                RequestRule(
                    // this will be fired for first FAILING_TRIES
                    Condition::True,
                    RequestReaction::drop_frame(),
                ),
            ]),
            None,
        )]);
        let running_proxy = proxy.run().await.unwrap();

        let mock_node_listener = TcpListener::bind(node1_real_addr).await.unwrap();

        let params = FrameParams {
            flags: 0,
            version: 0x04,
            stream: 0,
        };
        let opcode = FrameOpcode::Request(RequestOpcode::Options);
        let body = random_body();

        let (mut driver, mut node) = {
            let results = join(
                TcpStream::connect(node1_proxy_addr),
                mock_node_listener.accept(),
            )
            .await;
            (results.0.unwrap(), results.1.unwrap().0)
        };

        async fn request(
            driver: &mut TcpStream,
            node: &mut TcpStream,
            params: FrameParams,
            opcode: FrameOpcode,
            body: &Bytes,
        ) -> Result<RequestFrame, FrameError> {
            let (send_res, recv_res) = join(
                write_frame(params, opcode, &body.clone(), driver),
                read_request_frame(node),
            )
            .await;
            send_res.unwrap();
            recv_res
        }

        for _ in 0..FAILING_TRIES {
            tokio::select! {
                res = request(&mut driver, &mut node, params, opcode, &body) => panic!("Rules did not work: received response {:?}", res),
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => (),
            };
        }

        for _ in 0..PASSING_TRIES {
            let RequestFrame {
                params: recvd_params,
                opcode: recvd_opcode,
                body: recvd_body,
            } = request(&mut driver, &mut node, params, opcode, &body)
                .await
                .unwrap();
            assert_eq!(recvd_params, params);
            assert_eq!(FrameOpcode::Request(recvd_opcode), opcode);
            assert_eq!(recvd_body, body);
        }

        for _ in 0..3 {
            // any further number of requests should fail
            tokio::select! {
                res = request(&mut driver, &mut node, params, opcode, &body) => panic!("Rules did not work: received response {:?}", res),
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => (),
            };
        }

        running_proxy.finish().await.unwrap();
    }

    #[tokio::test]
    #[ntest::timeout(1000)]
    async fn proxy_reports_requests_and_responses_as_feedback() {
        let node1_real_addr = next_local_address_with_port(9876);
        let node1_proxy_addr = next_local_address_with_port(9876);

        let (request_feedback_tx, mut request_feedback_rx) = mpsc::unbounded_channel();
        let (response_feedback_tx, mut response_feedback_rx) = mpsc::unbounded_channel();
        let proxy = Proxy::new([Node::new(
            node1_real_addr,
            node1_proxy_addr,
            ShardAwareness::Unaware,
            Some(vec![RequestRule(
                Condition::True,
                RequestReaction::drop_frame().with_feedback_when_performed(request_feedback_tx),
            )]),
            Some(vec![ResponseRule(
                Condition::True,
                ResponseReaction::drop_frame().with_feedback_when_performed(response_feedback_tx),
            )]),
        )]);
        let running_proxy = proxy.run().await.unwrap();

        let mock_node_listener = TcpListener::bind(node1_real_addr).await.unwrap();

        let params = FrameParams {
            flags: 0,
            version: 0x04,
            stream: 0,
        };
        let request_opcode = FrameOpcode::Request(RequestOpcode::Options);
        let response_opcode = FrameOpcode::Response(ResponseOpcode::Ready);

        let body = random_body();

        let mock_driver_action = async {
            let mut conn = TcpStream::connect(node1_proxy_addr).await.unwrap();
            write_frame(params, request_opcode, &body, &mut conn)
                .await
                .unwrap();
            conn
        };

        let mock_node_action = async {
            let (mut conn, _) = mock_node_listener.accept().await.unwrap();
            write_frame(params.for_response(), response_opcode, &body, &mut conn)
                .await
                .unwrap();
            conn
        };

        // we keep the connections open until proxy finishes to let it perform clean exit with no disconnects
        let (_node_conn, _driver_conn) = join(mock_node_action, mock_driver_action).await;

        let feedback_request = request_feedback_rx.recv().await.unwrap();
        assert_eq!(feedback_request.params, params);
        assert_eq!(
            FrameOpcode::Request(feedback_request.opcode),
            request_opcode
        );
        assert_eq!(feedback_request.body, body);
        let feedback_response = response_feedback_rx.recv().await.unwrap();
        assert_eq!(feedback_response.params, params.for_response());
        assert_eq!(
            FrameOpcode::Response(feedback_response.opcode),
            response_opcode
        );
        assert_eq!(feedback_response.body, body);

        running_proxy.finish().await.unwrap();
    }

    #[tokio::test]
    #[ntest::timeout(1000)]
    async fn sanity_check_reports_errors() {
        let node1_real_addr = next_local_address_with_port(9876);
        let node1_proxy_addr = next_local_address_with_port(9876);
        let proxy = Proxy::new([Node::new(
            node1_real_addr,
            node1_proxy_addr,
            ShardAwareness::Unaware,
            None,
            None,
        )]);
        let mut running_proxy = proxy.run().await.unwrap();

        let mock_node_listener = TcpListener::bind(node1_real_addr).await.unwrap();

        let mock_driver_action = async {
            let mut conn = TcpStream::connect(node1_proxy_addr).await.unwrap();

            conn.write_all(b"uselessJunk").await.unwrap();
            conn
        };

        let mock_node_action = async {
            let (conn, _) = mock_node_listener.accept().await.unwrap();
            conn
        };

        let (node_conn, driver_conn) = join(mock_node_action, mock_driver_action).await;

        running_proxy.sanity_check().unwrap();

        mem::drop(driver_conn);
        assert_matches!(
            running_proxy.wait_for_error().await,
            Some(ProxyError::Worker(WorkerError::DriverDisconnected(_)))
        );
        running_proxy.sanity_check().unwrap();

        mem::drop(node_conn);
        assert_matches!(
            running_proxy.wait_for_error().await,
            Some(ProxyError::Worker(WorkerError::NodeDisconnected(_)))
        );
        running_proxy.sanity_check().unwrap();

        // we keep the connections open until proxy finishes to let it perform clean exit with no disconnects
        let _ = running_proxy.finish().await;
    }

    #[tokio::test]
    #[ntest::timeout(1000)]
    async fn proxy_processes_requests_concurrently() {
        init_logger();
        let node1_real_addr = next_local_address_with_port(9876);
        let node1_proxy_addr = next_local_address_with_port(9876);

        let delay = Duration::from_millis(30);

        let proxy = Proxy::new([Node::new(
            node1_real_addr,
            node1_proxy_addr,
            ShardAwareness::Unaware,
            Some(vec![RequestRule(
                Condition::TrueForLimitedTimes(1),
                RequestReaction::delay(delay),
            )]),
            None,
        )]);
        let running_proxy = proxy.run().await.unwrap();

        let mock_node_listener = TcpListener::bind(node1_real_addr).await.unwrap();

        let params1 = FrameParams {
            flags: 0,
            version: 0x04,
            stream: 0,
        };
        let opcode1 = FrameOpcode::Request(RequestOpcode::Options);

        let body1 = random_body();

        let params2 = FrameParams {
            flags: 0,
            version: 0x04,
            stream: 0,
        };
        let opcode2 = FrameOpcode::Request(RequestOpcode::Register);

        let body2 = random_body();

        let mock_driver_action = async {
            let mut conn = TcpStream::connect(node1_proxy_addr).await.unwrap();

            write_frame(params1, opcode1, &body1, &mut conn)
                .await
                .unwrap();
            write_frame(params2, opcode2, &body2, &mut conn)
                .await
                .unwrap();
            conn
        };

        let mock_node_action = async {
            let (mut conn, _) = mock_node_listener.accept().await.unwrap();
            let RequestFrame {
                params: recvd_params,
                opcode: recvd_opcode,
                body: recvd_body,
            } = read_request_frame(&mut conn).await.unwrap();
            assert_eq!(recvd_params, params2);
            assert_eq!(FrameOpcode::Request(recvd_opcode), opcode2);
            assert_eq!(recvd_body, body2);
            conn
        };

        // we keep the connections open until proxy finishes to let it perform clean exit with no disconnects
        let (_node_conn, _driver_conn) =
            tokio::time::timeout(delay, join(mock_node_action, mock_driver_action))
                .await
                .expect("Request processing was not concurrent");
        running_proxy.finish().await.unwrap();
    }
}
