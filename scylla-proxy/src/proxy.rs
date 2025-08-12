use crate::actions::{EvaluationContext, RequestRule, ResponseRule};
use crate::errors::{DoorkeeperError, ProxyError, WorkerError};
use crate::frame::{
    self, read_response_frame, write_frame, FrameOpcode, FrameParams, RequestFrame, ResponseFrame,
};
use crate::{RequestOpcode, TargetShard};
use bytes::Bytes;
use compression::no_compression;
use scylla_cql::frame::types::read_string_multimap;
use std::collections::HashMap;
use std::fmt::Display;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{broadcast, mpsc};
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
    /// If the queried node does not provide sharding info (e.g. in case of a Cassandra node),
    /// then this mode behaves as Unaware.
    QueryNode,
    /// Binds to the port that is the same as the driver's port modulo the provided number of shards.
    FixedNum(u16),
}

impl ShardAwareness {
    pub fn is_aware(&self) -> bool {
        !matches!(self, Self::Unaware)
    }
}

/// Node can be either Real (truly backed by a Scylla node) or Simulated
/// (driver believes it's real, but we merely simulate it with the proxy).
/// In Simulated mode, no node address is provided and proxy does not attempt
/// to establish connection with a Scylla node.
///
/// For Real node, all workers are created, so such frame flow is possible:
/// [driver] -> receiver_from_driver -> requests_processor -> sender_to_cluster -> [node] (ordinary request flow)
///                                        |                    /\
///     (forging response) ++--------------+      +-------------++ (forging request)
///                        \/                     |
/// [driver] <- sender_to_driver <- response_processor <- receiver_from_cluster <- [node] (ordinary response flow)
///
/// For Simulated node, it looks like this:
/// [driver] -> receiver_from_driver -> requests_processor -+
///                                                         |   (forging response)
/// [driver] <- sender_to_driver <--------------------------+
///
/// For Real node, the default reaction to a frame is to pass it to its intended addresse.
/// For Simulated node, the default reaction to a request is to drop it.
enum NodeType {
    Real {
        real_addr: SocketAddr,
        shard_awareness: ShardAwareness,
        response_rules: Option<Vec<ResponseRule>>,
    },
    Simulated,
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

    /// Creates a simulated node that is not backed by any real Scylla node.
    pub fn new_dry_mode(proxy_addr: SocketAddr, request_rules: Option<Vec<RequestRule>>) -> Self {
        Self {
            proxy_addr,
            request_rules,
            node_type: NodeType::Simulated,
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

    /// Creates a simulated node that is not backed by any real Scylla node.
    pub fn build_dry_mode(self) -> Node {
        Node {
            proxy_addr: self.proxy_addr.expect("Proxy addr is required!"),
            request_rules: self.request_rules,
            node_type: NodeType::Simulated,
        }
    }
}

#[derive(Clone, Copy)]
struct DisplayableRealAddrOption(Option<SocketAddr>);
impl Display for DisplayableRealAddrOption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(addr) = self.0 {
            write!(f, "{addr}")
        } else {
            write!(f, "<dry mode>")
        }
    }
}

#[derive(Clone, Copy)]
struct DisplayableShard(Option<TargetShard>);
impl Display for DisplayableShard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(shard) = self.0 {
            write!(f, "shard {shard}")
        } else {
            write!(f, "unknown shard")
        }
    }
}

enum InternalNode {
    Real {
        real_addr: SocketAddr,
        proxy_addr: SocketAddr,
        shard_awareness: ShardAwareness,
        request_rules: Arc<Mutex<Vec<RequestRule>>>,
        response_rules: Arc<Mutex<Vec<ResponseRule>>>,
    },
    Simulated {
        proxy_addr: SocketAddr,
        request_rules: Arc<Mutex<Vec<RequestRule>>>,
    },
}

impl InternalNode {
    fn proxy_addr(&self) -> SocketAddr {
        match *self {
            InternalNode::Real { proxy_addr, .. } => proxy_addr,
            InternalNode::Simulated { proxy_addr, .. } => proxy_addr,
        }
    }
    fn real_addr(&self) -> Option<SocketAddr> {
        match *self {
            InternalNode::Real { real_addr, .. } => Some(real_addr),
            InternalNode::Simulated { .. } => None,
        }
    }
    fn request_rules(&self) -> &Arc<Mutex<Vec<RequestRule>>> {
        match self {
            InternalNode::Real { request_rules, .. } => request_rules,
            InternalNode::Simulated { request_rules, .. } => request_rules,
        }
    }
}

impl From<Node> for InternalNode {
    fn from(node: Node) -> Self {
        match node.node_type {
            NodeType::Real {
                real_addr,
                shard_awareness,
                response_rules,
            } => InternalNode::Real {
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
            NodeType::Simulated => InternalNode::Simulated {
                proxy_addr: node.proxy_addr,
                request_rules: node
                    .request_rules
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
            if let &InternalNode::Real {
                real_addr,
                proxy_addr,
                ..
            } = node
            {
                translation_map.insert(real_addr, proxy_addr);
                let shard_aware_real_addr = SocketAddr::new(real_addr.ip(), 19042);
                translation_map.insert(shard_aware_real_addr, proxy_addr);
            }
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
                let running = {
                    let (request_rules, response_rules) = match node {
                        InternalNode::Real {
                            ref request_rules,
                            ref response_rules,
                            ..
                        } => (request_rules, Some(response_rules)),
                        InternalNode::Simulated {
                            ref request_rules, ..
                        } => (request_rules, None),
                    };
                    RunningNode {
                        request_rules: request_rules.clone(),
                        response_rules: response_rules.cloned(),
                    }
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
    pub fn change_request_rules(&mut self, rules: Option<Vec<RequestRule>>) {
        *self.request_rules.lock().unwrap() = rules.unwrap_or_default();
    }

    /// Replaces the previous response rules with the new ones.
    pub fn change_response_rules(&mut self, rules: Option<Vec<ResponseRule>>) {
        *self
            .response_rules
            .as_ref()
            .expect("No response rules on a simulated node!")
            .lock()
            .unwrap() = rules.unwrap_or_default();
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
    pub fn turn_off_rules(&mut self) {
        for (request_rules, response_rules) in self
            .running_nodes
            .iter_mut()
            .map(|node| (&node.request_rules, &node.response_rules))
        {
            request_rules.lock().unwrap().clear();
            if let Some(response_rules) = response_rules {
                response_rules.lock().unwrap().clear();
            }
        }
    }

    /// Attempts to fetch the first error that has occurred in proxy since last check.
    /// If no errors occurred, returns Ok(()).
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

    /// Waits until an error occurs in proxy. If proxy finishes with no errors occurred, returns Err(()).
    pub async fn wait_for_error(&mut self) -> Option<ProxyError> {
        self.error_sink.recv().await
    }

    /// Requests termination of all proxy workers and awaits its completion.
    /// Returns the first error that occurred in proxy.
    pub async fn finish(mut self) -> Result<(), ProxyError> {
        self.terminate_signaler.send(()).map_err(|err| {
            ProxyError::AwaitFinishFailure(format!(
                "Send error in terminate_signaler: {err} (bug!)"
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
    shards_count: Option<u16>,
    error_propagator: ErrorPropagator,
}

impl Doorkeeper {
    async fn spawn(
        node: InternalNode,
        terminate_signaler: TerminateSignaler,
        finish_guard: FinishGuard,
        error_propagator: ErrorPropagator,
    ) -> Result<(), DoorkeeperError> {
        let listener = TcpListener::bind(node.proxy_addr())
            .await
            .map_err(|err| DoorkeeperError::DriverConnectionAttempt(node.proxy_addr(), err))?;

        if let InternalNode::Real {
            shard_awareness,
            real_addr,
            ..
        } = node
        {
            info!(
                "Spawned a {} doorkeeper for pair real:{} - proxy:{}.",
                if shard_awareness.is_aware() {
                    "shard-aware"
                } else {
                    "shard-unaware"
                },
                real_addr,
                node.proxy_addr(),
            );
        } else {
            info!(
                "Spawned a dry-mode doorkeeper for proxy:{}.",
                node.proxy_addr(),
            )
        };

        let doorkeeper = Doorkeeper {
            shards_count: None, // temporarily, until Doorkeeper examines its ShardAwareness
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
        self.update_shards_count().await;
        let mut own_terminate_notifier = self.terminate_signaler.subscribe();
        let (connection_close_tx, _connection_close_rx) = broadcast::channel::<()>(2);
        let mut connection_no: usize = 0;
        loop {
            tokio::select! {
                res = self.accept_connection(&connection_close_tx, connection_no) => {
                    match res {
                        Ok(()) => connection_no += 1,
                        Err(err) => {
                            error!(
                                "Error in doorkeeper with addr {} for node {}: {}",
                                self.node.proxy_addr(),
                                DisplayableRealAddrOption(self.node.real_addr()),
                                err
                            );
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
            self.node.proxy_addr(),
            DisplayableRealAddrOption(self.node.real_addr())
        );
    }

    async fn update_shards_count(&mut self) {
        if let InternalNode::Real {
            real_addr,
            shard_awareness,
            ..
        } = self.node
        {
            self.shards_count = match shard_awareness {
                ShardAwareness::Unaware => None,
                ShardAwareness::FixedNum(shards_num) => Some(shards_num),
                ShardAwareness::QueryNode => match self.obtain_shards_count(real_addr).await {
                    Ok(shards) => Some(shards),
                    // If a node offers no sharding info, change proxy ShardAwareness to Unaware.
                    Err(DoorkeeperError::ObtainingShardNumberNoShardInfo) => {
                        info!(
                            "Doorkeeper with addr {} found no shard info in node {}; falling back to ShardAwareness::Unaware",
                            self.node.proxy_addr(),
                            DisplayableRealAddrOption(self.node.real_addr()),
                        );
                        None
                    }
                    Err(e) => {
                        error!(
                            "Error in doorkeeper with addr {} while querying shard info from node {}: {}",
                            self.node.proxy_addr(),
                            DisplayableRealAddrOption(self.node.real_addr()),
                            e
                        );
                        None
                    }
                },
            }
        }
    }

    async fn spawn_workers(
        &mut self,
        driver_addr: SocketAddr,
        connection_close_tx: &ConnectionCloseSignaler,
        connection_no: usize,
        driver_stream: TcpStream,
        cluster_stream: Option<TcpStream>,
        shard: Option<TargetShard>,
    ) {
        let (driver_read, driver_write) = driver_stream.into_split();

        let new_worker = || ProxyWorker {
            terminate_notifier: self.terminate_signaler.subscribe(),
            finish_guard: self.finish_guard.clone(),
            connection_close_notifier: connection_close_tx.subscribe(),
            error_propagator: self.error_propagator.clone(),
            driver_addr,
            real_addr: self.node.real_addr(),
            proxy_addr: self.node.proxy_addr(),
            shard,
        };

        let (tx_request, rx_request) = mpsc::unbounded_channel::<RequestFrame>();
        let (tx_response, rx_response) = mpsc::unbounded_channel::<ResponseFrame>();
        let (tx_cluster, rx_cluster) = mpsc::unbounded_channel::<RequestFrame>();
        let (tx_driver, rx_driver) = mpsc::unbounded_channel::<ResponseFrame>();
        let event_register_flag = Arc::new(AtomicBool::new(false));

        let (
            compression_writer_request_processor,
            compression_reader_receiver_from_driver,
            compression_reader_receiver_from_cluster,
            compression_reader_sender_to_driver,
            compression_reader_sender_to_cluster,
        ) = compression::make_compression_infra();

        tokio::task::spawn(new_worker().receiver_from_driver(
            driver_read,
            tx_request,
            compression_reader_receiver_from_driver,
        ));
        tokio::task::spawn(new_worker().sender_to_driver(
            driver_write,
            rx_driver,
            connection_close_tx.subscribe(),
            self.terminate_signaler.subscribe(),
            compression_reader_sender_to_driver,
        ));
        tokio::task::spawn(new_worker().request_processor(
            rx_request,
            tx_driver.clone(),
            tx_cluster.clone(),
            connection_no,
            self.node.request_rules().clone(),
            connection_close_tx.clone(),
            event_register_flag.clone(),
            compression_writer_request_processor,
        ));
        if let InternalNode::Real {
            ref response_rules, ..
        } = self.node
        {
            let (cluster_read, cluster_write) = cluster_stream.unwrap().into_split();
            tokio::task::spawn(new_worker().sender_to_cluster(
                cluster_write,
                rx_cluster,
                connection_close_tx.subscribe(),
                self.terminate_signaler.subscribe(),
                compression_reader_sender_to_cluster,
            ));
            tokio::task::spawn(new_worker().receiver_from_cluster(
                cluster_read,
                tx_response,
                compression_reader_receiver_from_cluster,
            ));
            tokio::task::spawn(new_worker().response_processor(
                rx_response,
                tx_driver,
                tx_cluster,
                connection_no,
                response_rules.clone(),
                connection_close_tx.clone(),
                event_register_flag.clone(),
            ));
        }
        debug!(
            "Doorkeeper with addr {} of node {} spawned workers.",
            self.node.proxy_addr(),
            DisplayableRealAddrOption(self.node.real_addr())
        );
    }

    async fn accept_connection(
        &mut self,
        connection_close_tx: &ConnectionCloseSignaler,
        connection_no: usize,
    ) -> Result<(), DoorkeeperError> {
        let (driver_stream, driver_addr) = self.make_driver_stream(connection_no).await?;
        let (cluster_stream, shard) = match self.node {
            InternalNode::Real { real_addr, .. } => {
                let (cluster_stream, shard) =
                    self.make_cluster_stream(driver_addr, real_addr).await?;
                (Some(cluster_stream), shard)
            }
            InternalNode::Simulated { .. } => (None, None),
        };

        self.spawn_workers(
            driver_addr,
            connection_close_tx,
            connection_no,
            driver_stream,
            cluster_stream,
            shard,
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
                DoorkeeperError::DriverConnectionAttempt(self.node.proxy_addr(), err)
            })?;
        info!(
            "Connected driver from {} to {}, connection no={}.",
            driver_addr,
            self.node.proxy_addr(),
            connection_no
        );
        Ok((driver_stream, driver_addr))
    }

    async fn make_cluster_stream(
        &mut self,
        driver_addr: SocketAddr,
        real_addr: SocketAddr,
    ) -> Result<(TcpStream, Option<TargetShard>), DoorkeeperError> {
        let mut cluster_stream = if let Some(shards) = self.shards_count {
            let socket = match self.node.proxy_addr().ip() {
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

            let stream = socket.connect(real_addr).await;
            if let Ok(ok) = &stream {
                info!(
                    "Connected to the cluster from {} at {}, intended shard {}.",
                    ok.local_addr().unwrap(),
                    real_addr,
                    shard_preserving_addr.port() % shards
                );
            }
            stream
        } else {
            let stream = TcpStream::connect(real_addr).await;
            if stream.is_ok() {
                info!("Connected to the cluster at {}.", real_addr);
            }
            stream
        }
        .map_err(|err| DoorkeeperError::NodeConnectionAttempt(real_addr, err))?;

        // If ShardAwareness is aware (QueryNode or FixedNum variants) and the
        // proxy succeeded to know the shards count (in FixedNum we get it for
        // free, in QueryNode the initial Options query succeeded and Supported
        // contained SCYLLA_SHARDS_NUM), then upon opening each connection to the
        // node, the proxy issues another Options requests and acknowledges the
        // shard it got connected to.
        let shard = if self.shards_count.is_some() {
            self.obtain_shard_number(real_addr, &mut cluster_stream)
                .await?
        } else {
            None
        };

        Ok((cluster_stream, shard))
    }

    fn next_port_to_same_shard(&self, port: u16) -> u16 {
        port.wrapping_add(self.shards_count.unwrap())
    }

    async fn get_supported_options(
        connection: &mut TcpStream,
    ) -> Result<HashMap<String, Vec<String>>, DoorkeeperError> {
        write_frame(
            HARDCODED_OPTIONS_PARAMS,
            FrameOpcode::Request(RequestOpcode::Options),
            &Bytes::new(),
            connection,
            &no_compression(),
        )
        .await
        .map_err(DoorkeeperError::ObtainingShardNumber)?;

        let supported_frame = read_response_frame(connection, &compression::no_compression())
            .await
            .map_err(DoorkeeperError::ObtainingShardNumberFrame)?;

        let options = read_string_multimap(&mut supported_frame.body.as_ref())
            .map_err(DoorkeeperError::ObtainingShardNumberParseOptions)?;

        Ok(options)
    }

    async fn obtain_shards_count(&self, real_addr: SocketAddr) -> Result<u16, DoorkeeperError> {
        let mut connection = TcpStream::connect(real_addr)
            .await
            .map_err(|err| DoorkeeperError::NodeConnectionAttempt(real_addr, err))?;
        let options = Self::get_supported_options(&mut connection).await?;
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

    async fn obtain_shard_number(
        &self,
        real_addr: SocketAddr,
        connection: &mut TcpStream,
    ) -> Result<Option<TargetShard>, DoorkeeperError> {
        let options = Self::get_supported_options(connection).await?;
        let shard_entry = options.get("SCYLLA_SHARD");
        let shard = shard_entry
            .and_then(|vec| vec.first())
            .map(|s| {
                s.parse::<u16>()
                    .map_err(DoorkeeperError::ObtainingShardNumberParseShardNumber)
            })
            .transpose()?;
        info!("Connected to node {}, shard {:?}", real_addr, shard);
        Ok(shard)
    }
}

mod compression {
    use std::error::Error;
    use std::sync::{Arc, OnceLock};

    use bytes::Bytes;
    use scylla_cql::frame::frame_errors::{
        CqlRequestSerializationError, FrameBodyExtensionsParseError,
    };
    use scylla_cql::frame::request::{
        options, DeserializableRequest as _, RequestDeserializationError, Startup,
    };
    use scylla_cql::frame::{compress_append, decompress, flag, Compression};
    use tracing::{error, warn};

    #[derive(Debug, thiserror::Error)]
    pub(crate) enum CompressionError {
        /// Body Snap compression failed.
        #[error("Snap compression error: {0}")]
        SnapCompressError(Arc<dyn Error + Sync + Send>),

        /// Frame is to be compressed, but no compression was negotiated for the connection.
        #[error("Frame is to be compressed, but no compression negotiated for connection.")]
        NoCompressionNegotiated,
    }

    type CompressionInfo = Arc<OnceLock<Option<Compression>>>;

    /// The write end of compression config for a connection.
    ///
    /// Used by the request processor upon STARTUP frame captured
    /// and compression setting retrieved from it.
    #[derive(Debug, Clone)]
    pub(crate) struct CompressionWriter(CompressionInfo);
    impl CompressionWriter {
        pub(crate) fn set(
            &self,
            compression: Option<Compression>,
        ) -> Result<(), Option<Compression>> {
            self.0.set(compression)
        }

        pub(crate) fn set_from_startup(
            &self,
            mut body: &[u8],
        ) -> Result<Option<Compression>, RequestDeserializationError> {
            let startup = Startup::deserialize(&mut body)?;
            let maybe_compression = startup.options.get(options::COMPRESSION);
            let maybe_compression = maybe_compression.and_then(|compression| {
                compression
                    .parse::<Compression>()
                    .inspect_err(|err| error!("STARTUP compression error: {}", err))
                    .ok()
            });
            let _ = self.set(maybe_compression).inspect_err(|_| {
                warn!("Captured second or further STARTUP frame on the same connection")
            });

            Ok(maybe_compression)
        }
    }

    /// The read end of compression config for a connection.
    ///
    /// Used by frame (de)serializers.
    #[derive(Debug, Clone)]
    pub(crate) struct CompressionReader(CompressionInfo);
    impl CompressionReader {
        /// Return the compression negotiated for the connection.
        ///
        /// Outer Option signifies whether the negotiation took place,
        /// inner Option is the compression (or lack of it) negotiated.
        pub(crate) fn get(&self) -> Option<Option<Compression>> {
            self.0.get().copied()
        }

        pub(crate) fn maybe_compress_body(
            &self,
            flags: u8,
            body: &[u8],
        ) -> Result<Option<Bytes>, CompressionError> {
            match (flags & flag::COMPRESSION != 0, self.get().flatten()) {
                (true, Some(compression)) => {
                    let mut buf = Vec::new();
                    compress_append(body, compression, &mut buf).map_err(|err| {
                        let CqlRequestSerializationError::SnapCompressError(err) = err else {
                            unreachable!("BUG: compress_append returned variant different than SnapCompressError")
                        };
                        CompressionError::SnapCompressError(err)
                    })?;
                    Ok(Some(Bytes::from(buf)))
                }
                (true, None) => Err(CompressionError::NoCompressionNegotiated),
                (false, _) => Ok(None),
            }
        }

        pub(crate) fn maybe_decompress_body(
            &self,
            flags: u8,
            body: Bytes,
        ) -> Result<Bytes, FrameBodyExtensionsParseError> {
            match (flags & flag::COMPRESSION != 0, self.get().flatten()) {
                (true, Some(compression)) => decompress(&body, compression).map(Into::into),
                (true, None) => Err(FrameBodyExtensionsParseError::NoCompressionNegotiated),
                (false, _) => Ok(body),
            }
        }
    }

    pub(crate) fn make_compression_infra() -> (
        CompressionWriter,
        CompressionReader,
        CompressionReader,
        CompressionReader,
        CompressionReader,
    ) {
        let info = Arc::new(OnceLock::new());
        (
            CompressionWriter(info.clone()),
            CompressionReader(info.clone()),
            CompressionReader(info.clone()),
            CompressionReader(info.clone()),
            CompressionReader(info),
        )
    }

    fn mock_compression_reader(compression: Option<Compression>) -> CompressionReader {
        CompressionReader(Arc::new({
            let once = OnceLock::new();
            once.set(compression).unwrap();
            once
        }))
    }

    // Compression explicitly turned off.
    pub(crate) fn no_compression() -> CompressionReader {
        mock_compression_reader(None)
    }

    // Compression explicitly turned on.
    #[cfg(test)] // Currently only used for tests.
    pub(crate) fn with_compression(compression: Compression) -> CompressionReader {
        mock_compression_reader(Some(compression))
    }
}
pub(crate) use compression::{CompressionReader, CompressionWriter};

struct ProxyWorker {
    terminate_notifier: TerminateNotifier,
    finish_guard: FinishGuard,
    connection_close_notifier: ConnectionCloseNotifier,
    error_propagator: ErrorPropagator,
    driver_addr: SocketAddr,
    real_addr: Option<SocketAddr>,
    proxy_addr: SocketAddr,
    shard: Option<TargetShard>,
}

impl ProxyWorker {
    fn exit(self, duty: &'static str) {
        debug!(
            "Worker exits: [driver: {}, proxy: {}, node: {}, {}]::{}.",
            self.driver_addr,
            self.proxy_addr,
            DisplayableRealAddrOption(self.real_addr),
            DisplayableShard(self.shard),
            duty
        );
        std::mem::drop(self.finish_guard);
    }

    async fn run_until_interrupted<F, Fut>(mut self, worker_name: &'static str, f: F)
    where
        F: FnOnce(SocketAddr, SocketAddr, Option<SocketAddr>) -> Fut,
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
        compression: CompressionReader,
    ) {
        let shard = self.shard;
        self.run_until_interrupted(
            "receiver_from_driver",
            |driver_addr, proxy_addr, _real_addr| async move {
                loop {
                    let frame = frame::read_request_frame(&mut read_half, &compression)
                        .await
                        .map_err(|err| {
                            warn!("Request reception from {} error: {}", driver_addr, err);
                            WorkerError::DriverDisconnected(driver_addr)
                        })?;

                    debug!(
                        "Intercepted Driver ({}) -> Cluster ({}) ({}) frame. opcode: {:?}.",
                        driver_addr,
                        proxy_addr,
                        DisplayableShard(shard),
                        &frame.opcode
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
        compression: CompressionReader,
    ) {
        let shard = self.shard;
        self.run_until_interrupted(
            "receiver_from_cluster",
            |driver_addr, _proxy_addr, real_addr| async move {
                let real_addr = real_addr.expect("BUG: no real_addr in cluster worker");
                loop {
                    let frame = frame::read_response_frame(&mut read_half, &compression)
                        .await
                        .map_err(|err| {
                            warn!("Response reception from {} error: {}", real_addr, err);
                            WorkerError::NodeDisconnected(real_addr)
                        })?;

                    debug!(
                        "Intercepted Cluster ({}) ({}) -> Driver ({}) frame. opcode: {:?}.",
                        real_addr,
                        DisplayableShard(shard),
                        driver_addr,
                        &frame.opcode
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
        compression: CompressionReader,
    ) {
        let shard = self.shard;
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
                        "Sending Proxy ({}) ({}) -> Driver ({}) frame. opcode: {:?}.",
                        proxy_addr,
                        DisplayableShard(shard),
                        driver_addr,
                        &response.opcode
                    );
                    if response.write(&mut write_half, &compression).await.is_err() {
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
        compression: CompressionReader,
    ) {
        let shard = self.shard;
        self.run_until_interrupted(
            "sender_to_driver",
            |_driver_addr, proxy_addr, real_addr| async move {
                let real_addr = real_addr.expect("BUG: no real_addr in cluster worker");
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
                        "Sending Proxy ({}) -> Cluster ({}) ({}) frame. opcode: {:?}.",
                        proxy_addr,
                        real_addr,
                        DisplayableShard(shard),
                        &request.opcode
                    );

                    if request.write(&mut write_half, &compression).await.is_err() {
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

    #[expect(clippy::too_many_arguments)]
    async fn request_processor(
        self,
        mut requests_rx: mpsc::UnboundedReceiver<RequestFrame>,
        driver_tx: mpsc::UnboundedSender<ResponseFrame>,
        cluster_tx: mpsc::UnboundedSender<RequestFrame>,
        connection_no: usize,
        request_rules: Arc<Mutex<Vec<RequestRule>>>,
        connection_close_signaler: ConnectionCloseSignaler,
        event_registered_flag: Arc<AtomicBool>,
        compression: CompressionWriter,
    ) {
        let shard = self.shard;
        self.run_until_interrupted("request_processor", |driver_addr, _, real_addr| async move {
            'mainloop: loop {
                match requests_rx.recv().await {
                    Some(request) => {
                        if request.opcode == RequestOpcode::Register {
                            event_registered_flag.store(true, Ordering::Relaxed);
                        } else if request.opcode == RequestOpcode::Startup {
                            match compression.set_from_startup(&request.body) {
                                Err(err) => error!("Failed to deserialize STARTUP frame: {}", err),
                                Ok(read_compression) => info!(
                                    "Intercepted STARTUP frame ({} -> {} ({})), so set compression accordingly to {:?}.",
                                    driver_addr,
                                    DisplayableRealAddrOption(real_addr),
                                    DisplayableShard(shard),
                                    read_compression
                                )
                            };
                        }

                        let ctx = EvaluationContext {
                            connection_seq_no: connection_no,
                            opcode: FrameOpcode::Request(request.opcode),
                            frame_body: request.body.clone(),
                            connection_has_events: event_registered_flag.load(Ordering::Relaxed),
                        };
                        let mut guard = request_rules.lock().unwrap();
                        '_ruleloop: for (i, request_rule) in guard.iter_mut().enumerate() {
                            if request_rule.0.eval(&ctx) {
                                info!("Applying rule no={} to request ({} -> {} ({})).", i, driver_addr, DisplayableRealAddrOption(real_addr), DisplayableShard(shard));
                                debug!("-> Applied rule: {:?}", request_rule);
                                debug!("-> To request: {:?}", ctx.opcode);
                                trace!("{:?}", request);

                                if let Some(ref tx) = request_rule.1.feedback_channel {
                                    tx.send((request.clone(), shard)).unwrap_or_else(|err|
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
                                        if let Some(time) = delay {
                                            tokio::time::sleep(*time).await;
                                        }
                                        // close connection.
                                        info!(
                                            "Dropping connection between {} and {} ({}) (as requested by a proxy rule)!",
                                            driver_addr,
                                            DisplayableRealAddrOption(real_addr),
                                            DisplayableShard(shard),
                                        );
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

    #[expect(clippy::too_many_arguments)]
    async fn response_processor(
        self,
        mut responses_rx: mpsc::UnboundedReceiver<ResponseFrame>,
        driver_tx: mpsc::UnboundedSender<ResponseFrame>,
        cluster_tx: mpsc::UnboundedSender<RequestFrame>,
        connection_no: usize,
        response_rules: Arc<Mutex<Vec<ResponseRule>>>,
        connection_close_signaler: ConnectionCloseSignaler,
        event_registered_flag: Arc<AtomicBool>,
    ) {
        let shard = self.shard;
        self.run_until_interrupted("request_processor", |driver_addr, _, real_addr| async move {
            'mainloop: loop {
                match responses_rx.recv().await {
                    Some(response) => {
                        let ctx = EvaluationContext {
                            connection_seq_no: connection_no,
                            opcode: FrameOpcode::Response(response.opcode),
                            frame_body: response.body.clone(),
                            connection_has_events: event_registered_flag.load(Ordering::Relaxed),
                        };
                        let mut guard = response_rules.lock().unwrap();
                        '_ruleloop: for (i, response_rule) in guard.iter_mut().enumerate() {
                            if response_rule.0.eval(&ctx) {
                                info!("Applying rule no={} to request ({} -> {} ({})).", i, DisplayableRealAddrOption(real_addr), driver_addr, DisplayableShard(shard));
                                debug!("-> Applied rule: {:?}", response_rule);
                                debug!("-> To response: {:?}", ctx.opcode);
                                trace!("{:?}", response);

                                if let Some(ref tx) = response_rule.1.feedback_channel {
                                    tx.send((response.clone(), shard)).unwrap_or_else(|err| warn!(
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
                                        if let Some(time) = delay {
                                            tokio::time::sleep(*time).await;
                                        }
                                        // close connection.
                                        info!(
                                            "Dropping connection between {} and {} ({}) (as requested by a proxy rule)!",
                                            driver_addr,
                                            real_addr.expect("BUG: response rules are unavailable for dry-mode proxy!"),
                                            DisplayableShard(shard)
                                        );
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

// Returns next free IP address for another proxy instance.
// Useful for concurrent testing.
pub fn get_exclusive_local_address() -> IpAddr {
    // A big enough number reduces possibility of clashes with user-taken addresses:
    static ADDRESS_LOWER_THREE_OCTETS: AtomicU32 = AtomicU32::new(4242);
    let next_addr = ADDRESS_LOWER_THREE_OCTETS.fetch_add(1, Ordering::Relaxed);
    if next_addr > (u32::MAX >> 8) {
        panic!("Loopback address pool for tests depleted");
    }
    let next_addr_bytes = next_addr.to_le_bytes();
    IpAddr::V4(Ipv4Addr::new(
        127,
        next_addr_bytes[2],
        next_addr_bytes[1],
        next_addr_bytes[0],
    ))
}

#[cfg(test)]
mod tests {
    use super::compression::no_compression;
    use super::*;
    use crate::errors::ReadFrameError;
    use crate::frame::{read_frame, read_request_frame, FrameType};
    use crate::proxy::compression::with_compression;
    use crate::{
        setup_tracing, Condition, Reaction as _, RequestReaction, ResponseOpcode, ResponseReaction,
    };
    use assert_matches::assert_matches;
    use bytes::{BufMut, BytesMut};
    use futures::future::{join, join3};
    use rand::RngCore;
    use scylla_cql::frame::request::options;
    use scylla_cql::frame::request::{SerializableRequest as _, Startup};
    use scylla_cql::frame::types::write_string_multimap;
    use scylla_cql::frame::{flag, Compression};
    use std::collections::HashMap;
    use std::mem;
    use std::str::FromStr;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::sync::oneshot;

    fn random_body() -> Bytes {
        let body_len = (rand::random::<u32>() % 1000) as usize;
        let mut body = BytesMut::zeroed(body_len);
        rand::rng().fill_bytes(body.as_mut());
        body.freeze()
    }

    async fn respond_with_supported(
        conn: &mut TcpStream,
        supported_options: &HashMap<String, Vec<String>>,
        compression: &CompressionReader,
    ) {
        let RequestFrame {
            params: recvd_params,
            opcode: recvd_opcode,
            body: recvd_body,
        } = read_request_frame(conn, compression).await.unwrap();
        assert_eq!(recvd_params, HARDCODED_OPTIONS_PARAMS);
        assert_eq!(recvd_opcode, RequestOpcode::Options);
        assert_eq!(recvd_body, Bytes::new()); // body should be empty

        let mut body = BytesMut::new();
        write_string_multimap(supported_options, &mut body).unwrap();

        let body = body.freeze();

        write_frame(
            HARDCODED_OPTIONS_PARAMS.for_response(),
            FrameOpcode::Response(ResponseOpcode::Supported),
            &body,
            conn,
            &no_compression(),
        )
        .await
        .unwrap();
    }

    fn supported_shards_count(shards_count: u16) -> HashMap<String, Vec<String>> {
        let mut sharded_info = HashMap::new();
        sharded_info.insert(
            String::from("SCYLLA_NR_SHARDS"),
            vec![shards_count.to_string()],
        );
        sharded_info
    }

    fn supported_shard_number(shard_num: TargetShard) -> HashMap<String, Vec<String>> {
        let mut sharded_info = HashMap::new();
        sharded_info.insert(String::from("SCYLLA_SHARD"), vec![shard_num.to_string()]);
        sharded_info
    }

    async fn respond_with_shards_count(
        conn: &mut TcpStream,
        shards_count: u16,
        compression: &CompressionReader,
    ) {
        respond_with_supported(conn, &supported_shards_count(shards_count), compression).await;
    }

    async fn respond_with_shard_num(
        conn: &mut TcpStream,
        shard_num: TargetShard,
        compression: &CompressionReader,
    ) {
        respond_with_supported(conn, &supported_shard_number(shard_num), compression).await;
    }

    fn next_local_address_with_port(port: u16) -> SocketAddr {
        SocketAddr::new(get_exclusive_local_address(), port)
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

        let send_frame_to_shard = async {
            let mut conn = TcpStream::connect(node1_proxy_addr).await.unwrap();

            write_frame(params, opcode, &body, &mut conn, &no_compression())
                .await
                .unwrap();
            conn
        };

        let mock_node_action = async {
            if let ShardAwareness::QueryNode = shard_awareness {
                respond_with_shards_count(
                    &mut mock_node_listener.accept().await.unwrap().0,
                    1,
                    &no_compression(),
                )
                .await;
            }
            let (mut conn, _) = mock_node_listener.accept().await.unwrap();
            if shard_awareness.is_aware() {
                respond_with_shard_num(&mut conn, 1, &no_compression()).await;
            }
            let RequestFrame {
                params: recvd_params,
                opcode: recvd_opcode,
                body: recvd_body,
            } = read_request_frame(&mut conn, &no_compression())
                .await
                .unwrap();
            assert_eq!(recvd_params, params);
            assert_eq!(FrameOpcode::Request(recvd_opcode), opcode);
            assert_eq!(recvd_body, body);
            conn
        };

        // we keep the connections open until proxy finishes to let it perform clean exit with no disconnects
        let (_node_conn, _driver_conn) = join(mock_node_action, send_frame_to_shard).await;
        running_proxy.finish().await.unwrap();
    }

    #[tokio::test]
    #[ntest::timeout(1000)]
    async fn identity_shard_unaware_proxy_does_not_mutate_frames() {
        setup_tracing();
        identity_proxy_does_not_mutate_frames(ShardAwareness::Unaware).await
    }

    #[tokio::test]
    #[ntest::timeout(1000)]
    async fn identity_shard_aware_proxy_does_not_mutate_frames() {
        setup_tracing();
        identity_proxy_does_not_mutate_frames(ShardAwareness::QueryNode).await
    }

    #[tokio::test]
    #[ntest::timeout(1000)]
    async fn shard_aware_proxy_is_transparent_for_connection_to_shards() {
        setup_tracing();
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

            let send_frame_to_shard = async {
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
            let (_node_conn, _driver_conn) = join(mock_node_action, send_frame_to_shard).await;
            running_proxy.finish().await.unwrap();
        }

        for shard_num in 1..6 {
            test_for_shards_num(shard_num).await;
        }
    }

    #[tokio::test]
    #[ntest::timeout(1000)]
    async fn shard_aware_proxy_queries_shards_number() {
        setup_tracing();
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
                let send_frame_to_shard = async {
                    let socket = TcpSocket::new_v4().unwrap();
                    socket
                        .bind(mock_driver_addr)
                        .unwrap_or_else(|_| panic!("driver_addr failed: {mock_driver_addr}"));
                    driver_addr_tx.send(socket.local_addr().unwrap()).unwrap();
                    socket.connect(node1_proxy_addr).await.unwrap()
                };

                let mock_node_action = async {
                    respond_with_shards_count(
                        &mut mock_node_listener.accept().await.unwrap().0,
                        shards_num,
                        &no_compression(),
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

                let (_node_conn, _driver_conn) = join(mock_node_action, send_frame_to_shard).await;
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
        setup_tracing();
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
            flags: 2,
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

        let send_frame_to_shard = async {
            let mut conn = TcpStream::connect(node1_proxy_addr).await.unwrap();

            write_frame(params1, opcode1, &body1, &mut conn, &no_compression())
                .await
                .unwrap();
            write_frame(params2, opcode2, &body2, &mut conn, &no_compression())
                .await
                .unwrap();
            write_frame(params3, opcode3, &body3, &mut conn, &no_compression())
                .await
                .unwrap();

            let ResponseFrame {
                params: recvd_params,
                opcode: recvd_opcode,
                body: recvd_body,
            } = read_response_frame(&mut conn, &no_compression())
                .await
                .unwrap();
            assert_eq!(recvd_params, params1.for_response());
            assert_eq!(recvd_opcode, ResponseOpcode::Ready);
            assert_eq!(recvd_body, Bytes::new());

            let ResponseFrame {
                params: recvd_params,
                opcode: recvd_opcode,
                body: recvd_body,
            } = read_response_frame(&mut conn, &no_compression())
                .await
                .unwrap();
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
            } = read_request_frame(&mut conn, &no_compression())
                .await
                .unwrap();
            assert_eq!(recvd_params, params3);
            assert_eq!(FrameOpcode::Request(recvd_opcode), opcode3);
            assert_eq!(recvd_body, body3);

            conn
        };

        let (mut node_conn, mut driver_conn) = join(mock_node_action, send_frame_to_shard).await;

        running_proxy.finish().await.unwrap();

        assert_matches!(driver_conn.read(&mut [0u8; 1]).await, Ok(0));
        assert_matches!(node_conn.read(&mut [0u8; 1]).await, Ok(0));
    }

    #[tokio::test]
    #[ntest::timeout(1000)]
    async fn ad_hoc_rules_changing() {
        setup_tracing();
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
        ) -> Result<RequestFrame, ReadFrameError> {
            let (send_res, recv_res) = join(
                write_frame(params, opcode, &body.clone(), driver, &no_compression()),
                read_request_frame(node, &no_compression()),
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
        running_proxy.running_nodes[0].change_request_rules(Some(vec![RequestRule(
            Condition::True,
            RequestReaction::drop_frame(),
        )]));

        {
            // one run with custom rules
            tokio::select! {
                res = request(&mut driver, &mut node, params, opcode, &body) => panic!("Rules did not work: received response {:?}", res),
                _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => (),
            };
        }

        running_proxy.turn_off_rules();

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
        setup_tracing();
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
        ) -> Result<RequestFrame, ReadFrameError> {
            let (send_res, recv_res) = join(
                write_frame(params, opcode, &body.clone(), driver, &no_compression()),
                read_request_frame(node, &no_compression()),
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
        setup_tracing();
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

        let send_frame_to_shard = async {
            let mut conn = TcpStream::connect(node1_proxy_addr).await.unwrap();
            write_frame(params, request_opcode, &body, &mut conn, &no_compression())
                .await
                .unwrap();
            conn
        };

        let mock_node_action = async {
            let (mut conn, _) = mock_node_listener.accept().await.unwrap();
            write_frame(
                params.for_response(),
                response_opcode,
                &body,
                &mut conn,
                &no_compression(),
            )
            .await
            .unwrap();
            conn
        };

        // we keep the connections open until proxy finishes to let it perform clean exit with no disconnects
        let (_node_conn, _driver_conn) = join(mock_node_action, send_frame_to_shard).await;

        let (feedback_request, _shard) = request_feedback_rx.recv().await.unwrap();
        assert_eq!(feedback_request.params, params);
        assert_eq!(
            FrameOpcode::Request(feedback_request.opcode),
            request_opcode
        );
        assert_eq!(feedback_request.body, body);
        let (feedback_response, _shard) = response_feedback_rx.recv().await.unwrap();
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
        setup_tracing();
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

        let send_frame_to_shard = async {
            let mut conn = TcpStream::connect(node1_proxy_addr).await.unwrap();

            conn.write_all(b"uselessJunk").await.unwrap();
            conn
        };

        let mock_node_action = async {
            let (conn, _) = mock_node_listener.accept().await.unwrap();
            conn
        };

        let (node_conn, driver_conn) = join(mock_node_action, send_frame_to_shard).await;

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
        setup_tracing();
        let node1_real_addr = next_local_address_with_port(9876);
        let node1_proxy_addr = next_local_address_with_port(9876);

        let delay = Duration::from_millis(60);

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

        let send_frame_to_shard = async {
            let mut conn = TcpStream::connect(node1_proxy_addr).await.unwrap();

            write_frame(params1, opcode1, &body1, &mut conn, &no_compression())
                .await
                .unwrap();
            write_frame(params2, opcode2, &body2, &mut conn, &no_compression())
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
            } = read_request_frame(&mut conn, &no_compression())
                .await
                .unwrap();
            assert_eq!(recvd_params, params2);
            assert_eq!(FrameOpcode::Request(recvd_opcode), opcode2);
            assert_eq!(recvd_body, body2);
            conn
        };

        // we keep the connections open until proxy finishes to let it perform clean exit with no disconnects
        let (_node_conn, _driver_conn) =
            tokio::time::timeout(delay, join(mock_node_action, send_frame_to_shard))
                .await
                .expect("Request processing was not concurrent");
        running_proxy.finish().await.unwrap();
    }

    #[tokio::test]
    #[ntest::timeout(1000)]
    async fn dry_mode_proxy_drops_incoming_frames() {
        setup_tracing();
        let node1_proxy_addr = next_local_address_with_port(9876);
        let proxy = Proxy::new([Node::new_dry_mode(node1_proxy_addr, None)]);
        let running_proxy = proxy.run().await.unwrap();

        let params = FrameParams {
            flags: 0,
            version: 0x04,
            stream: 0,
        };
        let opcode = FrameOpcode::Request(RequestOpcode::Options);

        let body = random_body();

        let mut conn = TcpStream::connect(node1_proxy_addr).await.unwrap();

        write_frame(params, opcode, &body, &mut conn, &no_compression())
            .await
            .unwrap();
        // We assert that after sufficiently long time, no error happens inside proxy.
        tokio::time::sleep(Duration::from_millis(3)).await;
        running_proxy.finish().await.unwrap();
    }

    #[tokio::test]
    #[ntest::timeout(1000)]
    async fn dry_mode_forger_proxy_forges_response() {
        setup_tracing();
        let node1_proxy_addr = next_local_address_with_port(9876);

        let this_shall_pass = b"This.Shall.Pass.";
        let test_msg = b"Test";

        let proxy = Proxy::new([Node::new_dry_mode(
            node1_proxy_addr,
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
        )]);
        let running_proxy = proxy.run().await.unwrap();

        let params1 = FrameParams {
            flags: 2,
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

        let mut conn = TcpStream::connect(node1_proxy_addr).await.unwrap();

        write_frame(params1, opcode1, &body1, &mut conn, &no_compression())
            .await
            .unwrap();
        write_frame(params2, opcode2, &body2, &mut conn, &no_compression())
            .await
            .unwrap();
        write_frame(params3, opcode3, &body3, &mut conn, &no_compression())
            .await
            .unwrap();

        let ResponseFrame {
            params: recvd_params,
            opcode: recvd_opcode,
            body: recvd_body,
        } = read_response_frame(&mut conn, &no_compression())
            .await
            .unwrap();
        assert_eq!(recvd_params, params1.for_response());
        assert_eq!(recvd_opcode, ResponseOpcode::Ready);
        assert_eq!(recvd_body, Bytes::new());

        let ResponseFrame {
            params: recvd_params,
            opcode: recvd_opcode,
            body: recvd_body,
        } = read_response_frame(&mut conn, &no_compression())
            .await
            .unwrap();
        assert_eq!(recvd_params, params2.for_response());
        assert_eq!(recvd_opcode, ResponseOpcode::Event);
        assert_eq!(recvd_body, Bytes::from_static(test_msg));

        running_proxy.finish().await.unwrap();

        assert_matches!(conn.read(&mut [0u8; 1]).await, Ok(0));
    }

    // The test asserts that once a (mock) driver connects to the proxy from some port,
    // the proxy will connect to a shard corresponding to that port and that the target
    // shard number will be sent through the feedback channel.
    #[tokio::test]
    #[ntest::timeout(1000)]
    async fn proxy_reports_target_shard_as_feedback() {
        setup_tracing();

        let node_port = 10101;
        let node_real_addr = next_local_address_with_port(node_port);
        let mock_node_listener = TcpListener::bind(node_real_addr).await.unwrap();

        let params = FrameParams {
            flags: 0,
            version: 0x04,
            stream: 0,
        };
        let request_opcode = FrameOpcode::Request(RequestOpcode::Options);
        let response_opcode = FrameOpcode::Response(ResponseOpcode::Ready);

        let body = random_body();

        for shards_count in 2..9 {
            // Two driver connections are simulated, each to a different shard.
            let driver1_shard = shards_count - 1;
            let driver2_shard = shards_count - 2;
            let node_proxy_addr = next_local_address_with_port(node_port);

            let (request_feedback_tx, mut request_feedback_rx) = mpsc::unbounded_channel();
            let (response_feedback_tx, mut response_feedback_rx) = mpsc::unbounded_channel();

            let proxy = Proxy::new([Node::new(
                node_real_addr,
                node_proxy_addr,
                ShardAwareness::FixedNum(shards_count),
                Some(vec![RequestRule(
                    Condition::True,
                    RequestReaction::drop_frame().with_feedback_when_performed(request_feedback_tx),
                )]),
                Some(vec![ResponseRule(
                    Condition::True,
                    ResponseReaction::drop_frame()
                        .with_feedback_when_performed(response_feedback_tx),
                )]),
            )]);
            let running_proxy = proxy.run().await.unwrap();

            /// Choose a source port `p` such that `shard == shard_of_source_port(p)`.
            fn draw_source_port_for_shard(shards_count: u16, shard: u16) -> u16 {
                assert!(shard < shards_count);
                49152u16.div_ceil(shards_count) * shards_count + shard
            }

            async fn bind_socket_for_shard(shards_count: u16, shard: u16) -> TcpSocket {
                let socket = TcpSocket::new_v4().unwrap();
                let initial_port = draw_source_port_for_shard(shards_count, shard);

                let mut desired_addr =
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), initial_port);
                while socket.bind(desired_addr).is_err() {
                    // in search for a port that translates to the desired shard
                    let next_port = desired_addr.port().wrapping_add(shards_count);
                    if next_port == initial_port {
                        panic!("No more ports left");
                    }
                    desired_addr.set_port(next_port);
                }

                socket
            }

            let body_ref = &body;
            let send_frame_to_shard = |driver_shard: u16| async move {
                let socket = bind_socket_for_shard(shards_count, driver_shard).await;
                let mut conn = socket.connect(node_proxy_addr).await.unwrap();

                write_frame(
                    params,
                    request_opcode,
                    body_ref,
                    &mut conn,
                    &no_compression(),
                )
                .await
                .unwrap();
                conn
            };

            let mock_driver1_action = send_frame_to_shard(driver1_shard);
            let mock_driver2_action = send_frame_to_shard(driver2_shard);

            // Accepts two connections and sends a response to each of them.
            let mock_node_action = async {
                let mut conns_futs = (0..2)
                    .map(|_| async {
                        let (mut conn, driver_addr) = mock_node_listener.accept().await.unwrap();
                        respond_with_shard_num(
                            &mut conn,
                            driver_addr.port() % shards_count,
                            &no_compression(),
                        )
                        .await;
                        write_frame(
                            params.for_response(),
                            response_opcode,
                            body_ref,
                            &mut conn,
                            &no_compression(),
                        )
                        .await
                        .unwrap();
                        conn
                    })
                    .collect::<Vec<_>>();
                let conn2 = conns_futs.pop().unwrap().await;
                let conn1 = conns_futs.pop().unwrap().await;
                (conn1, conn2)
            };

            // we keep the connections open until proxy finishes to let it perform clean exit with no disconnects
            let (_node_conns, _driver1_conn, _driver2_conn) =
                join3(mock_node_action, mock_driver1_action, mock_driver2_action).await;

            let assert_feedback_request = |feedback_request: RequestFrame| {
                assert_eq!(feedback_request.params, params);
                assert_eq!(
                    FrameOpcode::Request(feedback_request.opcode),
                    request_opcode
                );
                assert_eq!(feedback_request.body, body);
            };

            let assert_feedback_response = |feedback_response: ResponseFrame| {
                assert_eq!(feedback_response.params, params.for_response());
                assert_eq!(
                    FrameOpcode::Response(feedback_response.opcode),
                    response_opcode
                );
                assert_eq!(feedback_response.body, body);
            };

            let (feedback_request, shard1) = request_feedback_rx.recv().await.unwrap();
            assert_feedback_request(feedback_request);
            let (feedback_request, shard2) = request_feedback_rx.recv().await.unwrap();
            assert_feedback_request(feedback_request);
            let (feedback_response, shard3) = response_feedback_rx.recv().await.unwrap();
            assert_feedback_response(feedback_response);
            let (feedback_response, shard4) = response_feedback_rx.recv().await.unwrap();
            assert_feedback_response(feedback_response);

            // expected: {driver1_shard request, driver1_shard response, driver2_shard request, driver2_shard response}
            let mut expected_shards = [driver1_shard, driver1_shard, driver2_shard, driver2_shard];
            expected_shards.sort_unstable();

            let mut got_shards = [
                shard1.unwrap(),
                shard2.unwrap(),
                shard3.unwrap(),
                shard4.unwrap(),
            ];
            got_shards.sort_unstable();

            assert_eq!(expected_shards, got_shards);

            running_proxy.finish().await.unwrap();
        }
    }

    #[tokio::test]
    #[ntest::timeout(1000)]
    async fn proxy_ignores_control_connection_messages() {
        setup_tracing();
        let node1_real_addr = next_local_address_with_port(9876);
        let node1_proxy_addr = next_local_address_with_port(9876);

        let (request_feedback_tx, mut request_feedback_rx) = mpsc::unbounded_channel();
        let (response_feedback_tx, mut response_feedback_rx) = mpsc::unbounded_channel();
        let proxy = Proxy::new([Node::new(
            node1_real_addr,
            node1_proxy_addr,
            ShardAwareness::Unaware,
            Some(vec![RequestRule(
                Condition::not(Condition::ConnectionRegisteredAnyEvent),
                RequestReaction::noop().with_feedback_when_performed(request_feedback_tx),
            )]),
            Some(vec![ResponseRule(
                Condition::not(Condition::ConnectionRegisteredAnyEvent),
                ResponseReaction::noop().with_feedback_when_performed(response_feedback_tx),
            )]),
        )]);
        let running_proxy = proxy.run().await.unwrap();

        let mock_node_listener = TcpListener::bind(node1_real_addr).await.unwrap();

        let (mut client_socket, mut server_socket) = join(
            async { TcpStream::connect(node1_proxy_addr).await.unwrap() },
            async { mock_node_listener.accept().await.unwrap().0 },
        )
        .await;

        async fn perform_reqest_response<'a>(
            req_opcode: RequestOpcode,
            resp_opcode: ResponseOpcode,
            client_socket_ref: &'a mut TcpStream,
            server_socket_ref: &'a mut TcpStream,
            body_base: &'a str,
        ) {
            let params = FrameParams {
                flags: 0,
                version: 0x04,
                stream: 0,
            };

            write_frame(
                params,
                FrameOpcode::Request(req_opcode),
                (body_base.to_string() + "|request|").as_bytes(),
                client_socket_ref,
                &no_compression(),
            )
            .await
            .unwrap();

            let received_request =
                read_frame(server_socket_ref, FrameType::Request, &no_compression())
                    .await
                    .unwrap();
            assert_eq!(received_request.1, FrameOpcode::Request(req_opcode));

            write_frame(
                params.for_response(),
                FrameOpcode::Response(resp_opcode),
                (body_base.to_string() + "|response|").as_bytes(),
                server_socket_ref,
                &no_compression(),
            )
            .await
            .unwrap();

            let received_response =
                read_frame(client_socket_ref, FrameType::Response, &no_compression())
                    .await
                    .unwrap();
            assert_eq!(received_response.1, FrameOpcode::Response(resp_opcode));
        }

        // Messages before REGISTER should be fed back to channels
        for i in 0..5 {
            perform_reqest_response(
                RequestOpcode::Query,
                ResponseOpcode::Result,
                &mut client_socket,
                &mut server_socket,
                &format!("message_before_{i}"),
            )
            .await
        }

        perform_reqest_response(
            RequestOpcode::Register,
            ResponseOpcode::Result,
            &mut client_socket,
            &mut server_socket,
            "message_register",
        )
        .await;

        // Messages after REGISTER should be passed through without feedback
        for i in 0..5 {
            perform_reqest_response(
                RequestOpcode::Query,
                ResponseOpcode::Result,
                &mut client_socket,
                &mut server_socket,
                &format!("message_after_{i}"),
            )
            .await
        }

        running_proxy.finish().await.unwrap();

        for _ in 0..5 {
            let (feedback_request, _shard) = request_feedback_rx.recv().await.unwrap();
            assert_eq!(feedback_request.opcode, RequestOpcode::Query);
            let (feedback_response, _shard) = response_feedback_rx.recv().await.unwrap();
            assert_eq!(feedback_response.opcode, ResponseOpcode::Result);
        }

        // Response to REGISTER and further requests / responses should be ignored
        let _ = request_feedback_rx.try_recv().unwrap_err();
        let _ = response_feedback_rx.try_recv().unwrap_err();
    }

    #[tokio::test]
    #[ntest::timeout(1000)]
    async fn proxy_compresses_and_decompresses_frames_iff_compression_negotiated() {
        setup_tracing();
        let node1_real_addr = next_local_address_with_port(9876);
        let node1_proxy_addr = next_local_address_with_port(9876);

        let (request_feedback_tx, mut request_feedback_rx) = mpsc::unbounded_channel();
        let (response_feedback_tx, mut response_feedback_rx) = mpsc::unbounded_channel();
        let proxy = Proxy::builder()
            .with_node(
                Node::builder()
                    .real_address(node1_real_addr)
                    .proxy_address(node1_proxy_addr)
                    .shard_awareness(ShardAwareness::Unaware)
                    .request_rules(vec![RequestRule(
                        Condition::True,
                        RequestReaction::noop().with_feedback_when_performed(request_feedback_tx),
                    )])
                    .response_rules(vec![ResponseRule(
                        Condition::True,
                        ResponseReaction::noop().with_feedback_when_performed(response_feedback_tx),
                    )])
                    .build(),
            )
            .build();
        let running_proxy = proxy.run().await.unwrap();

        let mock_node_listener = TcpListener::bind(node1_real_addr).await.unwrap();

        const PARAMS_REQUEST_NO_COMPRESSION: FrameParams = FrameParams {
            flags: 0,
            version: 0x04,
            stream: 0,
        };
        const PARAMS_REQUEST_COMPRESSION: FrameParams = FrameParams {
            flags: flag::COMPRESSION,
            ..PARAMS_REQUEST_NO_COMPRESSION
        };
        const PARAMS_RESPONSE_NO_COMPRESSION: FrameParams =
            PARAMS_REQUEST_NO_COMPRESSION.for_response();
        const PARAMS_RESPONSE_COMPRESSION: FrameParams =
            PARAMS_REQUEST_NO_COMPRESSION.for_response();

        let make_driver_conn = async { TcpStream::connect(node1_proxy_addr).await.unwrap() };
        let make_node_conn = async { mock_node_listener.accept().await.unwrap() };

        let (mut driver_conn, (mut node_conn, _)) = join(make_driver_conn, make_node_conn).await;

        /* Outline of the test:
         * 1. "driver" sends an uncompressed, e.g., QUERY frame, feedback returns its uncompressed body,
         *    and "node" receives the uncompressed frame.
         * 2. "node" responds with an uncompressed RESULT frame, feedback returns its uncompressed body,
         *    and "driver" receives the uncompressed frame.
         * 3. "driver" sends an uncompressed STARTUP frame, feedback returns its uncompressed body,
         *    and "node" receives the uncompressed frame. This step also triggers `CompressionWriter::set()`
         *    in the proxy, so the associated `CompressionReader`s are notified about it (and can use
         *    the negotiated compression algorithm to (de)compress the frames sent in steps 4. and 5.).
         * 4. "driver" sends a compressed, e.g., QUERY frame, feedback returns its uncompressed body,
         *    and "node" receives the compressed frame.
         * 5. "node" responds with a compressed RESULT frame, feedback returns its uncompressed body,
         *    and "driver" receives the compressed frame.
         */

        // 1. "driver" sends an uncompressed, e.g., QUERY frame, feedback returns its uncompressed body,
        //    and "node" receives the uncompressed frame.
        {
            let sent_frame = RequestFrame {
                params: PARAMS_REQUEST_NO_COMPRESSION,
                opcode: RequestOpcode::Query,
                body: random_body(),
            };

            sent_frame
                .write(&mut driver_conn, &no_compression())
                .await
                .unwrap();

            let (captured_frame, _) = request_feedback_rx.recv().await.unwrap();
            assert_eq!(captured_frame, sent_frame);

            let received_frame = read_request_frame(&mut node_conn, &no_compression())
                .await
                .unwrap();
            assert_eq!(received_frame, sent_frame);
        }

        // 2. "node" responds with an uncompressed RESULT frame, feedback returns its uncompressed body,
        //    and "driver" receives the uncompressed frame.
        {
            let sent_frame = ResponseFrame {
                params: PARAMS_RESPONSE_NO_COMPRESSION,
                opcode: ResponseOpcode::Result,
                body: random_body(),
            };

            sent_frame
                .write(&mut node_conn, &no_compression())
                .await
                .unwrap();

            let (captured_frame, _) = response_feedback_rx.recv().await.unwrap();
            assert_eq!(captured_frame, sent_frame);

            let received_frame = read_response_frame(&mut driver_conn, &no_compression())
                .await
                .unwrap();
            assert_eq!(received_frame, sent_frame);
        }

        // 3. "driver" sends an uncompressed STARTUP frame, feedback returns its uncompressed body,
        //    and "node" receives the uncompressed frame. This step also triggers `CompressionWriter::set()`
        //    in the proxy, so the associated `CompressionReader`s are notified about it (and can use
        //    the negotiated compression algorithm to (de)compress the frames sent in steps 4. and 5.).
        {
            let startup_body = Startup {
                options: std::iter::once((
                    options::COMPRESSION.into(),
                    Compression::Lz4.as_str().into(),
                ))
                .collect(),
            }
            .to_bytes()
            .unwrap();

            let sent_frame = RequestFrame {
                params: PARAMS_REQUEST_NO_COMPRESSION,
                opcode: RequestOpcode::Startup,
                body: startup_body,
            };

            sent_frame
                .write(&mut driver_conn, &no_compression())
                .await
                .unwrap();

            let (captured_frame, _) = request_feedback_rx.recv().await.unwrap();
            assert_eq!(captured_frame, sent_frame);

            let received_frame = read_request_frame(&mut node_conn, &no_compression())
                .await
                .unwrap();
            assert_eq!(received_frame, sent_frame);
        }

        // 4. "driver" sends a compressed, e.g., QUERY frame, feedback returns its uncompressed body,
        //    and "node" receives the compressed frame.
        {
            let sent_frame = RequestFrame {
                params: PARAMS_REQUEST_COMPRESSION,
                opcode: RequestOpcode::Query,
                body: random_body(),
            };

            sent_frame
                .write(&mut driver_conn, &with_compression(Compression::Lz4))
                .await
                .unwrap();

            let (captured_frame, _) = request_feedback_rx.recv().await.unwrap();
            assert_eq!(captured_frame, sent_frame);

            let received_frame =
                read_request_frame(&mut node_conn, &with_compression(Compression::Lz4))
                    .await
                    .unwrap();
            assert_eq!(received_frame, sent_frame);
        }

        // 5. "node" responds with a compressed RESULT frame, feedback returns its uncompressed body,
        //    and "driver" receives the compressed frame.
        {
            let sent_frame = ResponseFrame {
                params: PARAMS_RESPONSE_COMPRESSION,
                opcode: ResponseOpcode::Result,
                body: random_body(),
            };

            sent_frame
                .write(&mut node_conn, &with_compression(Compression::Lz4))
                .await
                .unwrap();

            let (captured_frame, _) = response_feedback_rx.recv().await.unwrap();
            assert_eq!(captured_frame, sent_frame);

            let received_frame =
                read_response_frame(&mut driver_conn, &with_compression(Compression::Lz4))
                    .await
                    .unwrap();
            assert_eq!(received_frame, sent_frame);
        }

        running_proxy.finish().await.unwrap();
    }
}
