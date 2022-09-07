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
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{debug, error, info, warn};

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

pub struct Node {
    real_addr: SocketAddr,
    proxy_addr: SocketAddr,
    shard_awareness: ShardAwareness,
    request_rules: Option<Vec<RequestRule>>,
    response_rules: Option<Vec<ResponseRule>>,
}

impl Node {
    pub fn new(
        real_addr: SocketAddr,
        proxy_addr: SocketAddr,
        shard_awareness: ShardAwareness,
        request_rules: Option<Vec<RequestRule>>,
        response_rules: Option<Vec<ResponseRule>>,
    ) -> Node {
        Node {
            real_addr,
            proxy_addr,
            shard_awareness,
            request_rules,
            response_rules,
        }
    }
}

struct InternalNode {
    real_addr: SocketAddr,
    proxy_addr: SocketAddr,
    shard_awareness: ShardAwareness,
    request_rules: Arc<RwLock<Vec<RequestRule>>>,
    response_rules: Arc<RwLock<Vec<ResponseRule>>>,
}

pub struct Proxy {
    nodes: Vec<InternalNode>,
}

impl Proxy {
    pub fn new(nodes: impl IntoIterator<Item = Node>) -> Self {
        Proxy {
            nodes: nodes
                .into_iter()
                .map(|node| InternalNode {
                    real_addr: node.real_addr,
                    proxy_addr: node.proxy_addr,
                    shard_awareness: node.shard_awareness,
                    request_rules: node
                        .request_rules
                        .map(|rules| Arc::new(RwLock::new(rules)))
                        .unwrap_or_default(),
                    response_rules: node
                        .response_rules
                        .map(|rules| Arc::new(RwLock::new(rules)))
                        .unwrap_or_default(),
                })
                .collect(),
        }
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
                    response_rules: node.response_rules.clone(),
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
    request_rules: Arc<RwLock<Vec<RequestRule>>>,
    response_rules: Arc<RwLock<Vec<ResponseRule>>>,
}

impl RunningNode {
    /// Replaces the previous request rules with the new ones.
    pub async fn change_request_rules(&mut self, rules: Option<Vec<RequestRule>>) {
        *self.request_rules.write().await = rules.unwrap_or_default();
    }

    /// Replaces the previous response rules with the new ones.
    pub async fn change_response_rules(&mut self, rules: Option<Vec<ResponseRule>>) {
        *self.response_rules.write().await = rules.unwrap_or_default();
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
            request_rules.write().await.clear();
            response_rules.write().await.clear();
        }
    }

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
            node.proxy_addr
        );
        let doorkeeper = Doorkeeper {
            shards_number: match node.shard_awareness {
                ShardAwareness::FixedNum(shards_num) => Some(shards_num),
                _ => None,
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
                res = self.make_streams(connection_no) => { match res {
                    Ok((driver_addr, driver_stream, cluster_stream)) => {
                        let (cluster_read, cluster_write) = cluster_stream.into_split();
                        let (driver_read, driver_write) = driver_stream.into_split();

                        let new_worker = || ProxyWorker{
                            terminate_notifier: self.terminate_signaler.subscribe(),
                            finish_guard:  self.finish_guard.clone(),
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

                        tokio::task::spawn(new_worker()
                            .receiver_from_driver(driver_read, tx_request));
                        tokio::task::spawn(new_worker()
                            .receiver_from_cluster(cluster_read, tx_response));
                        tokio::task::spawn(new_worker()
                            .sender_to_cluster(cluster_write, rx_cluster, connection_close_tx.subscribe(), self.terminate_signaler.subscribe()));
                        tokio::task::spawn(new_worker()
                            .sender_to_driver(driver_write, rx_driver, connection_close_tx.subscribe(), self.terminate_signaler.subscribe()));
                        tokio::task::spawn(new_worker().request_processor(
                            rx_request,
                            tx_driver.clone(),
                            tx_cluster.clone(),
                            connection_no,
                            self.node.request_rules.clone(),
                            connection_close_tx.clone()
                        ));
                        tokio::task::spawn(new_worker().response_processor(
                            rx_response,
                            tx_driver,
                            tx_cluster,
                            connection_no,
                            self.node.response_rules.clone(),
                            connection_close_tx.clone()
                        ));
                        debug!("Doorkeeper of node {} spawned workers.", self.node.real_addr);
                        connection_no += 1;
                    }
                    Err(err) => {
                        error!("Error in doorkeeper for node {}: {}", self.node.real_addr, err);
                        let _ = self.error_propagator.send(err.into());
                        break;
                    }
                }}
                _terminate = own_terminate_notifier.recv() => break
            }
        }
        debug!(
            "Doorkeeper exits: proxy {}, node {}.",
            self.node.proxy_addr, self.node.real_addr
        );
    }

    async fn make_streams(
        &mut self,
        connection_no: usize,
    ) -> Result<(SocketAddr, TcpStream, TcpStream), DoorkeeperError> {
        let (driver_stream, driver_addr) =
            self.listener.accept().await.map_err(|err| {
                DoorkeeperError::DriverConnectionAttempt(self.node.real_addr, err)
            })?;
        info!(
            "Connected driver from {} to {}, connection no={}.",
            driver_addr, self.node.proxy_addr, connection_no
        );

        let cluster_stream = if self.node.shard_awareness.is_aware() {
            let shards = match self.shards_number {
                None => {
                    let temporary_stream =
                        TcpStream::connect(self.node.real_addr)
                            .await
                            .map_err(|err| {
                                DoorkeeperError::NodeConnectionAttempt(self.node.real_addr, err)
                            })?;
                    let shards = self.obtain_shards_number(temporary_stream).await?;
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

            socket.connect(self.node.real_addr).await.map(|ok| {
                info!(
                    "Connected to the cluster from {} at {}, shard {}.",
                    ok.local_addr().unwrap(),
                    self.node.real_addr,
                    shard_preserving_addr.port() % shards
                );
                ok
            })
        } else {
            TcpStream::connect(self.node.real_addr).await.map(|ok| {
                info!("Connected to the cluster at {}.", self.node.real_addr);
                ok
            })
        }
        .map_err(|err| DoorkeeperError::NodeConnectionAttempt(self.node.real_addr, err))?;

        Ok((driver_addr, driver_stream, cluster_stream))
    }

    fn next_port_to_same_shard(&self, port: u16) -> u16 {
        port.wrapping_add(self.shards_number.unwrap())
    }

    async fn obtain_shards_number(
        &self,
        mut connection: TcpStream,
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
        info!(
            "Obtained shards number on node {}: {}",
            self.node.real_addr, shards
        );
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
        request_rules: Arc<RwLock<Vec<RequestRule>>>,
        connection_close_signaler: ConnectionCloseSignaler,
    ) {
        self.run_until_interrupted("request_processor", |_, _, _| async move {
            'mainloop: loop {
                match requests_rx.recv().await {
                    Some(request) => {
                        let ctx = EvaluationContext {
                            connection_seq_no: connection_no,
                            opcode: FrameOpcode::Request(request.opcode),
                            frame_body: request.body.clone(),
                        };
                        let guard = request_rules.read().await;
                        '_ruleloop: for RequestRule(condition, reaction) in guard.iter() {
                            if condition.eval(&ctx) {
                                let cluster_tx_clone = cluster_tx.clone();
                                let request_clone = request.clone();
                                let pass_action = async move {
                                    if let Some(ref pass_action) = reaction.to_addressee {
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
                                    if let Some(ref forge_action) = reaction.to_sender {
                                        if let Some(time) = forge_action.delay {
                                            tokio::time::sleep(time).await;
                                        }
                                        let forged_frame = {
                                            let processor =
                                                forge_action.msg_processor.as_ref().expect(
                                                    "Frame processor is required to forge a frame.",
                                                );
                                            processor(request_clone)
                                        };
                                        let _ = driver_tx_clone.send(forged_frame);
                                    };
                                };

                                let connection_close_signaler_clone =
                                    connection_close_signaler.clone();
                                let drop_action = async move {
                                    if let Some(ref delay) = reaction.drop_connection {
                                        if let Some(ref time) = delay {
                                            tokio::time::sleep(*time).await;
                                        }
                                        // close connection.
                                        info!(
                                            "Dropping connection (as requested by a proxy rule)!"
                                        );
                                        let _ = connection_close_signaler_clone.send(());
                                    }
                                };

                                futures::join!(pass_action, forge_action, drop_action);

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
        response_rules: Arc<RwLock<Vec<ResponseRule>>>,
        connection_close_signaler: ConnectionCloseSignaler,
    ) {
        self.run_until_interrupted("request_processor", |_, _, _| async move {
            'mainloop: loop {
                match responses_rx.recv().await {
                    Some(response) => {
                        let ctx = EvaluationContext {
                            connection_seq_no: connection_no,
                            opcode: FrameOpcode::Response(response.opcode),
                            frame_body: response.body.clone(),
                        };
                        let guard = response_rules.read().await;
                        '_ruleloop: for ResponseRule(condition, reaction) in guard.iter() {
                            if condition.eval(&ctx) {
                                let response_clone = response.clone();
                                let driver_tx_clone = driver_tx.clone();
                                let pass_action = async move {
                                    if let Some(ref pass_action) = reaction.to_addressee {
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
                                    if let Some(ref forge_action) = reaction.to_sender {
                                        if let Some(time) = forge_action.delay {
                                            tokio::time::sleep(time).await;
                                        }
                                        let forged_frame = {
                                            let processor =
                                                forge_action.msg_processor.as_ref().expect(
                                                    "Frame processor is required to forge a frame.",
                                                );
                                            processor(response_clone)
                                        };
                                        let _ = cluster_tx_clone.send(forged_frame);
                                    };
                                };

                                let connection_close_signaler_clone =
                                    connection_close_signaler.clone();
                                let drop_action = async move {
                                    if let Some(ref delay) = reaction.drop_connection {
                                        if let Some(ref time) = delay {
                                            tokio::time::sleep(*time).await;
                                        }
                                        // close connection.
                                        info!(
                                            "Dropping connection (as requested by a proxy rule)!"
                                        );
                                        let _ = connection_close_signaler_clone.send(());
                                    }
                                };

                                futures::join!(pass_action, forge_action, drop_action);

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
