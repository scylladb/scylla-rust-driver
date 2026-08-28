//! The OpenSSL 0.10 TLS backend of the driver.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, LazyLock, Mutex};

use openssl::error::ErrorStack;
use openssl::ex_data::Index;
use openssl::ssl::{
    Ssl, SslConnectorBuilder, SslContext, SslContextBuilder, SslSession, SslSessionCacheMode,
    SslSessionRef, SslVersion,
};

use crate::client::session::TlsContext;

/// How many TLS 1.3 tickets we are willing to bank for a single node.
///
/// TLS 1.3 tickets are single use, so unused ones accumulate; without a cap a long-lived
/// session talking to a chatty server would grow the store without bound. Eight matches
/// rustls' own per-host limit, which makes the two backends behave alike under load.
const MAX_TLS13_TICKETS_PER_HOST: usize = 8;

/// How many nodes we are willing to bank sessions for.
///
/// Far above any real cluster's node count, so it only ever trips on churn: rolling
/// restarts and elastic scale-in/out retire node IPs the driver will never reconnect to,
/// whose tickets would otherwise be held (a DER `SSL_SESSION` embeds the peer's
/// certificate, so ~10 kB each) for the process lifetime.
const MAX_NODES: usize = 256;

/// A single TLS session banked for a node, DER-encoded.
#[derive(Clone)]
struct NodeTicket(Vec<u8>);

impl NodeTicket {
    fn der(&self) -> &[u8] {
        &self.0
    }
}

/// Sessions banked for a single node.
#[derive(Default)]
struct TicketBank {
    /// The latest TLS 1.2 session, if any.
    ///
    /// RFC 5077 tickets are reusable - nothing forbids offering the same one again - so
    /// we only keep the newest and hand out copies of it indefinitely.
    tls12: Option<NodeTicket>,

    /// TLS 1.3 tickets, oldest first.
    ///
    /// A TLS 1.3 ticket is single use: RFC 8446 appendix C.4 ("Client Tracking
    /// Prevention") says clients SHOULD NOT reuse one, because a resumed ClientHello
    /// carries the ticket as its `PskIdentity` and reuse makes connections linkable by a
    /// passive observer. Hence a FIFO queue that a ticket leaves for good when used.
    tls13: VecDeque<NodeTicket>,

    /// Value of [`Nodes::clock`] at the last [`TicketStore::store`] for this node,
    /// which orders nodes by staleness when the [`MAX_NODES`] cap is reached.
    ///
    /// A counter rather than an `Instant`: no clock reads, and it cannot tie.
    stored_at: u64,
}

/// The [`TicketStore`]'s contents, all of it behind one lock.
#[derive(Default)]
struct Nodes {
    tickets: HashMap<IpAddr, TicketBank>,
    /// Ticks once per [`TicketStore::store`].
    clock: u64,
}

/// TLS sessions harvested from completed handshakes, to be offered by later connections.
///
/// Keyed per node address, because a ticket is only resumable against the server that
/// issued it. Offering a TLS 1.3 ticket to the wrong node would additionally spend it for
/// nothing, as it is single use.
/// We could key by host id, which would help in some cases where multiple translated IPs
/// connect to a single node (e.g. some client routes deployments) but:
/// - It would deviate from rustls behavior.
/// - It would be more complicated (for example when handling connections for which
///   host id is unknown, like control connection).
///
/// Sessions are kept as DER rather than as live `SslSession` objects. That is
/// important: OpenSSL's legacy behaviour is to mark an `SSL_SESSION` as not resumable
/// once its connection is torn down without a clean TLS shutdown, which is exactly how
/// the driver's connections end. A DER encoding is immune to that, and decoding it gives
/// every connection its own fresh session object.
/// Doing that is not a problem: the session invalidation behavior was removed in TLS 1.1
/// spec, it's just still present in OpenSSL for legacy reasons, so we need to work around it.
///
/// Bounded in both directions, because nothing else prunes it: at most
/// [`MAX_TLS13_TICKETS_PER_HOST`] TLS 1.3 tickets plus one TLS 1.2 session per node, and
/// at most [`MAX_NODES`] nodes, the least recently banked one being dropped to make room.
/// A node whose sessions all get used up stops being tracked at all.
#[derive(Default)]
pub(crate) struct TicketStore {
    nodes: Mutex<Nodes>,
}

impl TicketStore {
    /// Banks a session issued by the node at `node_ip`.
    fn store(&self, node_ip: IpAddr, session: &SslSessionRef) {
        // Done before taking the lock: it can fail, and it allocates.
        let ticket = match session.to_der() {
            Ok(der) => NodeTicket(der),
            Err(err) => {
                tracing::debug!("Failed to serialize a TLS session for node {node_ip}: {err}");
                return;
            }
        };
        let single_use = session.protocol_version() == SslVersion::TLS1_3;

        let mut nodes = self.nodes.lock().unwrap();
        let nodes = &mut *nodes;
        nodes.clock += 1;

        if nodes.tickets.len() >= MAX_NODES && !nodes.tickets.contains_key(&node_ip) {
            let stalest = nodes
                .tickets
                .iter()
                .min_by_key(|(_, bank)| bank.stored_at)
                .map(|(&ip, _)| ip);
            if let Some(ip) = stalest {
                nodes.tickets.remove(&ip);
            }
        }

        let bank = nodes.tickets.entry(node_ip).or_default();
        bank.stored_at = nodes.clock;
        if single_use {
            if bank.tls13.len() >= MAX_TLS13_TICKETS_PER_HOST {
                bank.tls13.pop_front();
            }
            bank.tls13.push_back(ticket);
        } else {
            bank.tls12 = Some(ticket);
        }
    }

    /// Produces the DER of a session to offer to the node at `node_ip`, if we have one.
    ///
    /// A TLS 1.3 ticket is removed from the store by this call; a TLS 1.2 session is not.
    fn take(&self, node_ip: IpAddr) -> Option<NodeTicket> {
        let mut nodes = self.nodes.lock().unwrap();
        let Entry::Occupied(mut entry) = nodes.tickets.entry(node_ip) else {
            return None;
        };
        let bank = entry.get_mut();
        let taken = bank.tls13.pop_front().or_else(|| bank.tls12.clone());
        // Nothing left to offer this node, so stop tracking it: node addresses come and
        // go as a cluster is restarted or resized, and empty entries would accumulate.
        if bank.tls12.is_none() && bank.tls13.is_empty() {
            entry.remove();
        }
        taken
    }
}

/// The ex-data slot in which the driver records, before the handshake, which node a
/// given [`Ssl`] is talking to - so that the new-session callback knows where to file the
/// sessions it receives.
///
/// OpenSSL ex-data indices are process-global and must not be allocated more than once,
/// hence a single lazily initialized static rather than a per-context index.
static NODE_IP_INDEX: LazyLock<Option<Index<Ssl, IpAddr>>> =
    LazyLock::new(|| match Ssl::new_ex_index() {
        Ok(index) => Some(index),
        Err(err) => {
            tracing::warn!(
                "Failed to allocate an OpenSSL ex-data index; \
                 TLS session resumption will be disabled: {err}"
            );
            None
        }
    });

/// A TLS context backed by OpenSSL 0.10.
///
/// This is the recommended way of configuring the OpenSSL backend of the driver.
/// Create it from an [`SslConnectorBuilder`] with [`OpenSsl010Config::new`].
#[derive(Clone)] // Cheaply clonable - reference counted.
pub struct OpenSsl010Config {
    context: SslContext,
    tickets: Arc<TicketStore>,
    use_tls_tickets: bool,
}

impl OpenSsl010Config {
    /// Creates a new context from an [`SslConnectorBuilder`].
    ///
    /// This is the recommended constructor: [`SslConnector`](openssl::ssl::SslConnector)
    /// comes with safe defaults, most notably peer certificate verification.
    pub fn new(mut builder: SslConnectorBuilder) -> Self {
        let tickets = Arc::new(TicketStore::default());
        // `SslConnectorBuilder` cannot be unwrapped into its `SslContextBuilder`,
        // but it dereferences to it, which is enough to configure it.
        Self::configure(&mut builder, &tickets);
        Self {
            context: builder.build().into_context(),
            tickets,
            use_tls_tickets: true,
        }
    }

    /// Creates a new context from a raw [`SslContextBuilder`].
    ///
    /// This is **dangerous**: unlike [`SslConnector`](openssl::ssl::SslConnector),
    /// a bare [`SslContextBuilder`] has insecure defaults - in particular, it does
    /// not verify the peer's certificate at all. Prefer [`OpenSsl010Config::new`]
    /// unless you really need full control over the context.
    pub fn from_dangerous_builder(mut builder: SslContextBuilder) -> Self {
        let tickets = Arc::new(TicketStore::default());
        Self::configure(&mut builder, &tickets);
        Self {
            context: builder.build(),
            tickets,
            use_tls_tickets: true,
        }
    }

    /// The single place where the driver configures a context builder,
    /// shared by both constructors.
    fn configure(builder: &mut SslContextBuilder, tickets: &Arc<TicketStore>) {
        // `CLIENT`, because client-side session caching is off by default and without it
        // the new-session callback would never be called at all. `NO_INTERNAL_STORE`,
        // because our own store replaces OpenSSL's internal one - unlike OpenSSL's, it is
        // keyed per node and it respects the TLS 1.3 single-use rule.
        builder.set_session_cache_mode(
            SslSessionCacheMode::CLIENT | SslSessionCacheMode::NO_INTERNAL_STORE,
        );

        let tickets = Arc::clone(tickets);
        builder.set_new_session_callback(move |ssl, session| {
            let Some(node_ip) = NODE_IP_INDEX.and_then(|index| ssl.ex_data(index).copied()) else {
                // No node address means nothing will ever offer this session, so drop it
                // rather than bank it: either tickets are disabled, which deliberately
                // leaves the address unrecorded (see `OpenSsl010Config::new_ssl`), or the
                // ex-data index could not be allocated.
                tracing::debug!(
                    "Received a TLS session for a connection with no node address recorded; \
                     dropping it"
                );
                return;
            };
            tickets.store(node_ip, &session);
        });
    }

    /// Whether the driver offers banked TLS session tickets on new connections,
    /// resuming earlier TLS sessions instead of performing a full handshake.
    ///
    /// Enabled by default, matching the rustls backend, which resumes natively.
    pub fn use_tls_tickets(&self) -> bool {
        self.use_tls_tickets
    }

    /// Enables or disables the use of banked TLS session tickets.
    ///
    /// Enabled by default, matching the rustls backend, which resumes natively.
    ///
    /// With tickets disabled the driver neither offers a banked session on a new
    /// connection nor banks the ones the cluster issues, so the store stays empty.
    pub fn set_use_tls_tickets(mut self, use_tls_tickets: bool) -> Self {
        self.use_tls_tickets = use_tls_tickets;
        self
    }

    /// Creates a fresh per-connection [`Ssl`] object out of this context.
    pub(crate) fn new_ssl(&self, node_address: SocketAddr) -> Result<Ssl, ErrorStack> {
        let node_ip = node_address.ip();
        let mut ssl = new_ssl(&self.context, node_address)?;

        if !self.use_tls_tickets {
            // Leaving the node address unrecorded is what keeps the store empty: the
            // new-session callback drops every session it cannot file under a node, so a
            // connection that would never offer a ticket does not bank one either.
            return Ok(ssl);
        }

        // Lets the new-session callback know which node the sessions it receives
        // belong to.
        if let Some(index) = *NODE_IP_INDEX {
            ssl.set_ex_data(index, node_ip);
        }

        if let Some(ticket) = self.tickets.take(node_ip) {
            match SslSession::from_der(ticket.der()) {
                // SAFETY: `SSL_set_session` requires the session to belong to the same
                // `SslContext` as the `Ssl`. A session decoded from DER is not attached
                // to any context, so there is no context to mismatch.
                //
                // A session the connection turns out not to be able to use - a TLS 1.3
                // ticket on a connection that negotiates TLS 1.2, say - is accepted here
                // and then simply never resumed, costing a full handshake and nothing
                // else.
                Ok(session) => unsafe {
                    if let Err(err) = ssl.set_session(&session) {
                        // Not fatal: we just perform a full handshake instead.
                        tracing::debug!("Failed to offer a TLS session to node {node_ip}: {err}");
                    }
                },
                Err(err) => {
                    // Also not fatal - the ticket is simply gone.
                    tracing::debug!(
                        "Failed to decode a banked TLS session for node {node_ip}: {err}"
                    );
                }
            }
        }

        Ok(ssl)
    }
}

/// Creates a fresh per-connection [`Ssl`] object out of an [`SslContext`].
pub(crate) fn new_ssl(context: &SslContext, node_address: SocketAddr) -> Result<Ssl, ErrorStack> {
    let mut ssl = Ssl::new(context)?;
    ssl.set_connect_state();
    // Makes OpenSSL verify the node's certificate against its IP address.
    // Corresponds to `X509_VERIFY_PARAM_set1_ip`.
    ssl.param_mut().set_ip(node_address.ip())?;
    Ok(ssl)
}

impl From<SslConnectorBuilder> for OpenSsl010Config {
    fn from(builder: SslConnectorBuilder) -> Self {
        Self::new(builder)
    }
}

impl From<OpenSsl010Config> for TlsContext {
    fn from(context: OpenSsl010Config) -> Self {
        TlsContext::OpenSsl010Config(context)
    }
}

impl From<SslConnectorBuilder> for TlsContext {
    fn from(builder: SslConnectorBuilder) -> Self {
        TlsContext::OpenSsl010Config(OpenSsl010Config::new(builder))
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read as _, Write as _};
    use std::net::{Ipv4Addr, TcpListener, TcpStream};
    use std::thread::JoinHandle;

    use openssl::pkey::PKey;
    use openssl::ssl::{SslMethod, SslStream, SslVerifyMode};
    use openssl::x509::X509;
    use rcgen::{CertificateParams, CertifiedIssuer, KeyPair, SanType};

    use super::*;

    const NODE_A: IpAddr = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
    const NODE_B: IpAddr = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2));

    /// An in-process OpenSSL server, pinned to exactly one TLS version, that serves a
    /// fixed number of connections and then exits.
    struct TestServer {
        addr: SocketAddr,
        thread: JoinHandle<()>,
    }

    impl TestServer {
        /// Starts a server with a freshly generated self-signed certificate.
        fn start(version: SslVersion, connections: usize) -> Self {
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::new(vec![]).unwrap();
            params
                .subject_alt_names
                .push(SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)));
            let cert = CertifiedIssuer::self_signed(params, key).unwrap();

            let mut builder = SslContext::builder(SslMethod::tls_server()).unwrap();
            builder
                .set_certificate(&X509::from_der(cert.der()).unwrap())
                .unwrap();
            builder
                .set_private_key(&PKey::private_key_from_der(&cert.key().serialize_der()).unwrap())
                .unwrap();
            builder.set_min_proto_version(Some(version)).unwrap();
            builder.set_max_proto_version(Some(version)).unwrap();
            let context = builder.build();

            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
            let addr = listener.local_addr().unwrap();
            let thread = std::thread::spawn(move || {
                for _ in 0..connections {
                    let (stream, _) = listener.accept().unwrap();
                    let mut ssl = Ssl::new(&context).unwrap();
                    ssl.set_accept_state();
                    let mut stream = SslStream::new(ssl, stream).unwrap();
                    stream.accept().unwrap();
                    // TLS 1.3 tickets are only sent after the handshake, so the client has
                    // to read something to see them.
                    stream.write_all(b"x").unwrap();
                    stream.flush().unwrap();
                    // Read the client's goodbye, so that it is not left blocked on write.
                    let mut byte = [0u8; 1];
                    let _ = stream.read(&mut byte);
                }
            });

            Self { addr, thread }
        }

        fn join(self) {
            self.thread.join().unwrap();
        }
    }

    /// A driver context that negotiates within `min..=max`.
    ///
    /// Certificate verification is off: the point of these tests is to obtain genuine
    /// sessions, not to test OpenSSL's certificate validation.
    fn client_context(min: SslVersion, max: SslVersion) -> OpenSsl010Config {
        let mut builder = SslContext::builder(SslMethod::tls_client()).unwrap();
        builder.set_verify(SslVerifyMode::NONE);
        builder.set_min_proto_version(Some(min)).unwrap();
        builder.set_max_proto_version(Some(max)).unwrap();
        OpenSsl010Config::from_dangerous_builder(builder)
    }

    /// Performs one real TLS handshake against `server`, with the driver told that it is
    /// talking to the node at `node_ip`. Returns whether OpenSSL considered the session
    /// resumed.
    ///
    /// This goes through [`OpenSsl010Config::new_ssl`], so it exercises the whole path:
    /// the session cache mode, the new-session callback, the ex-data key that tells the
    /// callback which node a session belongs to, and offering a banked session.
    fn handshake(context: &OpenSsl010Config, server: &TestServer, node_ip: IpAddr) -> bool {
        let tcp = TcpStream::connect(server.addr).unwrap();
        // A clone per connection, as the driver does: `TlsProvider::make_tls_config`
        // clones the context for every `TlsConfig` it hands out, so the store only works
        // if it is shared rather than copied.
        // The port is the server's, but it is the IP that the store keys on.
        let ssl = context
            .clone()
            .new_ssl(SocketAddr::new(node_ip, server.addr.port()))
            .unwrap();
        let mut stream = SslStream::new(ssl, tcp).unwrap();
        stream.connect().unwrap();
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).unwrap();
        assert_eq!(&byte, b"x");
        stream.write_all(b"y").unwrap();
        stream.flush().unwrap();
        stream.ssl().session_reused()
    }

    /// One handshake per entry of `node_ips`, in order, all through a single
    /// [`OpenSsl010Config`] against a single server pinned to `version`.
    ///
    /// Returns that context and, per connection, whether its session was resumed.
    fn handshakes(
        version: SslVersion,
        node_ips: &[IpAddr],
        use_tickets: bool,
    ) -> (OpenSsl010Config, Vec<bool>) {
        let server = TestServer::start(version, node_ips.len());
        let context = client_context(version, version).set_use_tls_tickets(use_tickets);
        let reused = node_ips
            .iter()
            .map(|&node_ip| handshake(&context, &server, node_ip))
            .collect();
        server.join();
        (context, reused)
    }

    /// Everything the new-session callback banked for `node_ip`, oldest first.
    fn drain(context: &OpenSsl010Config, node_ip: IpAddr) -> Vec<NodeTicket> {
        let tickets = context
            .tickets
            .nodes
            .lock()
            .unwrap()
            .tickets
            .remove(&node_ip)
            .unwrap_or_default();
        let mut out: Vec<NodeTicket> = tickets.tls13.into_iter().collect();
        out.extend(tickets.tls12);
        out
    }

    /// A genuine TLS 1.2 session, obtained from a real handshake.
    fn tls12_session() -> SslSession {
        let (context, _) = handshakes(SslVersion::TLS1_2, &[NODE_A], true);
        let ticket = drain(&context, NODE_A)
            .pop()
            .expect("no TLS 1.2 session was harvested");
        let session = SslSession::from_der(ticket.der()).unwrap();
        assert_eq!(session.protocol_version(), SslVersion::TLS1_2);
        session
    }

    /// `count` genuine, distinct TLS 1.3 tickets, obtained from real handshakes.
    ///
    /// One connection at a time, each against a fresh server and through a fresh context:
    /// a single context's store is capped at [`MAX_TLS13_TICKETS_PER_HOST`], which is
    /// fewer tickets than the cap test itself needs.
    fn tls13_tickets(count: usize) -> Vec<SslSession> {
        let mut tickets: Vec<NodeTicket> = Vec::with_capacity(count);
        while tickets.len() < count {
            let (context, _) = handshakes(SslVersion::TLS1_3, &[NODE_A], true);
            let banked = drain(&context, NODE_A);
            assert!(
                !banked.is_empty(),
                "a TLS 1.3 handshake banked no ticket at all"
            );
            tickets.extend(banked);
        }
        tickets.truncate(count);
        let sessions: Vec<SslSession> = tickets
            .iter()
            .map(|ticket| SslSession::from_der(ticket.der()).unwrap())
            .collect();
        for session in &sessions {
            assert_eq!(session.protocol_version(), SslVersion::TLS1_3);
        }
        sessions
    }

    /// The harvesting and offering paths work end to end: a second connection to a node
    /// resumes the session the first one banked, and neither resumes nor banks anything
    /// when tickets are switched off.
    #[test]
    fn banked_sessions_are_offered_and_resumed() {
        for version in [SslVersion::TLS1_2, SslVersion::TLS1_3] {
            let (_, reused) = handshakes(version, &[NODE_A, NODE_A], true);
            assert_eq!(
                reused,
                vec![false, true],
                "{version:?}: expected the second connection, and only it, to resume"
            );

            let (context, reused) = handshakes(version, &[NODE_A, NODE_A], false);
            assert_eq!(
                reused,
                vec![false, false],
                "{version:?}: a connection resumed with TLS tickets disabled"
            );
            assert!(
                drain(&context, NODE_A).is_empty(),
                "{version:?}: a session was banked with TLS tickets disabled"
            );
        }
    }

    /// A session banked for one node is never offered to another: it would not resume
    /// there, and a TLS 1.3 ticket would be spent for nothing.
    ///
    /// The two connections to [`NODE_A`] come first so that the third one is known to be
    /// refused resumption because of the node it is for, and not because this server
    /// would not have resumed anything at that point anyway.
    #[test]
    fn banked_sessions_are_not_offered_to_another_node() {
        for version in [SslVersion::TLS1_2, SslVersion::TLS1_3] {
            let (_, reused) = handshakes(version, &[NODE_A, NODE_A, NODE_B], true);
            assert_eq!(
                reused,
                vec![false, true, false],
                "{version:?}: expected only the second connection, the one repeating a node, to resume"
            );
        }
    }

    /// TLS 1.2 sessions are reusable, so the store keeps handing the latest one out.
    #[test]
    fn tls12_session_is_reusable() {
        let session = tls12_session();
        let der = session.to_der().unwrap();

        let store = TicketStore::default();
        store.store(NODE_A, &session);

        for i in 0..3 {
            assert_eq!(
                store.take(NODE_A).as_ref().map(NodeTicket::der),
                Some(der.as_slice()),
                "the TLS 1.2 session was not handed out on attempt {i}"
            );
        }
    }

    /// The per-node TLS 1.3 queue is bounded and drops the oldest ticket on overflow; what
    /// is left is handed out oldest first and at most once each, after which the node
    /// stops being tracked rather than leaving an empty entry behind for an address the
    /// cluster may never use again.
    #[test]
    fn tls13_ticket_queue_is_bounded_fifo_and_single_use() {
        let over = MAX_TLS13_TICKETS_PER_HOST + 2;
        let sessions = tls13_tickets(over);
        let ders: Vec<Vec<u8>> = sessions.iter().map(|s| s.to_der().unwrap()).collect();
        assert_eq!(
            ders.iter().collect::<std::collections::HashSet<_>>().len(),
            ders.len(),
            "the harvested TLS 1.3 tickets are not distinct, the test cannot tell order"
        );

        let store = TicketStore::default();
        for session in &sessions {
            store.store(NODE_A, session);
        }

        // Only the newest MAX_TLS13_TICKETS_PER_HOST survived, still in FIFO order.
        for (i, der) in ders[over - MAX_TLS13_TICKETS_PER_HOST..].iter().enumerate() {
            assert_eq!(
                store.take(NODE_A).as_ref().map(NodeTicket::der),
                Some(der.as_slice()),
                "ticket {i} was not the {i}-th one handed out"
            );
        }
        assert!(
            store.take(NODE_A).is_none(),
            "a ticket was handed out twice, or more than {MAX_TLS13_TICKETS_PER_HOST} were kept"
        );
        assert!(
            store.nodes.lock().unwrap().tickets.is_empty(),
            "an emptied node entry was left behind"
        );
    }

    /// The number of tracked nodes is bounded, and overflow drops the least recently
    /// banked node - the one whose tickets a churning cluster left behind.
    ///
    /// One real session, stored under many synthetic IPs: the store cannot tell them
    /// apart, and a handshake per node would be gratuitous.
    #[test]
    fn node_keys_are_bounded_and_evict_the_stalest_node() {
        let session = tls12_session();
        let ip = |i: usize| IpAddr::V4(Ipv4Addr::from((i as u32).to_be_bytes()));

        let store = TicketStore::default();
        for i in 0..MAX_NODES {
            store.store(ip(i), &session);
        }
        assert_eq!(store.nodes.lock().unwrap().tickets.len(), MAX_NODES);

        // Refresh the very first node, so that the second one becomes the stalest.
        store.store(ip(0), &session);

        // One node over the cap.
        let newcomer = ip(MAX_NODES);
        store.store(newcomer, &session);

        let tracked = store.nodes.lock().unwrap().tickets.len();
        assert_eq!(
            tracked, MAX_NODES,
            "the store tracks {tracked} nodes, more than the cap of {MAX_NODES}"
        );
        assert!(
            store.take(newcomer).is_some(),
            "the newly banked node was not kept"
        );
        assert!(
            store.take(ip(1)).is_none(),
            "the stalest node's tickets survived; something else was evicted"
        );
        assert!(
            store.take(ip(0)).is_some(),
            "a refreshed node was evicted as if it were stale"
        );
    }

    /// A banked session that a connection cannot use is not fatal: the driver falls back
    /// to a full handshake.
    ///
    /// Both ways that happens, against one TLS 1.2-only server reached by a client that
    /// would also speak TLS 1.3:
    /// - a TLS 1.3 ticket offered on a connection that negotiates TLS 1.2, which
    ///   `SSL_set_session` accepts without complaint and the handshake then never
    ///   resumes;
    /// - a banked entry that does not decode as a session at all.
    #[test]
    fn an_unusable_banked_session_falls_back_to_a_full_handshake() {
        let server = TestServer::start(SslVersion::TLS1_2, 2);
        let context = client_context(SslVersion::TLS1_2, SslVersion::TLS1_3);

        context.tickets.store(NODE_A, &tls13_tickets(1)[0]);
        assert!(
            !handshake(&context, &server, NODE_A),
            "a TLS 1.3 ticket was resumed by a TLS 1.2 connection"
        );

        context.tickets.nodes.lock().unwrap().tickets.insert(
            NODE_B,
            TicketBank {
                tls12: Some(NodeTicket(b"not a session".to_vec())),
                ..Default::default()
            },
        );
        assert!(
            !handshake(&context, &server, NODE_B),
            "an undecodable banked session was somehow resumed"
        );

        server.join();
    }
}
