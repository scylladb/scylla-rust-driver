//! Tests verifying TLS session resumption (via TLS session tickets / PSK) behaviour
//! of the driver's two TLS backends.
//!
//! Expectation being verified:
//! - the rustls backend reuses session tickets, so subsequent connections resume
//!   previous TLS sessions,
//! - the openssl backend (as used by the driver) does NOT reuse session tickets,
//!   so every connection performs a full handshake.
//!
//! This is verified for both TLS 1.2 and TLS 1.3.
//!
//! # How it works
//!
//! We put a small, TLS-transparent TCP proxy in front of every node. The proxy does
//! not terminate TLS; it merely forwards bytes and passively inspects the plaintext
//! parts of the TLS handshake (ClientHello, ServerHello, and - for TLS 1.2 - whether
//! the server performs a full or an abbreviated handshake). For each connection it
//! records:
//! - the negotiated TLS version,
//! - whether the client *offered* resumption (non-empty SessionTicket extension for
//!   TLS 1.2, or a pre_shared_key extension for TLS 1.3),
//! - whether the server *accepted* resumption (pre_shared_key extension in ServerHello
//!   for TLS 1.3, or an abbreviated handshake - i.e. no Certificate message - for TLS 1.2).
//!
//! A connection is considered *resumed* iff the client offered and the server accepted.
//!
//! To route the driver's connections through the proxies we use an [`AddressTranslator`]
//! that maps each node's real address to its proxy address. Because the driver verifies
//! the contact point against the proxy IP, but discovered peers are reached via translated
//! (also proxy) IPs, we generate node certificates with BOTH the node's real IP and its
//! proxy IP in the SAN.
//!
//! [`AddressTranslator`]: scylla::policies::address_translator::AddressTranslator

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use openssl::ssl::{SslContext, SslMethod, SslVerifyMode, SslVersion};
use rcgen::{CertificateParams, CertifiedIssuer, KeyPair, SanType};
use rustls::ClientConfig;
use scylla::client::PoolSize;
use scylla::client::session::TlsContext;
use scylla::client::session_builder::SessionBuilder;
use scylla_proxy::get_exclusive_local_address;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::tls::{build_openssl_ca_store, run_ccm_tls_test};
use crate::ccm::lib::cluster::Cluster;
use crate::ccm::lib::node::Node;
use crate::utils::{check_session_works_and_fully_connected, setup_tracing};

const CQL_PORT: u16 = 9042;

/// TLS protocol version, as observed on the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TlsVersion {
    V1_2,
    V1_3,
}

/// The TLS backend under test.
#[derive(Clone, Copy, Debug)]
enum Backend {
    Rustls,
    OpenSsl,
}

/// Result of inspecting a single TLS handshake as it passed through the proxy.
#[derive(Clone, Copy, Debug, Default)]
struct ConnRecord {
    /// Real IP of the node this connection was forwarded to.
    node_ip: Option<IpAddr>,
    /// Negotiated TLS version, determined from the ServerHello.
    version: Option<TlsVersion>,
    /// Whether the client offered to resume a previous session.
    client_offered_resumption: bool,
    /// Whether the server accepted resumption.
    server_accepted_resumption: bool,
}

impl ConnRecord {
    fn resumed(&self) -> bool {
        self.client_offered_resumption && self.server_accepted_resumption
    }
}

/// Thread-safe collection of per-connection handshake inspection results.
#[derive(Clone)]
struct SharedRecords(Arc<Mutex<Vec<Arc<Mutex<ConnRecord>>>>>);

impl SharedRecords {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    fn push(&self, rec: Arc<Mutex<ConnRecord>>) {
        self.0.lock().unwrap().push(rec);
    }

    fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }

    /// Snapshot of all records recorded since `start` (inclusive).
    fn collect_from(&self, start: usize) -> Vec<ConnRecord> {
        let guard = self.0.lock().unwrap();
        guard[start..].iter().map(|r| *r.lock().unwrap()).collect()
    }
}

// -----------------------------------------------------------------------------
// TLS handshake inspection
// -----------------------------------------------------------------------------

// TLS record content types.
const CONTENT_TYPE_CHANGE_CIPHER_SPEC: u8 = 20;
const CONTENT_TYPE_HANDSHAKE: u8 = 22;

// Handshake message types.
const HS_CLIENT_HELLO: u8 = 1;
const HS_SERVER_HELLO: u8 = 2;
const HS_CERTIFICATE: u8 = 11;

// TLS extension types.
const EXT_SESSION_TICKET: u16 = 35;
const EXT_PRE_SHARED_KEY: u16 = 41;
const EXT_SUPPORTED_VERSIONS: u16 = 43;

#[derive(Clone, Copy)]
enum Direction {
    ClientToServer,
    ServerToClient,
}

/// Information extracted from a ClientHello or ServerHello message.
#[derive(Default)]
struct HelloInfo {
    /// Version selected by the server (`supported_versions` extension in ServerHello).
    selected_version: Option<TlsVersion>,
    /// Whether a non-empty SessionTicket extension is present (used by TLS 1.2 clients
    /// to present a ticket).
    nonempty_session_ticket: bool,
    /// Whether a pre_shared_key extension is present.
    has_psk: bool,
}

/// Parses a ClientHello/ServerHello handshake message body (i.e. the bytes after the
/// 4-byte handshake message header). Best-effort and bounds-checked: on any malformed
/// input it simply returns whatever it managed to parse.
fn parse_hello(body: &[u8], is_server: bool) -> HelloInfo {
    let mut info = HelloInfo::default();

    // legacy_version (2) + random (32)
    if body.len() < 34 {
        return info;
    }
    let mut idx = 34usize;

    // session_id
    if idx >= body.len() {
        return info;
    }
    let sid_len = body[idx] as usize;
    idx += 1 + sid_len;

    if is_server {
        // cipher_suite (2) + compression_method (1)
        idx += 3;
    } else {
        // cipher_suites
        if idx + 2 > body.len() {
            return info;
        }
        let cs_len = u16::from_be_bytes([body[idx], body[idx + 1]]) as usize;
        idx += 2 + cs_len;
        // compression_methods
        if idx >= body.len() {
            return info;
        }
        let comp_len = body[idx] as usize;
        idx += 1 + comp_len;
    }

    // extensions
    if idx + 2 > body.len() {
        return info;
    }
    let ext_total = u16::from_be_bytes([body[idx], body[idx + 1]]) as usize;
    idx += 2;
    let end = (idx + ext_total).min(body.len());

    while idx + 4 <= end {
        let etype = u16::from_be_bytes([body[idx], body[idx + 1]]);
        let elen = u16::from_be_bytes([body[idx + 2], body[idx + 3]]) as usize;
        let dstart = idx + 4;
        let dend = (dstart + elen).min(body.len());
        let data = &body[dstart..dend];

        match etype {
            EXT_SESSION_TICKET => {
                if elen > 0 {
                    info.nonempty_session_ticket = true;
                }
            }
            EXT_PRE_SHARED_KEY => {
                info.has_psk = true;
            }
            EXT_SUPPORTED_VERSIONS if is_server && data.len() >= 2 => {
                // In a ServerHello this carries the single selected version.
                let v = u16::from_be_bytes([data[0], data[1]]);
                info.selected_version = Some(if v == 0x0304 {
                    TlsVersion::V1_3
                } else {
                    TlsVersion::V1_2
                });
            }
            _ => {}
        }

        idx = dstart + elen;
    }

    info
}

/// Incrementally parses one direction of a TLS connection, updating a [`ConnRecord`]
/// with resumption-related observations. Once enough has been observed, it stops parsing.
struct HandshakeParser {
    dir: Direction,
    /// Reassembly buffer for handshake-record payloads.
    buf: Vec<u8>,
    /// Negotiated version, once the ServerHello has been parsed (server direction only).
    version: Option<TlsVersion>,
    /// Whether the ServerHello has been parsed already (server direction only).
    server_hello_parsed: bool,
    /// Whether we are done inspecting this direction.
    done: bool,
}

impl HandshakeParser {
    fn new(dir: Direction) -> Self {
        Self {
            dir,
            buf: Vec::new(),
            version: None,
            server_hello_parsed: false,
            done: false,
        }
    }

    fn feed(&mut self, content_type: u8, body: &[u8], rec: &Arc<Mutex<ConnRecord>>) {
        if self.done {
            return;
        }
        match content_type {
            CONTENT_TYPE_CHANGE_CIPHER_SPEC => {
                // For TLS 1.2, a ChangeCipherSpec arriving from the server before any
                // Certificate message means an abbreviated (resumed) handshake.
                if matches!(self.dir, Direction::ServerToClient)
                    && self.version == Some(TlsVersion::V1_2)
                {
                    rec.lock().unwrap().server_accepted_resumption = true;
                    self.done = true;
                }
            }
            CONTENT_TYPE_HANDSHAKE => {
                self.buf.extend_from_slice(body);
                self.parse_messages(rec);
            }
            _ => {}
        }
    }

    fn parse_messages(&mut self, rec: &Arc<Mutex<ConnRecord>>) {
        loop {
            if self.done || self.buf.len() < 4 {
                return;
            }
            let mlen = ((self.buf[1] as usize) << 16)
                | ((self.buf[2] as usize) << 8)
                | (self.buf[3] as usize);
            if self.buf.len() < 4 + mlen {
                return;
            }
            let mtype = self.buf[0];
            let mbody = self.buf[4..4 + mlen].to_vec();
            self.handle_message(mtype, &mbody, rec);
            self.buf.drain(0..4 + mlen);
        }
    }

    fn handle_message(&mut self, mtype: u8, mbody: &[u8], rec: &Arc<Mutex<ConnRecord>>) {
        match self.dir {
            Direction::ClientToServer => {
                if mtype == HS_CLIENT_HELLO {
                    let info = parse_hello(mbody, false);
                    rec.lock().unwrap().client_offered_resumption =
                        info.nonempty_session_ticket || info.has_psk;
                    self.done = true;
                }
            }
            Direction::ServerToClient => {
                if mtype == HS_SERVER_HELLO {
                    let info = parse_hello(mbody, true);
                    let version = info
                        .selected_version
                        .unwrap_or(TlsVersion::V1_2 /* legacy, either 1.2 or 1.0/1.1 */);
                    self.version = Some(version);
                    {
                        let mut r = rec.lock().unwrap();
                        r.version = Some(version);
                        if version == TlsVersion::V1_3 {
                            r.server_accepted_resumption = info.has_psk;
                        }
                    }
                    if version == TlsVersion::V1_3 {
                        // For TLS 1.3 everything after ServerHello is encrypted; the
                        // ServerHello alone tells us whether resumption was accepted.
                        self.done = true;
                    } else {
                        self.server_hello_parsed = true;
                    }
                } else if self.server_hello_parsed
                    && self.version == Some(TlsVersion::V1_2)
                    && mtype == HS_CERTIFICATE
                {
                    // A Certificate message means a full (non-resumed) TLS 1.2 handshake.
                    rec.lock().unwrap().server_accepted_resumption = false;
                    self.done = true;
                }
            }
        }
    }
}

// -----------------------------------------------------------------------------
// TLS-transparent TCP proxy
// -----------------------------------------------------------------------------

/// Forwards TLS records from `reader` to `writer`, inspecting the handshake as it goes.
async fn forward<R, W>(mut reader: R, mut writer: W, dir: Direction, rec: Arc<Mutex<ConnRecord>>)
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut parser = HandshakeParser::new(dir);
    let mut header = [0u8; 5];
    loop {
        if reader.read_exact(&mut header).await.is_err() {
            break;
        }
        let content_type = header[0];
        let len = u16::from_be_bytes([header[3], header[4]]) as usize;
        let mut body = vec![0u8; len];
        if len > 0 && reader.read_exact(&mut body).await.is_err() {
            break;
        }

        parser.feed(content_type, &body, &rec);

        if writer.write_all(&header).await.is_err()
            || writer.write_all(&body).await.is_err()
            || writer.flush().await.is_err()
        {
            break;
        }
    }
}

/// Accepts connections on `listener`, forwards each to `real_addr`, and records the
/// result of inspecting the TLS handshake into `records`.
async fn run_proxy(
    listener: TcpListener,
    real_addr: SocketAddr,
    node_ip: IpAddr,
    records: SharedRecords,
) {
    loop {
        let client = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(err) => {
                tracing::warn!("TLS ticket proxy accept error: {err}");
                continue;
            }
        };
        let server = match TcpStream::connect(real_addr).await {
            Ok(stream) => stream,
            Err(err) => {
                tracing::warn!("TLS ticket proxy failed to connect to {real_addr}: {err}");
                continue;
            }
        };

        let rec = Arc::new(Mutex::new(ConnRecord {
            node_ip: Some(node_ip),
            ..Default::default()
        }));
        records.push(Arc::clone(&rec));

        let (client_read, client_write) = client.into_split();
        let (server_read, server_write) = server.into_split();

        tokio::spawn(forward(
            client_read,
            server_write,
            Direction::ClientToServer,
            Arc::clone(&rec),
        ));
        tokio::spawn(forward(
            server_read,
            client_write,
            Direction::ServerToClient,
            rec,
        ));
    }
}

// -----------------------------------------------------------------------------
// TLS context construction
// -----------------------------------------------------------------------------

fn make_openssl_context(ca: &CertifiedIssuer<'_, KeyPair>, version: TlsVersion) -> SslContext {
    let mut builder = SslContext::builder(SslMethod::tls()).unwrap();
    builder.set_verify(SslVerifyMode::PEER);
    builder.set_cert_store(build_openssl_ca_store(ca));
    let ssl_version = match version {
        TlsVersion::V1_2 => SslVersion::TLS1_2,
        TlsVersion::V1_3 => SslVersion::TLS1_3,
    };
    builder.set_min_proto_version(Some(ssl_version)).unwrap();
    builder.set_max_proto_version(Some(ssl_version)).unwrap();
    builder.build()
}

fn make_rustls_config(ca: &CertifiedIssuer<'_, KeyPair>, version: TlsVersion) -> Arc<ClientConfig> {
    let mut store = rustls::RootCertStore::empty();
    store.add(ca.der().to_owned()).unwrap();
    let versions: &[&rustls::SupportedProtocolVersion] = match version {
        TlsVersion::V1_2 => &[&rustls::version::TLS12],
        TlsVersion::V1_3 => &[&rustls::version::TLS13],
    };
    let config = ClientConfig::builder_with_protocol_versions(versions)
        .with_root_certificates(store)
        .with_no_client_auth();
    Arc::new(config)
}

// -----------------------------------------------------------------------------
// Sub-test
// -----------------------------------------------------------------------------

/// Runs a single sub-test for the given backend and TLS version against the
/// already-running cluster (fronted by proxies).
///
/// It creates two sessions sharing the same TLS context. The first performs full
/// handshakes; the second, if the backend reuses session tickets, resumes them.
/// The proxy observations are then asserted.
async fn run_subtest(
    backend: Backend,
    version: TlsVersion,
    contact: SocketAddr,
    translation: &HashMap<SocketAddr, SocketAddr>,
    ca: &CertifiedIssuer<'static, KeyPair>,
    records: &SharedRecords,
    node_count: usize,
) {
    tracing::info!("TLS session ticket sub-test: {backend:?} / {version:?}");

    let start_idx = records.len();

    let tls_context: TlsContext = match backend {
        Backend::Rustls => TlsContext::Rustls023(make_rustls_config(ca, version)),
        Backend::OpenSsl => TlsContext::OpenSsl010(make_openssl_context(ca, version)),
    };

    // Session A: establishes the sessions and obtains session tickets.
    let session_a = SessionBuilder::new()
        .known_node_addr(contact)
        .address_translator(Arc::new(translation.clone()))
        .tls_context(Some(tls_context.clone()))
        .disallow_shard_aware_port(true)
        .pool_size(PoolSize::PerHost(NonZeroUsize::new(1).unwrap()))
        .build()
        .await
        .unwrap();
    check_session_works_and_fully_connected(node_count, &session_a).await;

    // Session B: shares the same TLS context, so a ticket-reusing backend will resume.
    let session_b = SessionBuilder::new()
        .known_node_addr(contact)
        .address_translator(Arc::new(translation.clone()))
        .tls_context(Some(tls_context.clone()))
        .disallow_shard_aware_port(true)
        .pool_size(PoolSize::PerHost(NonZeroUsize::new(1).unwrap()))
        .build()
        .await
        .unwrap();
    check_session_works_and_fully_connected(node_count, &session_b).await;

    let observed = records.collect_from(start_idx);
    assert!(
        !observed.is_empty(),
        "{backend:?}/{version:?}: proxy observed no connections"
    );

    // Sanity: every observed connection negotiated the pinned TLS version.
    for rec in &observed {
        assert_eq!(
            rec.version,
            Some(version),
            "{backend:?}/{version:?}: connection negotiated unexpected TLS version {:?}",
            rec.version
        );
    }

    // Aggregate per node: (total connections, resumed connections).
    let mut per_node: HashMap<IpAddr, (usize, usize)> = HashMap::new();
    for rec in &observed {
        let ip = rec.node_ip.expect("proxy always records the node IP");
        let entry = per_node.entry(ip).or_default();
        entry.0 += 1;
        if rec.resumed() {
            entry.1 += 1;
        }
    }

    assert_eq!(
        per_node.len(),
        node_count,
        "{backend:?}/{version:?}: expected connections through all {node_count} proxies, got {}",
        per_node.len()
    );

    match backend {
        Backend::Rustls => {
            // The rustls backend caches session tickets in the (shared) ClientConfig, so
            // once the first connection to a node has obtained a ticket, subsequent
            // connections to that node resume the session.
            for (ip, (total, resumed)) in &per_node {
                assert!(
                    *resumed >= 1,
                    "{backend:?}/{version:?}: node {ip} had no resumed connections (out of {total}); \
                     rustls is expected to reuse session tickets"
                );
            }
        }
        Backend::OpenSsl => {
            // The driver's openssl backend creates a fresh SSL object per connection and
            // does not reuse session tickets, so no connection ever resumes.
            for (ip, (total, resumed)) in &per_node {
                assert_eq!(
                    *resumed, 0,
                    "{backend:?}/{version:?}: node {ip} resumed {resumed} connection(s) (out of \
                     {total}); the driver's openssl backend is expected never to resume"
                );
            }
        }
    }

    drop(session_a);
    drop(session_b);
}

// -----------------------------------------------------------------------------
// Test
// -----------------------------------------------------------------------------

/// Enables session tickets on the server. Scylla does not issue TLS session tickets by
/// default (the `enable_session_tickets` client-encryption option defaults to `false`),
/// so without opting in no backend could ever resume a session. Note that although the
/// option is documented as controlling "TLS 1.3 session tickets", the underlying GnuTLS
/// implementation also issues RFC 5077 tickets for TLS 1.2, so enabling it makes
/// resumption possible for both TLS versions.
async fn enable_session_tickets(mut cluster: Cluster) -> Cluster {
    cluster
        .updateconf([("client_encryption_options.enable_session_tickets", "true")])
        .await
        .unwrap();
    cluster
}

/// Verifies TLS session-ticket resumption behaviour of both TLS backends, for both
/// TLS 1.2 and TLS 1.3.
///
/// All backend/version combinations are exercised against a single CCM cluster, since
/// creating CCM clusters is expensive.
#[tokio::test]
async fn test_tls_session_tickets() {
    setup_tracing();

    // Real node IP -> proxy IP. Populated lazily while preparing certificates (which
    // happens before the cluster starts), then read in the test body to start the
    // proxies and build the address translator.
    let proxy_map: Arc<Mutex<HashMap<IpAddr, IpAddr>>> = Arc::new(Mutex::new(HashMap::new()));

    let prepare_cert = {
        let proxy_map = Arc::clone(&proxy_map);
        move |mut params: CertificateParams, node: &Node| {
            let real_ip = node.broadcast_rpc_address();
            let proxy_ip = *proxy_map
                .lock()
                .unwrap()
                .entry(real_ip)
                .or_insert_with(get_exclusive_local_address);
            // The driver verifies the contact point against the proxy IP, and discovered
            // peers against their (translated, also proxy) IPs; include the real IP too so
            // the same cert works regardless of which address is used for verification.
            params.subject_alt_names.push(SanType::IpAddress(real_ip));
            params.subject_alt_names.push(SanType::IpAddress(proxy_ip));
            params
        }
    };

    let test = {
        let proxy_map = Arc::clone(&proxy_map);
        async move |ca: &CertifiedIssuer<'static, KeyPair>, cluster: &mut Cluster| {
            let node_count = cluster.nodes().len();
            let real_ips: Vec<IpAddr> = cluster
                .nodes()
                .iter()
                .map(|node| node.broadcast_rpc_address())
                .collect();

            let map = proxy_map.lock().unwrap().clone();
            let records = SharedRecords::new();

            // Start a proxy in front of every node.
            let mut translation: HashMap<SocketAddr, SocketAddr> = HashMap::new();
            let mut proxy_tasks = Vec::new();
            for (&real_ip, &proxy_ip) in &map {
                let real_addr = SocketAddr::new(real_ip, CQL_PORT);
                let proxy_addr = SocketAddr::new(proxy_ip, CQL_PORT);
                let listener = TcpListener::bind(proxy_addr).await.unwrap();
                translation.insert(real_addr, proxy_addr);
                proxy_tasks.push(tokio::spawn(run_proxy(
                    listener,
                    real_addr,
                    real_ip,
                    records.clone(),
                )));
            }

            // Use the first node's proxy as the contact point.
            let contact = SocketAddr::new(map[&real_ips[0]], CQL_PORT);

            for (backend, version) in [
                (Backend::Rustls, TlsVersion::V1_2),
                (Backend::Rustls, TlsVersion::V1_3),
                (Backend::OpenSsl, TlsVersion::V1_2),
                (Backend::OpenSsl, TlsVersion::V1_3),
            ] {
                run_subtest(
                    backend,
                    version,
                    contact,
                    &translation,
                    ca,
                    &records,
                    node_count,
                )
                .await;
            }

            for task in proxy_tasks {
                task.abort();
            }
        }
    };

    run_ccm_tls_test(prepare_cert, enable_session_tickets, test).await
}
