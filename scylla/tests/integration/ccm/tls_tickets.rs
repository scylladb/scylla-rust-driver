//! Tests verifying TLS session resumption (via TLS session tickets / PSK) behaviour
//! of the driver's two TLS backends.
//!
//! Expectation being verified:
//! - the rustls backend reuses session tickets; TLS 1.2 pools resume every connection after
//!   the first, while TLS 1.3 pools resume at least two connections per node,
//! - the openssl backend (as used by the driver) does NOT reuse session tickets, so every
//!   connection performs a full handshake.
//!
//! This is verified for both TLS 1.2 and TLS 1.3.
//!
//! # How it works
//!
//! One session per sub-test, with a pool of [`POOL_SIZE`] connections per node.
//!
//! Every node is fronted by a TLS-transparent TCP proxy (see
//! [`super::tls_inspecting_proxy`]) that forwards bytes without terminating TLS and
//! passively classifies every handshake it carries: which TLS version the server
//! selected, whether the client offered to resume an earlier session, and whether the
//! server accepted. A connection counts as *resumed* iff the client offered and the
//! server accepted.
//!
//! How many of a pool's connections can resume is not the same for the two TLS versions,
//! and the reason is worth knowing before touching the numbers here:
//! see [`min_resumptions_per_node`].
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
use std::time::Duration;

use openssl::ssl::{SslContext, SslMethod, SslVerifyMode, SslVersion};
use rcgen::{CertificateParams, CertifiedIssuer, KeyPair, SanType};
use rustls::ClientConfig;
use scylla::client::PoolSize;
use scylla::client::session::TlsContext;
use scylla::client::session_builder::SessionBuilder;
use scylla_proxy::get_exclusive_local_address;
use tokio::net::TcpListener;

use super::tls::{build_openssl_ca_store, run_ccm_tls_test};
use super::tls_inspecting_proxy::{ConnRecord, Handshake, SharedRecords, TlsVersion, run_proxy};
use crate::utils::setup_tracing;
use scylla_ccm_bridge::cluster::Cluster;
use scylla_ccm_bridge::node::Node;

const CQL_PORT: u16 = 9042;

/// The TLS backend under test.
#[derive(Clone, Copy, Debug)]
enum Backend {
    Rustls023,
    OpenSsl010,
}

impl Backend {
    /// Whether the driver reuses TLS session tickets on this backend.
    fn reuses_session_tickets(self) -> bool {
        match self {
            Backend::Rustls023 => true,
            // The driver builds a fresh `Ssl` from the `SslContext` per connection and
            // never calls `SSL_set_session`.
            Backend::OpenSsl010 => false,
        }
    }
}

/// The fewest resumptions a pool of `pool_size` connections to one node must show, for a
/// backend that reuses tickets.
///
/// The driver fills a pool by opening one connection while the pool is empty and only
/// then the rest, concurrently (`connection_pool.rs`, `start_filling`). So the first
/// connection has always completed - and banked whatever tickets the server issued -
/// before its siblings start.
///
/// From there the two TLS versions differ, and not by a little:
///
/// - In TLS 1.2 an RFC 5077 ticket is *reusable*: nothing in that RFC says otherwise, so
///   the single ticket banked by the first connection serves every remaining one and all
///   but the first resume.
/// - In TLS 1.3 a ticket is spent when it is used, so the concurrent siblings can only
///   resume as many times as there are tickets already banked. ScyllaDB issues two per
///   handshake. ScyllaDB also ignores the RFC 9149 `ticket_request` extension that allows
///   clients to request specific amount of tickets.
///
/// The single-use rule is *not* an anti-replay measure - RFC 8446 section 8.1
/// ("Single-Use Tickets") is about a **server** optionally spending each ticket once to
/// blunt 0-RTT replay, and asks nothing of clients. The client-side rule is a privacy one,
/// RFC 8446 appendix C.4 ("Client Tracking Prevention"):
///
/// > Clients SHOULD NOT reuse a ticket for multiple connections. Reuse of a ticket allows
/// > passive observers to correlate different connections.
///
/// Reuse is linkable because a resumed ClientHello carries the ticket as its
/// `PskIdentity`; section 4.2.11.1 notes that `obfuscated_ticket_age` hides the
/// correlation only "unless tickets are reused". rustls encodes the rule in the shape of
/// its store API rather than citing the RFC: TLS 1.2 gets a plain getter
/// (`ClientSessionStore::tls12_session`), TLS 1.3 a taker whose contract is that
/// implementations "must return each value [...] _at most once_"
/// (`ClientSessionStore::take_tls13_ticket`).
///
/// Worth noting which side is actually falling short of C.4 here. It also says servers
/// "SHOULD offer at least as many tickets as the number of connections that a client
/// might use" and "SHOULD issue new tickets with every connection" - so a pool of
/// [`POOL_SIZE`] wanting [`POOL_SIZE`] tickets is the behaviour the RFC anticipates, and
/// two-per-handshake is the server declining to supply it. rustls is doing exactly as it
/// should; the floor below is a server limitation, not a client one.
///
/// OTOH Rustls has a hardcoded limitation of only storing 8 tickets per host.
/// This means even if server provides ticket count equal to shard count, on big deployments
/// most of those tickets would be dropped by rustls.
const fn min_resumptions_per_node(version: TlsVersion, pool_size: usize) -> usize {
    match version {
        TlsVersion::V1_2 => pool_size - 1,
        TlsVersion::V1_3 => TLS13_TICKETS_PER_HANDSHAKE,
    }
}

/// Session tickets ScyllaDB issues per TLS 1.3 handshake. Fewer than a pool needs; see
/// [`min_resumptions_per_node`].
///
/// It is not a ScyllaDB setting, and there is no server-side knob for it. ScyllaDB only
/// passes a boolean down (`db/config.cc` turns `enable_session_tickets` into
/// `seastar::tls::session_resume_mode::TLS13_SESSION_TICKET`), and Seastar in turn only
/// calls `gnutls_session_ticket_enable_server` - never `gnutls_session_ticket_send(nr)`,
/// which is the API that would set a count. Two is also what rustls's *server* side and
/// OpenSSL default to, so it is the cross-implementation norm rather than a ScyllaDB
/// quirk.
///
/// GnuTLS raised it from one to two in 2019 (commit `c6754cf52`, "handshake: increase the
/// default number of tickets we send to 2"), reasoning:
///
/// > This makes it easier for clients which perform multiple connections to the server to
/// > use the tickets sent by a default server. That's because 2 tickets allow for 2 new
/// > connections (if one is using each ticket once as recommended), which in turn lead to
/// > 4 new and so on.
///
/// That compounding argument assumes connections are opened *one after another*, each
/// banking its own two tickets before the next needs one. A connection pool defeats it:
/// the driver opens the first connection alone and then the rest at once, so every
/// sibling draws on the same two tickets the first one banked. The doubling never gets a
/// round to happen, which is why the floor here is two and not `pool_size - 1`.
///
/// Relevant server issue: https://scylladb.atlassian.net/browse/SCYLLADB-3949
const TLS13_TICKETS_PER_HANDSHAKE: usize = 2;

// -----------------------------------------------------------------------------
// TLS context construction
// -----------------------------------------------------------------------------

fn make_openssl_010_context(ca: &CertifiedIssuer<'_, KeyPair>, version: TlsVersion) -> SslContext {
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

fn make_rustls_023_config(
    ca: &CertifiedIssuer<'_, KeyPair>,
    version: TlsVersion,
) -> Arc<ClientConfig> {
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

/// Connections the driver is told to keep in each node's pool. Larger than one so that a
/// single session, on its own, has to open several connections per node: the first must
/// perform a full handshake and the rest can resume it. That is a property of one pool
/// rather than of two sessions sharing a TLS context, which is what makes it meaningful
/// for any backend that caches tickets, however it scopes the cache.
const POOL_SIZE: usize = 5;

/// Upper bound for [`wait_for_handshakes`]. Generous, because it is only ever reached
/// when the driver fails to fill its pools at all.
const POOL_FILL_TIMEOUT: Duration = Duration::from_secs(20);

/// Builds a session that reaches the cluster through the proxies.
async fn connect(
    contact: SocketAddr,
    translation: &HashMap<SocketAddr, SocketAddr>,
    tls_context: TlsContext,
) -> scylla::client::session::Session {
    SessionBuilder::new()
        .known_node_addr(contact)
        .address_translator(Arc::new(translation.clone()))
        .tls_context(Some(tls_context))
        .disallow_shard_aware_port(true)
        .pool_size(PoolSize::PerHost(NonZeroUsize::new(POOL_SIZE).unwrap()))
        .build()
        .await
        .unwrap()
}

/// Waits until the proxies have observed at least `expected` connections, all of them
/// fully classified.
///
/// Pool filling is asynchronous and the driver exposes no completion signal for it:
/// `Node::is_connected()` only means the pool holds *one* connection, so waiting on that
/// would race the remaining [`POOL_SIZE`] - 1 handshakes. The proxies see every
/// connection, so they are the thing to wait on.
async fn wait_for_handshakes(
    records: &SharedRecords,
    first: usize,
    expected: usize,
    case: &str,
) -> Vec<Handshake> {
    let wait = async {
        loop {
            // Counting costs a single lock, whereas snapshotting locks every record it
            // copies - and until enough connections have even been accepted, there is
            // nothing a snapshot could tell us. So poll on the count and take at most one
            // snapshot per attempt that could actually succeed.
            if records.count_from(first) >= expected {
                let observed = records.collect_from(first);
                let classified: Vec<Handshake> =
                    observed.iter().filter_map(ConnRecord::finish).collect();
                if classified.len() == observed.len() {
                    return classified;
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::time::timeout(POOL_FILL_TIMEOUT, wait)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{case}: timed out waiting for {expected} fully classified handshake(s); \
                 the proxies saw {:#?}",
                records.collect_from(first)
            )
        })
}

/// Runs a single sub-test for the given backend and TLS version against the
/// already-running cluster (fronted by proxies).
async fn run_subtest(
    backend: Backend,
    version: TlsVersion,
    contact: SocketAddr,
    translation: &HashMap<SocketAddr, SocketAddr>,
    ca: &CertifiedIssuer<'static, KeyPair>,
    records: &SharedRecords,
    node_count: usize,
) {
    let case = format!("{backend:?}/{version:?}");
    tracing::info!("TLS session ticket sub-test: {case}");

    // A fresh TLS context per sub-test, hence a fresh (empty) session cache.
    let tls_context: TlsContext = match backend {
        Backend::Rustls023 => TlsContext::Rustls023(make_rustls_023_config(ca, version)),
        Backend::OpenSsl010 => TlsContext::OpenSsl010(make_openssl_010_context(ca, version)),
    };

    // One pool of POOL_SIZE connections per node, plus the single control connection,
    // which is not part of any pool.
    let expected = node_count * POOL_SIZE + 1;

    let first = records.len();
    // Kept alive until the end of the sub-test: dropping it would close its connections
    // and could make the driver open fresh ones mid-assertion.
    let _session = connect(contact, translation, tls_context).await;
    let handshakes = wait_for_handshakes(records, first, expected, &case).await;

    tracing::debug!("{case}: {} handshake(s): {handshakes:#?}", handshakes.len());

    // The count is deterministic: `PerHost(n)` targets exactly n connections (it is not
    // scaled per shard) and keeps no excess ones, so anything else means a connection was
    // re-established behind our back and the per-node conclusions below would be drawn
    // from a shifted picture.
    assert_eq!(
        handshakes.len(),
        expected,
        "{case}: expected {expected} connections ({node_count} x {POOL_SIZE} pooled + 1 \
         control), the proxies saw {}: {handshakes:#?}",
        handshakes.len()
    );

    // The pinned version must be the one actually negotiated - otherwise the sub-test
    // would be silently checking resumption for some other TLS version.
    for hs in &handshakes {
        assert_eq!(
            hs.version,
            version.wire(),
            "{case}: connection to node {} negotiated TLS version {}, expected {}",
            hs.node_ip,
            hs.version,
            version.wire()
        );
    }

    // Grouped per node, in accept order.
    let mut per_node: HashMap<IpAddr, Vec<Handshake>> = HashMap::new();
    for hs in &handshakes {
        per_node.entry(hs.node_ip).or_default().push(*hs);
    }
    assert_eq!(
        per_node.len(),
        node_count,
        "{case}: expected connections to all {node_count} nodes, saw {:?}",
        per_node.keys()
    );

    for (ip, conns) in &per_node {
        // Every node carries a full pool; one of them also carries the control
        // connection, which belongs to no pool.
        assert!(
            conns.len() >= POOL_SIZE,
            "{case}: node {ip} got {} connection(s), expected at least the pool's \
             {POOL_SIZE}: {conns:#?}",
            conns.len()
        );

        // The first connection to a node cannot resume: the TLS context, and with it the
        // session cache, is created fresh for each sub-test. This is what proves the
        // checks below can tell the two outcomes apart at all.
        assert!(
            !conns[0].resumed(),
            "{case}: the first connection to node {ip} resumed a session, which is \
             impossible with a freshly created TLS context: {:?}",
            conns[0]
        );

        let resumed: Vec<&Handshake> = conns.iter().filter(|hs| hs.resumed()).collect();
        if backend.reuses_session_tickets() {
            let least = min_resumptions_per_node(version, conns.len());
            assert!(
                resumed.len() >= least,
                "{case}: node {ip} resumed {} of {} connection(s), expected at least \
                 {least}; the pool is expected to reuse the ticket(s) banked by its first \
                 connection: {conns:#?}",
                resumed.len(),
                conns.len()
            );
            // And by presenting a *ticket* - not, say, by falling back to a cached TLS 1.2
            // session id, which the server may accept for reasons of its own.
            for hs in resumed {
                assert!(
                    hs.presented_ticket,
                    "{case}: connection to node {ip} resumed without presenting a session \
                     ticket, so this says nothing about ticket reuse: {hs:?}"
                );
            }
        } else {
            assert!(
                resumed.is_empty(),
                "{case}: node {ip} resumed {} connection(s); this backend is expected \
                 never to reuse a session: {conns:#?}",
                resumed.len()
            );
            for hs in conns {
                assert!(
                    !hs.offered_resumption,
                    "{case}: connection to node {ip} offered resumption; this backend is \
                     expected never to present a cached session: {hs:?}"
                );
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Test
// -----------------------------------------------------------------------------

/// Enables session tickets on the server. Scylla does not issue TLS session tickets by
/// default (the `enable_session_tickets` client-encryption option defaults to `false`),
/// so without opting in no backend could ever resume a session.
///
/// The option is documented as controlling "TLS 1.3 session tickets", but the underlying
/// GnuTLS implementation also issues RFC 5077 tickets for TLS 1.2 - so it is what makes
/// resumption possible for *both* TLS versions here. Flipping it back to `false` makes
/// the TLS 1.2 sub-test fail too, not just the TLS 1.3 one.
async fn enable_session_tickets(mut cluster: Cluster) -> Cluster {
    cluster
        .updateconf([("client_encryption_options.enable_session_tickets", "true")])
        .await
        .unwrap();
    cluster
}

/// Verifies TLS session-ticket resumption behaviour of both TLS backends, for both
/// TLS 1.2 and TLS 1.3.
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

            // A proxy in front of every node, all served by this task: nothing is
            // spawned, so no proxy - and no connection it carries - can outlive the
            // sub-tests below.
            let mut translation: HashMap<SocketAddr, SocketAddr> = HashMap::new();
            let mut proxies = Vec::new();
            for (&real_ip, &proxy_ip) in &map {
                let real_addr = SocketAddr::new(real_ip, CQL_PORT);
                let proxy_addr = SocketAddr::new(proxy_ip, CQL_PORT);
                let listener = TcpListener::bind(proxy_addr).await.unwrap();
                translation.insert(real_addr, proxy_addr);
                proxies.push(run_proxy(listener, real_addr, real_ip, records.clone()));
            }

            // Use the first node's proxy as the contact point.
            let contact = SocketAddr::new(map[&real_ips[0]], CQL_PORT);

            let subtests = async {
                for (backend, version) in [
                    (Backend::Rustls023, TlsVersion::V1_2),
                    (Backend::Rustls023, TlsVersion::V1_3),
                    (Backend::OpenSsl010, TlsVersion::V1_2),
                    (Backend::OpenSsl010, TlsVersion::V1_3),
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
            };

            // The proxies never finish on their own, so this runs the sub-tests with the
            // proxies serving, then drops the proxies the moment the sub-tests are done.
            tokio::select! {
                () = subtests => {}
                _ = futures::future::join_all(proxies) => {
                    unreachable!("a proxy only stops when dropped")
                }
            }
        }
    };

    run_ccm_tls_test(prepare_cert, enable_session_tickets, test).await
}
