//! A TLS-transparent TCP proxy that classifies the handshakes passing through it, used by
//! [`super::tls_tickets`] to observe whether the driver's TLS backends resume TLS sessions.
//!
//! It does not terminate TLS: it forwards every byte verbatim, reads only what is in the
//! clear, and stops inspecting a connection once it has determined
//! - the TLS version the server selected,
//! - whether the client offered resumption: `pre_shared_key` in TLS 1.3, a non-empty
//!   `session_ticket` extension or legacy `session_id` in TLS 1.2,
//! - whether the server accepted it: `pre_shared_key` in the ServerHello in TLS 1.3, an
//!   abbreviated first flight (ChangeCipherSpec with no Certificate before it) in TLS 1.2.
//!
//! Messages and extensions are parsed by `tls-parser`. Reassembly across records, message
//! dispatch and the ordering that the TLS 1.2 signal needs are done here.

use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};

use futures::stream::{FuturesUnordered, StreamExt};
use tls_parser::nom::IResult;
use tls_parser::nom::bytes::streaming::take;
use tls_parser::nom::number::streaming::{be_u8, be_u24};
use tls_parser::{
    TlsExtension, TlsHandshakeType, TlsRecordType, TlsVersion as WireVersion,
    parse_tls_client_hello_extensions, parse_tls_handshake_client_hello,
    parse_tls_handshake_server_hello, parse_tls_record_header, parse_tls_server_hello_extensions,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A TLS version a test pins.
#[derive(Clone, Copy, Debug)]
pub(super) enum TlsVersion {
    V1_2,
    V1_3,
}

impl TlsVersion {
    /// Its on-the-wire value.
    pub(super) const fn wire(self) -> WireVersion {
        match self {
            TlsVersion::V1_2 => WireVersion::Tls12,
            TlsVersion::V1_3 => WireVersion::Tls13,
        }
    }
}

// -----------------------------------------------------------------------------
// Handshake observations
// -----------------------------------------------------------------------------

/// What the proxy saw in a connection's ClientHello.
#[derive(Clone, Copy, Debug)]
struct ClientHelloInfo {
    /// A non-empty `session_ticket` extension: an RFC 5077 ticket. Clients that merely
    /// *support* tickets send the extension empty, which does not count.
    presents_ticket: bool,
    /// A `pre_shared_key` extension: a TLS 1.3 PSK, which here can only be a ticket.
    presents_psk: bool,
    /// A non-empty legacy `session_id`. Only means anything in TLS 1.2: a TLS 1.3 client
    /// always sends a random one, for middlebox compatibility.
    nonempty_session_id: bool,
}

/// What the proxy saw in a connection's ServerHello.
#[derive(Clone, Copy, Debug)]
struct ServerHelloInfo {
    /// The version the server selected.
    version: WireVersion,
    /// A `pre_shared_key` extension: the server accepted TLS 1.3 resumption.
    accepts_psk: bool,
}

/// Everything the proxy observed about a single connection's handshake.
#[derive(Clone, Copy, Debug)]
pub(super) struct ConnRecord {
    /// Real IP of the node this connection was forwarded to.
    node_ip: IpAddr,
    client_hello: Option<ClientHelloInfo>,
    server_hello: Option<ServerHelloInfo>,
    /// Pre-1.3: whether the server sent ChangeCipherSpec with no preceding Certificate,
    /// which is how it signals that it accepted resumption. `None` in TLS 1.3, where the
    /// flight after the ServerHello is encrypted - and where the ServerHello already says.
    abbreviated_flight: Option<bool>,
}

/// A fully classified handshake: what the proxy *concluded* about one connection.
#[derive(Clone, Copy, Debug)]
pub(super) struct Handshake {
    pub(super) node_ip: IpAddr,
    /// The TLS version the server selected.
    pub(super) version: WireVersion,
    /// Whether the client presented a *session ticket*: a non-empty `session_ticket`
    /// extension in TLS 1.2, a PSK in TLS 1.3. The mechanism under test.
    pub(super) presented_ticket: bool,
    /// Whether the client offered to resume by any means. Weaker than
    /// [`Handshake::presented_ticket`]: a TLS 1.2 client may offer a cached session id
    /// instead, which is what rustls does when it holds no ticket.
    pub(super) offered_resumption: bool,
    /// Whether the server accepted resumption.
    pub(super) accepted_resumption: bool,
}

impl ConnRecord {
    fn new(node_ip: IpAddr) -> Self {
        Self {
            node_ip,
            client_hello: None,
            server_hello: None,
            abbreviated_flight: None,
        }
    }

    /// Interprets the observations, or `None` if the handshake was not observed in full.
    /// The test asserts only about complete ones, and separately requires that every
    /// connection got there - a missed handshake must not pass as "nothing resumed".
    pub(super) fn finish(&self) -> Option<Handshake> {
        let client = self.client_hello?;
        let server = self.server_hello?;
        let (presented_ticket, offered_resumption, accepted_resumption) =
            if server.version == TlsVersion::V1_3.wire() {
                // In TLS 1.3 resumption is always PSK-based, and the only PSKs in play
                // here come from tickets.
                (client.presents_psk, client.presents_psk, server.accepts_psk)
            } else {
                // TLS 1.2 and earlier: the client offers a ticket and/or a cached session
                // id, and the server signals acceptance by abbreviating its first flight.
                (
                    client.presents_ticket,
                    client.presents_ticket || client.nonempty_session_id,
                    self.abbreviated_flight?,
                )
            };
        Some(Handshake {
            node_ip: self.node_ip,
            version: server.version,
            presented_ticket,
            offered_resumption,
            accepted_resumption,
        })
    }
}

impl Handshake {
    pub(super) fn resumed(&self) -> bool {
        self.offered_resumption && self.accepted_resumption
    }
}

/// Thread-safe log of per-connection handshake observations, in the order the proxies
/// accepted the connections.
#[derive(Clone)]
pub(super) struct SharedRecords(Arc<Mutex<Vec<Arc<Mutex<ConnRecord>>>>>);

impl SharedRecords {
    pub(super) fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    fn push(&self, rec: Arc<Mutex<ConnRecord>>) {
        self.0.lock().unwrap().push(rec);
    }

    pub(super) fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }

    /// How many records have been logged since `start` (inclusive). Cheaper than
    /// [`SharedRecords::collect_from`], which locks every record it copies.
    pub(super) fn count_from(&self, start: usize) -> usize {
        self.0.lock().unwrap().len() - start
    }

    /// Snapshot of all records logged since `start` (inclusive).
    pub(super) fn collect_from(&self, start: usize) -> Vec<ConnRecord> {
        let guard = self.0.lock().unwrap();
        guard[start..].iter().map(|r| *r.lock().unwrap()).collect()
    }
}

// -----------------------------------------------------------------------------
// TLS handshake inspection
// -----------------------------------------------------------------------------

/// `ContentType` + `ProtocolVersion` + length.
const RECORD_HEADER_LEN: usize = 5;

#[derive(Clone, Copy)]
enum Direction {
    ClientToServer,
    ServerToClient,
}

/// The `random` a TLS 1.3 server puts in a ServerHello to mark it as a
/// HelloRetryRequest: SHA-256 of the string "HelloRetryRequest" (RFC 8446 section 4.1.3).
const HELLO_RETRY_REQUEST_RANDOM: [u8; 32] = [
    0xcf, 0x21, 0xad, 0x74, 0xe5, 0x9a, 0x61, 0x11, 0xbe, 0x1d, 0x8c, 0x02, 0x1e, 0x65, 0xb8, 0x91,
    0xc2, 0xa2, 0x11, 0x16, 0x7a, 0xbb, 0x8c, 0x5e, 0x07, 0x9e, 0x09, 0xe2, 0xc8, 0xa8, 0x33, 0x9c,
];

/// Fields of interest of a ClientHello or ServerHello.
struct Hello {
    /// The version the message names: a ServerHello's `supported_versions` extension if
    /// it has one (that is how TLS 1.3 is signalled), otherwise the legacy version field.
    version: WireVersion,
    nonempty_session_id: bool,
    nonempty_session_ticket: bool,
    has_psk: bool,
    /// Whether this "ServerHello" is really a HelloRetryRequest, told apart only by its
    /// `random`.
    is_hello_retry_request: bool,
}

/// Cuts one handshake message - its type and body - off the front of `input`. Built from
/// the *streaming* nom parsers, so a message that has not fully arrived fails with
/// `Incomplete` rather than as malformed.
fn parse_handshake_message(input: &[u8]) -> IResult<&[u8], (TlsHandshakeType, &[u8])> {
    let (input, msg_type) = be_u8(input)?;
    let (input, len) = be_u24(input)?;
    let (input, body) = take(len)(input)?;
    Ok((input, (TlsHandshakeType(msg_type), body)))
}

/// Parses a ClientHello/ServerHello message body. `None` if it is truncated or malformed,
/// never a partly populated result: the caller must not read a failed parse as "the
/// client did not offer resumption".
fn parse_hello(body: &[u8], is_server: bool) -> Option<Hello> {
    // A Hello that leaves part of its body unconsumed is malformed: that is how an
    // extensions block whose length prefix overruns the message shows up.
    let (version, random, session_id, block) = if is_server {
        let (rest, h) = parse_tls_handshake_server_hello(body).ok()?;
        rest.is_empty()
            .then_some((h.version, h.random, h.session_id, h.ext))?
    } else {
        let (rest, h) = parse_tls_handshake_client_hello(body).ok()?;
        rest.is_empty()
            .then_some((h.version, h.random, h.session_id, h.ext))?
    };

    let mut hello = Hello {
        version,
        nonempty_session_id: session_id.is_some_and(|id| !id.is_empty()),
        nonempty_session_ticket: false,
        has_psk: false,
        is_hello_retry_request: is_server && random == HELLO_RETRY_REQUEST_RANDOM.as_slice(),
    };

    // An extension the crate cannot parse silently stops the walk, so leftover bytes fail
    // the Hello too: the resumption signal may be the extension behind it.
    let (rest, extensions) = match block {
        Some(block) if is_server => parse_tls_server_hello_extensions(block).ok()?,
        Some(block) => parse_tls_client_hello_extensions(block).ok()?,
        None => (&[][..], Vec::new()),
    };
    if !rest.is_empty() {
        return None;
    }
    for extension in extensions {
        match extension {
            TlsExtension::SessionTicket(data) => hello.nonempty_session_ticket = !data.is_empty(),
            TlsExtension::PreSharedKey(_) => hello.has_psk = true,
            // In a ServerHello this extension carries the single selected version.
            TlsExtension::SupportedVersions(v) if is_server && v.len() == 1 => hello.version = v[0],
            _ => {}
        }
    }

    Some(hello)
}

/// Classifies one direction of a connection into the shared [`ConnRecord`], stopping as
/// soon as it has seen enough.
struct HandshakeParser {
    dir: Direction,
    /// Reassembly buffer: a message may span records, a record may carry several messages.
    buf: Vec<u8>,
    /// The version from the ServerHello, once seen. Server direction only.
    selected_version: Option<WireVersion>,
    done: bool,
}

impl HandshakeParser {
    fn new(dir: Direction) -> Self {
        Self {
            dir,
            buf: Vec::new(),
            selected_version: None,
            done: false,
        }
    }

    /// Whether this direction has been classified.
    fn done(&self) -> bool {
        self.done
    }

    /// Whether the server picked TLS 1.2 or older, where resumption is visible from the
    /// shape of its first flight.
    fn is_pre_tls13(&self) -> bool {
        self.selected_version
            .is_some_and(|version| version != TlsVersion::V1_3.wire())
    }

    fn feed(&mut self, record_type: TlsRecordType, body: &[u8], rec: &Mutex<ConnRecord>) {
        if self.done {
            return;
        }
        match record_type {
            TlsRecordType::Handshake => {
                self.buf.extend_from_slice(body);
                self.parse_messages(rec);
            }
            // No Certificate before it, so the flight was abbreviated - i.e. resumed.
            TlsRecordType::ChangeCipherSpec
                if matches!(self.dir, Direction::ServerToClient) && self.is_pre_tls13() =>
            {
                rec.lock().unwrap().abbreviated_flight = Some(true);
                self.done = true;
            }
            _ => {}
        }
    }

    fn parse_messages(&mut self, rec: &Mutex<ConnRecord>) {
        // `handle_message` needs `&mut self`, so the buffer is moved out and the
        // unconsumed remainder moved back afterwards.
        let mut buf = std::mem::take(&mut self.buf);
        let mut rest: &[u8] = &buf;
        while !self.done {
            // Only `Incomplete` is reachable: a fixed-size header plus a body taken at
            // its declared length cannot be malformed, only unfinished.
            let Ok((tail, (msg_type, body))) = parse_handshake_message(rest) else {
                break;
            };
            self.handle_message(msg_type, body, rec);
            rest = tail;
        }
        let consumed = buf.len() - rest.len();
        buf.drain(..consumed);
        self.buf = buf;
    }

    fn handle_message(&mut self, msg_type: TlsHandshakeType, body: &[u8], rec: &Mutex<ConnRecord>) {
        match self.dir {
            Direction::ClientToServer => {
                if msg_type != TlsHandshakeType::ClientHello {
                    return;
                }
                // What follows is either irrelevant or (in TLS 1.3) encrypted.
                self.done = true;
                if let Some(hello) = parse_hello(body, false) {
                    rec.lock().unwrap().client_hello = Some(ClientHelloInfo {
                        presents_ticket: hello.nonempty_session_ticket,
                        presents_psk: hello.has_psk,
                        nonempty_session_id: hello.nonempty_session_id,
                    });
                }
            }
            Direction::ServerToClient => match msg_type {
                TlsHandshakeType::ServerHello => {
                    let Some(hello) = parse_hello(body, true) else {
                        self.done = true;
                        return;
                    };
                    // A HelloRetryRequest shares the ServerHello encoding but never
                    // carries `pre_shared_key`, so taking it for the ServerHello would
                    // report a resumed session as refused. The real one follows it in the
                    // clear, and the PSK offer was read from the first ClientHello.
                    if hello.is_hello_retry_request {
                        return;
                    }
                    self.selected_version = Some(hello.version);
                    rec.lock().unwrap().server_hello = Some(ServerHelloInfo {
                        version: hello.version,
                        accepts_psk: hello.has_psk,
                    });
                    // In TLS 1.3 everything after the ServerHello is encrypted.
                    self.done = !self.is_pre_tls13();
                }
                // A Certificate means a full handshake, so resumption was not accepted.
                // In TLS 1.3 the message is encrypted, hence the version guard.
                TlsHandshakeType::Certificate if self.is_pre_tls13() => {
                    rec.lock().unwrap().abbreviated_flight = Some(false);
                    self.done = true;
                }
                _ => {}
            },
        }
    }
}

// -----------------------------------------------------------------------------
// Forwarding
// -----------------------------------------------------------------------------

/// Forwards one direction of a connection, inspecting the handshake as it goes and then
/// copying the rest - all the CQL traffic - as an opaque byte stream.
async fn forward<R, W>(mut reader: R, mut writer: W, dir: Direction, rec: &Mutex<ConnRecord>)
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut parser = HandshakeParser::new(dir);
    let mut fragment = Vec::new();
    while !parser.done() {
        let mut header_bytes = [0u8; RECORD_HEADER_LEN];
        if reader.read_exact(&mut header_bytes).await.is_err() {
            return;
        }
        // Infallible: five fixed-size bytes, all of them in hand.
        let (_, header) = parse_tls_record_header(&header_bytes).unwrap();
        fragment.clear();
        fragment.resize(header.len as usize, 0);
        if reader.read_exact(&mut fragment).await.is_err() {
            return;
        }

        parser.feed(header.record_type, &fragment, rec);

        if writer.write_all(&header_bytes).await.is_err()
            || writer.write_all(&fragment).await.is_err()
            || writer.flush().await.is_err()
        {
            return;
        }
    }
    let _ = tokio::io::copy(&mut reader, &mut writer).await;
}

/// Carries one proxied connection until both directions are done. One future drives both,
/// so the connection has a single owner and nothing is detached.
async fn proxy_connection(client: TcpStream, server: TcpStream, rec: &Mutex<ConnRecord>) {
    let (client_read, client_write) = client.into_split();
    let (server_read, server_write) = server.into_split();
    tokio::join!(
        forward(client_read, server_write, Direction::ClientToServer, rec),
        forward(server_read, client_write, Direction::ServerToClient, rec),
    );
}

/// Accepts connections on `listener`, forwards each to `real_addr`, and logs what their
/// handshakes revealed into `records`.
///
/// Never returns. Nothing is spawned, so dropping it closes the listener and every
/// connection it carries, and a panic while forwarding surfaces in whoever polls it.
/// Accepting is serialised with dialling the node so that `records` is in accept order,
/// which the test slices by index to tell one session's connections from another's.
pub(super) async fn run_proxy(
    listener: TcpListener,
    real_addr: SocketAddr,
    node_ip: IpAddr,
    records: SharedRecords,
) {
    // Owns every connection accepted so far; completed ones are reaped as it is polled.
    let mut connections = FuturesUnordered::new();
    loop {
        tokio::select! {
            // Keep the connections already accepted moving.
            Some(()) = connections.next() => {}
            accepted = listener.accept() => {
                let client = match accepted {
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

                let rec = Arc::new(Mutex::new(ConnRecord::new(node_ip)));
                records.push(Arc::clone(&rec));
                connections.push(async move { proxy_connection(client, server, &rec).await });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tls_parser::{TlsExtensionType as Ext, TlsHandshakeType as Hs};

    use super::*;

    const NODE_IP: IpAddr = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);

    /// Wraps a handshake message body in its `HandshakeType` + 24-bit length header.
    fn handshake_msg(msg_type: Hs, body: &[u8]) -> Vec<u8> {
        let len = body.len();
        let mut out = vec![msg_type.0, (len >> 16) as u8, (len >> 8) as u8, len as u8];
        out.extend_from_slice(body);
        out
    }

    fn extension(ext_type: Ext, data: &[u8]) -> Vec<u8> {
        let mut out = ext_type.0.to_be_bytes().to_vec();
        out.extend_from_slice(&(data.len() as u16).to_be_bytes());
        out.extend_from_slice(data);
        out
    }

    /// A ServerHello body declaring `declared_ext_len` for its extensions block, which
    /// is `extensions.len()` unless a test is deliberately lying.
    fn server_hello_declaring(
        random: [u8; 32],
        declared_ext_len: u16,
        extensions: &[u8],
    ) -> Vec<u8> {
        let mut out = server_hello_prefix(random);
        out.extend_from_slice(&declared_ext_len.to_be_bytes());
        out.extend_from_slice(extensions);
        out
    }

    fn server_hello(random: [u8; 32], extensions: &[u8]) -> Vec<u8> {
        server_hello_declaring(random, extensions.len() as u16, extensions)
    }

    /// A ServerHello up to (not including) its extensions block - on its own a
    /// well-formed TLS 1.2 one, whose version can only come from the legacy field.
    fn server_hello_prefix(random: [u8; 32]) -> Vec<u8> {
        let mut out = vec![0x03, 0x03]; // legacy_version
        out.extend_from_slice(&random);
        out.push(0); // empty legacy_session_id_echo
        out.extend_from_slice(&[0x13, 0x01]); // cipher_suite
        out.push(0); // legacy_compression_method
        out
    }

    fn client_hello(session_id: &[u8], extensions: &[u8]) -> Vec<u8> {
        let mut out = vec![0x03, 0x03]; // legacy_version
        out.extend_from_slice(&[0x22; 32]); // random
        out.push(session_id.len() as u8);
        out.extend_from_slice(session_id);
        out.extend_from_slice(&2u16.to_be_bytes()); // cipher_suites
        out.extend_from_slice(&[0x13, 0x01]);
        out.push(1); // compression_methods
        out.push(0);
        out.extend_from_slice(&(extensions.len() as u16).to_be_bytes());
        out.extend_from_slice(extensions);
        out
    }

    /// Feeds `records` to one server-direction parser, in order, one TLS record each.
    fn feed_server(records: &[&[u8]]) -> ConnRecord {
        let rec = Mutex::new(ConnRecord::new(NODE_IP));
        let mut parser = HandshakeParser::new(Direction::ServerToClient);
        for record in records {
            parser.feed(TlsRecordType::Handshake, record, &rec);
        }
        rec.into_inner().unwrap()
    }

    fn conn_record(
        client_hello: Option<ClientHelloInfo>,
        server_hello: Option<ServerHelloInfo>,
        abbreviated_flight: Option<bool>,
    ) -> ConnRecord {
        ConnRecord {
            node_ip: NODE_IP,
            client_hello,
            server_hello,
            abbreviated_flight,
        }
    }

    /// A HelloRetryRequest never carries `pre_shared_key`, so mistaking it for the
    /// ServerHello would report a session that did resume as refused.
    #[test]
    fn hello_retry_request_is_not_mistaken_for_the_server_hello() {
        let tls13 = extension(Ext::SupportedVersions, &[0x03, 0x04]);
        let mut accepting = tls13.clone();
        accepting.extend(extension(Ext::PreSharedKey, &[0x00, 0x00]));

        let mut stream = handshake_msg(
            Hs::ServerHello,
            &server_hello(HELLO_RETRY_REQUEST_RANDOM, &tls13),
        );
        stream.extend(handshake_msg(
            Hs::ServerHello,
            &server_hello([0x11; 32], &accepting),
        ));

        let server_hello = feed_server(&[&stream])
            .server_hello
            .expect("the real ServerHello must still be recorded");
        assert_eq!(server_hello.version, TlsVersion::V1_3.wire());
        assert!(
            server_hello.accepts_psk,
            "PSK acceptance is signalled by the real ServerHello, not the retry request"
        );
    }

    /// An extensions block whose length prefix overruns the message must discard the
    /// Hello: read as "no extensions" it is indistinguishable from "no resumption
    /// offered".
    #[test]
    fn overlong_extensions_length_discards_the_hello() {
        let extensions = extension(Ext::PreSharedKey, &[0x00, 0x00]);
        let declared = extensions.len() as u16 + 1;
        let body = server_hello_declaring([0x11; 32], declared, &extensions);

        assert!(
            feed_server(&[&handshake_msg(Hs::ServerHello, &body)])
                .server_hello
                .is_none(),
            "a malformed Hello must leave the record unclassified, not read as 'no PSK'"
        );
    }

    /// The counterpart: a TLS 1.2 ServerHello may legally carry no extensions block, and
    /// must then be read at its legacy version.
    #[test]
    fn server_hello_without_extensions_uses_the_legacy_version() {
        let server_hello = feed_server(&[&handshake_msg(
            Hs::ServerHello,
            &server_hello_prefix([0x11; 32]),
        )])
        .server_hello
        .expect("a ServerHello without extensions is well-formed");

        assert_eq!(server_hello.version, TlsVersion::V1_2.wire());
        assert!(!server_hello.accepts_psk);
    }

    /// Splitting a two-message flight at every offset walks every path through the
    /// reassembly buffer: a partial header, a partial body, and the exact boundary
    /// between two messages. The integration test reaches none of them - on loopback
    /// every message arrives in a single record.
    #[test]
    fn a_flight_split_at_any_offset_is_reassembled() {
        let mut flight = handshake_msg(Hs::ServerHello, &server_hello_prefix([0x11; 32]));
        flight.extend(handshake_msg(Hs::Certificate, &[0xcc; 12]));

        for split in 0..=flight.len() {
            let record = feed_server(&[&flight[..split], &flight[split..]]);

            let server_hello = record
                .server_hello
                .unwrap_or_else(|| panic!("ServerHello lost when split at {split}"));
            assert_eq!(
                server_hello.version,
                TlsVersion::V1_2.wire(),
                "split at {split}"
            );
            assert_eq!(
                record.abbreviated_flight,
                Some(false),
                "the Certificate must still be seen when split at {split}"
            );
        }
    }

    /// A message must not be acted on before it has fully arrived. A Certificate is
    /// dispatched on its type alone, so acting on a partial one leaves the final verdict
    /// unchanged - the bug is only visible mid-flight.
    #[test]
    fn an_incomplete_message_is_not_acted_on_before_its_last_byte() {
        let certificate = handshake_msg(Hs::Certificate, &[0xcc; 12]);
        let (all_but_last, last) = certificate.split_at(certificate.len() - 1);
        let mut first_record = handshake_msg(Hs::ServerHello, &server_hello_prefix([0x11; 32]));
        first_record.extend_from_slice(all_but_last);

        let rec = Mutex::new(ConnRecord::new(NODE_IP));
        let mut parser = HandshakeParser::new(Direction::ServerToClient);

        parser.feed(TlsRecordType::Handshake, &first_record, &rec);
        assert!(
            rec.lock().unwrap().server_hello.is_some(),
            "the complete ServerHello ahead of it must have been read"
        );
        assert!(
            rec.lock().unwrap().abbreviated_flight.is_none(),
            "the Certificate is one byte short, so the flight is not classified yet"
        );

        parser.feed(TlsRecordType::Handshake, last, &rec);
        assert_eq!(
            rec.into_inner().unwrap().abbreviated_flight,
            Some(false),
            "and once its last byte arrives, it is"
        );
    }

    /// What counts as "fully observed" is version-dependent: a pre-1.3 verdict also needs
    /// the shape of the server's first flight, while a TLS 1.3 ServerHello suffices on its
    /// own. Every record the integration test sees is complete, so only these cases
    /// exercise the rule.
    #[test]
    fn finish_withholds_a_verdict_until_the_handshake_is_fully_observed() {
        let client = ClientHelloInfo {
            presents_ticket: true,
            presents_psk: true,
            nonempty_session_id: true,
        };
        let tls12 = ServerHelloInfo {
            version: TlsVersion::V1_2.wire(),
            accepts_psk: false,
        };
        let tls13 = ServerHelloInfo {
            version: TlsVersion::V1_3.wire(),
            accepts_psk: true,
        };

        assert!(
            conn_record(None, Some(tls13), None).finish().is_none(),
            "no ClientHello was ever seen"
        );
        assert!(
            conn_record(Some(client), None, None).finish().is_none(),
            "no ServerHello was ever seen"
        );
        assert!(
            conn_record(Some(client), Some(tls12), None)
                .finish()
                .is_none(),
            "a pre-1.3 flight that was never classified cannot yield a verdict"
        );
        assert!(
            conn_record(Some(client), Some(tls13), None)
                .finish()
                .is_some(),
            "a TLS 1.3 ServerHello says on its own whether the PSK was accepted"
        );
    }

    /// A TLS 1.2 client with no ticket can offer its cached session id instead, and be
    /// accepted - what rustls does when the server issues no tickets. The two signals must
    /// stay apart: the openssl assertions turn on "offered resumption at all", the rustls
    /// ones demand a ticket.
    #[test]
    fn a_tls12_session_id_offer_resumes_without_presenting_a_ticket() {
        let handshake = conn_record(
            Some(ClientHelloInfo {
                presents_ticket: false,
                presents_psk: false,
                nonempty_session_id: true,
            }),
            Some(ServerHelloInfo {
                version: TlsVersion::V1_2.wire(),
                accepts_psk: false,
            }),
            Some(true),
        )
        .finish()
        .expect("the handshake was fully observed");

        assert!(
            !handshake.presented_ticket,
            "no session_ticket extension was present"
        );
        assert!(
            handshake.offered_resumption,
            "a cached session id is an offer to resume"
        );
        assert!(
            handshake.resumed(),
            "and the server abbreviated its flight, so it accepted"
        );
    }

    /// `parse_hello` must never report a signal it did not see. A Hello truncated before
    /// its extensions is legal input (they are optional pre-1.3), so the tempting bug is
    /// to report the defaults as fact; every truncation of one bearing a ticket and a PSK
    /// is checked.
    #[test]
    fn truncating_a_hello_cannot_manufacture_a_resumption_signal() {
        let mut extensions = extension(Ext::SessionTicketTLS, &[0xab; 16]);
        extensions.extend(extension(Ext::PreSharedKey, &[0x00, 0x00]));

        for (is_server, body) in [
            (false, client_hello(&[0x33; 32], &extensions)),
            (true, server_hello([0x11; 32], &extensions)),
        ] {
            for len in 0..body.len() {
                if let Some(hello) = parse_hello(&body[..len], is_server) {
                    assert!(
                        !hello.nonempty_session_ticket,
                        "is_server={is_server}: reported a ticket it could not have seen, \
                         truncated to {len} of {} bytes",
                        body.len()
                    );
                    assert!(
                        !hello.has_psk,
                        "is_server={is_server}: reported a PSK it could not have seen, \
                         truncated to {len} of {} bytes",
                        body.len()
                    );
                }
            }

            // The signals really are in the untruncated bytes: the loop is not vacuous.
            let hello = parse_hello(&body, is_server).expect("the whole Hello parses");
            assert!(hello.nonempty_session_ticket && hello.has_psk);
        }
    }
}
