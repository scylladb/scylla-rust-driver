use bytes::Bytes;
use futures::{future::RemoteHandle, FutureExt};
use scylla_cql::frame::frame_errors::CqlResponseParseError;
use scylla_cql::frame::request::options::{self, Options};
use scylla_cql::frame::request::CqlRequestKind;
use scylla_cql::frame::response::result::{ResultMetadata, TableSpec};
use scylla_cql::frame::response::Error;
use scylla_cql::frame::response::{self, error};
use scylla_cql::frame::types::SerialConsistency;
use scylla_cql::types::serialize::batch::{BatchValues, BatchValuesIterator};
use scylla_cql::types::serialize::raw_batch::RawBatchValuesAdapter;
use scylla_cql::types::serialize::row::{RowSerializationContext, SerializedValues};
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{split, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tracing::{debug, error, trace, warn};
use uuid::Uuid;

use std::borrow::Cow;
#[cfg(feature = "ssl")]
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
#[cfg(feature = "ssl")]
use tokio_openssl::SslStream;

#[cfg(feature = "ssl")]
pub(crate) use ssl_config::SslConfig;

use crate::authentication::AuthenticatorProvider;
use crate::transport::errors::{
    BadKeyspaceName, BrokenConnectionError, BrokenConnectionErrorKind, ConnectionError,
    ConnectionSetupRequestError, ConnectionSetupRequestErrorKind, CqlEventHandlingError, DbError,
    QueryError, RequestError, ResponseParseError, TranslationError, UserRequestError,
};
use scylla_cql::frame::response::authenticate::Authenticate;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::convert::TryFrom;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::{
    cmp::Ordering,
    net::{Ipv4Addr, Ipv6Addr},
};

use super::errors::{ProtocolError, UseKeyspaceProtocolError};
use super::iterator::{LegacyRowIterator, RawIterator};
use super::locator::tablets::{RawTablet, TabletParsingError};
use super::query_result::QueryResult;
use super::session::AddressTranslator;
use super::topology::{PeerEndpoint, UntranslatedEndpoint, UntranslatedPeer};
use super::NodeAddr;
#[cfg(feature = "cloud")]
use crate::cloud::CloudConfig;

use crate::batch::{Batch, BatchStatement};
use crate::frame::protocol_features::ProtocolFeatures;
use crate::frame::{
    self,
    request::{self, batch, execute, query, register, SerializableRequest},
    response::{event::Event, result, NonErrorResponse, Response, ResponseOpcode},
    server_event_type::EventType,
    FrameParams, SerializedRequest,
};
use crate::query::Query;
use crate::routing::ShardInfo;
use crate::statement::prepared_statement::PreparedStatement;
use crate::statement::{Consistency, PageSize, PagingState, PagingStateResponse};
use crate::transport::Compression;

// Queries for schema agreement
const LOCAL_VERSION: &str = "SELECT schema_version FROM system.local WHERE key='local'";

// FIXME: Make this constants configurable
// The term "orphan" refers to stream ids, that were allocated for a {request, response} that no
// one is waiting anymore (due to cancellation of `Connection::send_request`). Old orphan refers to
// a stream id that is orphaned for a long time. This long time is defined below
// (`OLD_AGE_ORPHAN_THRESHOLD`). Connection that has a big number (`OLD_ORPHAN_COUNT_THRESHOLD`)
// of old orphans is shut down (and created again by a connection management layer).
const OLD_ORPHAN_COUNT_THRESHOLD: usize = 1024;
const OLD_AGE_ORPHAN_THRESHOLD: std::time::Duration = std::time::Duration::from_secs(1);

pub(crate) struct Connection {
    _worker_handle: RemoteHandle<()>,

    connect_address: SocketAddr,
    config: ConnectionConfig,
    features: ConnectionFeatures,
    router_handle: Arc<RouterHandle>,
}

struct RouterHandle {
    submit_channel: mpsc::Sender<Task>,

    // Each request send by `Connection::send_request` needs a unique request id.
    // This field is a monotonic generator of such ids.
    request_id_generator: AtomicU64,
    // If a `Connection::send_request` is cancelled, it sends notification
    // about orphaning via the sender below.
    // Also, this sender is unbounded, because only unbounded channels support
    // pushing values in a synchronous way (without an `.await`), which is
    // needed for pushing values in `Drop` implementations.
    orphan_notification_sender: mpsc::UnboundedSender<RequestId>,
}

impl RouterHandle {
    fn allocate_request_id(&self) -> RequestId {
        self.request_id_generator
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    async fn send_request(
        &self,
        request: &impl SerializableRequest,
        compression: Option<Compression>,
        tracing: bool,
    ) -> Result<TaskResponse, RequestError> {
        let serialized_request = SerializedRequest::make(request, compression, tracing)?;
        let request_id = self.allocate_request_id();

        let (response_sender, receiver) = oneshot::channel();
        let response_handler = ResponseHandler {
            response_sender,
            request_id,
        };

        // Dropping `notifier` (before calling `notifier.disable()`) will send a notification to
        // `Connection::router`. This notification is then used to mark a `stream_id` associated
        // with this request as orphaned and free associated resources.
        let notifier = OrphanhoodNotifier::new(request_id, &self.orphan_notification_sender);

        self.submit_channel
            .send(Task {
                serialized_request,
                response_handler,
            })
            .await
            .map_err(|_| -> BrokenConnectionError {
                BrokenConnectionErrorKind::ChannelError.into()
            })?;

        let task_response = receiver.await.map_err(|_| -> BrokenConnectionError {
            BrokenConnectionErrorKind::ChannelError.into()
        })?;

        // Response was successfully received, so it's time to disable
        // notification about orphaning.
        notifier.disable();

        task_response
    }
}

#[derive(Default)]
pub(crate) struct ConnectionFeatures {
    shard_info: Option<ShardInfo>,
    shard_aware_port: Option<u16>,
    protocol_features: ProtocolFeatures,
}

type RequestId = u64;

struct ResponseHandler {
    response_sender: oneshot::Sender<Result<TaskResponse, RequestError>>,
    request_id: RequestId,
}

// Used to notify `Connection::orphaner` about `Connection::send_request`
// future being dropped before receiving response.
struct OrphanhoodNotifier<'a> {
    enabled: bool,
    request_id: RequestId,
    notification_sender: &'a mpsc::UnboundedSender<RequestId>,
}

impl<'a> OrphanhoodNotifier<'a> {
    fn new(
        request_id: RequestId,
        notification_sender: &'a mpsc::UnboundedSender<RequestId>,
    ) -> Self {
        Self {
            enabled: true,
            request_id,
            notification_sender,
        }
    }

    fn disable(mut self) {
        self.enabled = false;
    }
}

impl<'a> Drop for OrphanhoodNotifier<'a> {
    fn drop(&mut self) {
        if self.enabled {
            let _ = self.notification_sender.send(self.request_id);
        }
    }
}

struct Task {
    serialized_request: SerializedRequest,
    response_handler: ResponseHandler,
}

struct TaskResponse {
    params: FrameParams,
    opcode: ResponseOpcode,
    body: Bytes,
}

pub(crate) struct QueryResponse {
    pub(crate) response: Response,
    pub(crate) tracing_id: Option<Uuid>,
    pub(crate) warnings: Vec<String>,
    #[allow(dead_code)] // This is not exposed to user (yet?)
    pub(crate) custom_payload: Option<HashMap<String, Vec<u8>>>,
}

// A QueryResponse in which response can not be Response::Error
pub(crate) struct NonErrorQueryResponse {
    pub(crate) response: NonErrorResponse,
    pub(crate) tracing_id: Option<Uuid>,
    pub(crate) warnings: Vec<String>,
}

impl QueryResponse {
    pub(crate) fn into_non_error_query_response(
        self,
    ) -> Result<NonErrorQueryResponse, UserRequestError> {
        Ok(NonErrorQueryResponse {
            response: self.response.into_non_error_response()?,
            tracing_id: self.tracing_id,
            warnings: self.warnings,
        })
    }

    pub(crate) fn into_query_result_and_paging_state(
        self,
    ) -> Result<(QueryResult, PagingStateResponse), UserRequestError> {
        self.into_non_error_query_response()?
            .into_query_result_and_paging_state()
    }

    pub(crate) fn into_query_result(self) -> Result<QueryResult, QueryError> {
        self.into_non_error_query_response()?.into_query_result()
    }
}

impl NonErrorQueryResponse {
    pub(crate) fn as_set_keyspace(&self) -> Option<&result::SetKeyspace> {
        match &self.response {
            NonErrorResponse::Result(result::Result::SetKeyspace(sk)) => Some(sk),
            _ => None,
        }
    }

    pub(crate) fn as_schema_change(&self) -> Option<&result::SchemaChange> {
        match &self.response {
            NonErrorResponse::Result(result::Result::SchemaChange(sc)) => Some(sc),
            _ => None,
        }
    }

    pub(crate) fn into_query_result_and_paging_state(
        self,
    ) -> Result<(QueryResult, PagingStateResponse), UserRequestError> {
        let (raw_rows, paging_state_response) = match self.response {
            NonErrorResponse::Result(result::Result::Rows((rs, paging_state_response))) => {
                (Some(rs), paging_state_response)
            }
            NonErrorResponse::Result(_) => (None, PagingStateResponse::NoMorePages),
            _ => {
                return Err(UserRequestError::UnexpectedResponse(
                    self.response.to_response_kind(),
                ))
            }
        };

        Ok((
            QueryResult::new(raw_rows, self.tracing_id, self.warnings),
            paging_state_response,
        ))
    }

    pub(crate) fn into_query_result(self) -> Result<QueryResult, QueryError> {
        let (result, paging_state) = self.into_query_result_and_paging_state()?;

        if !paging_state.finished() {
            error!(
                "Internal driver API misuse or a server bug: nonfinished paging state\
                would be discarded by `NonErrorQueryResponse::into_query_result`"
            );
            return Err(ProtocolError::NonfinishedPagingState.into());
        }

        Ok(result)
    }
}

pub(crate) enum NonErrorStartupResponse {
    Ready,
    Authenticate(response::authenticate::Authenticate),
}

pub(crate) enum NonErrorAuthResponse {
    AuthChallenge(response::authenticate::AuthChallenge),
    AuthSuccess(response::authenticate::AuthSuccess),
}

#[cfg(feature = "ssl")]
mod ssl_config {
    use openssl::{
        error::ErrorStack,
        ssl::{Ssl, SslContext},
    };
    #[cfg(feature = "cloud")]
    use uuid::Uuid;

    /// This struct encapsulates all Ssl-regarding configuration and helps pass it tidily through the code.
    //
    // There are 3 possible options for SslConfig, whose behaviour is somewhat subtle.
    // Option 1: No ssl configuration. Then it is None every time.
    // Option 2: User-provided global SslContext. Then, a SslConfig is created upon Session creation
    // and henceforth stored in the ConnectionConfig.
    // Option 3: Serverless Cloud. The Option<SslConfig> remains None in ConnectionConfig until it reaches
    // NodeConnectionPool::new(). Inside that function, the field is mutated to contain SslConfig specific
    // for the particular node. (The SslConfig must be different, because SNIs differ for different nodes.)
    // Thenceforth, all connections to that node share the same SslConfig.
    #[derive(Clone)]
    pub(crate) struct SslConfig {
        context: SslContext,
        #[cfg(feature = "cloud")]
        sni: Option<String>,
    }

    impl SslConfig {
        // Used in case when the user provided their own SslContext to be used in all connections.
        pub(crate) fn new_with_global_context(context: SslContext) -> Self {
            Self {
                context,
                #[cfg(feature = "cloud")]
                sni: None,
            }
        }

        // Used in case of Serverless Cloud connections.
        #[cfg(feature = "cloud")]
        pub(crate) fn new_for_sni(
            context: SslContext,
            domain_name: &str,
            host_id: Option<Uuid>,
        ) -> Self {
            Self {
                context,
                #[cfg(feature = "cloud")]
                sni: Some(if let Some(host_id) = host_id {
                    format!("{}.{}", host_id, domain_name)
                } else {
                    domain_name.into()
                }),
            }
        }

        // Produces a new Ssl object that is able to wrap a TCP stream.
        pub(crate) fn new_ssl(&self) -> Result<Ssl, ErrorStack> {
            #[allow(unused_mut)]
            let mut ssl = Ssl::new(&self.context)?;
            #[cfg(feature = "cloud")]
            if let Some(sni) = self.sni.as_ref() {
                ssl.set_hostname(sni)?;
            }
            Ok(ssl)
        }
    }
}

/// Driver and application self-identifying information,
/// to be sent in STARTUP message.
#[derive(Debug, Clone, Default)]
pub struct SelfIdentity<'id> {
    // Custom driver identity can be set if a custom driver build is running,
    // or an entirely different driver is operating on top of Rust driver
    // (e.g. cpp-rust-driver).
    custom_driver_name: Option<Cow<'id, str>>,
    custom_driver_version: Option<Cow<'id, str>>,

    // ### Q: Where do APPLICATION_NAME, APPLICATION_VERSION and CLIENT_ID come from?
    // - there are no columns in system.clients dedicated to those attributes,
    // - APPLICATION_NAME / APPLICATION_VERSION are not present in Scylla's source code at all,
    // - only 2 results in Cassandra source is some example in docs:
    //   https://github.com/apache/cassandra/blob/d3cbf9c1f72057d2a5da9df8ed567d20cd272931/doc/modules/cassandra/pages/managing/operating/virtualtables.adoc?plain=1#L218.
    //   APPLICATION_NAME and APPLICATION_VERSION appears in client_options which
    //   is an arbitrary dict where client can send any keys.
    // - driver variables are mentioned in protocol v5
    //   (https://github.com/apache/cassandra/blob/d3cbf9c1f72057d2a5da9df8ed567d20cd272931/doc/native_protocol_v5.spec#L480),
    //   application variables are not.
    //
    // ### A:
    // The following options are not exposed anywhere in Scylla tables.
    // They come directly from CPP driver, and they are supported in Cassandra
    //
    // See https://github.com/scylladb/cpp-driver/blob/fa0f27069a625057984d1fa58f434ea99b86c83f/include/cassandra.h#L2916.
    // As we want to support as big subset of its API as possible in cpp-rust-driver, I decided to expose API for setting
    // those particular key-value pairs, similarly to what cpp-driver does, and not an API to set arbitrary key-value pairs.
    //
    // Allowing users to set arbitrary options could break the driver by overwriting options that bear special meaning,
    // e.g. the shard-aware port. Therefore, I'm against such liberal API. OTOH, we need to expose APPLICATION_NAME,
    // APPLICATION_VERSION and CLIENT_ID for cpp-rust-driver.

    // Application identity can be set to distinguish different applications
    // connected to the same cluster.
    application_name: Option<Cow<'id, str>>,
    application_version: Option<Cow<'id, str>>,

    // A (unique) client ID can be set to distinguish different instances
    // of the same application connected to the same cluster.
    client_id: Option<Cow<'id, str>>,
}

impl<'id> SelfIdentity<'id> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Advertises a custom driver name, which can be used if a custom driver build is running,
    /// or an entirely different driver is operating on top of Rust driver
    /// (e.g. cpp-rust-driver).
    pub fn set_custom_driver_name(&mut self, custom_driver_name: impl Into<Cow<'id, str>>) {
        self.custom_driver_name = Some(custom_driver_name.into());
    }

    /// Advertises a custom driver name. See [Self::set_custom_driver_name] for use cases.
    pub fn with_custom_driver_name(mut self, custom_driver_name: impl Into<Cow<'id, str>>) -> Self {
        self.custom_driver_name = Some(custom_driver_name.into());
        self
    }

    /// Custom driver name to be advertised. See [Self::set_custom_driver_name] for use cases.
    pub fn get_custom_driver_name(&self) -> Option<&str> {
        self.custom_driver_name.as_deref()
    }

    /// Advertises a custom driver version. See [Self::set_custom_driver_name] for use cases.
    pub fn set_custom_driver_version(&mut self, custom_driver_version: impl Into<Cow<'id, str>>) {
        self.custom_driver_version = Some(custom_driver_version.into());
    }

    /// Advertises a custom driver version. See [Self::set_custom_driver_name] for use cases.
    pub fn with_custom_driver_version(
        mut self,
        custom_driver_version: impl Into<Cow<'id, str>>,
    ) -> Self {
        self.custom_driver_version = Some(custom_driver_version.into());
        self
    }

    /// Custom driver version to be advertised. See [Self::set_custom_driver_version] for use cases.
    pub fn get_custom_driver_version(&self) -> Option<&str> {
        self.custom_driver_version.as_deref()
    }

    /// Advertises an application name, which can be used to distinguish different applications
    /// connected to the same cluster.
    pub fn set_application_name(&mut self, application_name: impl Into<Cow<'id, str>>) {
        self.application_name = Some(application_name.into());
    }

    /// Advertises an application name. See [Self::set_application_name] for use cases.
    pub fn with_application_name(mut self, application_name: impl Into<Cow<'id, str>>) -> Self {
        self.application_name = Some(application_name.into());
        self
    }

    /// Application name to be advertised. See [Self::set_application_name] for use cases.
    pub fn get_application_name(&self) -> Option<&str> {
        self.application_name.as_deref()
    }

    /// Advertises an application version. See [Self::set_application_name] for use cases.
    pub fn set_application_version(&mut self, application_version: impl Into<Cow<'id, str>>) {
        self.application_version = Some(application_version.into());
    }

    /// Advertises an application version. See [Self::set_application_name] for use cases.
    pub fn with_application_version(
        mut self,
        application_version: impl Into<Cow<'id, str>>,
    ) -> Self {
        self.application_version = Some(application_version.into());
        self
    }

    /// Application version to be advertised. See [Self::set_application_version] for use cases.
    pub fn get_application_version(&self) -> Option<&str> {
        self.application_version.as_deref()
    }

    /// Advertises a client ID, which can be set to distinguish different instances
    /// of the same application connected to the same cluster.
    pub fn set_client_id(&mut self, client_id: impl Into<Cow<'id, str>>) {
        self.client_id = Some(client_id.into());
    }

    /// Advertises a client ID. See [Self::set_client_id] for use cases.
    pub fn with_client_id(mut self, client_id: impl Into<Cow<'id, str>>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Client ID to be advertised. See [Self::set_client_id] for use cases.
    pub fn get_client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }
}

impl<'id: 'map, 'map> SelfIdentity<'id> {
    fn add_startup_options(&'id self, options: &'map mut HashMap<Cow<'id, str>, Cow<'id, str>>) {
        /* Driver identity. */
        let driver_name = self
            .custom_driver_name
            .as_deref()
            .unwrap_or(options::DEFAULT_DRIVER_NAME);
        options.insert(
            Cow::Borrowed(options::DRIVER_NAME),
            Cow::Borrowed(driver_name),
        );

        let driver_version = self
            .custom_driver_version
            .as_deref()
            .unwrap_or(options::DEFAULT_DRIVER_VERSION);
        options.insert(
            Cow::Borrowed(options::DRIVER_VERSION),
            Cow::Borrowed(driver_version),
        );

        /* Application identity. */
        if let Some(application_name) = self.application_name.as_deref() {
            options.insert(
                Cow::Borrowed(options::APPLICATION_NAME),
                Cow::Borrowed(application_name),
            );
        }

        if let Some(application_version) = self.application_version.as_deref() {
            options.insert(
                Cow::Borrowed(options::APPLICATION_VERSION),
                Cow::Borrowed(application_version),
            );
        }

        /* Client identity. */
        if let Some(client_id) = self.client_id.as_deref() {
            options.insert(Cow::Borrowed(options::CLIENT_ID), Cow::Borrowed(client_id));
        }
    }
}

#[derive(Clone)]
pub(crate) struct ConnectionConfig {
    pub(crate) compression: Option<Compression>,
    pub(crate) tcp_nodelay: bool,
    pub(crate) tcp_keepalive_interval: Option<Duration>,
    #[cfg(feature = "ssl")]
    pub(crate) ssl_config: Option<SslConfig>,
    pub(crate) connect_timeout: std::time::Duration,
    // should be Some only in control connections,
    pub(crate) event_sender: Option<mpsc::Sender<Event>>,
    pub(crate) default_consistency: Consistency,
    #[cfg(feature = "cloud")]
    pub(crate) cloud_config: Option<Arc<CloudConfig>>,
    pub(crate) authenticator: Option<Arc<dyn AuthenticatorProvider>>,
    pub(crate) address_translator: Option<Arc<dyn AddressTranslator>>,
    pub(crate) enable_write_coalescing: bool,

    pub(crate) keepalive_interval: Option<Duration>,
    pub(crate) keepalive_timeout: Option<Duration>,
    pub(crate) tablet_sender: Option<mpsc::Sender<(TableSpec<'static>, RawTablet)>>,

    pub(crate) identity: SelfIdentity<'static>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            compression: None,
            tcp_nodelay: true,
            tcp_keepalive_interval: None,
            event_sender: None,
            #[cfg(feature = "ssl")]
            ssl_config: None,
            connect_timeout: std::time::Duration::from_secs(5),
            default_consistency: Default::default(),
            authenticator: None,
            address_translator: None,
            #[cfg(feature = "cloud")]
            cloud_config: None,
            enable_write_coalescing: true,

            // Note: this is different than SessionConfig default values.
            keepalive_interval: None,
            keepalive_timeout: None,

            tablet_sender: None,

            identity: SelfIdentity::default(),
        }
    }
}

impl ConnectionConfig {
    #[cfg(feature = "ssl")]
    fn is_ssl(&self) -> bool {
        #[cfg(feature = "cloud")]
        if self.cloud_config.is_some() {
            return true;
        }
        self.ssl_config.is_some()
    }

    #[cfg(not(feature = "ssl"))]
    fn is_ssl(&self) -> bool {
        false
    }
}

// Used to listen for fatal error in connection
pub(crate) type ErrorReceiver = tokio::sync::oneshot::Receiver<ConnectionError>;

impl Connection {
    // Returns new connection and ErrorReceiver which can be used to wait for a fatal error
    /// Opens a connection and makes it ready to send/receive CQL frames on it,
    /// but does not yet send any frames (no OPTIONS/STARTUP handshake nor REGISTER requests).
    pub(crate) async fn new(
        addr: SocketAddr,
        source_port: Option<u16>,
        config: ConnectionConfig,
    ) -> Result<(Self, ErrorReceiver), ConnectionError> {
        let stream_connector = match source_port {
            Some(p) => {
                tokio::time::timeout(config.connect_timeout, connect_with_source_port(addr, p))
                    .await
            }
            None => tokio::time::timeout(config.connect_timeout, TcpStream::connect(addr)).await,
        };
        let stream = match stream_connector {
            Ok(stream) => stream?,
            Err(_) => {
                return Err(ConnectionError::ConnectTimeout);
            }
        };
        stream.set_nodelay(config.tcp_nodelay)?;

        if let Some(tcp_keepalive_interval) = config.tcp_keepalive_interval {
            Self::setup_tcp_keepalive(&stream, tcp_keepalive_interval)?;
        }

        // TODO: What should be the size of the channel?
        let (sender, receiver) = mpsc::channel(1024);
        let (error_sender, error_receiver) = tokio::sync::oneshot::channel();
        // Unbounded because it allows for synchronous pushes
        let (orphan_notification_sender, orphan_notification_receiver) = mpsc::unbounded_channel();

        let router_handle = Arc::new(RouterHandle {
            submit_channel: sender,
            request_id_generator: AtomicU64::new(0),
            orphan_notification_sender,
        });

        let _worker_handle = Self::run_router(
            config.clone(),
            stream,
            receiver,
            error_sender,
            orphan_notification_receiver,
            router_handle.clone(),
            addr.ip(),
        )
        .await?;

        let connection = Connection {
            _worker_handle,
            config,
            features: Default::default(),
            connect_address: addr,
            router_handle,
        };

        Ok((connection, error_receiver))
    }

    fn setup_tcp_keepalive(
        stream: &TcpStream,
        tcp_keepalive_interval: Duration,
    ) -> std::io::Result<()> {
        // It may be surprising why we call `with_time()` with `tcp_keepalive_interval`
        // and `with_interval() with some other value. This is due to inconsistent naming:
        // our interval means time after connection becomes idle until keepalives
        // begin to be sent (they call it "time"), and their interval is time between
        // sending keepalives.
        // We insist on our naming due to other drivers following the same convention.
        let mut tcp_keepalive = TcpKeepalive::new().with_time(tcp_keepalive_interval);

        // These cfg values are taken from socket2 library, which uses the same constraints.
        #[cfg(any(
            target_os = "android",
            target_os = "dragonfly",
            target_os = "freebsd",
            target_os = "fuchsia",
            target_os = "illumos",
            target_os = "ios",
            target_os = "linux",
            target_os = "macos",
            target_os = "netbsd",
            target_os = "tvos",
            target_os = "watchos",
            target_os = "windows",
        ))]
        {
            tcp_keepalive = tcp_keepalive.with_interval(Duration::from_secs(1));
        }

        #[cfg(any(
            target_os = "android",
            target_os = "dragonfly",
            target_os = "freebsd",
            target_os = "fuchsia",
            target_os = "illumos",
            target_os = "ios",
            target_os = "linux",
            target_os = "macos",
            target_os = "netbsd",
            target_os = "tvos",
            target_os = "watchos",
        ))]
        {
            tcp_keepalive = tcp_keepalive.with_retries(10);
        }

        let sf = SockRef::from(&stream);
        sf.set_tcp_keepalive(&tcp_keepalive)
    }

    pub(crate) async fn startup(
        &self,
        options: HashMap<Cow<'_, str>, Cow<'_, str>>,
    ) -> Result<NonErrorStartupResponse, ConnectionSetupRequestError> {
        let err = |kind: ConnectionSetupRequestErrorKind| {
            ConnectionSetupRequestError::new(CqlRequestKind::Startup, kind)
        };

        let req_result = self
            .send_request(&request::Startup { options }, false, false, None)
            .await;

        // Extract the response to STARTUP request and tidy up the errors.
        let response = match req_result {
            Ok(r) => match r.response {
                Response::Ready => NonErrorStartupResponse::Ready,
                Response::Authenticate(auth) => NonErrorStartupResponse::Authenticate(auth),
                Response::Error(Error { error, reason }) => {
                    return Err(err(ConnectionSetupRequestErrorKind::DbError(error, reason)))
                }
                _ => {
                    return Err(err(ConnectionSetupRequestErrorKind::UnexpectedResponse(
                        r.response.to_response_kind(),
                    )))
                }
            },
            Err(e) => match e {
                RequestError::CqlRequestSerialization(e) => return Err(err(e.into())),
                RequestError::BodyExtensionsParseError(e) => return Err(err(e.into())),
                RequestError::CqlResponseParseError(e) => match e {
                    // Parsing of READY response cannot fail, since its body is empty.
                    // Remaining valid responses are AUTHENTICATE and ERROR.
                    CqlResponseParseError::CqlAuthenticateParseError(e) => {
                        return Err(err(e.into()))
                    }
                    CqlResponseParseError::CqlErrorParseError(e) => return Err(err(e.into())),
                    _ => {
                        return Err(err(ConnectionSetupRequestErrorKind::UnexpectedResponse(
                            e.to_response_kind(),
                        )))
                    }
                },
                RequestError::BrokenConnection(e) => return Err(err(e.into())),
                RequestError::UnableToAllocStreamId => {
                    return Err(err(ConnectionSetupRequestErrorKind::UnableToAllocStreamId))
                }
            },
        };

        Ok(response)
    }

    pub(crate) async fn get_options(
        &self,
    ) -> Result<response::Supported, ConnectionSetupRequestError> {
        let err = |kind: ConnectionSetupRequestErrorKind| {
            ConnectionSetupRequestError::new(CqlRequestKind::Options, kind)
        };

        let req_result = self
            .send_request(&request::Options {}, false, false, None)
            .await;

        // Extract the supported options and tidy up the errors.
        let supported = match req_result {
            Ok(r) => match r.response {
                Response::Supported(supported) => supported,
                Response::Error(Error { error, reason }) => {
                    return Err(err(ConnectionSetupRequestErrorKind::DbError(error, reason)))
                }
                _ => {
                    return Err(err(ConnectionSetupRequestErrorKind::UnexpectedResponse(
                        r.response.to_response_kind(),
                    )))
                }
            },
            Err(e) => match e {
                RequestError::CqlRequestSerialization(e) => return Err(err(e.into())),
                RequestError::BodyExtensionsParseError(e) => return Err(err(e.into())),
                RequestError::CqlResponseParseError(e) => match e {
                    CqlResponseParseError::CqlSupportedParseError(e) => return Err(err(e.into())),
                    CqlResponseParseError::CqlErrorParseError(e) => return Err(err(e.into())),
                    _ => {
                        return Err(err(ConnectionSetupRequestErrorKind::UnexpectedResponse(
                            e.to_response_kind(),
                        )))
                    }
                },
                RequestError::BrokenConnection(e) => return Err(err(e.into())),
                RequestError::UnableToAllocStreamId => {
                    return Err(err(ConnectionSetupRequestErrorKind::UnableToAllocStreamId))
                }
            },
        };

        Ok(supported)
    }

    pub(crate) async fn prepare(
        &self,
        query: &Query,
    ) -> Result<PreparedStatement, UserRequestError> {
        let query_response = self
            .send_request(
                &request::Prepare {
                    query: &query.contents,
                },
                true,
                query.config.tracing,
                None,
            )
            .await?;

        let mut prepared_statement = match query_response.response {
            Response::Error(error::Error { error, reason }) => {
                return Err(UserRequestError::DbError(error, reason))
            }
            Response::Result(result::Result::Prepared(p)) => PreparedStatement::new(
                p.id,
                self.features
                    .protocol_features
                    .prepared_flags_contain_lwt_mark(p.prepared_metadata.flags as u32),
                p.prepared_metadata,
                Arc::new(p.result_metadata),
                query.contents.clone(),
                query.get_validated_page_size(),
                query.config.clone(),
            ),
            _ => {
                return Err(UserRequestError::UnexpectedResponse(
                    query_response.response.to_response_kind(),
                ))
            }
        };

        if let Some(tracing_id) = query_response.tracing_id {
            prepared_statement.prepare_tracing_ids.push(tracing_id);
        }
        Ok(prepared_statement)
    }

    pub(crate) async fn reprepare(
        &self,
        query: impl Into<Query>,
        previous_prepared: &PreparedStatement,
    ) -> Result<(), UserRequestError> {
        let reprepare_query: Query = query.into();
        let reprepared = self.prepare(&reprepare_query).await?;
        // Reprepared statement should keep its id - it's the md5 sum
        // of statement contents
        if reprepared.get_id() != previous_prepared.get_id() {
            Err(UserRequestError::RepreparedIdChanged {
                statement: reprepare_query.contents,
                expected_id: previous_prepared.get_id().clone().into(),
                reprepared_id: reprepared.get_id().clone().into(),
            })
        } else {
            Ok(())
        }
    }

    pub(crate) async fn authenticate_response(
        &self,
        response: Option<Vec<u8>>,
    ) -> Result<NonErrorAuthResponse, ConnectionSetupRequestError> {
        let err = |kind: ConnectionSetupRequestErrorKind| {
            ConnectionSetupRequestError::new(CqlRequestKind::AuthResponse, kind)
        };

        let req_result = self
            .send_request(&request::AuthResponse { response }, false, false, None)
            .await;

        // Extract non-error response to AUTH_RESPONSE request and tidy up errors.
        let response = match req_result {
            Ok(r) => match r.response {
                Response::AuthSuccess(auth_success) => {
                    NonErrorAuthResponse::AuthSuccess(auth_success)
                }
                Response::AuthChallenge(auth_challenge) => {
                    NonErrorAuthResponse::AuthChallenge(auth_challenge)
                }
                Response::Error(Error { error, reason }) => {
                    return Err(err(ConnectionSetupRequestErrorKind::DbError(error, reason)))
                }
                _ => {
                    return Err(err(ConnectionSetupRequestErrorKind::UnexpectedResponse(
                        r.response.to_response_kind(),
                    )))
                }
            },
            Err(e) => match e {
                RequestError::CqlRequestSerialization(e) => return Err(err(e.into())),
                RequestError::BodyExtensionsParseError(e) => return Err(err(e.into())),
                RequestError::CqlResponseParseError(e) => match e {
                    CqlResponseParseError::CqlAuthSuccessParseError(e) => {
                        return Err(err(e.into()))
                    }
                    CqlResponseParseError::CqlAuthChallengeParseError(e) => {
                        return Err(err(e.into()))
                    }
                    CqlResponseParseError::CqlErrorParseError(e) => return Err(err(e.into())),
                    _ => {
                        return Err(err(ConnectionSetupRequestErrorKind::UnexpectedResponse(
                            e.to_response_kind(),
                        )))
                    }
                },
                RequestError::BrokenConnection(e) => return Err(err(e.into())),
                RequestError::UnableToAllocStreamId => {
                    return Err(err(ConnectionSetupRequestErrorKind::UnableToAllocStreamId))
                }
            },
        };

        Ok(response)
    }

    #[allow(dead_code)]
    pub(crate) async fn query_single_page(
        &self,
        query: impl Into<Query>,
        paging_state: PagingState,
    ) -> Result<(QueryResult, PagingStateResponse), UserRequestError> {
        let query: Query = query.into();

        // This method is used only for driver internal queries, so no need to consult execution profile here.
        let consistency = query
            .config
            .determine_consistency(self.config.default_consistency);
        let serial_consistency = query.config.serial_consistency;

        self.query_single_page_with_consistency(
            query,
            paging_state,
            consistency,
            serial_consistency.flatten(),
        )
        .await
    }

    #[allow(dead_code)]
    pub(crate) async fn query_single_page_with_consistency(
        &self,
        query: impl Into<Query>,
        paging_state: PagingState,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
    ) -> Result<(QueryResult, PagingStateResponse), UserRequestError> {
        let query: Query = query.into();
        let page_size = query.get_validated_page_size();

        self.query_raw_with_consistency(
            &query,
            consistency,
            serial_consistency,
            Some(page_size),
            paging_state,
        )
        .await?
        .into_query_result_and_paging_state()
    }

    #[allow(dead_code)]
    pub(crate) async fn query_unpaged(
        &self,
        query: impl Into<Query>,
    ) -> Result<QueryResult, QueryError> {
        // This method is used only for driver internal queries, so no need to consult execution profile here.
        let query: Query = query.into();

        self.query_raw_unpaged(&query)
            .await
            .map_err(Into::into)
            .and_then(QueryResponse::into_query_result)
    }

    pub(crate) async fn query_raw_unpaged(
        &self,
        query: &Query,
    ) -> Result<QueryResponse, UserRequestError> {
        // This method is used only for driver internal queries, so no need to consult execution profile here.
        self.query_raw_with_consistency(
            query,
            query
                .config
                .determine_consistency(self.config.default_consistency),
            query.config.serial_consistency.flatten(),
            None,
            PagingState::start(),
        )
        .await
    }

    pub(crate) async fn query_raw_with_consistency(
        &self,
        query: &Query,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
        page_size: Option<PageSize>,
        paging_state: PagingState,
    ) -> Result<QueryResponse, UserRequestError> {
        let query_frame = query::Query {
            contents: Cow::Borrowed(&query.contents),
            parameters: query::QueryParameters {
                consistency,
                serial_consistency,
                values: Cow::Borrowed(SerializedValues::EMPTY),
                page_size: page_size.map(Into::into),
                paging_state,
                skip_metadata: false,
                timestamp: query.get_timestamp(),
            },
        };

        let response = self
            .send_request(&query_frame, true, query.config.tracing, None)
            .await?;

        Ok(response)
    }

    #[allow(dead_code)]
    pub(crate) async fn execute_unpaged(
        &self,
        prepared: &PreparedStatement,
        values: SerializedValues,
    ) -> Result<QueryResult, QueryError> {
        // This method is used only for driver internal queries, so no need to consult execution profile here.
        self.execute_raw_unpaged(prepared, values)
            .await
            .map_err(Into::into)
            .and_then(QueryResponse::into_query_result)
    }

    #[allow(dead_code)]
    pub(crate) async fn execute_raw_unpaged(
        &self,
        prepared: &PreparedStatement,
        values: SerializedValues,
    ) -> Result<QueryResponse, UserRequestError> {
        // This method is used only for driver internal queries, so no need to consult execution profile here.
        self.execute_raw_with_consistency(
            prepared,
            &values,
            prepared
                .config
                .determine_consistency(self.config.default_consistency),
            prepared.config.serial_consistency.flatten(),
            None,
            PagingState::start(),
        )
        .await
    }

    pub(crate) async fn execute_raw_with_consistency(
        &self,
        prepared_statement: &PreparedStatement,
        values: &SerializedValues,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
        page_size: Option<PageSize>,
        paging_state: PagingState,
    ) -> Result<QueryResponse, UserRequestError> {
        let execute_frame = execute::Execute {
            id: prepared_statement.get_id().to_owned(),
            parameters: query::QueryParameters {
                consistency,
                serial_consistency,
                values: Cow::Borrowed(values),
                page_size: page_size.map(Into::into),
                timestamp: prepared_statement.get_timestamp(),
                skip_metadata: prepared_statement.get_use_cached_result_metadata(),
                paging_state,
            },
        };

        let cached_metadata = prepared_statement
            .get_use_cached_result_metadata()
            .then(|| prepared_statement.get_result_metadata());

        let query_response = self
            .send_request(
                &execute_frame,
                true,
                prepared_statement.config.tracing,
                cached_metadata,
            )
            .await?;

        if let Some(spec) = prepared_statement.get_table_spec() {
            if let Err(e) = self
                .update_tablets_from_response(spec, &query_response)
                .await
            {
                tracing::warn!("Error while parsing tablet info from custom payload: {}", e);
            }
        }

        match &query_response.response {
            Response::Error(frame::response::Error {
                error: DbError::Unprepared { statement_id },
                ..
            }) => {
                debug!("Connection::execute: Got DbError::Unprepared - repreparing statement with id {:?}", statement_id);
                // Repreparation of a statement is needed
                self.reprepare(prepared_statement.get_statement(), prepared_statement)
                    .await?;
                let new_response = self
                    .send_request(
                        &execute_frame,
                        true,
                        prepared_statement.config.tracing,
                        cached_metadata,
                    )
                    .await?;

                if let Some(spec) = prepared_statement.get_table_spec() {
                    if let Err(e) = self.update_tablets_from_response(spec, &new_response).await {
                        tracing::warn!(
                            "Error while parsing tablet info from custom payload: {}",
                            e
                        );
                    }
                }

                Ok(new_response)
            }
            _ => Ok(query_response),
        }
    }

    /// Executes a query and fetches its results over multiple pages, using
    /// the asynchronous iterator interface.
    pub(crate) async fn query_iter(
        self: Arc<Self>,
        query: Query,
    ) -> Result<LegacyRowIterator, QueryError> {
        let consistency = query
            .config
            .determine_consistency(self.config.default_consistency);
        let serial_consistency = query.config.serial_consistency.flatten();

        RawIterator::new_for_connection_query_iter(query, self, consistency, serial_consistency)
            .await
            .map(RawIterator::into_legacy)
    }

    /// Executes a prepared statements and fetches its results over multiple pages, using
    /// the asynchronous iterator interface.
    pub(crate) async fn execute_iter(
        self: Arc<Self>,
        prepared_statement: PreparedStatement,
        values: SerializedValues,
    ) -> Result<LegacyRowIterator, QueryError> {
        let consistency = prepared_statement
            .config
            .determine_consistency(self.config.default_consistency);
        let serial_consistency = prepared_statement.config.serial_consistency.flatten();

        RawIterator::new_for_connection_execute_iter(
            prepared_statement,
            values,
            self,
            consistency,
            serial_consistency,
        )
        .await
        .map(RawIterator::into_legacy)
    }

    #[allow(dead_code)]
    pub(crate) async fn batch(
        &self,
        batch: &Batch,
        values: impl BatchValues,
    ) -> Result<QueryResult, QueryError> {
        self.batch_with_consistency(
            batch,
            values,
            batch
                .config
                .determine_consistency(self.config.default_consistency),
            batch.config.serial_consistency.flatten(),
        )
        .await
    }

    pub(crate) async fn batch_with_consistency(
        &self,
        init_batch: &Batch,
        values: impl BatchValues,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
    ) -> Result<QueryResult, QueryError> {
        let batch = self.prepare_batch(init_batch, &values).await?;

        let contexts = batch.statements.iter().map(|bs| match bs {
            BatchStatement::Query(_) => RowSerializationContext::empty(),
            BatchStatement::PreparedStatement(ps) => {
                RowSerializationContext::from_prepared(ps.get_prepared_metadata())
            }
        });

        let values = RawBatchValuesAdapter::new(values, contexts);

        let batch_frame = batch::Batch {
            statements: Cow::Borrowed(&batch.statements),
            values,
            batch_type: batch.get_type(),
            consistency,
            serial_consistency,
            timestamp: batch.get_timestamp(),
        };

        loop {
            let query_response = self
                .send_request(&batch_frame, true, batch.config.tracing, None)
                .await
                .map_err(UserRequestError::from)?;

            return match query_response.response {
                Response::Error(err) => match err.error {
                    DbError::Unprepared { statement_id } => {
                        debug!("Connection::batch: got DbError::Unprepared - repreparing statement with id {:?}", statement_id);
                        let prepared_statement = batch.statements.iter().find_map(|s| match s {
                            BatchStatement::PreparedStatement(s) if *s.get_id() == statement_id => {
                                Some(s)
                            }
                            _ => None,
                        });
                        if let Some(p) = prepared_statement {
                            self.reprepare(p.get_statement(), p).await?;
                            continue;
                        } else {
                            return Err(ProtocolError::RepreparedIdMissingInBatch.into());
                        }
                    }
                    _ => Err(err.into()),
                },
                Response::Result(_) => Ok(query_response.into_query_result()?),
                _ => Err(ProtocolError::UnexpectedResponse(
                    query_response.response.to_response_kind(),
                )
                .into()),
            };
        }
    }

    async fn prepare_batch<'b>(
        &self,
        init_batch: &'b Batch,
        values: impl BatchValues,
    ) -> Result<Cow<'b, Batch>, QueryError> {
        let mut to_prepare = HashSet::<&str>::new();

        {
            let mut values_iter = values.batch_values_iter();
            for stmt in &init_batch.statements {
                if let BatchStatement::Query(query) = stmt {
                    if let Some(false) = values_iter.is_empty_next() {
                        to_prepare.insert(&query.contents);
                    }
                } else {
                    values_iter.skip_next();
                }
            }
        }

        if to_prepare.is_empty() {
            return Ok(Cow::Borrowed(init_batch));
        }

        let mut prepared_queries = HashMap::<&str, PreparedStatement>::new();

        for query in &to_prepare {
            let prepared = self.prepare(&Query::new(query.to_string())).await?;
            prepared_queries.insert(query, prepared);
        }

        let mut batch: Cow<Batch> = Cow::Owned(Batch::new_from(init_batch));
        for stmt in &init_batch.statements {
            match stmt {
                BatchStatement::Query(query) => match prepared_queries.get(query.contents.as_str())
                {
                    Some(prepared) => batch.to_mut().append_statement(prepared.clone()),
                    None => batch.to_mut().append_statement(query.clone()),
                },
                BatchStatement::PreparedStatement(prepared) => {
                    batch.to_mut().append_statement(prepared.clone());
                }
            }
        }

        Ok(batch)
    }

    pub(crate) async fn use_keyspace(
        &self,
        keyspace_name: &VerifiedKeyspaceName,
    ) -> Result<(), QueryError> {
        // Trying to pass keyspace_name as bound value doesn't work
        // We have to send "USE " + keyspace_name
        let query: Query = match keyspace_name.is_case_sensitive {
            true => format!("USE \"{}\"", keyspace_name.as_str()).into(),
            false => format!("USE {}", keyspace_name.as_str()).into(),
        };

        let query_response = self.query_raw_unpaged(&query).await?;
        Self::verify_use_keyspace_result(keyspace_name, query_response)
    }

    fn verify_use_keyspace_result(
        keyspace_name: &VerifiedKeyspaceName,
        query_response: QueryResponse,
    ) -> Result<(), QueryError> {
        match query_response.response {
            Response::Result(result::Result::SetKeyspace(set_keyspace)) => {
                if !set_keyspace
                    .keyspace_name
                    .eq_ignore_ascii_case(keyspace_name.as_str())
                {
                    let expected_keyspace_name_lowercase = keyspace_name.as_str().to_lowercase();
                    let result_keyspace_name_lowercase = set_keyspace.keyspace_name.to_lowercase();

                    return Err(ProtocolError::UseKeyspace(
                        UseKeyspaceProtocolError::KeyspaceNameMismatch {
                            expected_keyspace_name_lowercase,
                            result_keyspace_name_lowercase,
                        },
                    )
                    .into());
                }

                Ok(())
            }
            Response::Error(err) => Err(err.into()),
            _ => Err(
                ProtocolError::UseKeyspace(UseKeyspaceProtocolError::UnexpectedResponse(
                    query_response.response.to_response_kind(),
                ))
                .into(),
            ),
        }
    }

    async fn register(
        &self,
        event_types_to_register_for: Vec<EventType>,
    ) -> Result<(), ConnectionSetupRequestError> {
        let err = |kind: ConnectionSetupRequestErrorKind| {
            ConnectionSetupRequestError::new(CqlRequestKind::Register, kind)
        };

        let register_frame = register::Register {
            event_types_to_register_for,
        };

        // Extract the response and tidy up the errors.
        match self.send_request(&register_frame, true, false, None).await {
            Ok(r) => match r.response {
                Response::Ready => Ok(()),
                Response::Error(Error { error, reason }) => {
                    Err(err(ConnectionSetupRequestErrorKind::DbError(error, reason)))
                }
                _ => Err(err(ConnectionSetupRequestErrorKind::UnexpectedResponse(
                    r.response.to_response_kind(),
                ))),
            },
            Err(e) => match e {
                RequestError::CqlRequestSerialization(e) => Err(err(e.into())),
                RequestError::BodyExtensionsParseError(e) => Err(err(e.into())),
                RequestError::CqlResponseParseError(e) => match e {
                    // Parsing the READY response cannot fail. Only remaining valid response is ERROR.
                    CqlResponseParseError::CqlErrorParseError(e) => Err(err(e.into())),
                    _ => Err(err(ConnectionSetupRequestErrorKind::UnexpectedResponse(
                        e.to_response_kind(),
                    ))),
                },
                RequestError::BrokenConnection(e) => Err(err(e.into())),
                RequestError::UnableToAllocStreamId => {
                    Err(err(ConnectionSetupRequestErrorKind::UnableToAllocStreamId))
                }
            },
        }
    }

    pub(crate) async fn fetch_schema_version(&self) -> Result<Uuid, QueryError> {
        let (version_id,) = self
            .query_unpaged(LOCAL_VERSION)
            .await?
            .into_legacy_result()?
            .single_row_typed()
            .map_err(ProtocolError::SchemaVersionFetch)?;
        Ok(version_id)
    }

    async fn send_request(
        &self,
        request: &impl SerializableRequest,
        compress: bool,
        tracing: bool,
        cached_metadata: Option<&Arc<ResultMetadata<'static>>>,
    ) -> Result<QueryResponse, RequestError> {
        let compression = if compress {
            self.config.compression
        } else {
            None
        };

        let task_response = self
            .router_handle
            .send_request(request, compression, tracing)
            .await?;

        let response = Self::parse_response(
            task_response,
            self.config.compression,
            &self.features.protocol_features,
            cached_metadata,
        )?;

        Ok(response)
    }

    fn parse_response(
        task_response: TaskResponse,
        compression: Option<Compression>,
        features: &ProtocolFeatures,
        cached_metadata: Option<&Arc<ResultMetadata<'static>>>,
    ) -> Result<QueryResponse, ResponseParseError> {
        let body_with_ext = frame::parse_response_body_extensions(
            task_response.params.flags,
            compression,
            task_response.body,
        )?;

        for warn_description in &body_with_ext.warnings {
            warn!(
                warning = warn_description.as_str(),
                "Response from the database contains a warning",
            );
        }

        let response = Response::deserialize(
            features,
            task_response.opcode,
            body_with_ext.body,
            cached_metadata,
        )?;

        Ok(QueryResponse {
            response,
            warnings: body_with_ext.warnings,
            tracing_id: body_with_ext.trace_id,
            custom_payload: body_with_ext.custom_payload,
        })
    }

    async fn run_router(
        config: ConnectionConfig,
        stream: TcpStream,
        receiver: mpsc::Receiver<Task>,
        error_sender: tokio::sync::oneshot::Sender<ConnectionError>,
        orphan_notification_receiver: mpsc::UnboundedReceiver<RequestId>,
        router_handle: Arc<RouterHandle>,
        node_address: IpAddr,
    ) -> Result<RemoteHandle<()>, std::io::Error> {
        #[cfg(feature = "ssl")]
        if let Some(ssl_config) = &config.ssl_config {
            let ssl = ssl_config.new_ssl()?;
            let mut stream = SslStream::new(ssl, stream)?;
            let _pin = Pin::new(&mut stream).connect().await;

            let (task, handle) = Self::router(
                config,
                stream,
                receiver,
                error_sender,
                orphan_notification_receiver,
                router_handle,
                node_address,
            )
            .remote_handle();
            tokio::task::spawn(task);
            return Ok(handle);
        }

        let (task, handle) = Self::router(
            config,
            stream,
            receiver,
            error_sender,
            orphan_notification_receiver,
            router_handle,
            node_address,
        )
        .remote_handle();
        tokio::task::spawn(task);
        Ok(handle)
    }

    async fn router(
        config: ConnectionConfig,
        stream: (impl AsyncRead + AsyncWrite),
        receiver: mpsc::Receiver<Task>,
        error_sender: tokio::sync::oneshot::Sender<ConnectionError>,
        orphan_notification_receiver: mpsc::UnboundedReceiver<RequestId>,
        router_handle: Arc<RouterHandle>,
        node_address: IpAddr,
    ) {
        let (read_half, write_half) = split(stream);
        // Why are we using a mutex here?
        //
        // The handler_map is supposed to be shared between reader and writer
        // futures, which will be run on the same task. The mutex should not
        // be normally required here, but Rust complains if we try to use
        // a RefCell instead of Mutex (the whole future becomes !Sync and we
        // cannot use it in tasks).
        //
        // Notice that this lock will have no contention, because reader
        // and writer futures are run on the same fiber, and both of them
        // are carefully written in such a way that they do not hold the lock
        // across .await points. Therefore, it should not be too expensive.
        let handler_map = StdMutex::new(ResponseHandlerMap::new());

        let enable_write_coalescing = config.enable_write_coalescing;

        let k = Self::keepaliver(
            router_handle,
            config.keepalive_interval,
            config.keepalive_timeout,
            node_address,
        );

        let r = Self::reader(
            BufReader::with_capacity(8192, read_half),
            &handler_map,
            config,
        );
        let w = Self::writer(
            BufWriter::with_capacity(8192, write_half),
            &handler_map,
            receiver,
            enable_write_coalescing,
        );
        let o = Self::orphaner(&handler_map, orphan_notification_receiver);

        let result = futures::try_join!(r, w, o, k);

        let error: BrokenConnectionError = match result {
            Ok(_) => return, // Connection was dropped, we can return
            Err(err) => err,
        };

        // Respond to all pending requests with the error
        let response_handlers: HashMap<i16, ResponseHandler> =
            handler_map.into_inner().unwrap().into_handlers();

        for (_, handler) in response_handlers {
            // Ignore sending error, request was dropped
            let _ = handler.response_sender.send(Err(error.clone().into()));
        }

        // If someone is listening for connection errors notify them
        let _ = error_sender.send(error.into());
    }

    async fn reader(
        mut read_half: (impl AsyncRead + Unpin),
        handler_map: &StdMutex<ResponseHandlerMap>,
        config: ConnectionConfig,
    ) -> Result<(), BrokenConnectionError> {
        loop {
            let (params, opcode, body) = frame::read_response_frame(&mut read_half)
                .await
                .map_err(BrokenConnectionErrorKind::FrameHeaderParseError)?;
            let response = TaskResponse {
                params,
                opcode,
                body,
            };

            match params.stream.cmp(&-1) {
                Ordering::Less => {
                    // The spec reserves negative-numbered streams for server-generated
                    // events. As of writing this driver, there are no other negative
                    // streams used apart from -1, so ignore it.
                    continue;
                }
                Ordering::Equal => {
                    if let Some(event_sender) = config.event_sender.as_ref() {
                        Self::handle_event(response, config.compression, event_sender)
                            .await
                            .map_err(BrokenConnectionErrorKind::CqlEventHandlingError)?
                    }
                    continue;
                }
                _ => {}
            }

            let handler_lookup_res = {
                // We are guaranteed here that handler_map will not be locked
                // by anybody else, so we can do try_lock().unwrap()
                let mut handler_map_guard = handler_map.try_lock().unwrap();
                handler_map_guard.lookup(params.stream)
            };

            use HandlerLookupResult::*;
            match handler_lookup_res {
                Handler(handler) => {
                    // Don't care if sending of the response fails. This must
                    // mean that the receiver side was impatient and is not
                    // waiting for the result anymore.
                    let _ = handler.response_sender.send(Ok(response));
                }
                Missing => {
                    // Unsolicited frame. This should not happen and indicates
                    // a bug either in the driver, or in the database
                    debug!(
                        "Received response with unexpected StreamId {}",
                        params.stream
                    );
                    return Err(BrokenConnectionErrorKind::UnexpectedStreamId(params.stream).into());
                }
                Orphaned => {
                    // Do nothing, handler was freed because this stream_id has
                    // been marked as orphaned
                }
            }
        }
    }

    fn alloc_stream_id(
        handler_map: &StdMutex<ResponseHandlerMap>,
        response_handler: ResponseHandler,
    ) -> Option<i16> {
        // We are guaranteed here that handler_map will not be locked
        // by anybody else, so we can do try_lock().unwrap()
        let mut handler_map_guard = handler_map.try_lock().unwrap();
        match handler_map_guard.allocate(response_handler) {
            Ok(stream_id) => Some(stream_id),
            Err(response_handler) => {
                error!("Could not allocate stream id");
                let _ = response_handler
                    .response_sender
                    .send(Err(RequestError::UnableToAllocStreamId));
                None
            }
        }
    }

    async fn writer(
        mut write_half: (impl AsyncWrite + Unpin),
        handler_map: &StdMutex<ResponseHandlerMap>,
        mut task_receiver: mpsc::Receiver<Task>,
        enable_write_coalescing: bool,
    ) -> Result<(), BrokenConnectionError> {
        // When the Connection object is dropped, the sender half
        // of the channel will be dropped, this task will return an error
        // and the whole worker will be stopped
        while let Some(mut task) = task_receiver.recv().await {
            let mut num_requests = 0;
            let mut total_sent = 0;
            while let Some(stream_id) = Self::alloc_stream_id(handler_map, task.response_handler) {
                let mut req = task.serialized_request;
                req.set_stream(stream_id);
                let req_data: &[u8] = req.get_data();
                total_sent += req_data.len();
                num_requests += 1;
                write_half
                    .write_all(req_data)
                    .await
                    .map_err(BrokenConnectionErrorKind::WriteError)?;
                task = match task_receiver.try_recv() {
                    Ok(t) => t,
                    Err(_) if enable_write_coalescing => {
                        // Yielding was empirically tested to inject a 1-300µs delay,
                        // much better than tokio::time::sleep's 1ms granularity.
                        // Also, yielding in a busy system let's the queue catch up with new items.
                        tokio::task::yield_now().await;
                        match task_receiver.try_recv() {
                            Ok(t) => t,
                            Err(_) => break,
                        }
                    }
                    Err(_) => break,
                }
            }
            trace!("Sending {} requests; {} bytes", num_requests, total_sent);
            write_half
                .flush()
                .await
                .map_err(BrokenConnectionErrorKind::WriteError)?;
        }

        Ok(())
    }

    // This task receives notifications from `OrphanhoodNotifier`s and tries to
    // mark streams as orphaned. It also checks count of old orphans periodically.
    // After an ald orphan threshold is reached, `orphaner` returns an error
    // causing the connection to break.
    async fn orphaner(
        handler_map: &StdMutex<ResponseHandlerMap>,
        mut orphan_receiver: mpsc::UnboundedReceiver<RequestId>,
    ) -> Result<(), BrokenConnectionError> {
        let mut interval = tokio::time::interval(OLD_AGE_ORPHAN_THRESHOLD);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // We are guaranteed here that handler_map will not be locked
                    // by anybody else, so we can do try_lock().unwrap()
                    let handler_map_guard = handler_map.try_lock().unwrap();
                    let old_orphan_count = handler_map_guard.old_orphans_count();
                    if old_orphan_count > OLD_ORPHAN_COUNT_THRESHOLD {
                        warn!(
                            "Too many old orphaned stream ids: {}",
                            old_orphan_count,
                        );
                        return Err(BrokenConnectionErrorKind::TooManyOrphanedStreamIds(old_orphan_count as u16).into())
                    }
                }
                Some(request_id) = orphan_receiver.recv() => {
                    trace!(
                        "Trying to orphan stream id associated with request_id = {}",
                        request_id,
                    );
                    let mut handler_map_guard = handler_map.try_lock().unwrap(); // Same as above
                    handler_map_guard.orphan(request_id);
                }
                else => { break }
            }
        }

        Ok(())
    }

    async fn keepaliver(
        router_handle: Arc<RouterHandle>,
        keepalive_interval: Option<Duration>,
        keepalive_timeout: Option<Duration>,
        node_address: IpAddr, // This address is only used to enrich the log messages
    ) -> Result<(), BrokenConnectionError> {
        async fn issue_keepalive_query(
            router_handle: &RouterHandle,
        ) -> Result<(), BrokenConnectionError> {
            router_handle
                .send_request(&Options, None, false)
                .await
                .map(|_| ())
                .map_err(|q_err| BrokenConnectionErrorKind::KeepaliveQueryError(q_err).into())
        }

        if let Some(keepalive_interval) = keepalive_interval {
            let mut interval = tokio::time::interval(keepalive_interval);
            interval.tick().await; // Use up the first, instant tick.

            // Default behaviour (Burst) is not suitable for sending keepalives.
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                interval.tick().await;

                let keepalive_query = issue_keepalive_query(&router_handle);
                let query_result = if let Some(timeout) = keepalive_timeout {
                    match tokio::time::timeout(timeout, keepalive_query).await {
                        Ok(res) => res,
                        Err(_) => {
                            warn!(
                                "Timed out while waiting for response to keepalive request on connection to node {}",
                                node_address
                            );
                            return Err(
                                BrokenConnectionErrorKind::KeepaliveTimeout(node_address).into()
                            );
                        }
                    }
                } else {
                    keepalive_query.await
                };
                if let Err(err) = query_result {
                    warn!(
                        "Failed to execute keepalive request on connection to node {} - {}",
                        node_address, err
                    );
                    return Err(err);
                }

                trace!(
                    "Keepalive request successful on connection to node {}",
                    node_address
                );
            }
        } else {
            // No keepalives are to be sent.
            Ok(())
        }
    }

    async fn handle_event(
        task_response: TaskResponse,
        compression: Option<Compression>,
        event_sender: &mpsc::Sender<Event>,
    ) -> Result<(), CqlEventHandlingError> {
        // Protocol features are negotiated during connection handshake.
        // However, the router is already created and sent to a different tokio
        // task before the handshake begins, therefore it's hard to cleanly
        // update the protocol features in the router at this point.
        // Making it possible would require restructuring the handshake process,
        // or passing the negotiated features via a channel/mutex/etc.
        // Fortunately, events do not need information about protocol features
        // to be serialized (yet), therefore I'm leaving this problem for
        // future implementers.
        let features = ProtocolFeatures::default(); // TODO: Use the right features

        let event = match Self::parse_response(task_response, compression, &features, None) {
            Ok(r) => match r.response {
                Response::Event(event) => event,
                _ => {
                    error!("Expected to receive Event response, got {:?}", r.response);
                    return Err(CqlEventHandlingError::UnexpectedResponse(
                        r.response.to_response_kind(),
                    ));
                }
            },
            Err(e) => match e {
                ResponseParseError::BodyExtensionsParseError(e) => return Err(e.into()),
                ResponseParseError::CqlResponseParseError(e) => match e {
                    CqlResponseParseError::CqlEventParseError(e) => return Err(e.into()),
                    // Received a response other than EVENT, but failed to deserialize it.
                    _ => {
                        return Err(CqlEventHandlingError::UnexpectedResponse(
                            e.to_response_kind(),
                        ))
                    }
                },
            },
        };

        event_sender
            .send(event)
            .await
            .map_err(|_| CqlEventHandlingError::SendError)
    }

    pub(crate) fn get_shard_info(&self) -> &Option<ShardInfo> {
        &self.features.shard_info
    }

    pub(crate) fn get_shard_aware_port(&self) -> Option<u16> {
        self.features.shard_aware_port
    }

    fn set_features(&mut self, features: ConnectionFeatures) {
        self.features = features;
    }

    pub(crate) fn get_connect_address(&self) -> SocketAddr {
        self.connect_address
    }

    async fn update_tablets_from_response(
        &self,
        table: &TableSpec<'_>,
        response: &QueryResponse,
    ) -> Result<(), TabletParsingError> {
        if let (Some(sender), Some(tablet_data)) = (
            self.config.tablet_sender.as_ref(),
            response.custom_payload.as_ref(),
        ) {
            let tablet = match RawTablet::from_custom_payload(tablet_data) {
                Some(Ok(v)) => v,
                Some(Err(e)) => return Err(e),
                None => return Ok(()),
            };
            tracing::trace!(
                "Received tablet info for table {}.{} in custom payload: {:?}",
                table.ks_name(),
                table.table_name(),
                tablet
            );
            let _ = sender.send((table.to_owned(), tablet)).await;
        }

        Ok(())
    }
}

async fn maybe_translated_addr(
    endpoint: UntranslatedEndpoint,
    address_translator: Option<&dyn AddressTranslator>,
) -> Result<SocketAddr, TranslationError> {
    match endpoint {
        UntranslatedEndpoint::ContactPoint(addr) => Ok(addr.address),
        UntranslatedEndpoint::Peer(PeerEndpoint {
            host_id,
            address,
            datacenter,
            rack,
        }) => match address {
            NodeAddr::Translatable(addr) => {
                // In this case, addr is subject to AddressTranslator.
                if let Some(translator) = address_translator {
                    let res = translator
                        .translate_address(&UntranslatedPeer {
                            host_id,
                            untranslated_address: addr,
                            datacenter,
                            rack,
                        })
                        .await;
                    if let Err(ref err) = res {
                        error!("Address translation failed for addr {}: {}", addr, err);
                    }
                    res
                } else {
                    Ok(addr)
                }
            }
            NodeAddr::Untranslatable(addr) => {
                // In this case, addr is considered to be translated, as it is the control connection's address.
                Ok(addr)
            }
        },
    }
}

/// Opens a connection and performs its setup on CQL level:
/// - performs OPTIONS/STARTUP handshake (chooses desired connections options);
/// - registers for all event types using REGISTER request (if this is control connection).
///
/// At the beginning, translates node's address, if it is subject to address translation.
pub(crate) async fn open_connection(
    endpoint: UntranslatedEndpoint,
    source_port: Option<u16>,
    config: &ConnectionConfig,
) -> Result<(Connection, ErrorReceiver), ConnectionError> {
    /* Translate the address, if applicable. */
    let addr = maybe_translated_addr(endpoint, config.address_translator.as_deref()).await?;

    /* Setup connection on TCP level and prepare for sending/receiving CQL frames. */
    let (mut connection, error_receiver) =
        Connection::new(addr, source_port, config.clone()).await?;

    /* Perform OPTIONS/SUPPORTED/STARTUP handshake. */

    // Get OPTIONS SUPPORTED by the cluster.
    let mut supported = connection.get_options().await?;

    let shard_aware_port_key = match config.is_ssl() {
        true => options::SCYLLA_SHARD_AWARE_PORT_SSL,
        false => options::SCYLLA_SHARD_AWARE_PORT,
    };

    // If this is ScyllaDB that we connected to, we received sharding information.
    let shard_info = ShardInfo::try_from(&supported.options).ok();
    let supported_compression = supported
        .options
        .remove(options::COMPRESSION)
        .unwrap_or_default();
    let shard_aware_port = supported
        .options
        .remove(shard_aware_port_key)
        .unwrap_or_default()
        .first()
        .and_then(|p| p.parse::<u16>().ok());

    // Parse nonstandard protocol extensions.
    let protocol_features = ProtocolFeatures::parse_from_supported(&supported.options);

    // At the beginning, Connection assumes no sharding and no protocol extensions;
    // now that we know them, let's turn them on in the driver.
    let features = ConnectionFeatures {
        shard_info,
        shard_aware_port,
        protocol_features,
    };
    connection.set_features(features);

    /* Prepare options that the driver opts-in in STARTUP frame. */
    let mut options = HashMap::new();
    protocol_features.add_startup_options(&mut options);

    // The only CQL protocol version supported by the driver.
    options.insert(
        Cow::Borrowed(options::CQL_VERSION),
        Cow::Borrowed(options::DEFAULT_CQL_PROTOCOL_VERSION),
    );

    // Application & driver's identity.
    config.identity.add_startup_options(&mut options);

    // Optional compression.
    if let Some(compression) = &config.compression {
        let compression_str = compression.as_str();
        if supported_compression.iter().any(|c| c == compression_str) {
            // Compression is reported to be supported by the server,
            // request it from the server
            options.insert(
                Cow::Borrowed(options::COMPRESSION),
                Cow::Borrowed(compression_str),
            );
        } else {
            // Fall back to no compression
            tracing::warn!(
                "Requested compression <{}> is not supported by the cluster. Falling back to no compression",
                compression_str
            );
            connection.config.compression = None;
        }
    }

    /* Send the STARTUP frame with all the requested options. */
    let startup_result = connection.startup(options).await?;
    match startup_result {
        NonErrorStartupResponse::Ready => {}
        NonErrorStartupResponse::Authenticate(authenticate) => {
            perform_authenticate(&mut connection, &authenticate).await?;
        }
    }

    /* If this is a control connection, REGISTER to receive all event types. */
    if connection.config.event_sender.is_some() {
        let all_event_types = vec![
            EventType::TopologyChange,
            EventType::StatusChange,
            EventType::SchemaChange,
        ];
        connection.register(all_event_types).await?;
    }

    Ok((connection, error_receiver))
}

async fn perform_authenticate(
    connection: &mut Connection,
    authenticate: &Authenticate,
) -> Result<(), ConnectionSetupRequestError> {
    let err = |kind: ConnectionSetupRequestErrorKind| {
        ConnectionSetupRequestError::new(CqlRequestKind::AuthResponse, kind)
    };

    let authenticator = &authenticate.authenticator_name as &str;

    match connection.config.authenticator {
        Some(ref authenticator_provider) => {
            let (mut response, mut auth_session) = authenticator_provider
                .start_authentication_session(authenticator)
                .await
                .map_err(|e| err(ConnectionSetupRequestErrorKind::StartAuthSessionError(e)))?;

            loop {
                match connection.authenticate_response(response).await? {
                    NonErrorAuthResponse::AuthChallenge(challenge) => {
                        response = auth_session
                            .evaluate_challenge(challenge.authenticate_message.as_deref())
                            .await
                            .map_err(|e| {
                                err(
                                    ConnectionSetupRequestErrorKind::AuthChallengeEvaluationError(
                                        e,
                                    ),
                                )
                            })?;
                    }
                    NonErrorAuthResponse::AuthSuccess(success) => {
                        auth_session
                            .success(success.success_message.as_deref())
                            .await
                            .map_err(|e| {
                                err(ConnectionSetupRequestErrorKind::AuthFinishError(e))
                            })?;
                        break;
                    }
                }
            }
        }
        None => return Err(err(ConnectionSetupRequestErrorKind::MissingAuthentication)),
    }

    Ok(())
}

async fn connect_with_source_port(
    addr: SocketAddr,
    source_port: u16,
) -> Result<TcpStream, std::io::Error> {
    match addr {
        SocketAddr::V4(_) => {
            let socket = TcpSocket::new_v4()?;
            socket.bind(SocketAddr::new(
                Ipv4Addr::new(0, 0, 0, 0).into(),
                source_port,
            ))?;
            Ok(socket.connect(addr).await?)
        }
        SocketAddr::V6(_) => {
            let socket = TcpSocket::new_v6()?;
            socket.bind(SocketAddr::new(
                Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0).into(),
                source_port,
            ))?;
            Ok(socket.connect(addr).await?)
        }
    }
}

struct OrphanageTracker {
    orphans: HashMap<i16, Instant>,
    by_orphaning_times: BTreeSet<(Instant, i16)>,
}

impl OrphanageTracker {
    fn new() -> Self {
        Self {
            orphans: HashMap::new(),
            by_orphaning_times: BTreeSet::new(),
        }
    }

    fn insert(&mut self, stream_id: i16) {
        let now = Instant::now();
        self.orphans.insert(stream_id, now);
        self.by_orphaning_times.insert((now, stream_id));
    }

    fn remove(&mut self, stream_id: i16) {
        if let Some(time) = self.orphans.remove(&stream_id) {
            self.by_orphaning_times.remove(&(time, stream_id));
        }
    }

    fn contains(&self, stream_id: i16) -> bool {
        self.orphans.contains_key(&stream_id)
    }

    fn orphans_older_than(&self, age: std::time::Duration) -> usize {
        let minimal_age = Instant::now() - age;
        self.by_orphaning_times
            .range(..(minimal_age, i16::MAX))
            .count() // This has linear time complexity, but in terms of
                     // the number of old orphans. Healthy connection - one
                     // that does not have old orphaned stream ids, will
                     // calculate this function quickly.
    }
}

struct ResponseHandlerMap {
    stream_set: StreamIdSet,
    handlers: HashMap<i16, ResponseHandler>,

    request_to_stream: HashMap<RequestId, i16>,
    orphanage_tracker: OrphanageTracker,
}

enum HandlerLookupResult {
    Orphaned,
    Handler(ResponseHandler),
    Missing,
}

impl ResponseHandlerMap {
    fn new() -> Self {
        Self {
            stream_set: StreamIdSet::new(),
            handlers: HashMap::new(),
            request_to_stream: HashMap::new(),
            orphanage_tracker: OrphanageTracker::new(),
        }
    }

    fn allocate(&mut self, response_handler: ResponseHandler) -> Result<i16, ResponseHandler> {
        if let Some(stream_id) = self.stream_set.allocate() {
            self.request_to_stream
                .insert(response_handler.request_id, stream_id);
            let prev_handler = self.handlers.insert(stream_id, response_handler);
            assert!(prev_handler.is_none());

            Ok(stream_id)
        } else {
            Err(response_handler)
        }
    }

    // Orphan stream_id (associated with this request_id) by moving it to
    // `orphanage_tracker`, and freeing its handler
    fn orphan(&mut self, request_id: RequestId) {
        if let Some(stream_id) = self.request_to_stream.get(&request_id) {
            debug!(
                "Orphaning stream_id = {} associated with request_id = {}",
                stream_id, request_id
            );
            self.orphanage_tracker.insert(*stream_id);
            self.handlers.remove(stream_id);
            self.request_to_stream.remove(&request_id);
        }
    }

    fn old_orphans_count(&self) -> usize {
        self.orphanage_tracker
            .orphans_older_than(OLD_AGE_ORPHAN_THRESHOLD)
    }

    fn lookup(&mut self, stream_id: i16) -> HandlerLookupResult {
        self.stream_set.free(stream_id);

        if self.orphanage_tracker.contains(stream_id) {
            self.orphanage_tracker.remove(stream_id);
            // This `stream_id` had been orphaned, so its handler got removed.
            // This is a valid state (as opposed to missing handler)
            return HandlerLookupResult::Orphaned;
        }

        if let Some(handler) = self.handlers.remove(&stream_id) {
            // A mapping `request_id` -> `stream_id` must be removed, to
            // prevent marking this `stream_id` as orphaned by some late
            // orphan notification.
            self.request_to_stream.remove(&handler.request_id);

            HandlerLookupResult::Handler(handler)
        } else {
            HandlerLookupResult::Missing
        }
    }

    // Retrieves the map of handlers, used after connection breaks
    // and we have to respond to all of them with an error
    fn into_handlers(self) -> HashMap<i16, ResponseHandler> {
        self.handlers
    }
}

struct StreamIdSet {
    used_bitmap: Box<[u64]>,
}

impl StreamIdSet {
    fn new() -> Self {
        const BITMAP_SIZE: usize = (i16::MAX as usize + 1) / 64;
        Self {
            used_bitmap: vec![0; BITMAP_SIZE].into_boxed_slice(),
        }
    }

    fn allocate(&mut self) -> Option<i16> {
        for (block_id, block) in self.used_bitmap.iter_mut().enumerate() {
            if *block != !0 {
                let off = block.trailing_ones();
                *block |= 1u64 << off;
                let stream_id = off as i16 + block_id as i16 * 64;
                return Some(stream_id);
            }
        }
        None
    }

    fn free(&mut self, stream_id: i16) {
        let block_id = stream_id as usize / 64;
        let off = stream_id as usize % 64;
        self.used_bitmap[block_id] &= !(1 << off);
    }
}

/// This type can only hold a valid keyspace name
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct VerifiedKeyspaceName {
    name: Arc<String>,
    pub(crate) is_case_sensitive: bool,
}

impl VerifiedKeyspaceName {
    pub(crate) fn new(
        keyspace_name: String,
        case_sensitive: bool,
    ) -> Result<Self, BadKeyspaceName> {
        Self::verify_keyspace_name_is_valid(&keyspace_name)?;

        Ok(VerifiedKeyspaceName {
            name: Arc::new(keyspace_name),
            is_case_sensitive: case_sensitive,
        })
    }

    pub(crate) fn as_str(&self) -> &str {
        self.name.as_str()
    }

    // "Keyspace names can have up to 48 alphanumeric characters and contain underscores;
    // only letters and numbers are supported as the first character."
    // https://docs.datastax.com/en/cql-oss/3.3/cql/cql_reference/cqlCreateKeyspace.html
    // Despite that cassandra accepts underscore as first character so we do too
    // https://github.com/scylladb/scylla/blob/62551b3bd382c7c47371eb3fc38173bd0cfed44d/test/cql-pytest/test_keyspace.py#L58
    // https://github.com/scylladb/scylla/blob/718976e794790253c4b24e2c78208e11f24e7502/cql3/statements/create_keyspace_statement.cc#L75
    fn verify_keyspace_name_is_valid(keyspace_name: &str) -> Result<(), BadKeyspaceName> {
        if keyspace_name.is_empty() {
            return Err(BadKeyspaceName::Empty);
        }

        // Verify that length <= 48
        let keyspace_name_len: usize = keyspace_name.chars().count(); // Only ascii allowed so it's equal to .len()
        if keyspace_name_len > 48 {
            return Err(BadKeyspaceName::TooLong(
                keyspace_name.to_string(),
                keyspace_name_len,
            ));
        }

        // Verify all chars are alphanumeric or underscore
        for character in keyspace_name.chars() {
            match character {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '_' => {}
                _ => {
                    return Err(BadKeyspaceName::IllegalCharacter(
                        keyspace_name.to_string(),
                        character,
                    ))
                }
            };
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use scylla_cql::frame::protocol_features::{
        LWT_OPTIMIZATION_META_BIT_MASK_KEY, SCYLLA_LWT_ADD_METADATA_MARK_EXTENSION,
    };
    use scylla_cql::frame::types;
    use scylla_proxy::{
        Condition, Node, Proxy, Reaction, RequestFrame, RequestOpcode, RequestReaction,
        RequestRule, ResponseFrame, ShardAwareness,
    };

    use tokio::select;
    use tokio::sync::mpsc;

    use super::ConnectionConfig;
    use crate::query::Query;
    use crate::test_utils::setup_tracing;
    use crate::transport::connection::open_connection;
    use crate::transport::node::ResolvedContactPoint;
    use crate::transport::topology::UntranslatedEndpoint;
    use crate::utils::test_utils::unique_keyspace_name;
    use crate::SessionBuilder;
    use futures::{StreamExt, TryStreamExt};
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    // Just like resolve_hostname in session.rs
    async fn resolve_hostname(hostname: &str) -> SocketAddr {
        match tokio::net::lookup_host(hostname).await {
            Ok(mut addrs) => addrs.next().unwrap(),
            Err(_) => {
                tokio::net::lookup_host((hostname, 9042)) // Port might not be specified, try default
                    .await
                    .unwrap()
                    .next()
                    .unwrap()
            }
        }
    }

    /// Tests for Connection::query_iter
    /// 1. SELECT from an empty table.
    /// 2. Create table and insert ints 0..100.
    ///    Then use query_iter with page_size set to 7 to select all 100 rows.
    /// 3. INSERT query_iter should work and not return any rows.
    #[tokio::test]
    #[cfg(not(scylla_cloud_tests))]
    async fn connection_query_iter_test() {
        setup_tracing();
        let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        let addr: SocketAddr = resolve_hostname(&uri).await;

        let (connection, _) = super::open_connection(
            UntranslatedEndpoint::ContactPoint(ResolvedContactPoint {
                address: addr,
                datacenter: None,
            }),
            None,
            &ConnectionConfig::default(),
        )
        .await
        .unwrap();
        let connection = Arc::new(connection);

        let ks = unique_keyspace_name();

        {
            // Preparation phase
            let session = SessionBuilder::new()
                .known_node_addr(addr)
                .build()
                .await
                .unwrap();
            session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks.clone()), &[]).await.unwrap();
            session.use_keyspace(ks.clone(), false).await.unwrap();
            session
                .query_unpaged("DROP TABLE IF EXISTS connection_query_iter_tab", &[])
                .await
                .unwrap();
            session
                .query_unpaged(
                    "CREATE TABLE IF NOT EXISTS connection_query_iter_tab (p int primary key)",
                    &[],
                )
                .await
                .unwrap();
        }

        connection
            .use_keyspace(&super::VerifiedKeyspaceName::new(ks, false).unwrap())
            .await
            .unwrap();

        // 1. SELECT from an empty table returns query result where rows are Some(Vec::new())
        let select_query = Query::new("SELECT p FROM connection_query_iter_tab").with_page_size(7);
        let empty_res = connection
            .clone()
            .query_iter(select_query.clone())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(empty_res.is_empty());

        // 2. Insert 100 and select using query_iter with page_size 7
        let values: Vec<i32> = (0..100).collect();
        let insert_query =
            Query::new("INSERT INTO connection_query_iter_tab (p) VALUES (?)").with_page_size(7);
        let prepared = connection.prepare(&insert_query).await.unwrap();
        let mut insert_futures = Vec::new();
        for v in &values {
            let values = prepared.serialize_values(&(*v,)).unwrap();
            let fut = async { connection.execute_raw_unpaged(&prepared, values).await };
            insert_futures.push(fut);
        }

        futures::future::try_join_all(insert_futures).await.unwrap();

        let mut results: Vec<i32> = connection
            .clone()
            .query_iter(select_query.clone())
            .await
            .unwrap()
            .into_typed::<(i32,)>()
            .map(|ret| ret.unwrap().0)
            .collect::<Vec<_>>()
            .await;
        results.sort_unstable(); // Clippy recommended to use sort_unstable instead of sort()
        assert_eq!(results, values);

        // 3. INSERT query_iter should work and not return any rows.
        let insert_res1 = connection
            .query_iter(Query::new(
                "INSERT INTO connection_query_iter_tab (p) VALUES (0)",
            ))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(insert_res1.is_empty());
    }

    #[tokio::test]
    #[cfg(not(scylla_cloud_tests))]
    async fn test_coalescing() {
        setup_tracing();
        // It's difficult to write a reliable test that checks whether coalescing
        // works like intended or not. Instead, this is a smoke test which is supposed
        // to trigger the coalescing logic and check that everything works fine
        // no matter whether coalescing is enabled or not.

        let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        let addr: SocketAddr = resolve_hostname(&uri).await;
        let ks = unique_keyspace_name();

        {
            // Preparation phase
            let session = SessionBuilder::new()
                .known_node_addr(addr)
                .build()
                .await
                .unwrap();
            session.query_unpaged(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks.clone()), &[]).await.unwrap();
            session.use_keyspace(ks.clone(), false).await.unwrap();
            session
                .query_unpaged(
                    "CREATE TABLE IF NOT EXISTS t (p int primary key, v blob)",
                    &[],
                )
                .await
                .unwrap();
        }

        let subtest = |enable_coalescing: bool, ks: String| async move {
            let (connection, _) = super::open_connection(
                UntranslatedEndpoint::ContactPoint(ResolvedContactPoint {
                    address: addr,
                    datacenter: None,
                }),
                None,
                &ConnectionConfig {
                    enable_write_coalescing: enable_coalescing,
                    ..ConnectionConfig::default()
                },
            )
            .await
            .unwrap();
            let connection = Arc::new(connection);

            connection
                .use_keyspace(&super::VerifiedKeyspaceName::new(ks, false).unwrap())
                .await
                .unwrap();

            connection.query_unpaged("TRUNCATE t").await.unwrap();

            let mut futs = Vec::new();

            const NUM_BATCHES: i32 = 10;

            for batch_size in 0..NUM_BATCHES {
                // Each future should issue more and more queries in the first poll
                let base = arithmetic_sequence_sum(batch_size);
                let conn = connection.clone();
                futs.push(tokio::task::spawn(async move {
                    let futs = (base..base + batch_size).map(|j| {
                        let q = Query::new("INSERT INTO t (p, v) VALUES (?, ?)");
                        let conn = conn.clone();
                        async move {
                            let prepared = conn.prepare(&q).await.unwrap();
                            let values = prepared
                                .serialize_values(&(j, vec![j as u8; j as usize]))
                                .unwrap();
                            let response =
                                conn.execute_raw_unpaged(&prepared, values).await.unwrap();
                            // QueryResponse might contain an error - make sure that there were no errors
                            let _nonerror_response =
                                response.into_non_error_query_response().unwrap();
                        }
                    });
                    let _joined: Vec<()> = futures::future::join_all(futs).await;
                }));

                tokio::task::yield_now().await;
            }

            let _joined: Vec<()> = futures::future::try_join_all(futs).await.unwrap();

            // Check that everything was written properly
            let range_end = arithmetic_sequence_sum(NUM_BATCHES);
            let mut results = connection
                .query_unpaged("SELECT p, v FROM t")
                .await
                .unwrap()
                .into_legacy_result()
                .unwrap()
                .rows_typed::<(i32, Vec<u8>)>()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            results.sort();

            let expected = (0..range_end)
                .map(|i| (i, vec![i as u8; i as usize]))
                .collect::<Vec<_>>();

            assert_eq!(results, expected);
        };

        subtest(true, ks.clone()).await;
        subtest(false, ks.clone()).await;
    }

    // Returns the sum of integral numbers in the range [0..n)
    fn arithmetic_sequence_sum(n: i32) -> i32 {
        n * (n - 1) / 2
    }

    #[tokio::test]
    async fn test_lwt_optimisation_mark_negotiation() {
        setup_tracing();
        const MASK: &str = "2137";

        let lwt_optimisation_entry = format!("{}={}", LWT_OPTIMIZATION_META_BIT_MASK_KEY, MASK);

        let proxy_addr = SocketAddr::new(scylla_proxy::get_exclusive_local_address(), 9042);

        let config = ConnectionConfig::default();

        let (startup_tx, mut startup_rx) = mpsc::unbounded_channel();

        let options_without_lwt_optimisation_support = HashMap::<String, Vec<String>>::new();
        let options_with_lwt_optimisation_support = [(
            SCYLLA_LWT_ADD_METADATA_MARK_EXTENSION.into(),
            vec![lwt_optimisation_entry.clone()],
        )]
        .into_iter()
        .collect::<HashMap<String, Vec<String>>>();

        let make_rules = |options| {
            vec![
                RequestRule(
                    Condition::RequestOpcode(RequestOpcode::Options),
                    RequestReaction::forge_response(Arc::new(move |frame: RequestFrame| {
                        ResponseFrame::forged_supported(frame.params, &options).unwrap()
                    })),
                ),
                RequestRule(
                    Condition::RequestOpcode(RequestOpcode::Startup),
                    RequestReaction::drop_frame().with_feedback_when_performed(startup_tx.clone()),
                ),
            ]
        };

        let mut proxy = Proxy::builder()
            .with_node(
                Node::builder()
                    .proxy_address(proxy_addr)
                    .request_rules(make_rules(options_without_lwt_optimisation_support))
                    .build_dry_mode(),
            )
            .build()
            .run()
            .await
            .unwrap();

        // We must interrupt the driver's full connection opening, because our proxy does not interact further after Startup.
        let (startup_without_lwt_optimisation, _shard) = select! {
            _ = open_connection(UntranslatedEndpoint::ContactPoint(ResolvedContactPoint{address: proxy_addr, datacenter: None}), None, &config) => unreachable!(),
            startup = startup_rx.recv() => startup.unwrap(),
        };

        proxy.running_nodes[0]
            .change_request_rules(Some(make_rules(options_with_lwt_optimisation_support)));

        let (startup_with_lwt_optimisation, _shard) = select! {
            _ = open_connection(UntranslatedEndpoint::ContactPoint(ResolvedContactPoint{address: proxy_addr, datacenter: None}), None, &config) => unreachable!(),
            startup = startup_rx.recv() => startup.unwrap(),
        };

        let _ = proxy.finish().await;

        let chosen_options =
            types::read_string_map(&mut &*startup_without_lwt_optimisation.body).unwrap();
        assert!(!chosen_options.contains_key(SCYLLA_LWT_ADD_METADATA_MARK_EXTENSION));

        let chosen_options =
            types::read_string_map(&mut &startup_with_lwt_optimisation.body[..]).unwrap();
        assert!(chosen_options.contains_key(SCYLLA_LWT_ADD_METADATA_MARK_EXTENSION));
        assert_eq!(
            chosen_options
                .get(SCYLLA_LWT_ADD_METADATA_MARK_EXTENSION)
                .unwrap(),
            &lwt_optimisation_entry
        )
    }

    #[tokio::test]
    #[ntest::timeout(20000)]
    #[cfg(not(scylla_cloud_tests))]
    async fn connection_is_closed_on_no_response_to_keepalives() {
        use crate::transport::errors::BrokenConnectionErrorKind;

        setup_tracing();

        let proxy_addr = SocketAddr::new(scylla_proxy::get_exclusive_local_address(), 9042);
        let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        let node_addr: SocketAddr = resolve_hostname(&uri).await;

        let drop_options_rule = RequestRule(
            Condition::RequestOpcode(RequestOpcode::Options),
            RequestReaction::drop_frame(),
        );

        let config = ConnectionConfig {
            keepalive_interval: Some(Duration::from_millis(500)),
            keepalive_timeout: Some(Duration::from_secs(1)),
            ..Default::default()
        };

        let mut proxy = Proxy::builder()
            .with_node(
                Node::builder()
                    .proxy_address(proxy_addr)
                    .real_address(node_addr)
                    .shard_awareness(ShardAwareness::QueryNode)
                    .build(),
            )
            .build()
            .run()
            .await
            .unwrap();

        // Setup connection normally, without obstruction
        let (conn, mut error_receiver) = open_connection(
            UntranslatedEndpoint::ContactPoint(ResolvedContactPoint {
                address: proxy_addr,
                datacenter: None,
            }),
            None,
            &config,
        )
        .await
        .unwrap();

        // As everything is normal, these queries should succeed.
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            conn.query_unpaged("SELECT host_id FROM system.local")
                .await
                .unwrap();
        }
        // As everything is normal, no error should have been reported.
        assert_matches!(
            error_receiver.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        );

        // Set up proxy to drop keepalive messages
        proxy.running_nodes[0].change_request_rules(Some(vec![drop_options_rule]));

        // Wait until keepaliver gots impatient and terminates router.
        // Then, the error from keepaliver will be propagated to the error receiver.
        let err = error_receiver.await.unwrap();
        let err_inner: &BrokenConnectionErrorKind = match err {
            crate::transport::connection::ConnectionError::BrokenConnection(ref e) => {
                e.downcast_ref().unwrap()
            }
            _ => panic!("Bad error type. Expected keepalive timeout."),
        };
        assert_matches!(err_inner, BrokenConnectionErrorKind::KeepaliveTimeout(_));

        // As the router is invalidated, all further queries should immediately
        // return error.
        conn.query_unpaged("SELECT host_id FROM system.local")
            .await
            .unwrap_err();

        let _ = proxy.finish().await;
    }
}
