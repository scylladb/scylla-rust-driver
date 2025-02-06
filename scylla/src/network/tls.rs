//! This module contains abstractions related to the TLS layer of driver connections.
//!
//! The full picture looks like this:
//!
//! ┌─←─ TlsContext (openssl::SslContext / rustls::ClientConfig)
//! │
//! ├─←─ CloudConfig (powered by either TLS backend)
//! │
//! │ gets wrapped in
//! │
//! ↳TlsProvider (same for all connections)
//!   │
//!   │ produces
//!   │
//!   ↳TlsConfig (specific for the particular connection)
//!     │
//!     │ produces
//!     │
//!     ↳Tls (wrapper over TCP stream which adds encryption)

#[cfg(not(any(feature = "openssl-010", feature = "rustls-023")))]
compile_error!(
    r#""__tls" feature requires a TLS backend: at least one of ["openssl-010", "rustls-023"] is needed"#
);

use std::io;
#[cfg(feature = "unstable-cloud")]
use std::sync::Arc;

#[cfg(feature = "unstable-cloud")]
use tracing::warn;
#[cfg(feature = "unstable-cloud")]
use uuid::Uuid;

use crate::client::session::TlsContext;
#[cfg(feature = "unstable-cloud")]
use crate::cloud::CloudConfig;
#[cfg(feature = "unstable-cloud")]
use crate::cluster::metadata::PeerEndpoint;
use crate::cluster::metadata::UntranslatedEndpoint;
#[cfg(feature = "unstable-cloud")]
use crate::cluster::node::ResolvedContactPoint;

/// Abstraction capable of producing TlsConfig for connections on-demand.
#[derive(Clone)] // Cheaply clonable (reference-counted)
pub(crate) enum TlsProvider {
    GlobalContext(TlsContext),
    #[cfg(feature = "unstable-cloud")]
    ScyllaCloud(Arc<CloudConfig>),
}

impl TlsProvider {
    // Used in case when the user provided their own TlsContext to be used in all connections.
    pub(crate) fn new_with_global_context(context: TlsContext) -> Self {
        Self::GlobalContext(context)
    }

    // Used in the cloud case.
    #[cfg(feature = "unstable-cloud")]
    pub(crate) fn new_cloud(cloud_config: Arc<CloudConfig>) -> Self {
        Self::ScyllaCloud(cloud_config)
    }

    pub(crate) fn make_tls_config(
        &self,
        #[allow(unused)] endpoint: &UntranslatedEndpoint,
    ) -> Option<TlsConfig> {
        match self {
            TlsProvider::GlobalContext(context) => {
                Some(TlsConfig::new_with_global_context(context.clone()))
            }
            #[cfg(feature = "unstable-cloud")]
            TlsProvider::ScyllaCloud(cloud_config) => {
                let (host_id, address, dc) = match *endpoint {
                    UntranslatedEndpoint::ContactPoint(ResolvedContactPoint {
                        address,
                        ref datacenter,
                    }) => (None, address, datacenter.as_deref()), // FIXME: Pass DC in ContactPoint
                    UntranslatedEndpoint::Peer(PeerEndpoint {
                        host_id,
                        address,
                        ref datacenter,
                        ..
                    }) => (Some(host_id), address.into_inner(), datacenter.as_deref()),
                };

                cloud_config.make_tls_config_for_scylla_cloud_host(host_id, dc, address)
                        // inspect_err() is stable since 1.76.
                        // TODO: use inspect_err once we bump MSRV to at least 1.76.
                        .map_err(|err| {
                            warn!(
                                "TlsProvider for SNI connection to Scylla Cloud node {{ host_id={:?}, dc={:?} at {} }} could not be set up: {}\n Proceeding with attempting probably nonworking connection",
                                host_id,
                                dc,
                                address,
                                err
                            );
                        }).ok().flatten()
            }
        }
    }
}

/// This struct encapsulates all TLS-regarding configuration and helps pass it tidily through the code.
//
// There are 3 possible options for TlsConfig, whose behaviour is somewhat subtle.
// Option 1: No TLS configuration. Then it is None every time.
// Option 2: User-provided global TlsContext. Then, a TlsConfig is created upon Session creation
// and henceforth stored in the ConnectionConfig.
// Option 3: Serverless Cloud. The Option<TlsConfig> remains None in ConnectionConfig until it reaches
// NodeConnectionPool::new(). Inside that function, the field is mutated to contain TlsConfig specific
// for the particular node. (The TlsConfig must be different, because SNIs differ for different nodes.)
// Thenceforth, all connections to that node share the same TlsConfig.
#[derive(Clone)]
pub(crate) struct TlsConfig {
    context: TlsContext,
    #[cfg(feature = "unstable-cloud")]
    sni: Option<String>,
}

pub(crate) enum Tls {
    #[cfg(feature = "openssl-010")]
    OpenSsl010(openssl::ssl::Ssl),
    #[cfg(feature = "rustls-023")]
    Rustls023 {
        connector: tokio_rustls::TlsConnector,
        #[cfg(feature = "unstable-cloud")]
        sni: Option<rustls::pki_types::ServerName<'static>>,
    },
}

/// A wrapper around a TLS error.
///
/// The original error came from one of the supported TLS backends.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
#[non_exhaustive]
pub enum TlsError {
    #[cfg(feature = "openssl-010")]
    OpenSsl010(openssl::error::ErrorStack),
    #[cfg(feature = "rustls-023")]
    InvalidName(rustls::pki_types::InvalidDnsNameError),
    #[cfg(feature = "rustls-023")]
    PemParse(rustls::pki_types::pem::Error),
    #[cfg(feature = "rustls-023")]
    Rustls023(rustls::Error),
}

impl From<TlsError> for io::Error {
    fn from(value: TlsError) -> Self {
        match value {
            #[cfg(feature = "openssl-010")]
            TlsError::OpenSsl010(e) => e.into(),
            #[cfg(feature = "rustls-023")]
            TlsError::InvalidName(e) => io::Error::new(io::ErrorKind::Other, e),
            #[cfg(feature = "rustls-023")]
            TlsError::PemParse(e) => io::Error::new(io::ErrorKind::Other, e),
            #[cfg(feature = "rustls-023")]
            TlsError::Rustls023(e) => io::Error::new(io::ErrorKind::Other, e),
        }
    }
}

#[cfg(feature = "openssl-010")]
impl From<openssl::error::ErrorStack> for TlsError {
    fn from(error: openssl::error::ErrorStack) -> Self {
        TlsError::OpenSsl010(error)
    }
}

#[cfg(feature = "rustls-023")]
impl From<rustls::pki_types::InvalidDnsNameError> for TlsError {
    fn from(error: rustls::pki_types::InvalidDnsNameError) -> Self {
        TlsError::InvalidName(error)
    }
}

#[cfg(feature = "rustls-023")]
impl From<rustls::pki_types::pem::Error> for TlsError {
    fn from(error: rustls::pki_types::pem::Error) -> Self {
        TlsError::PemParse(error)
    }
}

#[cfg(feature = "rustls-023")]
impl From<rustls::Error> for TlsError {
    fn from(error: rustls::Error) -> Self {
        TlsError::Rustls023(error)
    }
}

impl TlsConfig {
    // Used in case when the user provided their own TlsContext to be used in all connections.
    pub(crate) fn new_with_global_context(context: TlsContext) -> Self {
        Self {
            context,
            #[cfg(feature = "unstable-cloud")]
            sni: None,
        }
    }

    // Used in case of Serverless Cloud connections.
    #[cfg(feature = "unstable-cloud")]
    pub(crate) fn new_for_sni(
        context: TlsContext,
        domain_name: &str,
        host_id: Option<Uuid>,
    ) -> Self {
        Self {
            context,
            #[cfg(feature = "unstable-cloud")]
            sni: Some(if let Some(host_id) = host_id {
                format!("{}.{}", host_id, domain_name)
            } else {
                domain_name.into()
            }),
        }
    }

    // Produces a new Tls object that is able to wrap a TCP stream.
    pub(crate) fn new_tls(&self) -> Result<Tls, TlsError> {
        let tls = match &self.context {
            #[cfg(feature = "openssl-010")]
            TlsContext::OpenSsl010(context) => {
                #[allow(unused_mut)]
                let mut ssl = openssl::ssl::Ssl::new(context)?;
                #[cfg(feature = "unstable-cloud")]
                if let Some(sni) = self.sni.as_ref() {
                    ssl.set_hostname(sni)?;
                }
                Tls::OpenSsl010(ssl)
            }
            #[cfg(feature = "rustls-023")]
            TlsContext::Rustls023(config) => {
                let connector = tokio_rustls::TlsConnector::from(config.clone());
                #[cfg(feature = "unstable-cloud")]
                let sni = self
                    .sni
                    .as_deref()
                    .map(rustls::pki_types::ServerName::try_from)
                    .transpose()?
                    .map(|s| s.to_owned());

                Tls::Rustls023 {
                    connector,
                    #[cfg(feature = "unstable-cloud")]
                    sni,
                }
            }
        };

        Ok(tls)
    }
}
