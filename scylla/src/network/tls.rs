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

/// Abstraction capable of producing [TlsConfig] for connections on-demand.
#[derive(Clone)] // Cheaply clonable (reference-counted)
pub(crate) enum TlsProvider {
    GlobalContext(TlsContext),
    #[cfg(feature = "unstable-cloud")]
    ScyllaCloud(Arc<CloudConfig>),
}

impl TlsProvider {
    /// Used in case when the user provided their own [TlsContext] to be used in all connections.
    pub(crate) fn new_with_global_context(context: TlsContext) -> Self {
        Self::GlobalContext(context)
    }

    /// Used in the cloud case.
    #[cfg(feature = "unstable-cloud")]
    pub(crate) fn new_cloud(cloud_config: Arc<CloudConfig>) -> Self {
        Self::ScyllaCloud(cloud_config)
    }

    /// Produces a [TlsConfig] that is specific for the given endpoint.
    pub(crate) fn make_tls_config(
        &self,
        // Currently, this is only used for cloud; but it makes abstract sense to pass endpoint here
        // also for non-cloud cases, so let's just allow(unused).
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
                    .inspect_err(|err| {
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

/// Encapsulates TLS-regarding configuration that is specific for a particular endpoint.
///
/// Both use cases are supported:
/// 1. User-provided global TlsContext. Then, the global TlsContext is simply cloned here.
/// 2. Serverless Cloud. Then the TlsContext is customized for the given endpoint,
///    and its SNI information is stored alongside.
#[derive(Clone)]
pub(crate) struct TlsConfig {
    context: TlsContext,
    #[cfg(feature = "unstable-cloud")]
    sni: Option<String>,
}

/// An abstraction over connection's TLS layer which holds its state and configuration.
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
    OpenSsl010(#[from] openssl::error::ErrorStack),
    #[cfg(feature = "rustls-023")]
    InvalidName(#[from] rustls::pki_types::InvalidDnsNameError),
    #[cfg(feature = "rustls-023")]
    PemParse(#[from] rustls::pki_types::pem::Error),
    #[cfg(feature = "rustls-023")]
    Rustls023(#[from] rustls::Error),
}

impl From<TlsError> for io::Error {
    fn from(value: TlsError) -> Self {
        match value {
            #[cfg(feature = "openssl-010")]
            TlsError::OpenSsl010(e) => e.into(),
            #[cfg(feature = "rustls-023")]
            TlsError::InvalidName(e) => io::Error::other(e),
            #[cfg(feature = "rustls-023")]
            TlsError::PemParse(e) => io::Error::other(e),
            #[cfg(feature = "rustls-023")]
            TlsError::Rustls023(e) => io::Error::other(e),
        }
    }
}

impl TlsConfig {
    /// Used in case when the user provided their own TlsContext to be used in all connections.
    pub(crate) fn new_with_global_context(context: TlsContext) -> Self {
        Self {
            context,
            #[cfg(feature = "unstable-cloud")]
            sni: None,
        }
    }

    /// Used in case of Serverless Cloud connections.
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
                format!("{host_id}.{domain_name}")
            } else {
                domain_name.into()
            }),
        }
    }

    /// Produces a new Tls object that is able to wrap a TCP stream.
    pub(crate) fn new_tls(&self) -> Result<Tls, TlsError> {
        match self.context {
            #[cfg(feature = "openssl-010")]
            TlsContext::OpenSsl010(ref context) => {
                #[allow(unused_mut)]
                let mut ssl = openssl::ssl::Ssl::new(context)?;
                #[cfg(feature = "unstable-cloud")]
                if let Some(sni) = self.sni.as_ref() {
                    ssl.set_hostname(sni)?;
                }
                Ok(Tls::OpenSsl010(ssl))
            }
            #[cfg(feature = "rustls-023")]
            TlsContext::Rustls023(ref config) => {
                let connector = tokio_rustls::TlsConnector::from(config.clone());
                #[cfg(feature = "unstable-cloud")]
                let sni = self
                    .sni
                    .as_deref()
                    .map(rustls::pki_types::ServerName::try_from)
                    .transpose()?
                    .map(|s| s.to_owned());

                Ok(Tls::Rustls023 {
                    connector,
                    #[cfg(feature = "unstable-cloud")]
                    sni,
                })
            }
        }
    }
}
