//! This module contains abstractions related to the TLS layer of driver connections.
//!
//! The full picture looks like this:
//!
//! ┌─←─ TlsContext (openssl::SslContext / rustls::ClientConfig)
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
//!
//! Currently it is not strictly necessary to have both `TlsContext` and `TlsProvider`.
//! The reason for having both is historical. It is about the old Scylla Cloud Serverless, which
//! used SNI proxy to allow driver to connect to the nodes. `TlsProvider` used to have `CloudConfig` variant,
//! which stored both TLS context and cloud configuration. This would be then transformed into `TlsConfig`,
//! which had additional field for SNI hostname.
//! We could remove `TlsProvider`, and maybe even `TlsConfig`, but for now we kept it - it may be useful in the future,
//! for example if we wanted to support more elastic hostname verification.

use std::io;

use crate::client::session::TlsContext;
use crate::cluster::metadata::UntranslatedEndpoint;

/// Abstraction capable of producing [TlsConfig] for connections on-demand.
#[derive(Clone)] // Cheaply clonable (reference-counted)
pub(crate) enum TlsProvider {
    GlobalContext(TlsContext),
}

impl TlsProvider {
    /// Used in case when the user provided their own [TlsContext] to be used in all connections.
    pub(crate) fn new_with_global_context(context: TlsContext) -> Self {
        Self::GlobalContext(context)
    }

    /// Produces a [TlsConfig] that is specific for the given endpoint.
    pub(crate) fn make_tls_config(
        &self,
        // This was only used for serverless cloud; but it makes abstract sense to pass endpoint here
        // also for non-cloud cases, so let's just allow(unused).
        #[expect(unused)] endpoint: &UntranslatedEndpoint,
    ) -> Option<TlsConfig> {
        match self {
            TlsProvider::GlobalContext(context) => {
                #[cfg_attr(
                    not(any(feature = "openssl-010", feature = "rustls-023")),
                    // TODO: make this expect() once MSRV is 1.92+.
                    allow(unreachable_code)
                )]
                Some(TlsConfig::new_with_global_context(context.clone()))
            }
        }
    }
}

/// Encapsulates TLS-regarding configuration that is specific for a particular endpoint.
///
/// Currently we don't need any host-specific parameters, but that may change in the future.
#[derive(Clone)]
pub(crate) struct TlsConfig {
    context: TlsContext,
}

/// An abstraction over connection's TLS layer which holds its state and configuration.
pub(crate) enum Tls {
    #[cfg(feature = "openssl-010")]
    OpenSsl010(openssl::ssl::Ssl),
    #[cfg(feature = "rustls-023")]
    Rustls023 {
        connector: tokio_rustls::TlsConnector,
    },
}

/// A wrapper around a TLS error.
///
/// The original error came from one of the supported TLS backends.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
#[non_exhaustive]
pub enum TlsError {
    /// Collection of errors coming from OpenSSL 0.10.
    #[cfg(feature = "openssl-010")]
    OpenSsl010(#[from] openssl::error::ErrorStack),
    /// Invalid DNS name error, coming from rustls 0.23.
    #[cfg(feature = "rustls-023")]
    InvalidName(#[from] rustls::pki_types::InvalidDnsNameError),
    /// PEM parsing error, coming from rustls 0.23.
    #[cfg(feature = "rustls-023")]
    PemParse(#[from] rustls::pki_types::pem::Error),
    /// General error coming from rustls 0.23.
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
        Self { context }
    }

    /// Produces a new Tls object that is able to wrap a TCP stream.
    pub(crate) fn new_tls(&self) -> Result<Tls, TlsError> {
        match self.context {
            #[cfg(feature = "openssl-010")]
            TlsContext::OpenSsl010(ref context) => {
                #[allow(unused_mut)]
                let mut ssl = openssl::ssl::Ssl::new(context)?;
                ssl.set_connect_state();
                Ok(Tls::OpenSsl010(ssl))
            }
            #[cfg(feature = "rustls-023")]
            TlsContext::Rustls023(ref config) => {
                let connector = tokio_rustls::TlsConnector::from(config.clone());

                Ok(Tls::Rustls023 { connector })
            }
        }
    }
}
