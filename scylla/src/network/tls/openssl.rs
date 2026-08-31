//! The OpenSSL 0.10 TLS backend of the driver.

use std::net::SocketAddr;

use openssl::error::ErrorStack;
use openssl::ssl::{Ssl, SslConnectorBuilder, SslContext, SslContextBuilder};

use crate::client::session::TlsContext;

/// A TLS context backed by OpenSSL 0.10.
///
/// This is the recommended way of configuring the OpenSSL backend of the driver.
/// Create it from an [`SslConnectorBuilder`] with [`OpenSsl010Config::new`].
#[derive(Clone)] // Cheaply clonable - reference counted.
pub struct OpenSsl010Config {
    context: SslContext,
}

impl OpenSsl010Config {
    /// Creates a new context from an [`SslConnectorBuilder`].
    ///
    /// This is the recommended constructor: [`SslConnector`](openssl::ssl::SslConnector)
    /// comes with safe defaults, most notably peer certificate verification.
    pub fn new(mut builder: SslConnectorBuilder) -> Self {
        // `SslConnectorBuilder` cannot be unwrapped into its `SslContextBuilder`,
        // but it dereferences to it, which is enough to configure it.
        Self::configure(&mut builder);
        Self {
            context: builder.build().into_context(),
        }
    }

    /// Creates a new context from a raw [`SslContextBuilder`].
    ///
    /// This is **dangerous**: unlike [`SslConnector`](openssl::ssl::SslConnector),
    /// a bare [`SslContextBuilder`] has insecure defaults - in particular, it does
    /// not verify the peer's certificate at all. Prefer [`OpenSsl010Config::new`]
    /// unless you really need full control over the context.
    pub fn from_dangerous_builder(mut builder: SslContextBuilder) -> Self {
        Self::configure(&mut builder);
        Self {
            context: builder.build(),
        }
    }

    /// The single place where the driver configures a context builder,
    /// shared by both constructors.
    // For now it is empty. It will be used in the future for TLS tickets,
    // and possibly other stuff.
    fn configure(_builder: &mut SslContextBuilder) {}

    /// Creates a fresh per-connection [`Ssl`] object out of this context.
    pub(crate) fn new_ssl(&self, node_address: SocketAddr) -> Result<Ssl, ErrorStack> {
        new_ssl(&self.context, node_address)
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
