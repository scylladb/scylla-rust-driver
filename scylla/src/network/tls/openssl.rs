//! The OpenSSL 0.10 TLS backend of the driver.

use openssl::error::ErrorStack;
use openssl::ssl::{Ssl, SslContext};

/// Creates a fresh per-connection [`Ssl`] object out of an [`SslContext`].
pub(crate) fn new_ssl(context: &SslContext) -> Result<Ssl, ErrorStack> {
    let mut ssl = Ssl::new(context)?;
    ssl.set_connect_state();
    Ok(ssl)
}
