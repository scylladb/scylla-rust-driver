mod config;

use std::net::SocketAddr;

pub use config::CloudConfig;
pub use config::CloudConfigError;
use tracing::warn;
use uuid::Uuid;

use crate::client::session::TlsContext;
use crate::network::{ConnectionConfig, TlsConfig};

pub(crate) fn set_tls_config_for_scylla_cloud_host(
    host_id: Option<Uuid>,
    dc: Option<&str>,
    proxy_address: SocketAddr,
    connection_config: &mut ConnectionConfig,
) -> Result<(), crate::network::TlsError> {
    if connection_config.tls_config.is_some() {
        // This can only happen if the user builds SessionConfig by hand, as SessionBuilder in cloud mode prevents setting custom TlsConfig.
        warn!(
            "Overriding user-provided TlsConfig with Scylla Cloud TlsConfig due \
                to CloudConfig being provided. This is certainly an API misuse - Cloud \
                may not be combined with user's own TLS config."
        )
    }

    let cloud_config = connection_config
        .cloud_config
        .as_deref()
        .expect("BUG: CloudConfig presence in ConnectionConfig should have been checked before calling this function");
    let datacenter = dc.and_then(|dc| cloud_config.get_datacenters().get(dc));
    if let Some(datacenter) = datacenter {
        let domain_name = datacenter.get_node_domain();
        let auth_info = cloud_config.get_current_auth_info();

        let tls_context = match auth_info.get_tls() {
            #[cfg(feature = "openssl")]
            config::TlsInfo::OpenSsl { key, cert } => {
                use openssl::ssl::{SslContext, SslMethod, SslVerifyMode};
                let mut builder = SslContext::builder(SslMethod::tls())?;
                builder.set_verify(if datacenter.get_insecure_skip_tls_verify() {
                    SslVerifyMode::NONE
                } else {
                    SslVerifyMode::PEER
                });
                let ca = datacenter.openssl_ca();
                builder.cert_store_mut().add_cert(ca.clone())?;
                builder.set_certificate(cert)?;
                builder.set_private_key(key)?;
                let context = builder.build();
                TlsContext::OpenSsl(context)
            }
            #[cfg(feature = "rustls")]
            config::TlsInfo::Rustls { cert_chain, key } => {
                use rustls::ClientConfig;
                use std::sync::Arc;

                let mut root_store = rustls::RootCertStore::empty();
                root_store.add(datacenter.rustls_ca().clone())?;
                let builder = ClientConfig::builder();
                let builder = if datacenter.get_insecure_skip_tls_verify() {
                    let supported = builder.crypto_provider().signature_verification_algorithms;
                    builder
                        .dangerous()
                        .with_custom_certificate_verifier(Arc::new(NoCertificateVerification {
                            supported,
                        }))
                } else {
                    builder.with_root_certificates(root_store)
                };

                let config = builder.with_client_auth_cert(cert_chain.clone(), key.clone_key())?;
                TlsContext::Rustls(Arc::new(config))
            }
        };

        let tls_config = TlsConfig::new_for_sni(tls_context, domain_name, host_id);
        connection_config.tls_config = Some(tls_config);
    } else {
        warn!("Datacenter {:?} of node {:?} with addr {} not described in cloud config. Proceeding without setting SNI for the node, which will most probably result in nonworking connections,.",
               dc, host_id, proxy_address);
    }
    Ok(())
}

#[cfg(feature = "rustls")]
#[derive(Debug)]
struct NoCertificateVerification {
    supported: rustls::crypto::WebPkiSupportedAlgorithms,
}

#[cfg(feature = "rustls")]
impl rustls::client::danger::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.supported.supported_schemes()
    }
}
