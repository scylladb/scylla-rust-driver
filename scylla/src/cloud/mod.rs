mod config;

use std::net::SocketAddr;

pub use config::CloudConfig;
pub use config::CloudConfigError;
use tracing::warn;
use uuid::Uuid;

use crate::client::session::TlsContext;
use crate::network::{ConnectionConfig, TlsConfig, TlsError};

pub(crate) fn set_tls_config_for_scylla_cloud_host(
    host_id: Option<Uuid>,
    dc: Option<&str>,
    proxy_address: SocketAddr,
    connection_config: &mut ConnectionConfig,
) -> Result<(), TlsError> {
    if connection_config.tls_config.is_some() {
        // This can only happen if the user builds SessionConfig by hand, as SessionBuilder in cloud mode prevents setting custom TlsContext.
        warn!(
            "Overriding user-provided TlsContext with Scylla Cloud TlsContext due \
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
            #[cfg(feature = "openssl-010")]
            config::TlsInfo::OpenSsl010 { key, cert } => {
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
                TlsContext::OpenSsl010(context)
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
