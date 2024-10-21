mod config;

use std::net::SocketAddr;

pub use config::CloudConfig;
pub use config::CloudConfigError;
use openssl::{
    error::ErrorStack,
    ssl::{SslContext, SslMethod, SslVerifyMode},
};
use tracing::warn;
use uuid::Uuid;

use crate::transport::connection::{ConnectionConfig, SslConfig};

pub(crate) fn set_ssl_config_for_scylla_cloud_host(
    host_id: Option<Uuid>,
    dc: Option<&str>,
    proxy_address: SocketAddr,
    connection_config: &mut ConnectionConfig,
) -> Result<(), ErrorStack> {
    if connection_config.ssl_config.is_some() {
        // This can only happen if the user builds SessionConfig by hand, as SessionBuilder in cloud mode prevents setting custom SslContext.
        warn!(
            "Overriding user-provided SslContext with Scylla Cloud SslContext due \
                to CloudConfig being provided. This is certainly an API misuse - Cloud \
                may not be combined with user's own SSL config."
        )
    }

    let cloud_config = connection_config
        .cloud_config
        .as_deref()
        .expect("BUG: CloudConfig presence in ConnectionConfig should have been checked before calling this function");
    let datacenter = dc.and_then(|dc| cloud_config.get_datacenters().get(dc));
    if let Some(datacenter) = datacenter {
        let domain_name = datacenter.get_node_domain();
        let ca = datacenter.get_certificate_authority();
        let auth_info = cloud_config.get_current_auth_info();
        let key = auth_info.get_key();
        let cert = auth_info.get_cert();

        let ssl_context = {
            let mut builder = SslContext::builder(SslMethod::tls())?;
            builder.set_verify(if datacenter.get_insecure_skip_tls_verify() {
                SslVerifyMode::NONE
            } else {
                SslVerifyMode::PEER
            });
            builder.cert_store_mut().add_cert(ca.clone())?;
            builder.set_certificate(cert)?;
            builder.set_private_key(key)?;
            builder.build()
        };
        let ssl_config = SslConfig::new_for_sni(ssl_context, domain_name, host_id);
        connection_config.ssl_config = Some(ssl_config);
    } else {
        warn!("Datacenter {:?} of node {:?} with addr {} not described in cloud config. Proceeding without setting SNI for the node, which will most probably result in nonworking connections,.",
               dc, host_id, proxy_address);
    }
    Ok(())
}
