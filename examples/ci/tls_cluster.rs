//! Support code shared by the `tls_openssl` and `tls_rustls` examples: it brings
//! up a throwaway, TLS-enabled ScyllaDB cluster with `scylla-ccm-bridge` so that
//! those examples can run unattended in CI, without anybody having to hand-craft
//! certificates and a `scylla.yaml` first.
//!
//! This is deliberately *not* what the TLS examples teach — it is the server-side
//! half of the setup. The client-side half, which is the actual lesson, lives in
//! the examples themselves.
//!
//! A single node is enough here: what the examples demonstrate is how a client
//! is configured to trust the cluster's CA, and one node exercises the full
//! handshake (including hostname verification against the node's IP) while
//! keeping CI fast.

use anyhow::{Context as _, Result};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DistinguishedName, DnType, IsCa, KeyPair,
    SanType,
};
use scylla::client::session_builder::SessionBuilder;
use scylla_ccm_bridge::CLUSTER_VERSION;
use scylla_ccm_bridge::cluster::{Cluster, ClusterOptions};
use std::path::Path;

/// A running TLS-enabled cluster, together with the certificate authority that
/// signed its nodes' certificates.
///
/// The cluster is destroyed when this value is dropped, so keep it alive for as
/// long as you use the session.
pub struct CiTlsCluster {
    cluster: Cluster,
    ca: CertifiedIssuer<'static, KeyPair>,
}

impl CiTlsCluster {
    /// The DER-encoded certificate of the CA that signed the nodes' certificates.
    ///
    /// A client has to trust it in order to complete the TLS handshake; this is
    /// the CI equivalent of the `ca.crt` file you would ship to your clients.
    pub fn ca_cert_der(&self) -> &[u8] {
        self.ca.der()
    }

    /// A [`SessionBuilder`] already pointed at the cluster's contact points.
    pub async fn session_builder(&self) -> SessionBuilder {
        self.cluster.make_session_builder().await
    }
}

/// Starts a one-node cluster named `cluster_name` with client encryption enabled.
pub async fn start(cluster_name: &str) -> Result<CiTlsCluster> {
    // The certificate authority that vouches for every node in the cluster.
    let ca = CertifiedIssuer::self_signed(authority_cert_params()?, KeyPair::generate()?)
        .context("failed to create the CA certificate")?;

    let mut cluster = Cluster::new(ClusterOptions {
        name: cluster_name.to_string(),
        version: CLUSTER_VERSION.clone(),
        nodes_per_dc: vec![1],
        ..Default::default()
    })
    .await?;
    cluster.init().await?;

    // Make the cluster trust the CA. This cluster-wide `updateconf` has to happen
    // before the per-node ones below, because scylla-ccm cannot handle
    // `ccm updateconf` after `ccm <node> updateconf`.
    // See: https://github.com/scylladb/scylla-ccm/issues/686
    let ca_cert_path = cluster.cluster_dir().join("ca.crt");
    tokio::fs::write(&ca_cert_path, ca.pem()).await?;
    cluster
        .updateconf([(
            "client_encryption_options.truststore",
            path_arg(&ca_cert_path)?,
        )])
        .await?;

    // Give every node a certificate signed by that CA. The subject alternative
    // name must cover the node's IP address, because the driver verifies the
    // node's identity against the address it connects to.
    for node in cluster.nodes_mut().iter_mut() {
        let mut params = CertificateParams::new(vec![])?;
        params
            .subject_alt_names
            .push(SanType::IpAddress(node.broadcast_rpc_address()));
        let key = KeyPair::generate()?;
        let cert = params.signed_by(&key, &ca)?;

        let cert_path = node.node_dir().join("db.cert");
        tokio::fs::write(&cert_path, cert.pem()).await?;
        let key_path = node.node_dir().join("db.key");
        tokio::fs::write(&key_path, key.serialize_pem()).await?;

        node.updateconf([
            ("client_encryption_options.enabled", "true"),
            (
                "client_encryption_options.certificate",
                path_arg(&cert_path)?,
            ),
            ("client_encryption_options.keyfile", path_arg(&key_path)?),
        ])
        .await?;
    }

    cluster.start(None).await?;

    Ok(CiTlsCluster { cluster, ca })
}

fn authority_cert_params() -> Result<CertificateParams> {
    let mut params = CertificateParams::new(vec!["scylla_rust_driver_examples_ca".to_owned()])?;
    params.distinguished_name = {
        let mut dn = DistinguishedName::new();
        dn.push(DnType::OrganizationName, "scylla_rust_driver");
        dn.push(DnType::CommonName, "scylla_rust_driver_examples_ca");
        dn
    };
    params.use_authority_key_identifier_extension = true;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);

    Ok(params)
}

fn path_arg(path: &Path) -> Result<&str> {
    path.to_str()
        .with_context(|| format!("path is not valid UTF-8: {}", path.display()))
}
