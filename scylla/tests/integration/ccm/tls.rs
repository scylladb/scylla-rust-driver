use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use futures::StreamExt;
use openssl::pkey::PKey;
use openssl::ssl::{SslContext, SslMethod, SslVerifyMode};
use openssl::x509::X509;
use openssl::x509::store::{X509Store, X509StoreBuilder};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DistinguishedName, DnType, IsCa, KeyPair,
    SanType,
};
use rustls::ClientConfig;
use rustls::pki_types::PrivatePkcs8KeyDer;
use scylla::client::session::{Session, TlsContext};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::ccm::lib::cluster::{Cluster, ClusterOptions};
use crate::ccm::lib::node::Node;
use crate::ccm::lib::{CLUSTER_VERSION, run_ccm_test_with_configuration};
use crate::utils::{execute_unprepared_statement_everywhere, setup_tracing};

fn cluster_3_nodes() -> ClusterOptions {
    ClusterOptions {
        name: "cluster_tls_3_node".to_string(),
        version: CLUSTER_VERSION.clone(),
        nodes_per_dc: vec![3],
        ..ClusterOptions::default()
    }
}

async fn configure_node_tls(
    node: &mut Node,
    ca: &CertifiedIssuer<'static, KeyPair>,
    prepare_cert: impl FnOnce(CertificateParams, &Node) -> CertificateParams,
) {
    let params = prepare_cert(CertificateParams::new(vec![]).unwrap(), node);
    let key = KeyPair::generate().unwrap();
    let cert = params.signed_by(&key, ca).unwrap();

    let cert_file_path = node.node_dir().join("db.cert");
    let mut cert_file = File::create_new(&cert_file_path).await.unwrap();
    cert_file.write_all(cert.pem().as_bytes()).await.unwrap();

    let key_file_path = node.node_dir().join("db.key");
    let mut key_file = File::create_new(&key_file_path).await.unwrap();
    key_file
        .write_all(key.serialize_pem().as_bytes())
        .await
        .unwrap();

    let args = [
        ("client_encryption_options.enabled", "true"),
        (
            "client_encryption_options.certificate",
            cert_file_path.to_str().unwrap(),
        ),
        (
            "client_encryption_options.keyfile",
            key_file_path.to_str().unwrap(),
        ),
    ];
    node.updateconf(args).await.unwrap();
}

fn prepare_authority_cert_params() -> CertificateParams {
    let mut params = CertificateParams::new(vec!["rust_integration_test_ca".to_owned()]).unwrap();
    params.distinguished_name = {
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CountryName, "PL");
        dn.push(DnType::LocalityName, "Warsaw");
        dn.push(DnType::StateOrProvinceName, "Maz");
        dn.push(DnType::OrganizationName, "scylla_rust_driver");
        dn.push(DnType::OrganizationalUnitName, "ccm_integration_tests");
        dn.push(DnType::CommonName, "rust_driver_ccm_tests");
        dn
    };
    params.use_authority_key_identifier_extension = true;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);

    params
}

async fn run_ccm_tls_test(
    prepare_cert: impl Fn(CertificateParams, &Node) -> CertificateParams,
    cluster_config: impl AsyncFnOnce(Cluster) -> Cluster,
    test: impl AsyncFnOnce(&CertifiedIssuer<'static, KeyPair>, &mut Cluster) -> (),
) {
    let ca_key = KeyPair::generate().unwrap();
    let params = prepare_authority_cert_params();
    let ca = Arc::new(CertifiedIssuer::self_signed(params, ca_key).unwrap());
    run_ccm_test_with_configuration(
        cluster_3_nodes,
        {
            let ca = Arc::clone(&ca);
            |mut cluster: Cluster| async move {
                let ca_key_file_path = cluster.cluster_dir().join("ca.key");
                let mut ca_key_file = File::create_new(&ca_key_file_path).await.unwrap();
                ca_key_file
                    .write_all(ca.key().serialize_pem().as_bytes())
                    .await
                    .unwrap();

                let ca_cert_file_path = cluster.cluster_dir().join("ca.crt");
                let mut ca_cert_file = File::create_new(&ca_cert_file_path).await.unwrap();
                ca_cert_file.write_all(ca.pem().as_bytes()).await.unwrap();

                cluster
                    .updateconf([(
                        "client_encryption_options.truststore",
                        ca_cert_file_path.to_str().unwrap(),
                    )])
                    .await
                    .unwrap();

                // I wanted to call this at the end of the function,
                // but scylla-ccm can't handle calling `ccm updateconf` after using `ccm <node> updateconf`.
                // See: https://github.com/scylladb/scylla-ccm/issues/686
                let mut cluster = cluster_config(cluster).await;

                futures::stream::iter(cluster.nodes_mut().iter_mut())
                    .for_each(|node| configure_node_tls(node, &ca, &prepare_cert))
                    .await;

                cluster
            }
        },
        async |cluster: &mut Cluster| test(&ca, cluster).await,
    )
    .await
}

async fn check_session_works_and_fully_connected(expected_nodes: usize, session: &Session) {
    let state = session.get_cluster_state();
    assert_eq!(state.get_nodes_info().len(), expected_nodes);
    assert!(
        state
            .get_nodes_info()
            .iter()
            .inspect(|node| {
                tracing::debug!(
                    "Node {}, address: {}, connected: {}",
                    node.host_id,
                    node.address,
                    node.is_connected()
                )
            })
            .all(|node| node.is_connected())
    );
    execute_unprepared_statement_everywhere(
        session,
        &state,
        &"SELECT * FROM system.local WHERE key='local'".into(),
        &(),
    )
    .await
    .unwrap();
}

fn build_openssl_ca_store(ca: &CertifiedIssuer<'_, KeyPair>) -> X509Store {
    let mut store_builder = X509StoreBuilder::new().unwrap();
    let ca = X509::from_der(ca.der()).unwrap();
    store_builder.add_cert(ca).unwrap();

    store_builder.build()
}

// Basic TLS test, with server not requiring client authentication.
// It checks that driver can connect to all nodes of TLS-enabled cluster,
// and execute requests on them.
#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn test_connect_tls_no_client_auth() {
    setup_tracing();

    fn prepare_cert(mut params: CertificateParams, node: &Node) -> CertificateParams {
        params
            .subject_alt_names
            .push(SanType::IpAddress(node.broadcast_rpc_address()));
        params
    }

    async fn test(ca: &CertifiedIssuer<'static, KeyPair>, cluster: &mut Cluster) {
        {
            tracing::info!("OpenSsl010 sub-test");
            let mut builder = SslContext::builder(SslMethod::tls()).unwrap();
            builder.set_verify(SslVerifyMode::PEER);
            builder.set_cert_store(build_openssl_ca_store(ca));
            let session = cluster
                .make_session_builder()
                .await
                .tls_context(Some(TlsContext::OpenSsl010(builder.build())))
                .build()
                .await
                .unwrap();
            check_session_works_and_fully_connected(cluster.nodes().len(), &session).await;
        }

        {
            tracing::info!("Rustls023 sub-test");
            let mut store = rustls::RootCertStore::empty();
            store.add(ca.der().to_owned()).unwrap();
            let client_config = ClientConfig::builder()
                .with_root_certificates(store)
                .with_no_client_auth();
            let session = cluster
                .make_session_builder()
                .await
                .tls_context(Some(TlsContext::Rustls023(Arc::new(client_config))))
                .build()
                .await
                .unwrap();
            check_session_works_and_fully_connected(cluster.nodes().len(), &session).await;
        }
    }

    run_ccm_tls_test(prepare_cert, async |c| c, test).await
}

// Verifies that if a node presents a certificate without a subject alternative name
// matching the node's IP address, the driver won't connect to it.
#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn test_tls_verifies_hostname() {
    setup_tracing();

    // Prepare a certificate with a subject alternative name that doesn't match the node's IP address
    fn prepare_cert(mut params: CertificateParams, _node: &Node) -> CertificateParams {
        params
            .subject_alt_names
            .push(SanType::IpAddress(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))));
        params
    }

    async fn test(ca: &CertifiedIssuer<'static, KeyPair>, cluster: &mut Cluster) {
        {
            tracing::info!("OpenSsl010 sub-test");
            let mut builder = SslContext::builder(SslMethod::tls()).unwrap();
            builder.set_verify(SslVerifyMode::PEER);
            builder.set_cert_store(build_openssl_ca_store(ca));
            let session = cluster
                .make_session_builder()
                .await
                .tls_context(Some(TlsContext::OpenSsl010(builder.build())))
                .build()
                .await
                // This should be unwrap_err, but hostname verification doesn't work with openssl.
                // This is a bug: https://github.com/scylladb/scylla-rust-driver/issues/1116
                .unwrap();
            check_session_works_and_fully_connected(cluster.nodes().len(), &session).await;
        }

        {
            tracing::info!("Rustls023 sub-test");
            let mut store = rustls::RootCertStore::empty();
            store.add(ca.der().to_owned()).unwrap();
            let client_config = ClientConfig::builder()
                .with_root_certificates(store)
                .with_no_client_auth();
            let _err = cluster
                .make_session_builder()
                .await
                .tls_context(Some(TlsContext::Rustls023(Arc::new(client_config))))
                .build()
                .await
                .unwrap_err();
        }
    }

    run_ccm_tls_test(prepare_cert, async |c| c, test).await
}

// Check that we can still connect if client_encryption_options.require_client_auth is true.
#[tokio::test]
#[cfg_attr(not(ccm_tests), ignore)]
async fn test_connect_tls_with_client_auth() {
    setup_tracing();

    fn prepare_cert(mut params: CertificateParams, node: &Node) -> CertificateParams {
        params
            .subject_alt_names
            .push(SanType::IpAddress(node.broadcast_rpc_address()));
        params
    }

    async fn require_client_auth(mut cluster: Cluster) -> Cluster {
        cluster
            .updateconf([("client_encryption_options.require_client_auth", "true")])
            .await
            .unwrap();

        cluster
    }

    async fn test(ca: &CertifiedIssuer<'static, KeyPair>, cluster: &mut Cluster) {
        {
            tracing::info!("Sanity check for client auth");
            // Sanity check verifies that we can't connect to the cluster
            // without setting up client auth, so the server correctly requires it.

            let mut store = rustls::RootCertStore::empty();
            store.add(ca.der().to_owned()).unwrap();
            let client_config = ClientConfig::builder()
                .with_root_certificates(store)
                .with_no_client_auth();
            let _err = cluster
                .make_session_builder()
                .await
                .tls_context(Some(TlsContext::Rustls023(Arc::new(client_config))))
                .build()
                .await
                .unwrap_err();
        }

        let client_key = KeyPair::generate().unwrap();
        let client_cert = CertificateParams::new(vec![])
            .unwrap()
            .signed_by(&client_key, ca)
            .unwrap();

        {
            tracing::info!("OpenSsl010 sub-test");
            let mut builder = SslContext::builder(SslMethod::tls()).unwrap();
            builder.set_verify(SslVerifyMode::PEER);
            builder.set_cert_store(build_openssl_ca_store(ca));
            builder
                .set_certificate(&X509::from_der(client_cert.der()).unwrap())
                .unwrap();
            builder
                .set_private_key(
                    &PKey::private_key_from_pkcs8(client_key.serialized_der()).unwrap(),
                )
                .unwrap();
            let session = cluster
                .make_session_builder()
                .await
                .tls_context(Some(TlsContext::OpenSsl010(builder.build())))
                .build()
                .await
                .unwrap();
            check_session_works_and_fully_connected(cluster.nodes().len(), &session).await;
        }

        {
            tracing::info!("Rustls023 sub-test");

            let mut store = rustls::RootCertStore::empty();
            store.add(ca.der().to_owned()).unwrap();
            let client_config = ClientConfig::builder()
                .with_root_certificates(store)
                .with_client_auth_cert(
                    vec![client_cert.der().to_owned()],
                    PrivatePkcs8KeyDer::from(client_key.serialize_der()).into(),
                )
                .unwrap();
            let session = cluster
                .make_session_builder()
                .await
                .tls_context(Some(TlsContext::Rustls023(Arc::new(client_config))))
                .build()
                .await
                .unwrap();

            check_session_works_and_fully_connected(cluster.nodes().len(), &session).await;
        }
    }

    run_ccm_tls_test(prepare_cert, require_client_auth, test).await
}
