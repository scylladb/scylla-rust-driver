use std::collections::HashMap;

use openssl::{
    pkey::{PKey, Private},
    x509::X509,
};
use scylla_cql::{frame::types::SerialConsistency, Consistency};

#[derive(Debug)]
pub(crate) struct CloudConfig {
    datacenters: HashMap<String, Datacenter>,
    auth_infos: HashMap<String, AuthInfo>,

    // contexts
    contexts: HashMap<String, Context>,
    current_context: String,

    // parameters
    default_consistency: Option<Consistency>,
    default_serial_consistency: Option<SerialConsistency>,
}

impl CloudConfig {
    pub(crate) fn get_datacenters(&self) -> &HashMap<String, Datacenter> {
        &self.datacenters
    }

    pub(crate) fn get_default_consistency(&self) -> Option<Consistency> {
        self.default_consistency
    }

    pub(crate) fn get_default_serial_consistency(&self) -> Option<SerialConsistency> {
        self.default_serial_consistency
    }

    pub(crate) fn get_current_context(&self) -> &Context {
        self.contexts.get(&self.current_context).expect(
            "BUG: Validation should have prevented current_context pointing to unknown context",
        )
    }

    pub(crate) fn get_current_auth_info(&self) -> &AuthInfo {
        let auth_info_name = self.get_current_context().auth_info_name.as_str();
        self.auth_infos.get(auth_info_name).expect(
            "BUG: Validation should have prevented current context's auth info pointing to unknown auth_info",
        )
    }
}

#[derive(Debug)]
pub(crate) struct AuthInfo {
    key: PKey<Private>,
    cert: X509,
    #[allow(unused)]
    username: Option<String>,
    #[allow(unused)]
    password: Option<String>,
}

impl AuthInfo {
    pub(crate) fn get_key(&self) -> &PKey<Private> {
        &self.key
    }

    pub(crate) fn get_cert(&self) -> &X509 {
        &self.cert
    }

    #[allow(unused)]
    pub(crate) fn get_username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    #[allow(unused)]
    pub(crate) fn get_password(&self) -> Option<&str> {
        self.password.as_deref()
    }
}

#[derive(Debug)]
pub(crate) struct Datacenter {
    certificate_authority: X509,
    server: String,
    #[allow(unused)]
    tls_server_name: Option<String>,
    node_domain: String,
    insecure_skip_tls_verify: bool,
    #[allow(unused)]
    proxy_url: Option<String>,
}

impl Datacenter {
    pub(crate) fn get_certificate_authority(&self) -> &X509 {
        &self.certificate_authority
    }

    pub(crate) fn get_server(&self) -> &str {
        &self.server
    }

    #[allow(unused)]
    pub(crate) fn get_tls_server_name(&self) -> Option<&str> {
        self.tls_server_name.as_deref()
    }

    pub(crate) fn get_node_domain(&self) -> &str {
        &self.node_domain
    }

    pub(crate) fn get_insecure_skip_tls_verify(&self) -> bool {
        self.insecure_skip_tls_verify
    }

    #[allow(unused)]
    pub(crate) fn get_proxy_url(&self) -> Option<&str> {
        self.proxy_url.as_deref()
    }
}

#[derive(Debug)]
pub(crate) struct Context {
    datacenter_name: String,
    auth_info_name: String,
}

mod deserialize {
    use scylla_cql::{frame::types::SerialConsistency, Consistency};
    use std::{collections::HashMap, fs::File, io::Read, path::Path};

    use serde::Deserialize;
    #[derive(Deserialize)]
    #[allow(non_snake_case)]
    struct RawCloudConfig {
        // Kind is a string value representing the REST resource this object represents.
        // Servers may infer this from the endpoint the client submits requests to.
        // In CamelCase.
        // +optional
        kind: Option<String>,

        // APIVersion defines the versioned schema of this representation of an object.
        // Servers should convert recognized schemas to the latest internal value, and
        // may reject unrecognized values.
        // +optional
        apiVersion: Option<String>,

        // Datacenters is a map of referencable names to datacenter configs.
        datacenters: HashMap<String, Datacenter>,

        // AuthInfos is a map of referencable names to authentication configs.
        authInfos: HashMap<String, AuthInfo>,

        // Contexts is a map of referencable names to context configs.
        contexts: HashMap<String, Context>,

        // CurrentContext is the name of the context that you would like to use by default.
        currentContext: String,

        // Parameters is a struct containing common driver configuration parameters.
        // +optional
        parameters: Option<Parameters>,
    }

    #[allow(non_snake_case)]
    #[derive(Deserialize)]
    struct AuthInfo {
        // ClientCertificateData contains PEM-encoded data from a client cert file for TLS. Overrides ClientCertificatePath.
        // +optional
        clientCertificateData: Option<String>,

        // ClientCertificatePath is the path to a client cert file for TLS.
        // +optional
        clientCertificatePath: Option<String>,

        // ClientKeyData contains PEM-encoded data from a client key file for TLS. Overrides ClientKeyPath.
        // +optional
        clientKeyData: Option<String>,

        // ClientKeyPath is the path to a client key file for TLS.
        // +optional
        clientKeyPath: Option<String>,

        // Username is the username for basic authentication to the Scylla cluster.
        // +optional
        username: Option<String>,
        // Password is the password for basic authentication to the Scylla cluster.
        // +optional
        password: Option<String>,
    }

    #[allow(non_snake_case)]
    #[derive(Deserialize)]
    struct Datacenter {
        // CertificateAuthorityPath is the path to a cert file for the certificate authority.
        // +optional
        certificateAuthorityPath: Option<String>,

        // CertificateAuthorityData contains PEM-encoded certificate authority certificates. Overrides CertificateAuthority.
        // +optional
        certificateAuthorityData: Option<String>,

        // Server is the initial contact point of the Scylla cluster.
        // Example: https://hostname:port
        server: String,

        // TLSServerName is used to check server certificates. If TLSServerName is empty, the hostname used to contact the server is used.
        // +optional
        tlsServerName: Option<String>,

        // NodeDomain the domain suffix that is concatenated with host_id of the node driver wants to connect to.
        // Example: host_id.<nodeDomain>
        nodeDomain: String,

        // InsecureSkipTLSVerify skips the validity check for the server's certificate. This will make your HTTPS connections insecure.
        // +optional
        insecureSkipTlsVerify: Option<bool>,

        // ProxyURL is the URL to the proxy to be used for all requests made by this
        // client. URLs with "http", "https", and "socks5" schemes are supported. If
        // this configuration is not provided or the empty string, the client
        // attempts to construct a proxy configuration from http_proxy and
        // https_proxy environment variables. If these environment variables are not
        // set, the client does not attempt to proxy requests.
        // +optional
        proxyUrl: Option<String>,
    }

    #[allow(non_snake_case)]
    #[derive(Deserialize)]
    struct Context {
        // DatacenterName is the name of the datacenter for this context.
        datacenterName: String,

        // AuthInfoName is the name of the authInfo for this context.
        authInfoName: String,
    }

    #[allow(non_snake_case)]
    #[derive(Deserialize, Debug)]
    struct Parameters {
        // DefaultConsistency is the default consistency level used for user queries.
        // +optional
        defaultConsistency: Option<Consistency>,

        // DefaultSerialConsistency is the default consistency level for the serial part of user queries.
        // +optional
        defaultSerialConsistency: Option<SerialConsistency>,
    }

    impl RawCloudConfig {
        fn try_from_reader<R: Read>(yaml: &mut R) -> Result<Self, serde_yaml::Error> {
            serde_yaml::from_reader(yaml)
        }
    }
}
