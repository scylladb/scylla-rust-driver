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
