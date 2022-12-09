use std::{collections::HashMap, io};

use openssl::{
    pkey::{PKey, Private},
    x509::X509,
};
use scylla_cql::{frame::types::SerialConsistency, Consistency};
use thiserror::Error;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum CloudConfigError {
    #[error("Error while opening cloud config yaml: {0}")]
    YamlOpen(#[from] io::Error),

    #[error("Error while parsing cloud config yaml: {0}")]
    YamlParse(#[from] serde_yaml::Error),

    #[error("Error while decoding base64 key/cert: {0}")]
    Base64(#[from] base64::DecodeError),

    #[error("Error during cloud config validation: {0}")]
    Validation(String),

    #[error("Error during key/cert parsing: {0}")]
    Ssl(#[from] openssl::error::ErrorStack),
}

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
    use super::CloudConfigError;
    use scylla_cql::{frame::types::SerialConsistency, Consistency};
    use std::{collections::HashMap, fs::File, io::Read, path::Path};

    use openssl::{pkey::PKey, x509::X509};

    use serde::Deserialize;
    use tracing::warn;
    use url::Url;

    static SUPPORTED_API_VERSIONS: &[&str] = &["cqlclient.scylla.scylladb.com/v1alpha1"];

    // Full hostname has limit of 255 chars.
    // Host UUID takes 32 chars for hex digits and 4 dashes.
    // Additional 1 is for separator dot before nodeDomain.
    const NODE_DOMAIN_MAX_LENGTH: usize = 255 - 32 - 4 - 1;

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

    fn get_pem_data_from_string_or_load_from_file(
        data_name: &str,    // how data in string is called
        path_name: &str,    // how data in file is called
        data: Option<&str>, // data in string
        path: Option<&str>, // path to data in file
    ) -> Result<Box<[u8]>, CloudConfigError> {
        let pem = if let Some(data) = data {
            base64::decode(data)?
        } else if let Some(path) = path {
            let mut buf = vec![];
            File::open(path)
                .and_then(|mut f| f.read_to_end(&mut buf))
                .map_err(|_| {
                    CloudConfigError::Validation(format!(
                        "Cannot read file at given {} {}.",
                        path_name, path,
                    ))
                })?;
            buf
        } else {
            return Err(CloudConfigError::Validation(format!(
                "Either {} or {} has to be provided for authInfo.",
                data_name, path_name,
            )));
        };
        Ok(pem.into_boxed_slice())
    }

    impl TryFrom<RawCloudConfig> for super::CloudConfig {
        type Error = CloudConfigError;

        fn try_from(config: RawCloudConfig) -> Result<Self, Self::Error> {
            if let Some(ref api_version) = config.apiVersion {
                if !SUPPORTED_API_VERSIONS
                    .iter()
                    .any(|supported| supported == api_version)
                {
                    warn!(
                        "Unknown API version: {}. Please update your driver.",
                        api_version
                    );
                }
            }
            if let Some(ref kind) = config.kind {
                if kind != "CQLConnectionConfig" {
                    warn!("Unknown kind: {}. Please update your driver.", kind);
                }
            }

            let datacenters = config
                .datacenters
                .into_iter()
                .map(|(dc_name, dc_data)| {
                    super::Datacenter::try_from(dc_data).map(|dc_data| (dc_name, dc_data))
                })
                .collect::<Result<HashMap<String, super::Datacenter>, CloudConfigError>>()?;

            let auth_infos = config
                .authInfos
                .into_iter()
                .map(|(auth_info_name, auth_info_data)| {
                    match super::AuthInfo::try_from(auth_info_data) {
                        Ok(auth_info_data) => Ok((auth_info_name, auth_info_data)),
                        Err(err) => Err(err),
                    }
                })
                .collect::<Result<HashMap<String, super::AuthInfo>, CloudConfigError>>()?;

            let contexts = config
                .contexts
                .into_iter()
                .map(|(context_name, context_data)| (context_name, context_data.into()))
                .collect::<HashMap<String, super::Context>>();

            let default_context = contexts.get(&config.currentContext).ok_or_else(|| {
                CloudConfigError::Validation("currentContext points to unknown context.".into())
            })?;
            if !datacenters.contains_key(&default_context.datacenter_name) {
                return Err(CloudConfigError::Validation(format!(
                    "context {} datacenter points to unknown datacenter.",
                    &config.currentContext
                )));
            }
            if !auth_infos.contains_key(&default_context.auth_info_name) {
                return Err(CloudConfigError::Validation(format!(
                    "context {} authInfo points to unknown authInfo.",
                    &config.currentContext
                )));
            }

            Ok(Self {
                datacenters,
                auth_infos,
                contexts,
                current_context: config.currentContext,
                default_consistency: config
                    .parameters
                    .as_ref()
                    .and_then(|p| p.defaultConsistency),
                default_serial_consistency: config
                    .parameters
                    .as_ref()
                    .and_then(|p| p.defaultSerialConsistency),
            })
        }
    }

    impl TryFrom<AuthInfo> for super::AuthInfo {
        type Error = CloudConfigError;

        fn try_from(auth_info: AuthInfo) -> Result<Self, Self::Error> {
            let cert_pem = get_pem_data_from_string_or_load_from_file(
                "clientCertificateData",
                "clientCertificatePath",
                auth_info.clientCertificateData.as_deref(),
                auth_info.clientCertificatePath.as_deref(),
            )?;
            let key_pem = get_pem_data_from_string_or_load_from_file(
                "clientKeyData",
                "clientKeyPath",
                auth_info.clientKeyData.as_deref(),
                auth_info.clientKeyPath.as_deref(),
            )?;

            let cert = X509::from_pem(&cert_pem[..]).map_err(CloudConfigError::Ssl)?;

            let key = PKey::private_key_from_pem(&key_pem[..]).map_err(CloudConfigError::Ssl)?;

            Ok(super::AuthInfo {
                key,
                cert,
                username: auth_info.username,
                password: auth_info.password,
            })
        }
    }

    impl TryFrom<Datacenter> for super::Datacenter {
        type Error = CloudConfigError;

        fn try_from(datacenter: Datacenter) -> Result<Self, Self::Error> {
            // Validate node domain
            // Using parts relevant to hostnames as we're dealing with a part of hostname
            // RFC-1123 Section 2.1 and RFC-952 1.
            let node_domain = datacenter.nodeDomain;
            if node_domain.is_empty() {
                return Err(CloudConfigError::Validation(
                    "nodeDomain property is required in datacenter description.".into(),
                ));
            }
            if node_domain.len() > NODE_DOMAIN_MAX_LENGTH {
                return Err(CloudConfigError::Validation(format!(
                    "Subdomain name too long (max {} ): {}",
                    NODE_DOMAIN_MAX_LENGTH, &node_domain
                )));
            }
            if node_domain.contains(' ') {
                return Err(CloudConfigError::Validation(format!(
                    "nodeDomain {} cannot contains spaces.",
                    &node_domain
                )));
            }
            if node_domain.starts_with('.') || node_domain.ends_with('.') {
                return Err(CloudConfigError::Validation(format!(
                    "nodeDomain {} cannot start or end with a dot.",
                    &node_domain
                )));
            }
            if node_domain.ends_with('-') {
                return Err(CloudConfigError::Validation(format!(
                    "nodeDomain {} cannot end with a minus sign.",
                    &node_domain
                )));
            }

            let components = node_domain.split('.');
            for component in components {
                if component.is_empty() {
                    return Err(CloudConfigError::Validation(format!(
                        "nodeDomain {} cannot have empty components between dots.",
                        &node_domain
                    )));
                }

                if component.starts_with('-') || component.ends_with('-') {
                    return Err(CloudConfigError::Validation(format!(
                        "nodeDomain {} components can have minus sign only as interior character,\
                            which component {} disobeys.",
                        &node_domain, component,
                    )));
                }

                for c in component.chars() {
                    if !c.is_alphanumeric() && c != '-' {
                        return Err(CloudConfigError::Validation(format!(
                            "nodeDomain {} contains illegal character: {}.",
                            &node_domain, c
                        )));
                    }
                }
            }

            let server_with_protocol = format!("https://{}", datacenter.server.as_str());
            // Validate server
            let server = Url::try_from(server_with_protocol.as_str()).map_err(|url_error| {
                CloudConfigError::Validation(format!(
                    "server property {} is not a valid URL: {}",
                    &datacenter.server, url_error
                ))
            })?;
            server.port().ok_or_else(|| {
                CloudConfigError::Validation(format!(
                    "server property {} does not contain a port.",
                    &datacenter.server
                ))
            })?;

            let cert_pem = get_pem_data_from_string_or_load_from_file(
                "certificateAuthorityData",
                "certificateAuthorityPath",
                datacenter.certificateAuthorityData.as_deref(),
                datacenter.certificateAuthorityPath.as_deref(),
            )?;

            let certificate_authority =
                X509::from_pem(&cert_pem[..]).map_err(CloudConfigError::Ssl)?;

            Ok(super::Datacenter {
                certificate_authority,
                server: datacenter.server,
                node_domain,
                insecure_skip_tls_verify: datacenter.insecureSkipTlsVerify.unwrap_or(false),
                tls_server_name: datacenter.tlsServerName,
                proxy_url: datacenter.proxyUrl,
            })
        }
    }

    impl From<Context> for super::Context {
        fn from(context: Context) -> Self {
            Self {
                datacenter_name: context.datacenterName,
                auth_info_name: context.authInfoName,
            }
        }
    }

    impl super::CloudConfig {
        pub fn read_from_yaml(config_path: impl AsRef<Path>) -> Result<Self, CloudConfigError> {
            let mut yaml = File::open(config_path)?;
            let config = RawCloudConfig::try_from_reader(&mut yaml)?;
            Self::try_from(config)
        }
    }
}
