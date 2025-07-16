use std::borrow::Cow;

/// Driver and application self-identifying information,
/// to be sent in STARTUP message.
#[derive(Debug, Clone, Default)]
pub struct SelfIdentity<'id> {
    // Custom driver identity can be set if a custom driver build is running,
    // or an entirely different driver is operating on top of Rust driver
    // (e.g. cpp-rust-driver).
    custom_driver_name: Option<Cow<'id, str>>,
    custom_driver_version: Option<Cow<'id, str>>,

    // ### Q: Where do APPLICATION_NAME, APPLICATION_VERSION and CLIENT_ID come from?
    // - there are no columns in system.clients dedicated to those attributes,
    // - APPLICATION_NAME / APPLICATION_VERSION are not present in Scylla's source code at all,
    // - only 2 results in Cassandra source is some example in docs:
    //   https://github.com/apache/cassandra/blob/d3cbf9c1f72057d2a5da9df8ed567d20cd272931/doc/modules/cassandra/pages/managing/operating/virtualtables.adoc?plain=1#L218.
    //   APPLICATION_NAME and APPLICATION_VERSION appears in client_options which
    //   is an arbitrary dict where client can send any keys.
    // - driver variables are mentioned in protocol v5
    //   (https://github.com/apache/cassandra/blob/d3cbf9c1f72057d2a5da9df8ed567d20cd272931/doc/native_protocol_v5.spec#L480),
    //   application variables are not.
    //
    // ### A:
    // The following options are not exposed anywhere in ScyllaDB tables.
    // They come directly from CPP driver, and they are supported in Cassandra
    //
    // See https://github.com/scylladb/cpp-driver/blob/fa0f27069a625057984d1fa58f434ea99b86c83f/include/cassandra.h#L2916.
    // As we want to support as big subset of its API as possible in cpp-rust-driver, I decided to expose API for setting
    // those particular key-value pairs, similarly to what cpp-driver does, and not an API to set arbitrary key-value pairs.
    //
    // Allowing users to set arbitrary options could break the driver by overwriting options that bear special meaning,
    // e.g. the shard-aware port. Therefore, I'm against such liberal API. OTOH, we need to expose APPLICATION_NAME,
    // APPLICATION_VERSION and CLIENT_ID for cpp-rust-driver.

    // Application identity can be set to distinguish different applications
    // connected to the same cluster.
    application_name: Option<Cow<'id, str>>,
    application_version: Option<Cow<'id, str>>,

    // A (unique) client ID can be set to distinguish different instances
    // of the same application connected to the same cluster.
    client_id: Option<Cow<'id, str>>,
}

impl<'id> SelfIdentity<'id> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Advertises a custom driver name, which can be used if a custom driver build is running,
    /// or an entirely different driver is operating on top of Rust driver
    /// (e.g. cpp-rust-driver).
    pub fn set_custom_driver_name(&mut self, custom_driver_name: impl Into<Cow<'id, str>>) {
        self.custom_driver_name = Some(custom_driver_name.into());
    }

    /// Advertises a custom driver name. See [Self::set_custom_driver_name] for use cases.
    pub fn with_custom_driver_name(mut self, custom_driver_name: impl Into<Cow<'id, str>>) -> Self {
        self.custom_driver_name = Some(custom_driver_name.into());
        self
    }

    /// Custom driver name to be advertised. See [Self::set_custom_driver_name] for use cases.
    pub fn get_custom_driver_name(&self) -> Option<&str> {
        self.custom_driver_name.as_deref()
    }

    /// Advertises a custom driver version. See [Self::set_custom_driver_name] for use cases.
    pub fn set_custom_driver_version(&mut self, custom_driver_version: impl Into<Cow<'id, str>>) {
        self.custom_driver_version = Some(custom_driver_version.into());
    }

    /// Advertises a custom driver version. See [Self::set_custom_driver_name] for use cases.
    pub fn with_custom_driver_version(
        mut self,
        custom_driver_version: impl Into<Cow<'id, str>>,
    ) -> Self {
        self.custom_driver_version = Some(custom_driver_version.into());
        self
    }

    /// Custom driver version to be advertised. See [Self::set_custom_driver_version] for use cases.
    pub fn get_custom_driver_version(&self) -> Option<&str> {
        self.custom_driver_version.as_deref()
    }

    /// Advertises an application name, which can be used to distinguish different applications
    /// connected to the same cluster.
    pub fn set_application_name(&mut self, application_name: impl Into<Cow<'id, str>>) {
        self.application_name = Some(application_name.into());
    }

    /// Advertises an application name. See [Self::set_application_name] for use cases.
    pub fn with_application_name(mut self, application_name: impl Into<Cow<'id, str>>) -> Self {
        self.application_name = Some(application_name.into());
        self
    }

    /// Application name to be advertised. See [Self::set_application_name] for use cases.
    pub fn get_application_name(&self) -> Option<&str> {
        self.application_name.as_deref()
    }

    /// Advertises an application version. See [Self::set_application_name] for use cases.
    pub fn set_application_version(&mut self, application_version: impl Into<Cow<'id, str>>) {
        self.application_version = Some(application_version.into());
    }

    /// Advertises an application version. See [Self::set_application_name] for use cases.
    pub fn with_application_version(
        mut self,
        application_version: impl Into<Cow<'id, str>>,
    ) -> Self {
        self.application_version = Some(application_version.into());
        self
    }

    /// Application version to be advertised. See [Self::set_application_version] for use cases.
    pub fn get_application_version(&self) -> Option<&str> {
        self.application_version.as_deref()
    }

    /// Advertises a client ID, which can be set to distinguish different instances
    /// of the same application connected to the same cluster.
    pub fn set_client_id(&mut self, client_id: impl Into<Cow<'id, str>>) {
        self.client_id = Some(client_id.into());
    }

    /// Advertises a client ID. See [Self::set_client_id] for use cases.
    pub fn with_client_id(mut self, client_id: impl Into<Cow<'id, str>>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Client ID to be advertised. See [Self::set_client_id] for use cases.
    pub fn get_client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }
}
