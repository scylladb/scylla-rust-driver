/// The purpose of this module is to have a simple Rust API over CCM command line.
/// This makes code of the user-facing CCM Rust API simpler.
/// The purpose of this module is NOT:
///  - Providing any type-level guarantees about the cluster
///  - Providing easy to use API
///
/// The only thing this module should do is translating Rust calls to CCM CLI.
pub(crate) mod cluster;
pub(crate) mod node;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum DBType {
    Scylla,
    #[expect(dead_code)]
    Cassandra,
}

/// Options to start the node with.
/// It controls `--no-wait`, `--wait-other-notice` and `--wait-for-binary-proto` ccm options.
pub(crate) struct NodeStartOptions {
    /// Don't wait for the node to start. Corresponds to `--no-wait` option in ccm.
    pub(super) no_wait: bool,
    /// Wait till other nodes recognize started node. Corresponds to `--wait-other-notice` option in ccm.
    pub(super) wait_other_notice: bool,
    /// Wait till started node report that client port is opened and operational.
    /// Corresponds to `--wait-for-binary-proto` option in ccm.
    pub(super) wait_for_binary_proto: bool,
}

/// The default start options. Enable following ccm options:
/// - `--wait-other-notice`
/// - `--wait-for-binary-proto`
///
/// The `--no-wait` option is not enabled.
impl Default for NodeStartOptions {
    fn default() -> Self {
        Self {
            no_wait: false,
            wait_other_notice: true,
            wait_for_binary_proto: true,
        }
    }
}

impl NodeStartOptions {
    pub(super) const NO_WAIT: &'static str = "--no-wait";
    pub(super) const WAIT_OTHER_NOTICE: &'static str = "--wait-other-notice";
    pub(super) const WAIT_FOR_BINARY_PROTO: &'static str = "--wait-for-binary-proto";

    /// Creates the default start options. Enables following ccm options:
    /// - `--wait-other-notice`
    /// - `--wait-for-binary-proto`
    ///
    /// The `--no-wait` option is not enabled.
    #[expect(dead_code)]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Enables or disables the `--no-wait` ccm option.
    #[expect(dead_code)]
    pub(crate) fn no_wait(mut self, no_wait: bool) -> Self {
        self.no_wait = no_wait;
        self
    }

    /// Enables or disables the `--wait-other-notice` ccm option.
    #[expect(dead_code)]
    pub(crate) fn wait_other_notice(mut self, wait_other_notice: bool) -> Self {
        self.wait_other_notice = wait_other_notice;
        self
    }

    /// Enables or disables the `--wait-for-binary-proto` ccm option.
    #[expect(dead_code)]
    pub(crate) fn wait_for_binary_proto(mut self, wait_for_binary_proto: bool) -> Self {
        self.wait_for_binary_proto = wait_for_binary_proto;
        self
    }
}

/// Options to stop the node with.
/// It allows to control the value of `--no-wait` and `--not-gently` ccm options.
#[derive(Default)]
pub(crate) struct NodeStopOptions {
    /// Dont't wait for the node to properly stop.
    pub(super) no_wait: bool,
    /// Force-terminate node with `kill -9`.
    pub(super) not_gently: bool,
}

impl NodeStopOptions {
    pub(super) const NO_WAIT: &'static str = "--no-wait";
    pub(super) const NOT_GENTLY: &'static str = "--not-gently";

    /// Create a new `NodeStopOptions` with default values.
    /// All ccm options are disabled by default.
    #[expect(dead_code)]
    pub(crate) fn new() -> Self {
        NodeStopOptions {
            no_wait: false,
            not_gently: false,
        }
    }

    /// Enables or disables the `--no-wait` cmm option.
    #[expect(dead_code)]
    pub(crate) fn no_wait(mut self, no_wait: bool) -> Self {
        self.no_wait = no_wait;
        self
    }

    /// Enables or disables the `--not-gently` ccm option.
    #[expect(dead_code)]
    pub(crate) fn not_gently(mut self, not_gently: bool) -> Self {
        self.not_gently = not_gently;
        self
    }
}
