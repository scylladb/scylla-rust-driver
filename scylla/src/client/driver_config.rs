//! Reporting of the driver's effective configuration to the cluster.
//!
//! The driver sends its configuration as a JSON document under the
//! `DRIVER_CONFIG` `STARTUP` option, where ScyllaDB exposes it in
//! `system.clients.client_options`. That makes the configuration a session
//! actually runs with inspectable from the server side, without having to
//! correlate it with client-side logs. The document follows an external schema
//! shared by all ScyllaDB drivers, so that one
//! query answers the same question regardless of which driver a client uses.
//! Only the control connection sends it: the value is identical for every
//! connection of a session, so sending it on all of them would only bloat their
//! `STARTUP` frames.

use serde::Serialize;

/// The major version of the reported configuration schema.
///
/// Adding keys is backwards compatible and does not bump this; only changing or
/// removing the meaning of an existing key does.
const REPORT_VERSION: u32 = 1;

/// Reports larger than this are omitted instead of being sent.
///
/// ScyllaDB's protocol-extensions spec requires drivers to validate the size
/// client side and omit the option above this limit, so that reporting the
/// configuration never prevents a connection from being established. 32 KiB is
/// the limit gocql and csharp-driver use, keeping it consistent across drivers.
///
/// It also leaves a 2x margin to the driver's own hard limit: `write_string`
/// prefixes every `STARTUP` value with a 16-bit length via `write_short_length`,
/// so a value above 64 KiB would fail `STARTUP` serialization and take down the
/// control-connection handshake, and with it session creation.
const MAX_REPORT_SIZE: usize = 32 * 1024;

/// The reported configuration document.
///
/// The `connection`, `control-plane` and `query` groups are yet to be added.
/// The schema requires all three, so the report this currently produces does
/// not validate against it.
#[derive(Serialize)]
struct Report {
    version: u32,
}

/// Builds the configuration report of a session.
///
/// Carries no configuration yet, because `{"version":1}` needs no inputs; the
/// configuration each group reports is added alongside that group.
pub(crate) struct DriverConfigReporter {}

impl DriverConfigReporter {
    pub(crate) fn new() -> Self {
        Self {}
    }

    /// Renders the report, or returns `None` when there is nothing safe to send.
    ///
    /// Never fails and never panics: the report is a diagnostic aid, and must
    /// never prevent a connection from being established. Problems are logged
    /// and the option omitted.
    ///
    /// The report is rebuilt on every call rather than cached, because it is not
    /// constant for the lifetime of a session: `target_is_scylladb` is only
    /// known after the `OPTIONS`/`SUPPORTED` exchange of the connection being
    /// established, and configuration read through an `ExecutionProfileHandle`
    /// can be remapped at runtime.
    ///
    /// `target_is_scylladb` tells whether the peer is a ScyllaDB node, as some
    /// reported values only apply to ScyllaDB. It is not read yet, hence the
    /// scoped `expect`: a leading underscore would only have to be renamed once
    /// a group starts using it.
    pub(crate) fn report(
        &self,
        #[expect(unused_variables)] target_is_scylladb: bool,
    ) -> Option<String> {
        let report = Report {
            version: REPORT_VERSION,
        };

        // `serde_json::to_string` cannot fail for the report's current shape; the
        // branch keeps reporting best effort once a future one can fail. It will
        // not catch a non-finite `f64`: `serde_json` emits those as `null`, which
        // the schema rejects under a numeric key, so such values have to be
        // filtered out where they are read rather than here.
        let report = match serde_json::to_string(&report) {
            Ok(report) => report,
            Err(err) => {
                tracing::warn!("Failed to serialize the driver configuration report: {err}");
                return None;
            }
        };

        if report.len() > MAX_REPORT_SIZE {
            tracing::warn!(
                "Driver configuration report is too large to be reported: {} bytes, the limit is {} bytes",
                report.len(),
                MAX_REPORT_SIZE
            );
            return None;
        }

        Some(report)
    }
}

#[cfg(test)]
#[path = "driver_config_tests.rs"]
mod tests;
