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

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;

use crate::client::session::SessionConfig;
use crate::network::{
    MAX_IN_FLIGHT_REQUESTS, OLD_ORPHAN_COUNT_THRESHOLD, PoolSize, TcpSocketOptions,
};
use crate::policies::host_filter::{DcHostFilter, HostFilter};
use crate::policies::reconnect::{
    ConstantReconnectPolicy, ExponentialReconnectPolicy, ReconnectPolicy,
};

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
/// The `control-plane` and `query` groups are yet to be added. The schema
/// requires them next to `connection`, so the report this currently produces
/// does not validate against it.
#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct Report {
    version: u32,
    connection: ConnectionReport,
}

/// The schema's `connection` group.
///
/// The schema's sibling `read`, `write` and `heartbeat` groups are never
/// populated. There is no socket read or write timeout to report at all. The
/// features that would belong under the other two - write coalescing
/// (`enable_write_coalescing`, `write_coalescing_delay`) and the CQL-level idle
/// heartbeat (`keepalive_interval`, `keepalive_timeout`) - do exist, but v1
/// declares `write.coalescing` and `heartbeat` as `additionalProperties: false`
/// with no properties: explicit placeholders for a future schema version, with
/// nowhere for those values to go.
#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct ConnectionReport {
    connect: ConnectReport,
    requests: RequestsReport,
    pool: PoolReport,
    socket: SocketReport,
    reconnection: ReconnectionReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_preference: Option<NodePreferenceReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tls: Option<TlsReport>,
}

/// `timeout-ms` is always reported: `connect_timeout` goes verbatim to
/// `tokio::time::timeout`, so it has no "unset" state that an absent key could
/// stand for.
#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct ConnectReport {
    timeout_ms: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct RequestsReport {
    in_flight: MaxReport,
    /// The driver's bound on orphaned requests is not quite what the schema
    /// asks for: it counts only orphans older than `OLD_AGE_ORPHAN_THRESHOLD`,
    /// a qualifier the schema cannot express. Reporting the threshold is still
    /// much closer to the truth than omitting the group, which means "no bound
    /// at all".
    orphaned: MaxReport,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct MaxReport {
    max: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct PoolReport {
    shard_aware: ShardAwareReport,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct ShardAwareReport {
    enabled: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct SocketReport {
    tcp_no_delay: bool,
    keep_alive: bool,
    reuse_address: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    linger: Option<LingerReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    receive_buffer: Option<BufferReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    send_buffer: Option<BufferReport>,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct LingerReport {
    /// Whole seconds, the only granularity the schema offers - and the only one
    /// `SO_LINGER` offers either. A configured linger below one second reports
    /// 0, which reads as "reset the connection on close", and that is exact
    /// rather than approximate: `apply_socket_options` sets the option through
    /// `socket2`, whose `into_linger` truncates with the very same `as_secs`, so
    /// the kernel is given the 0 this reports. Do not "fix" this to floor at 1;
    /// it would describe a lingering close the socket does not perform.
    interval_s: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct BufferReport {
    size_bytes: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct ReconnectionReport {
    /// The schema permits `null` here, meaning that no reconnection is ever
    /// attempted. This driver always has a policy in force, so it never is.
    policy: ReconnectionPolicyReport,
}

#[derive(Serialize)]
#[serde(
    tag = "type",
    rename_all = "kebab-case",
    rename_all_fields = "kebab-case"
)]
enum ReconnectionPolicyReport {
    /// `max-attempts` is omitted, which the schema defines as unlimited: the
    /// pool retries forever.
    Exponential {
        base_ms: u64,
        max_ms: u64,
    },
    Constant {
        delay_ms: u64,
    },
    Custom {
        name: &'static str,
    },
}

/// Which nodes the driver holds connections to at all - a different claim from
/// `query.load-balancing.node-preference`, which is about where a request may be
/// routed. Only a datacenter preference is representable: the schema has no
/// branch for selection by address, and no other built-in filter states a
/// location.
#[derive(Serialize)]
#[serde(
    tag = "type",
    rename_all = "kebab-case",
    rename_all_fields = "kebab-case"
)]
enum NodePreferenceReport {
    Dc { local_dc: String },
}

/// `hostname-verification` is deliberately absent, which the schema permits
/// "when this behavior is unknown", and here it genuinely is. With openssl the
/// driver never calls `X509_VERIFY_PARAM_set1_host` and `SslContextRef` exposes
/// no getter for the host verification parameter; with rustls the driver
/// connects with `ServerName::IpAddress`, and a user-installed dangerous
/// verifier is invisible to it. Reporting either boolean would be a guess.
#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct TlsReport {}

/// Milliseconds of a duration reported under a schema `positiveInteger` key.
///
/// Floored at 1: a sub-millisecond duration truncates to 0, which the schema
/// rejects, and a sub-millisecond timeout is still a timeout. Saturates instead
/// of truncating - `u64` milliseconds is half a billion years, so the clamp is
/// unreachable in practice.
fn positive_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis())
        .unwrap_or(u64::MAX)
        .max(1)
}

/// Milliseconds of a duration reported under a schema `nonNegativeInteger` key,
/// verbatim: there 0 is a meaningful value, not a disabled feature.
fn non_negative_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// A socket buffer size hint, or `None` when unset.
///
/// A configured 0 is reported as unset, because `size-bytes` is a schema
/// `positiveInteger` and 0 cannot be expressed under it. That loses a real
/// distinction: the driver does call `set_recv_buffer_size`/
/// `set_send_buffer_size` with 0, which pins the buffer to the kernel minimum,
/// whereas an unset size leaves auto-tuning on.
fn buffer_report(size: Option<usize>) -> Option<BufferReport> {
    size.filter(|size| *size > 0)
        .map(|size_bytes| BufferReport {
            // Truncating because this is what we do in the driver code when calling Tokio methods.
            size_bytes: size_bytes as u32,
        })
}

/// Builds the configuration report of a session.
///
/// Holds a snapshot of the configuration taken when the session was created,
/// because [`SessionConfig`] itself is consumed by session creation.
pub(crate) struct DriverConfigReporter {
    connect_timeout: Duration,
    socket_options: TcpSocketOptions,
    shard_aware_port_allowed: bool,
    tls_configured: bool,
    host_filter: Option<Arc<dyn HostFilter>>,
    reconnect_policy: Arc<dyn ReconnectPolicy>,
}

impl DriverConfigReporter {
    /// `socket_options` and `reconnect_policy` are passed in rather than derived
    /// from `config`, so that the report describes the values the session is
    /// actually built with: the socket options are assembled once by the caller,
    /// and the effective reconnect policy is not always the one on `config`
    /// (without the unstable feature the field does not exist and a default is
    /// used).
    pub(crate) fn new(
        config: &SessionConfig,
        socket_options: TcpSocketOptions,
        reconnect_policy: Arc<dyn ReconnectPolicy>,
    ) -> Self {
        Self {
            connect_timeout: config.connect_timeout,
            socket_options,
            // Two static conditions have to hold for a shard-aware connection to
            // ever be attempted, and the schema has one boolean for both: the
            // port must not be disallowed, and the pool must be per-shard -
            // `NodeConnectionPoolRefiller::start_filling` only takes the
            // shard-aware path for `PoolSize::PerShard`.
            shard_aware_port_allowed: !config.disallow_shard_aware_port
                && matches!(config.connection_pool_size, PoolSize::PerShard(_)),
            tls_configured: config.tls_context.is_some(),
            host_filter: config.host_filter.clone(),
            reconnect_policy,
        }
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
            connection: self.connection_report(),
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

    fn connection_report(&self) -> ConnectionReport {
        let options = &self.socket_options;

        ConnectionReport {
            connect: ConnectReport {
                timeout_ms: positive_millis(self.connect_timeout),
            },
            requests: RequestsReport {
                in_flight: MaxReport {
                    max: MAX_IN_FLIGHT_REQUESTS,
                },
                orphaned: MaxReport {
                    max: OLD_ORPHAN_COUNT_THRESHOLD,
                },
            },
            pool: PoolReport {
                shard_aware: ShardAwareReport {
                    enabled: self.shard_aware_port_allowed,
                },
            },
            socket: SocketReport {
                tcp_no_delay: options.nodelay,
                keep_alive: options.keepalive_interval.is_some(),
                // The schema asks for the effective value, and for the OS
                // default when nothing is configured. That default is off on
                // every platform the driver targets.
                reuse_address: options.reuse_address.unwrap_or(false),
                linger: options.linger.map(|linger| LingerReport {
                    interval_s: linger.as_secs(),
                }),
                receive_buffer: buffer_report(options.recv_buffer_size),
                send_buffer: buffer_report(options.send_buffer_size),
            },
            reconnection: ReconnectionReport {
                policy: self.reconnection_policy_report(),
            },
            node_preference: self.node_preference_report(),
            tls: self.tls_configured.then_some(TlsReport {}),
        }
    }

    fn reconnection_policy_report(&self) -> ReconnectionPolicyReport {
        let Some(policy) = self.reconnect_policy.as_any() else {
            return ReconnectionPolicyReport::Custom {
                name: self.reconnect_policy.reported_name(),
            };
        };

        if let Some(policy) = policy.downcast_ref::<ExponentialReconnectPolicy>() {
            // Both keys are required and `positiveInteger`, so neither may be
            // omitted or floored away. The schema also demands
            // `max-ms >= base-ms`; the policy's only constructors are `new` and
            // `with_backoff_limits`, which asserts `min <= max`, and flooring is
            // monotone, so the reported pair preserves the ordering.
            ReconnectionPolicyReport::Exponential {
                base_ms: positive_millis(policy.min_fill_backoff),
                max_ms: positive_millis(policy.max_fill_backoff),
            }
        } else if let Some(policy) = policy.downcast_ref::<ConstantReconnectPolicy>() {
            ReconnectionPolicyReport::Constant {
                delay_ms: non_negative_millis(policy.delay),
            }
        } else {
            ReconnectionPolicyReport::Custom {
                name: self.reconnect_policy.reported_name(),
            }
        }
    }

    fn node_preference_report(&self) -> Option<NodePreferenceReport> {
        let filter = self.host_filter.as_ref()?.as_any()?;
        let filter = filter.downcast_ref::<DcHostFilter>()?;

        // `local-dc` is a `nonEmptyString`, and an empty DC name accepts no node
        // anyway, so there is no preference worth reporting.
        (!filter.local_dc.is_empty()).then(|| NodePreferenceReport::Dc {
            local_dc: filter.local_dc.clone(),
        })
    }
}

#[cfg(test)]
#[path = "driver_config_tests.rs"]
mod tests;
