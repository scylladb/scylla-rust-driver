use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use super::{DriverConfigReporter, MAX_REPORT_SIZE, non_negative_millis, positive_millis};
use crate::client::session::SessionConfig;
use crate::cluster::metadata::Peer;
use crate::network::PoolSize;
use crate::policies::host_filter::{
    AcceptAllHostFilter, AllowListHostFilter, DcHostFilter, HostFilter,
};
use crate::policies::reconnect::{
    ConstantReconnectPolicy, ExponentialReconnectPolicy, ReconnectPolicy, ReconnectPolicySession,
};

/// A reconnection policy the driver cannot recognise: it keeps the trait's
/// introspection defaults, so it is neither downcastable nor named.
///
/// This and its three siblings below are what drive the schema's `custom`
/// branches. They are declared once here rather than per test, so that the
/// name each is reported under - its own type name, via
/// `policies::simple_type_name` - is written down in one place.
#[derive(Debug)]
struct ForeignReconnectPolicy;

impl ReconnectPolicy for ForeignReconnectPolicy {
    fn new_session(&self) -> Box<dyn ReconnectPolicySession> {
        unimplemented!("the report never runs the policy")
    }
}

/// A host filter that states no location, so that the report has nothing to
/// derive `connection.node-preference` from.
struct ForeignHostFilter;

impl HostFilter for ForeignHostFilter {
    fn accept(&self, _peer: &Peer) -> bool {
        true
    }
}

/// The value the report carries at `pointer`.
///
/// Assertions on one group of the document go through this rather than
/// through substring surgery on the serialized report. A group is delimited
/// by whichever key happens to follow it, so slicing one out by hand breaks
/// as soon as a key is added or reordered - and breaks by matching a
/// different span, not by failing to match. Key order is pinned by the
/// whole-report assertions instead, which is the one place it belongs.
fn group(report: &str, pointer: &str) -> serde_json::Value {
    optional_group(report, pointer)
        .unwrap_or_else(|| panic!("the report has nothing at {pointer}: {report}"))
}

/// [`group`], for a group the report may legitimately omit.
fn optional_group(report: &str, pointer: &str) -> Option<serde_json::Value> {
    let report: serde_json::Value = serde_json::from_str(report).unwrap();
    report.pointer(pointer).cloned()
}

/// The `node-preference` group naming a datacenter. The schema shares one
/// definition between `connection.node-preference`, which a host filter
/// feeds, and `query.load-balancing.node-preference`, which the load
/// balancing policy does, so the tests of both share this.
fn dc_preference(local_dc: &str) -> serde_json::Value {
    serde_json::json!({"type": "dc", "local-dc": local_dc})
}

/// `report` with each `find` replaced by its `with`.
///
/// The tests below that pin a whole document mostly differ from
/// [`DEFAULT_REPORT`] or [`CUSTOMISED_REPORT`] in a key or two, and saying
/// which keys those are is the point of the test; restating a kilobyte of
/// JSON leaves a reader to find the difference by eye. Each substring must
/// occur exactly once, so that a patch matching nothing - or matching
/// somewhere else as well - fails here instead of quietly asserting
/// something other than what it reads as.
fn patched(report: &str, patches: &[(&str, &str)]) -> String {
    let mut report = report.to_owned();
    for (find, with) in patches {
        assert_eq!(
            report.matches(find).count(),
            1,
            "{find} does not occur exactly once in {report}"
        );
        report = report.replace(find, with);
    }
    report
}

/// The report of an unconfigured session against a ScyllaDB target, spelled
/// out in full: the one document key order is pinned on, and the baseline
/// the single-axis cases below are expressed as patches of.
const DEFAULT_REPORT: &str = r#"{"version":1,"connection":{"connect":{"timeout-ms":5000},"requests":{"in-flight":{"max":32768},"orphaned":{"max":1024}},"pool":{"shard-aware":{"enabled":true}},"socket":{"tcp-no-delay":true,"keep-alive":false,"reuse-address":false},"reconnection":{"policy":{"type":"exponential","base-ms":50,"max-ms":10000}}},"control-plane":{"queries":{"system":{"timeout":{"client-side-ms":31000,"server-side-ms":30000}}},"schema":{"agreement":{"timeout-ms":60000}}}}"#;

fn reporter(config: &SessionConfig) -> DriverConfigReporter {
    reporter_with_policy(config, Arc::new(ExponentialReconnectPolicy::new()))
}

fn reporter_with_policy(
    config: &SessionConfig,
    policy: Arc<dyn ReconnectPolicy>,
) -> DriverConfigReporter {
    DriverConfigReporter::new(
        config,
        config.tcp_socket_options(),
        policy,
        config.metadata_request_timeouts(),
    )
}

#[test]
fn default_configuration_report() {
    let report = reporter(&SessionConfig::new()).report(true).unwrap();
    assert_eq!(report, DEFAULT_REPORT);
    assert!(report.len() < MAX_REPORT_SIZE / 10);
}

/// A per-host pool never dials the shard-aware port, whatever
/// `disallow_shard_aware_port` says.
#[test]
fn shard_aware_port_is_not_reported_as_enabled_for_a_per_host_pool() {
    let mut config = SessionConfig::new();
    config.disallow_shard_aware_port = false;
    config.connection_pool_size = PoolSize::PerHost(NonZeroUsize::new(2).unwrap());

    let report = reporter(&config).report(true).unwrap();
    assert_eq!(
        group(&report, "/connection/pool/shard-aware"),
        serde_json::json!({"enabled": false})
    );
}

/// Zero values are where the schema's `positiveInteger` keys can go invalid.
/// A zero buffer size cannot be expressed and is dropped; a zero connect
/// timeout is a real timeout and reports the schema minimum; `linger` is
/// `nonNegativeInteger`, so its 0 is reported verbatim.
#[test]
fn zero_valued_configuration_report() {
    let mut config = SessionConfig::new();
    config.connect_timeout = Duration::ZERO;
    config.tcp_recv_buffer_size = Some(0);
    config.tcp_linger = Some(Duration::ZERO);

    assert_eq!(
        reporter(&config).report(true).unwrap(),
        patched(
            DEFAULT_REPORT,
            &[
                (r#""timeout-ms":5000"#, r#""timeout-ms":1"#),
                (
                    r#""reuse-address":false"#,
                    r#""reuse-address":false,"linger":{"interval-s":0}"#,
                ),
            ],
        )
    );
}

/// Everything the customised configuration below shares with
/// [`tls_is_reported_as_an_empty_object`], which needs a TLS backend and so
/// must be feature-gated as a whole.
fn customised_config() -> SessionConfig {
    let mut config = SessionConfig::new();
    config.connect_timeout = Duration::from_millis(1234);
    config.tcp_nodelay = false;
    config.tcp_keepalive_interval = Some(Duration::from_secs(30));
    config.tcp_recv_buffer_size = Some(65536);
    config.tcp_send_buffer_size = Some(32768);
    config.tcp_reuse_address = Some(true);
    config.tcp_linger = Some(Duration::from_secs(7));
    config.disallow_shard_aware_port = true;
    config.host_filter = Some(Arc::new(DcHostFilter::new("dc1".to_owned())));
    config
}

fn customised_report(config: &SessionConfig) -> String {
    reporter_with_policy(
        config,
        Arc::new(ConstantReconnectPolicy::new(Duration::from_millis(250))),
    )
    .report(true)
    .unwrap()
}

/// The report of [`customised_config`], the second document spelled out in
/// full: nearly every key of it differs from [`DEFAULT_REPORT`], so there is
/// no difference to point at.
const CUSTOMISED_REPORT: &str = r#"{"version":1,"connection":{"connect":{"timeout-ms":1234},"requests":{"in-flight":{"max":32768},"orphaned":{"max":1024}},"pool":{"shard-aware":{"enabled":false}},"socket":{"tcp-no-delay":false,"keep-alive":true,"reuse-address":true,"linger":{"interval-s":7},"receive-buffer":{"size-bytes":65536},"send-buffer":{"size-bytes":32768}},"reconnection":{"policy":{"type":"constant","delay-ms":250}},"node-preference":{"type":"dc","local-dc":"dc1"}},"control-plane":{"queries":{"system":{"timeout":{"client-side-ms":31000,"server-side-ms":30000}}},"schema":{"agreement":{"timeout-ms":60000}}}}"#;

#[test]
fn fully_customised_configuration_report() {
    assert_eq!(customised_report(&customised_config()), CUSTOMISED_REPORT);
}

/// Needs a TLS backend to build a `TlsContext` at all, hence the gate.
#[cfg(feature = "openssl-010")]
#[test]
fn tls_is_reported_as_an_empty_object() {
    use openssl::ssl::{SslContextBuilder, SslMethod};

    let mut config = customised_config();
    config.tls_context = Some(
        SslContextBuilder::new(SslMethod::tls())
            .unwrap()
            .build()
            .into(),
    );

    assert_eq!(
        customised_report(&config),
        patched(
            CUSTOMISED_REPORT,
            &[(r#""local-dc":"dc1"}}"#, r#""local-dc":"dc1"},"tls":{}}"#)],
        )
    );
}

/// `server-side-ms` describes the `USING TIMEOUT` clause the control
/// connection appends, so it must be absent whenever no such clause is sent:
/// against Cassandra, or when the clause would read `0ms`. `client-side-ms`
/// bounds a client-side deadline and is reported either way.
#[test]
fn control_plane_report_cases() {
    let control_plane = |config: &SessionConfig, target_is_scylladb: bool| {
        let report = reporter(config).report(target_is_scylladb).unwrap();
        group(&report, "/control-plane")
    };

    // A Cassandra target honours no `USING TIMEOUT`, so the whole default
    // report drops that one key and nothing else.
    assert_eq!(
        reporter(&SessionConfig::new()).report(false).unwrap(),
        patched(DEFAULT_REPORT, &[(r#","server-side-ms":30000"#, "")])
    );

    // A sub-millisecond server-side timeout would be sent as
    // `USING TIMEOUT 0ms`, which `positiveInteger` cannot carry.
    let mut config = SessionConfig::new();
    config.metadata_request_serverside_timeout = Some(Duration::from_micros(500));
    assert_eq!(
        control_plane(&config, true),
        serde_json::json!({
            "queries": {"system": {"timeout": {"client-side-ms": 1000}}},
            "schema": {"agreement": {"timeout-ms": 60000}},
        })
    );

    // No server-side timeout at all: the client-side one falls back to its
    // own default, and there is no clause to report even against ScyllaDB.
    let mut config = SessionConfig::new();
    config.metadata_request_serverside_timeout = None;
    assert_eq!(
        control_plane(&config, true),
        serde_json::json!({
            "queries": {"system": {"timeout": {"client-side-ms": 30000}}},
            "schema": {"agreement": {"timeout-ms": 60000}},
        })
    );

    // An explicit client-side override wins over the derived value; a zero
    // schema agreement timeout means "do not wait", not "unset".
    let mut config = SessionConfig::new();
    config.metadata_request_serverside_timeout = Some(Duration::from_secs(5));
    config.metadata_request_clientside_timeout = Some(Duration::from_secs(7));
    config.schema_agreement_timeout = Duration::ZERO;
    assert_eq!(
        control_plane(&config, true),
        serde_json::json!({
            "queries": {"system": {"timeout": {
                "client-side-ms": 7000,
                "server-side-ms": 5000,
            }}},
            "schema": {"agreement": {"timeout-ms": 0}},
        })
    );
}

/// Every reconnection policy the report has a branch for, named, with the
/// `connection.reconnection.policy` object it must be reported as.
///
/// This and the three tables below are each read twice: by the test pinning
/// the objects, and by [`conformance_configurations`], which puts the same
/// policies through the schema. Listing them once is what keeps a policy
/// added to one of those from being missed by the other.
fn reconnection_policies() -> [(&'static str, Arc<dyn ReconnectPolicy>, Value); 4] {
    [
        (
            "default exponential reconnection",
            Arc::new(ExponentialReconnectPolicy::new()),
            serde_json::json!({"type": "exponential", "base-ms": 50, "max-ms": 10000}),
        ),
        (
            // Both bounds sub-millisecond, so both floor to the schema
            // minimum of 1 rather than to the 0 `as_millis` would give.
            "sub-millisecond exponential reconnection",
            Arc::new(
                ExponentialReconnectPolicy::new()
                    .with_backoff_limits(Duration::from_micros(1), Duration::from_micros(2)),
            ),
            serde_json::json!({"type": "exponential", "base-ms": 1, "max-ms": 1}),
        ),
        (
            "zero constant reconnection",
            Arc::new(ConstantReconnectPolicy::new(Duration::ZERO)),
            serde_json::json!({"type": "constant", "delay-ms": 0}),
        ),
        (
            "foreign reconnection policy",
            Arc::new(ForeignReconnectPolicy),
            serde_json::json!({"type": "custom", "name": "ForeignReconnectPolicy"}),
        ),
    ]
}

#[test]
fn reconnection_policy_variants() {
    let config = SessionConfig::new();

    for (name, policy, expected) in reconnection_policies() {
        let report = reporter_with_policy(&config, policy).report(true).unwrap();
        assert_eq!(
            group(&report, "/connection/reconnection/policy"),
            expected,
            "{name}"
        );
    }
}

/// [`reconnection_policies`] for the host filters, with the
/// `connection.node-preference` group each must be reported as. Only
/// [`DcHostFilter`] states a location at all, and only a non-empty
/// datacenter name is expressible.
fn host_filters() -> [(&'static str, Arc<dyn HostFilter>, Option<Value>); 5] {
    [
        (
            "dc host filter",
            Arc::new(DcHostFilter::new("dc1".to_owned())),
            Some(dc_preference("dc1")),
        ),
        (
            "empty dc host filter",
            Arc::new(DcHostFilter::new(String::new())),
            None,
        ),
        (
            "allow list host filter",
            Arc::new(AllowListHostFilter::new(["127.0.0.1:9042"]).unwrap()),
            None,
        ),
        (
            "accept all host filter",
            Arc::new(AcceptAllHostFilter),
            None,
        ),
        ("foreign host filter", Arc::new(ForeignHostFilter), None),
    ]
}

#[test]
fn node_preference_follows_the_host_filter() {
    for (name, filter, expected) in host_filters() {
        let mut config = SessionConfig::new();
        config.host_filter = Some(filter);
        let report = reporter(&config).report(true).unwrap();
        assert_eq!(
            optional_group(&report, "/connection/node-preference"),
            expected,
            "{name}"
        );
    }
}

#[test]
fn millisecond_helpers_handle_zero_and_sub_millisecond_durations() {
    assert_eq!(positive_millis(Duration::from_nanos(1)), 1);
    assert_eq!(positive_millis(Duration::ZERO), 1);
    assert_eq!(positive_millis(Duration::from_millis(7)), 7);
    assert_eq!(non_negative_millis(Duration::ZERO), 0);
    assert_eq!(non_negative_millis(Duration::from_nanos(1)), 0);
}

/// `interval-s` truncates rather than flooring at 1 like the report's
/// `positiveInteger` keys do, and that is not an oversight: `SO_LINGER`
/// carries whole seconds too, and `apply_socket_options` sets it through
/// `socket2`, whose `into_linger` truncates with the same `as_secs`. So a
/// sub-second linger really does reach the kernel as the abortive close that
/// a reported 0 describes, and reporting 1 would name a lingering close the
/// socket never performs.
#[test]
fn sub_second_linger_is_reported_as_zero() {
    let linger = |linger: Duration| {
        let mut config = SessionConfig::new();
        config.tcp_linger = Some(linger);
        let report = reporter(&config).report(true).unwrap();
        group(&report, "/connection/socket/linger")
    };

    let abortive = serde_json::json!({"interval-s": 0});
    assert_eq!(linger(Duration::ZERO), abortive);
    assert_eq!(linger(Duration::from_millis(1)), abortive);
    assert_eq!(linger(Duration::from_millis(999)), abortive);
    assert_eq!(
        linger(Duration::from_millis(1999)),
        serde_json::json!({"interval-s": 1})
    );
}
