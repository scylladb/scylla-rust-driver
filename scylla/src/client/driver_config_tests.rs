use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use uuid::Uuid;

use super::{DriverConfigReporter, MAX_REPORT_SIZE, non_negative_millis, positive_millis};
use crate::client::execution_profile::ExecutionProfile;
use crate::client::session::SessionConfig;
use crate::cluster::metadata::Peer;
use crate::cluster::{ClusterState, NodeRef};
use crate::network::PoolSize;
use crate::policies::host_filter::{
    AcceptAllHostFilter, AllowListHostFilter, DcHostFilter, HostFilter,
};
use crate::policies::load_balancing::{
    DefaultPolicy, FallbackPlan, LatencyAwarenessBuilder, LoadBalancingPolicy, NodeIdentifier,
    RoutingInfo, SingleTargetLoadBalancingPolicy,
};
use crate::policies::reconnect::{
    ConstantReconnectPolicy, ExponentialReconnectPolicy, ReconnectPolicy, ReconnectPolicySession,
};
use crate::policies::retry::{
    DefaultRetryPolicy, DowngradingConsistencyRetryPolicy, FallthroughRetryPolicy, RetryPolicy,
    RetrySession,
};
use crate::policies::speculative_execution::{
    Context as SpeculativeExecutionContext, SimpleSpeculativeExecutionPolicy,
    SpeculativeExecutionPolicy,
};
use crate::policies::timestamp_generator::SimpleTimestampGenerator;
use crate::routing::{NodeLocationPreference, Shard};
use crate::statement::{Consistency, SerialConsistency};

/// Every consistency level the schema enumerates, with the SCREAMING_SNAKE
/// name it must be reported under. Shared by the test pinning the mapping
/// and the sweep validating the names against the schema's own enum, so the
/// two cannot come to disagree about which levels exist.
const CONSISTENCY_NAMES: [(Consistency, &str); 11] = [
    (Consistency::Any, "ANY"),
    (Consistency::One, "ONE"),
    (Consistency::Two, "TWO"),
    (Consistency::Three, "THREE"),
    (Consistency::Quorum, "QUORUM"),
    (Consistency::All, "ALL"),
    (Consistency::LocalQuorum, "LOCAL_QUORUM"),
    (Consistency::EachQuorum, "EACH_QUORUM"),
    (Consistency::LocalOne, "LOCAL_ONE"),
    (Consistency::Serial, "SERIAL"),
    (Consistency::LocalSerial, "LOCAL_SERIAL"),
];

/// [`CONSISTENCY_NAMES`] for the serial levels.
const SERIAL_CONSISTENCY_NAMES: [(SerialConsistency, &str); 2] = [
    (SerialConsistency::Serial, "SERIAL"),
    (SerialConsistency::LocalSerial, "LOCAL_SERIAL"),
];

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

#[derive(Debug)]
struct ForeignRetryPolicy;

impl RetryPolicy for ForeignRetryPolicy {
    fn new_session(&self) -> Box<dyn RetrySession> {
        unimplemented!("the report never runs the policy")
    }
}

#[derive(Debug)]
struct ForeignSpeculativeExecutionPolicy;

impl SpeculativeExecutionPolicy for ForeignSpeculativeExecutionPolicy {
    fn max_retry_count(&self, _: &SpeculativeExecutionContext) -> usize {
        unimplemented!("the report never runs the policy")
    }

    fn retry_interval(&self, _: &SpeculativeExecutionContext) -> Duration {
        unimplemented!("the report never runs the policy")
    }
}

/// Unlike its siblings this one carries its `name`, because a load balancing
/// policy is reported by [`LoadBalancingPolicy::name`] rather than by type
/// name. That makes it the only input to the document with no bound on its
/// length, which [`oversized_reports_are_omitted`] relies on, and the only
/// one that can be empty, which `nonEmptyString` rejects.
#[derive(Debug)]
struct ForeignLoadBalancingPolicy(String);

impl ForeignLoadBalancingPolicy {
    fn new(name: impl Into<String>) -> Arc<Self> {
        Arc::new(Self(name.into()))
    }
}

impl LoadBalancingPolicy for ForeignLoadBalancingPolicy {
    fn pick<'a>(
        &'a self,
        _request: &'a RoutingInfo,
        _cluster: &'a ClusterState,
    ) -> Option<(NodeRef<'a>, Option<Shard>)> {
        unimplemented!("the report never runs the policy")
    }

    fn fallback<'a>(
        &'a self,
        _request: &'a RoutingInfo,
        _cluster: &'a ClusterState,
    ) -> FallbackPlan<'a> {
        unimplemented!("the report never runs the policy")
    }

    fn name(&self) -> String {
        self.0.clone()
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
const DEFAULT_REPORT: &str = r#"{"version":1,"connection":{"connect":{"timeout-ms":5000},"requests":{"in-flight":{"max":32768},"orphaned":{"max":1024}},"pool":{"shard-aware":{"enabled":true}},"socket":{"tcp-no-delay":true,"keep-alive":false,"reuse-address":false},"reconnection":{"policy":{"type":"exponential","base-ms":50,"max-ms":10000}}},"control-plane":{"queries":{"system":{"timeout":{"client-side-ms":31000,"server-side-ms":30000}}},"schema":{"agreement":{"timeout-ms":60000}}},"query":{"defaults":{"consistency":"LOCAL_QUORUM","serial-consistency":"LOCAL_SERIAL","idempotence":false,"client-timestamps":false,"page":{"size":5000},"request":{"timeout-ms":30000}},"retry":{"policy":{"type":"standard-error-aware"}},"load-balancing":{"policy":{"type":"token-aware","load-distribution":"shuffle","fallback-to-non-preferred-nodes":true}}}}"#;

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
    config.timestamp_generator = Some(Arc::new(SimpleTimestampGenerator::new()));
    config.default_execution_profile_handle = ExecutionProfile::builder()
        .consistency(Consistency::Quorum)
        .serial_consistency(Some(SerialConsistency::Serial))
        .request_timeout(Some(Duration::from_millis(1500)))
        .retry_policy(Arc::new(DowngradingConsistencyRetryPolicy::new()))
        .build()
        .into_handle();
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
const CUSTOMISED_REPORT: &str = r#"{"version":1,"connection":{"connect":{"timeout-ms":1234},"requests":{"in-flight":{"max":32768},"orphaned":{"max":1024}},"pool":{"shard-aware":{"enabled":false}},"socket":{"tcp-no-delay":false,"keep-alive":true,"reuse-address":true,"linger":{"interval-s":7},"receive-buffer":{"size-bytes":65536},"send-buffer":{"size-bytes":32768}},"reconnection":{"policy":{"type":"constant","delay-ms":250}},"node-preference":{"type":"dc","local-dc":"dc1"}},"control-plane":{"queries":{"system":{"timeout":{"client-side-ms":31000,"server-side-ms":30000}}},"schema":{"agreement":{"timeout-ms":60000}}},"query":{"defaults":{"consistency":"QUORUM","serial-consistency":"SERIAL","idempotence":false,"client-timestamps":true,"page":{"size":5000},"request":{"timeout-ms":1500}},"retry":{"policy":{"type":"downgrading-consistency"}},"load-balancing":{"policy":{"type":"token-aware","load-distribution":"shuffle","fallback-to-non-preferred-nodes":true}}}}"#;

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

fn reporter_with_profile(profile: ExecutionProfile) -> DriverConfigReporter {
    let mut config = SessionConfig::new();
    config.default_execution_profile_handle = profile.into_handle();
    reporter(&config)
}

/// The profile is read at report time, so remapping the handle a session was
/// built with must change what the next report says. Nothing would otherwise
/// stop the report from describing a profile the session no longer uses.
#[test]
fn remapping_the_default_profile_changes_the_report() {
    let mut handle = ExecutionProfile::builder().build().into_handle();
    let mut config = SessionConfig::new();
    config.default_execution_profile_handle = handle.clone();
    let reporter = reporter(&config);

    // The one group a profile feeds, in full, so that remapping is seen to
    // change every key of it that the new profile touches - and none other.
    let token_aware_load_balancing = serde_json::json!({
        "policy": {
            "type": "token-aware",
            "load-distribution": "shuffle",
            "fallback-to-non-preferred-nodes": true,
        },
    });

    let before = reporter.report(true).unwrap();
    assert_eq!(
        group(&before, "/query"),
        serde_json::json!({
            "defaults": {
                "consistency": "LOCAL_QUORUM",
                "serial-consistency": "LOCAL_SERIAL",
                "idempotence": false,
                "client-timestamps": false,
                "page": {"size": 5000},
                "request": {"timeout-ms": 30000},
            },
            "retry": {"policy": {"type": "standard-error-aware"}},
            "load-balancing": token_aware_load_balancing,
        })
    );

    handle.map_to_another_profile(
        ExecutionProfile::builder()
            .consistency(Consistency::EachQuorum)
            .serial_consistency(Some(SerialConsistency::Serial))
            .request_timeout(Some(Duration::from_secs(3)))
            .retry_policy(Arc::new(FallthroughRetryPolicy::new()))
            .build(),
    );

    let after = reporter.report(true).unwrap();
    assert_ne!(before, after);
    assert_eq!(
        group(&after, "/query"),
        serde_json::json!({
            "defaults": {
                "consistency": "EACH_QUORUM",
                "serial-consistency": "SERIAL",
                "idempotence": false,
                "client-timestamps": false,
                "page": {"size": 5000},
                "request": {"timeout-ms": 3000},
            },
            "retry": {"policy": {"type": "fallthrough"}},
            "load-balancing": token_aware_load_balancing,
        })
    );
}

/// [`reconnection_policies`] for the retry policies, with the `query.retry`
/// group each must be reported as.
fn retry_policies() -> [(&'static str, Arc<dyn RetryPolicy>, Value); 4] {
    [
        (
            "default retry",
            Arc::new(DefaultRetryPolicy::new()),
            serde_json::json!({"policy": {"type": "standard-error-aware"}}),
        ),
        (
            "fallthrough retry",
            Arc::new(FallthroughRetryPolicy::new()),
            serde_json::json!({"policy": {"type": "fallthrough"}}),
        ),
        (
            "downgrading consistency retry",
            Arc::new(DowngradingConsistencyRetryPolicy::new()),
            serde_json::json!({"policy": {"type": "downgrading-consistency"}}),
        ),
        (
            "foreign retry policy",
            Arc::new(ForeignRetryPolicy),
            serde_json::json!({"policy": {"type": "custom", "name": "ForeignRetryPolicy"}}),
        ),
    ]
}

#[test]
fn retry_policy_variants() {
    for (name, policy, expected) in retry_policies() {
        let profile = ExecutionProfile::builder().retry_policy(policy).build();
        let report = reporter_with_profile(profile).report(true).unwrap();
        assert_eq!(group(&report, "/query/retry"), expected, "{name}");
    }
}

/// The schema's consistency enums are SCREAMING_SNAKE, which neither
/// `Display` nor `Debug` produces, so every name is spelled out by hand -
/// and every one of them is checked here, both levels being an exhaustive
/// match that a variant added upstream must fail to compile against.
#[test]
fn consistency_names_follow_the_schema() {
    for (level, name) in CONSISTENCY_NAMES {
        let profile = ExecutionProfile::builder().consistency(level).build();
        let report = reporter_with_profile(profile).report(true).unwrap();
        assert_eq!(
            group(&report, "/query/defaults/consistency"),
            serde_json::json!(name)
        );
    }

    for (level, name) in SERIAL_CONSISTENCY_NAMES {
        let profile = ExecutionProfile::builder()
            .serial_consistency(Some(level))
            .build();
        let report = reporter_with_profile(profile).report(true).unwrap();
        assert_eq!(
            group(&report, "/query/defaults/serial-consistency"),
            serde_json::json!(name)
        );
    }
}

/// Both keys the schema leaves optional: an unset serial consistency drops
/// its key, a disabled request timeout drops the whole `request` object.
#[test]
fn unset_query_defaults_are_omitted() {
    let profile = ExecutionProfile::builder()
        .serial_consistency(None)
        .request_timeout(None)
        .build();

    let report = reporter_with_profile(profile).report(true).unwrap();
    assert_eq!(
        group(&report, "/query/defaults"),
        serde_json::json!({
            "consistency": "LOCAL_QUORUM",
            "idempotence": false,
            "client-timestamps": false,
            "page": {"size": 5000},
        })
    );
}

fn load_balancing_report(
    policy: Arc<dyn LoadBalancingPolicy>,
    session_preference: NodeLocationPreference,
) -> serde_json::Value {
    let mut config = SessionConfig::new();
    config.node_location_preference = session_preference;
    config.default_execution_profile_handle = ExecutionProfile::builder()
        .load_balancing_policy(policy)
        .build()
        .into_handle();

    let report = reporter(&config).report(true).unwrap();
    group(&report, "/query/load-balancing")
}

/// The expected `query.load-balancing` of a token-aware `DefaultPolicy`
/// with replica shuffling on and adaptive ordering off: the shape nearly
/// every case below shares, differing only in the derived
/// `fallback-to-non-preferred-nodes` and in the preference beside it. The
/// few cases that vary anything else fill in the difference themselves.
fn token_aware_expectation(
    fallback_to_non_preferred_nodes: bool,
    node_preference: Option<serde_json::Value>,
) -> serde_json::Value {
    let mut expected = serde_json::json!({
        "policy": {
            "type": "token-aware",
            "load-distribution": "shuffle",
            "fallback-to-non-preferred-nodes": fallback_to_non_preferred_nodes,
        },
    });

    if let Some(node_preference) = node_preference {
        expected["node-preference"] = node_preference;
    }
    expected
}

/// [`dc_preference`] naming a rack as well, which only a load balancing
/// policy ever does: no host filter states a rack.
fn rack_preference(local_dc: &str, local_rack: &str) -> serde_json::Value {
    serde_json::json!({"type": "rack", "local-dc": local_dc, "local-rack": local_rack})
}

/// Every combination of `DefaultPolicy`'s reportable knobs, and above all
/// every case of `fallback-to-non-preferred-nodes`: it is the one key
/// derived rather than read, and the rack case says `true` even with
/// datacenter failover disabled, which reads like a bug until one follows
/// `DefaultPolicy::fallback`'s plan composition.
///
/// A Tokio runtime is needed only by [`LatencyAwarenessBuilder::build`],
/// which spawns the task updating latency averages.
#[tokio::test]
async fn default_load_balancing_policy_matrix() {
    let dc = || "dc1".to_owned();
    let rack = || "rack1".to_owned();

    // Token awareness off: the schema's built-in branch presumes it, so the
    // policy is reported as custom - preference and failover included, since
    // the custom branch carries neither.
    assert_eq!(
        load_balancing_report(
            DefaultPolicy::builder()
                .token_aware(false)
                .prefer_datacenter(dc())
                .build(),
            NodeLocationPreference::Any,
        ),
        serde_json::json!({
            "policy": {"type": "custom", "name": "DefaultPolicy"},
            "node-preference": dc_preference("dc1"),
        })
    );

    // No preference at all: nothing confines requests, so failover cannot
    // change the answer.
    for permit_dc_failover in [false, true] {
        assert_eq!(
            load_balancing_report(
                DefaultPolicy::builder()
                    .permit_dc_failover(permit_dc_failover)
                    .build(),
                NodeLocationPreference::Any,
            ),
            token_aware_expectation(true, None)
        );
    }

    // A datacenter preference is the only case failover decides.
    for (permit_dc_failover, fallback) in [(false, false), (true, true)] {
        assert_eq!(
            load_balancing_report(
                DefaultPolicy::builder()
                    .prefer_datacenter(dc())
                    .permit_dc_failover(permit_dc_failover)
                    .build(),
                NodeLocationPreference::Any,
            ),
            token_aware_expectation(fallback, Some(dc_preference("dc1")))
        );
    }

    // A rack preference is always escaped: the fallback plan chains the
    // local datacenter's other racks after the local rack unconditionally.
    for permit_dc_failover in [false, true] {
        assert_eq!(
            load_balancing_report(
                DefaultPolicy::builder()
                    .prefer_datacenter_and_rack(dc(), rack())
                    .permit_dc_failover(permit_dc_failover)
                    .build(),
                NodeLocationPreference::Any,
            ),
            token_aware_expectation(true, Some(rack_preference("dc1", "rack1")))
        );
    }

    // Latency awareness is the only adaptive-ordering signal the driver has.
    let mut adaptive = token_aware_expectation(false, Some(dc_preference("dc1")));
    adaptive["policy"]["adaptive-ordering"] = serde_json::json!({"signals": ["latency"]});
    assert_eq!(
        load_balancing_report(
            DefaultPolicy::builder()
                .prefer_datacenter(dc())
                .permit_dc_failover(false)
                .latency_awareness(LatencyAwarenessBuilder::new())
                .build(),
            NodeLocationPreference::Any,
        ),
        adaptive
    );
}

/// `load-distribution` is the one key that must follow `fixed_seed` rather
/// than the policy's type: a fixed seed makes every query plan reuse the
/// same replica choice and the same permutation, which is not the schema's
/// randomised `shuffle`.
#[test]
fn load_distribution_follows_replica_shuffling() {
    for (enable_shuffling, distribution) in [(true, "shuffle"), (false, "replica-set")] {
        let mut expected = token_aware_expectation(true, None);
        expected["policy"]["load-distribution"] = serde_json::json!(distribution);

        assert_eq!(
            load_balancing_report(
                DefaultPolicy::builder()
                    .enable_shuffling_replicas(enable_shuffling)
                    .build(),
                NodeLocationPreference::Any,
            ),
            expected
        );
    }
}

/// `node-preference` reports the preference the policy actually routes by,
/// which `DefaultPolicy::routing_info` takes from the policy when it states
/// one and from the session otherwise. An empty datacenter name is no
/// preference at all; an empty rack name only drops the rack half.
#[test]
fn effective_node_preference_is_reported() {
    let unconfined = token_aware_expectation(true, None);

    // No policy-level preference: the session-level one is reported.
    assert_eq!(
        load_balancing_report(
            DefaultPolicy::builder().permit_dc_failover(false).build(),
            NodeLocationPreference::DatacenterAndRack("dc9".to_owned(), "rack9".to_owned()),
        ),
        token_aware_expectation(true, Some(rack_preference("dc9", "rack9")))
    );

    // A policy-level preference wins over the session-level one.
    assert_eq!(
        load_balancing_report(
            DefaultPolicy::builder()
                .prefer_datacenter("dc1".to_owned())
                .permit_dc_failover(false)
                .build(),
            NodeLocationPreference::Datacenter("dc9".to_owned()),
        ),
        token_aware_expectation(false, Some(dc_preference("dc1")))
    );

    // Including when it is an explicit "no preference", which also makes
    // `fallback-to-non-preferred-nodes` true despite failover being off.
    assert_eq!(
        load_balancing_report(
            DefaultPolicy::builder()
                .prefer_no_datacenter()
                .permit_dc_failover(false)
                .build(),
            NodeLocationPreference::Datacenter("dc9".to_owned()),
        ),
        unconfined
    );

    // An empty datacenter name leaves nothing expressible to report.
    let empty_datacenters = [
        NodeLocationPreference::Datacenter(String::new()),
        NodeLocationPreference::DatacenterAndRack(String::new(), "rack1".to_owned()),
        NodeLocationPreference::DatacenterAndRack(String::new(), String::new()),
    ];

    for preference in empty_datacenters {
        assert_eq!(
            load_balancing_report(
                DefaultPolicy::builder().permit_dc_failover(false).build(),
                preference,
            ),
            unconfined
        );
    }

    // An empty rack name drops only the rack half: the datacenter still
    // confines routing, so it is reported and `permit_dc_failover` decides
    // `fallback-to-non-preferred-nodes` exactly as for a plain datacenter
    // preference.
    for (permit_dc_failover, fallback) in [(false, false), (true, true)] {
        assert_eq!(
            load_balancing_report(
                DefaultPolicy::builder()
                    .permit_dc_failover(permit_dc_failover)
                    .build(),
                NodeLocationPreference::DatacenterAndRack("dc1".to_owned(), String::new()),
            ),
            token_aware_expectation(fallback, Some(dc_preference("dc1")))
        );
    }
}

/// Anything that is not a token-aware `DefaultPolicy` is named by its own
/// [`LoadBalancingPolicy::name`], which is user-implemented and may be
/// empty - and an empty `name` is not a value the schema accepts.
#[test]
fn custom_load_balancing_policies_are_reported_by_name() {
    let named: [(Arc<dyn LoadBalancingPolicy>, &str); 3] = [
        (
            ForeignLoadBalancingPolicy::new("ForeignPolicy"),
            "ForeignPolicy",
        ),
        // An empty name would fail the schema's `nonEmptyString`.
        (ForeignLoadBalancingPolicy::new(""), "unknown"),
        (
            SingleTargetLoadBalancingPolicy::new(NodeIdentifier::HostId(Uuid::nil()), None),
            "SingleTargetLoadBalancingPolicy",
        ),
    ];

    for (policy, name) in named {
        assert_eq!(
            load_balancing_report(policy, NodeLocationPreference::Any),
            serde_json::json!({"policy": {"type": "custom", "name": name}})
        );
    }
}

fn speculative_execution_report(
    policy: Option<Arc<dyn SpeculativeExecutionPolicy>>,
) -> Option<serde_json::Value> {
    let profile = ExecutionProfile::builder()
        .speculative_execution_policy(policy)
        .build();
    let report = reporter_with_profile(profile).report(true).unwrap();
    // Asserted on the serialized report rather than on the parsed value: a
    // non-finite `f64` reaches the document as a bare `null`, and the point
    // is to catch it in the bytes the server would be sent.
    assert!(!report.contains("null"), "{report}");
    optional_group(&report, "/query/speculative-execution")
}

/// [`reconnection_policies`] for the speculative execution policies, with
/// the `query.speculative-execution` group each must be reported as - which
/// for two of them is no group at all.
#[expect(clippy::type_complexity)]
fn speculative_execution_policies() -> [(
    &'static str,
    Option<Arc<dyn SpeculativeExecutionPolicy>>,
    Option<Value>,
); 5] {
    [
        // Speculative execution is disabled by default.
        ("no speculative execution", None, None),
        (
            "constant speculative execution",
            Some(Arc::new(SimpleSpeculativeExecutionPolicy {
                max_retry_count: 2,
                retry_interval: Duration::from_millis(150),
            })),
            Some(serde_json::json!({
                "policy": {"type": "constant", "max-executions": 2, "delay-ms": 150},
            })),
        ),
        (
            // A zero interval means "launch immediately", not "unset".
            "zero-interval speculative execution",
            Some(Arc::new(SimpleSpeculativeExecutionPolicy {
                max_retry_count: 1,
                retry_interval: Duration::ZERO,
            })),
            Some(serde_json::json!({
                "policy": {"type": "constant", "max-executions": 1, "delay-ms": 0},
            })),
        ),
        (
            // A policy permitting no extra execution never fires, so there
            // is no speculative execution to report.
            "zero-count speculative execution",
            Some(Arc::new(SimpleSpeculativeExecutionPolicy {
                max_retry_count: 0,
                retry_interval: Duration::from_millis(150),
            })),
            None,
        ),
        (
            "foreign speculative execution policy",
            Some(Arc::new(ForeignSpeculativeExecutionPolicy)),
            Some(serde_json::json!({
                "policy": {"type": "custom", "name": "ForeignSpeculativeExecutionPolicy"},
            })),
        ),
    ]
}

#[test]
fn speculative_execution_report_cases() {
    for (name, policy, expected) in speculative_execution_policies() {
        assert_eq!(speculative_execution_report(policy), expected, "{name}");
    }
}

/// [`PercentileSpeculativeExecutionPolicy::percentile`] is an unvalidated
/// `f64`, and the schema's bounds on it are exclusive. A non-finite value is
/// the dangerous one: `serde_json` renders it as `null`, which no downstream
/// check would catch, hence the `null`-free assertion on every report here.
#[cfg(feature = "metrics")]
#[test]
fn percentile_speculative_execution_report_cases() {
    use crate::policies::speculative_execution::PercentileSpeculativeExecutionPolicy;

    let percentile_report = |max_retry_count, percentile| {
        speculative_execution_report(Some(Arc::new(PercentileSpeculativeExecutionPolicy {
            max_retry_count,
            percentile,
        })))
    };

    assert_eq!(
        percentile_report(3, 99.0),
        Some(serde_json::json!({
            "policy": {"type": "percentile", "max-executions": 3, "percentile": 99.0},
        }))
    );

    // Out of the schema's exclusive range, or not a number at all: reported
    // as a policy the driver cannot describe rather than not at all, because
    // speculative execution does fire. `-1.0` is the only case reaching the
    // lower-bound comparison - the non-finite values are rejected before it
    // - so it is what pins `<= 0.0` rather than `== 0.0`.
    let unreportable = [
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        -1.0,
        0.0,
        100.0,
        150.0,
    ];

    for percentile in unreportable {
        assert_eq!(
            percentile_report(3, percentile),
            Some(serde_json::json!({
                "policy": {
                    "type": "custom",
                    "name": "PercentileSpeculativeExecutionPolicy",
                },
            })),
            "{percentile}"
        );
    }

    // The zero-count rule applies whatever the percentile is.
    assert_eq!(percentile_report(0, 99.0), None);
    assert_eq!(percentile_report(0, f64::NAN), None);
}
