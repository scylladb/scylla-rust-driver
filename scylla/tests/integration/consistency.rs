use crate::utils::{setup_tracing, test_with_3_node_cluster, unique_keyspace_name, PerformDDL};
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::execution_profile::{ExecutionProfileBuilder, ExecutionProfileHandle};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::cluster::NodeRef;
use scylla::policies::load_balancing::{DefaultPolicy, LoadBalancingPolicy, RoutingInfo};
use scylla::policies::retry::FallthroughRetryPolicy;
use scylla::routing::{Shard, Token};
use scylla::statement::batch::BatchStatement;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::unprepared::Statement;
use scylla::statement::SerialConsistency;
use scylla_cql::frame::response::result::TableSpec;
use scylla_cql::Consistency;
use scylla_proxy::ShardAwareness;
use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestFrame, RequestOpcode, RequestReaction, RequestRule,
    TargetShard, WorkerError,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

fn consistencies() -> impl Iterator<Item = Consistency> {
    [
        Consistency::All,
        Consistency::Any,
        Consistency::EachQuorum,
        Consistency::LocalOne,
        Consistency::LocalQuorum,
        Consistency::One,
        Consistency::Quorum,
        Consistency::Three,
        Consistency::Two,
    ]
    .into_iter()
}

fn serial_consistencies() -> impl Iterator<Item = SerialConsistency> {
    [SerialConsistency::Serial, SerialConsistency::LocalSerial].into_iter()
}

// Every consistency and every serial consistency is yielded at least once.
// These are NOT all combinations of those.
fn pairs_of_all_consistencies() -> impl Iterator<Item = (Consistency, Option<SerialConsistency>)> {
    let consistencies = consistencies();
    let serial_consistencies = serial_consistencies()
        .map(Some)
        .chain(std::iter::repeat(None));
    consistencies.zip(serial_consistencies)
}

const CREATE_TABLE_STR: &str = "CREATE TABLE consistency_tests (a int, b int, PRIMARY KEY (a, b))";
const QUERY_STR: &str = "INSERT INTO consistency_tests (a, b) VALUES (?, 1)";

async fn create_schema(session: &Session, ks: &str) {
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}", ks)).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();

    session.ddl(CREATE_TABLE_STR).await.unwrap();
}

// The following functions perform a request with consistencies set directly on a statement.
async fn query_consistency_set_directly(
    session: &Session,
    query: &Statement,
    c: Consistency,
    sc: Option<SerialConsistency>,
) {
    let mut query = query.clone();
    query.set_consistency(c);
    query.set_serial_consistency(sc);
    session.query_unpaged(query.clone(), (1,)).await.unwrap();
    session.query_iter(query, (1,)).await.unwrap();
}

async fn execute_consistency_set_directly(
    session: &Session,
    prepared: &PreparedStatement,
    c: Consistency,
    sc: Option<SerialConsistency>,
) {
    let mut prepared = prepared.clone();
    prepared.set_consistency(c);
    prepared.set_serial_consistency(sc);
    session.execute_unpaged(&prepared, (1,)).await.unwrap();
    session.execute_iter(prepared, (1,)).await.unwrap();
}

async fn batch_consistency_set_directly(
    session: &Session,
    batch: &Batch,
    c: Consistency,
    sc: Option<SerialConsistency>,
) {
    let mut batch = batch.clone();
    batch.set_consistency(c);
    batch.set_serial_consistency(sc);
    session.batch(&batch, ((1,),)).await.unwrap();
}

// The following functions perform a request with consistencies set on a per-statement execution profile.
async fn query_consistency_set_on_exec_profile(
    session: &Session,
    query: &Statement,
    profile: ExecutionProfileHandle,
) {
    let mut query = query.clone();
    query.set_execution_profile_handle(Some(profile));
    session.query_unpaged(query.clone(), (1,)).await.unwrap();
    session.query_iter(query, (1,)).await.unwrap();
}

async fn execute_consistency_set_on_exec_profile(
    session: &Session,
    prepared: &PreparedStatement,
    profile: ExecutionProfileHandle,
) {
    let mut prepared = prepared.clone();
    prepared.set_execution_profile_handle(Some(profile));
    session.execute_unpaged(&prepared, (1,)).await.unwrap();
    session.execute_iter(prepared, (1,)).await.unwrap();
}

async fn batch_consistency_set_on_exec_profile(
    session: &Session,
    batch: &Batch,
    profile: ExecutionProfileHandle,
) {
    let mut batch = batch.clone();
    batch.set_execution_profile_handle(Some(profile));
    session.batch(&batch, ((1,),)).await.unwrap();
}

// For all consistencies (as defined by `pairs_of_all_consistencies()`) and every method of setting consistencies
// (directly on statement, on per-statement exec profile, on default per-session exec profile)
// performs a request and calls `check_consistencies()` asserting function with `rx`, which is a generic way
// of input for assertions.
// `check_consistencies()` does not simply use &mut Rx, because then we enter the atrocious world of higher-order lifetimes
// whose validity the compiler can not yet prove.
async fn check_for_all_consistencies_and_setting_options<
    Fut1: std::future::Future<Output = Rx>,
    Fut2: std::future::Future<Output = Rx>,
    Rx,
>(
    session_builder: SessionBuilder,
    base_for_every_profile: ExecutionProfileBuilder,
    ks: &str,
    mut rx: Rx,
    after_session_init: impl Fn(Rx) -> Fut1,
    check_consistencies: impl Fn(Consistency, Option<SerialConsistency>, Rx) -> Fut2,
) {
    let session = session_builder
        .clone()
        .default_execution_profile_handle(base_for_every_profile.clone().build().into_handle())
        .build()
        .await
        .unwrap();
    create_schema(&session, ks).await;
    rx = after_session_init(rx).await;

    // We will be using these requests:
    let query = Statement::from(QUERY_STR);
    let prepared = session.prepare(QUERY_STR).await.unwrap();
    let batch = Batch::new_with_statements(
        BatchType::Logged,
        vec![BatchStatement::Query(Statement::from(QUERY_STR))],
    );

    for (consistency, serial_consistency) in pairs_of_all_consistencies() {
        // Some checks are double, because both non-paged and paged executions are done.
        // (queries and prepared statements are double, batches are single)

        // Set directly
        query_consistency_set_directly(&session, &query, consistency, serial_consistency).await;
        rx = check_consistencies(consistency, serial_consistency, rx).await;
        rx = check_consistencies(consistency, serial_consistency, rx).await;

        execute_consistency_set_directly(&session, &prepared, consistency, serial_consistency)
            .await;
        rx = check_consistencies(consistency, serial_consistency, rx).await;
        rx = check_consistencies(consistency, serial_consistency, rx).await;

        batch_consistency_set_directly(&session, &batch, consistency, serial_consistency).await;
        rx = check_consistencies(consistency, serial_consistency, rx).await;

        // Set on an exec profile
        let handle = base_for_every_profile
            .clone()
            .consistency(consistency)
            .serial_consistency(serial_consistency)
            .build()
            .into_handle();
        query_consistency_set_on_exec_profile(&session, &query, handle.clone()).await;
        rx = check_consistencies(consistency, serial_consistency, rx).await;
        rx = check_consistencies(consistency, serial_consistency, rx).await;

        execute_consistency_set_on_exec_profile(&session, &prepared, handle.clone()).await;
        rx = check_consistencies(consistency, serial_consistency, rx).await;
        rx = check_consistencies(consistency, serial_consistency, rx).await;

        batch_consistency_set_on_exec_profile(&session, &batch, handle.clone()).await;
        rx = check_consistencies(consistency, serial_consistency, rx).await;

        // Set on session's default exec profile
        let session_with_consistencies = session_builder
            .clone()
            .default_execution_profile_handle(handle)
            .build()
            .await
            .unwrap();
        session_with_consistencies
            .use_keyspace(ks, true)
            .await
            .unwrap();
        rx = after_session_init(rx).await;

        session_with_consistencies
            .query_unpaged(QUERY_STR, (1,))
            .await
            .unwrap();
        rx = check_consistencies(consistency, serial_consistency, rx).await;
        session_with_consistencies
            .query_iter(QUERY_STR, (1,))
            .await
            .unwrap();
        rx = check_consistencies(consistency, serial_consistency, rx).await;

        session_with_consistencies
            .execute_unpaged(&prepared, (1,))
            .await
            .unwrap();
        rx = check_consistencies(consistency, serial_consistency, rx).await;
        session_with_consistencies
            .execute_iter(prepared.clone(), (1,))
            .await
            .unwrap();
        rx = check_consistencies(consistency, serial_consistency, rx).await;

        session_with_consistencies
            .batch(&batch, ((1,),))
            .await
            .unwrap();
        rx = check_consistencies(consistency, serial_consistency, rx).await;
    }
}

// Checks that the expected consistency and serial_consistency are set
//  in the CQL request frame.
#[tokio::test]
#[ntest::timeout(60000)]
#[cfg(not(scylla_cloud_tests))]
async fn consistency_is_correctly_set_in_cql_requests() {
    setup_tracing();
    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let request_rule = |tx| {
                RequestRule(
                    Condition::and(
                        Condition::not(Condition::ConnectionRegisteredAnyEvent),
                        Condition::or(
                            Condition::RequestOpcode(RequestOpcode::Execute),
                            Condition::or(
                                Condition::RequestOpcode(RequestOpcode::Batch),
                                Condition::and(
                                    Condition::RequestOpcode(RequestOpcode::Query),
                                    Condition::BodyContainsCaseSensitive(Box::new(
                                        *b"INTO consistency_tests",
                                    )),
                                ),
                            ),
                        ),
                    ),
                    RequestReaction::noop().with_feedback_when_performed(tx),
                )
            };

            let (request_tx, request_rx) = mpsc::unbounded_channel();
            for running_node in running_proxy.running_nodes.iter_mut() {
                running_node.change_request_rules(Some(vec![request_rule(request_tx.clone())]));
            }

            let fallthrough_exec_profile_builder =
                ExecutionProfile::builder().retry_policy(Arc::new(FallthroughRetryPolicy));

            let translation_map = Arc::new(translation_map);

            // DB preparation phase
            let ks = unique_keyspace_name();
            let session_builder = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .keepalive_interval(Duration::from_secs(10000))
                .address_translator(translation_map.clone());

            async fn check_consistencies(
                consistency: Consistency,
                serial_consistency: Option<SerialConsistency>,
                mut request_rx: UnboundedReceiver<(RequestFrame, Option<TargetShard>)>,
            ) -> UnboundedReceiver<(RequestFrame, Option<TargetShard>)> {
                let (request_frame, _shard) = request_rx.recv().await.unwrap();
                let deserialized_request = request_frame.deserialize().unwrap();
                assert_eq!(deserialized_request.get_consistency().unwrap(), consistency);
                assert_eq!(
                    deserialized_request.get_serial_consistency().unwrap(),
                    serial_consistency
                );
                request_rx
            }

            check_for_all_consistencies_and_setting_options(
                session_builder,
                fallthrough_exec_profile_builder,
                &ks,
                request_rx,
                |rx| async move { rx },
                check_consistencies,
            )
            .await;

            running_proxy
        },
    )
    .await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OwnedRoutingInfo {
    consistency: Consistency,
    serial_consistency: Option<SerialConsistency>,

    #[allow(unused)]
    table: Option<TableSpec<'static>>,
    #[allow(unused)]
    token: Option<Token>,
    #[allow(unused)]
    is_confirmed_lwt: bool,
}

impl OwnedRoutingInfo {
    fn from(info: RoutingInfo) -> Self {
        let RoutingInfo {
            consistency,
            serial_consistency,
            token,
            table,
            is_confirmed_lwt,
            ..
        } = info;
        Self {
            consistency,
            serial_consistency,
            token,
            table: table.map(TableSpec::to_owned),
            is_confirmed_lwt,
        }
    }
}

// Wraps over DefaultPolicy to provide access to RoutingInfo fed into the load balancer.
#[derive(Debug)]
struct RoutingInfoReportingWrapper {
    wrapped: Arc<dyn LoadBalancingPolicy>,
    routing_info_tx: UnboundedSender<OwnedRoutingInfo>,
}

impl LoadBalancingPolicy for RoutingInfoReportingWrapper {
    fn pick<'a>(
        &'a self,
        query: &'a RoutingInfo,
        cluster: &'a scylla::cluster::ClusterState,
    ) -> Option<(NodeRef<'a>, Option<Shard>)> {
        self.routing_info_tx
            .send(OwnedRoutingInfo::from(query.clone()))
            .unwrap();
        self.wrapped.pick(query, cluster)
    }

    fn fallback<'a>(
        &'a self,
        query: &'a RoutingInfo,
        cluster: &'a scylla::cluster::ClusterState,
    ) -> scylla::policies::load_balancing::FallbackPlan<'a> {
        self.routing_info_tx
            .send(OwnedRoutingInfo::from(query.clone()))
            .unwrap();
        self.wrapped.fallback(query, cluster)
    }

    fn name(&self) -> String {
        "RoutingInfoReportingWrapper".to_owned()
    }
}

// Checks that the expected consistency and serial_consistency are set
// in the RoutingInfo that is exposed to the load balancer.
#[tokio::test]
#[ntest::timeout(60000)]
#[cfg(not(scylla_cloud_tests))]
async fn consistency_is_correctly_set_in_routing_info() {
    setup_tracing();
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let ks = unique_keyspace_name();

    let (routing_info_tx, routing_info_rx) = mpsc::unbounded_channel();

    let reporting_load_balancer = RoutingInfoReportingWrapper {
        wrapped: DefaultPolicy::builder().build(),
        routing_info_tx,
    };

    let exec_profile_builder = ExecutionProfile::builder()
        .retry_policy(Arc::new(FallthroughRetryPolicy))
        .load_balancing_policy(Arc::new(reporting_load_balancer));

    // DB preparation phase
    let session_builder = SessionBuilder::new()
        .known_node(uri.as_str())
        .keepalive_interval(Duration::from_secs(10000))
        .default_execution_profile_handle(exec_profile_builder.clone().build().into_handle());

    async fn on_session_init(
        mut routing_info_rx: UnboundedReceiver<OwnedRoutingInfo>,
    ) -> UnboundedReceiver<OwnedRoutingInfo> {
        // Clear RoutingInfo got from DB preparation (keyspace & table creation)
        while routing_info_rx.try_recv().is_ok() {}
        routing_info_rx
    }

    async fn check_consistencies(
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
        mut routing_info_rx: UnboundedReceiver<OwnedRoutingInfo>,
    ) -> UnboundedReceiver<OwnedRoutingInfo> {
        let info = routing_info_rx.recv().await.unwrap();
        assert_eq!(info.consistency, consistency);
        assert_eq!(info.serial_consistency, serial_consistency);
        routing_info_rx
    }

    check_for_all_consistencies_and_setting_options(
        session_builder,
        exec_profile_builder,
        &ks,
        routing_info_rx,
        on_session_init,
        check_consistencies,
    )
    .await;
}

// Performs a read using Paxos, by setting Consistency to Serial.
// This not really checks that functionality works properly, but stays here
// to ensure that it is even expressible to issue such query.
// Before, Consistency did not contain serial variants, so it used to be impossible.
#[tokio::test]
#[ntest::timeout(60000)]
#[cfg(not(scylla_cloud_tests))]
async fn consistency_allows_for_paxos_selects() {
    setup_tracing();
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    let session = SessionBuilder::new()
        .known_node(uri.as_str())
        .build()
        .await
        .unwrap();

    let mut query = Statement::from("SELECT host_id FROM system.peers WHERE peer = '127.0.0.1'");
    query.set_consistency(Consistency::Serial);
    session.query_unpaged(query, ()).await.unwrap();
}
