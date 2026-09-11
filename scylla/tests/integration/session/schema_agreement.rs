use std::sync::Arc;
use std::time::Duration;

use assert_matches::assert_matches;
use scylla::client::PoolSize;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::{
    DbError, ExecutionError, PagerExecutionError, RequestAttemptError, SchemaAgreementError,
};
use scylla::policies::load_balancing::{NodeIdentifier, SingleTargetLoadBalancingPolicy};
use scylla::response::PagingState;
use scylla::response::query_result::QueryResult;
use scylla::statement::Statement;
use scylla_cql::frame::response::event::{SchemaChangeEvent, SchemaChangeType};
use scylla_cql::frame::response::result::{ColumnSpec, ColumnType, NativeType, TableSpec};
use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestFrame, RequestOpcode, RequestReaction, RequestRule,
    ResponseFrame, RunningProxy, ShardAwareness, WorkerError,
};
use tracing::info;
use uuid::Uuid;

use crate::utils::{
    PerformDDL, calculate_proxy_host_ids, setup_tracing, test_with_3_node_cluster,
    unique_keyspace_name,
};

/// Matches the schema version query issued by agreement checks.
fn schema_version_query() -> Condition {
    Condition::not(Condition::ConnectionRegisteredAnyEvent)
        .and(Condition::RequestOpcode(RequestOpcode::Query))
        .and(Condition::BodyContainsCaseSensitive(Box::new(
            *b"system.local",
        )))
}

/// Matches the `CREATE KEYSPACE` statements executed by tests in this module.
fn create_keyspace_query() -> Condition {
    Condition::not(Condition::ConnectionRegisteredAnyEvent)
        .and(Condition::RequestOpcode(RequestOpcode::Query))
        .and(Condition::BodyContainsCaseSensitive(Box::new(
            *b"CREATE KEYSPACE",
        )))
}

/// Forges the `RESULT::SchemaChange` response a server sends after `CREATE KEYSPACE`.
fn forge_keyspace_created(ks: String) -> RequestReaction {
    RequestReaction::forge_response(Arc::new(move |request: RequestFrame| {
        let event = SchemaChangeEvent::KeyspaceChange {
            change_type: SchemaChangeType::Created,
            keyspace_name: ks.clone(),
        };
        ResponseFrame::forged_schema_change(request.params, &event).unwrap()
    }))
}

/// Forges the `RESULT::Rows` response to the schema version query,
/// with a single row holding `version`.
fn forge_schema_version(version: Uuid) -> RequestReaction {
    RequestReaction::forge_response(Arc::new(move |request: RequestFrame| {
        let col_specs = [ColumnSpec::owned(
            "schema_version".to_owned(),
            ColumnType::Native(NativeType::Uuid),
            TableSpec::owned("system".to_owned(), "local".to_owned()),
        )];
        ResponseFrame::forged_rows(request.params, &col_specs, [(version,)]).unwrap()
    }))
}

/// Builds the `CREATE KEYSPACE` statement whose response the proxy forges.
/// The name is unique, so that a DDL leaking to the cluster is caught as a stale test keyspace.
fn forged_create_keyspace(coordinator: NodeIdentifier) -> (String, Statement) {
    let ks = unique_keyspace_name();
    let mut request = Statement::new(format!(
        "CREATE KEYSPACE {ks}
            WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}"
    ));
    request.set_load_balancing_policy(Some(SingleTargetLoadBalancingPolicy::new(
        coordinator,
        None,
    )));
    (ks, request)
}

/// Executes `CREATE KEYSPACE` on `coordinator` while `paused` node is unreachable
/// for schema agreement checks, and returns the result of the DDL.
///
/// Nothing reaches the cluster: proxy forges the DDL response and the schema versions.
/// Real schema propagation is slow and, with other tests running DDLs concurrently,
/// unpredictable, which made this test time out on Cassandra.
async fn run_ddl_with_unreachable_node(
    session: &Session,
    coordinator: NodeIdentifier,
    paused: usize,
    running_proxy: &mut RunningProxy,
) -> Result<QueryResult, ExecutionError> {
    let (ks, request) = forged_create_keyspace(coordinator);
    let agreed_version = Uuid::new_v4();
    running_proxy.running_nodes.iter_mut().for_each(|node| {
        node.change_request_rules(Some(vec![
            RequestRule(create_keyspace_query(), forge_keyspace_created(ks.clone())),
            // First check goes without trouble: all nodes agree on the version.
            RequestRule(
                schema_version_query().and(Condition::TrueForLimitedTimes(1)),
                forge_schema_version(agreed_version),
            ),
            // Second check never completes on non-paused nodes, to time it out and
            // return the error stored from the first attempt.
            RequestRule(schema_version_query(), RequestReaction::drop_frame()),
        ]))
    });

    running_proxy.running_nodes[paused].prepend_request_rules(vec![RequestRule(
        schema_version_query(),
        // Simulates driver discovering that node is unreachable.
        RequestReaction::drop_connection(),
    )]);

    let result = session.query_unpaged(request, &[]).await;

    running_proxy
        .running_nodes
        .iter_mut()
        .for_each(|node| node.change_request_rules(Some(vec![])));

    result
}

// Verifies that auto schema agreement (performed after receiving response of DDL request) works correctly
// when a node is paused.
// How does it work?
// There are 4 sub-tests. For each subtest a fresh session is created, because previous subtest may have
// broken some connections, and we need fully connected session.
// Each subtest performs DDL (which triggers agreement wait), and checks the result.
// When coordinator is the same as unreachable node, we expect agreement to fail.
// When they are different, it should succeed.
// Node is simulated as paused by dropping connection on schema request.
// Normally, for coordinator this would cause `RequiredHostAbsent` error because
// all connections to required host would be dropped. We prevent that by not allowing more
// than one agreement check: during second check, other nodes never respond, triggering
// schema agreement timeout, which will return the error stored during the first check (broken connection).
#[tokio::test]
async fn test_schema_await_with_unreachable_node() {
    setup_tracing();

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let builder = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map.clone()))
                // Important in order to have a predictable amount of connections after session creation.
                // Shard connections are created asynchronously, so it's hard to predict how many will be opened
                // already when we check schema agreement.
                .pool_size(PoolSize::PerHost(1.try_into().unwrap()))
                // Speeds up session creation. Schema metadata is not needed in this test.
                .fetch_schema_metadata(false)
                // Avoid unnecessary warning
                .fetch_full_schema_metadata(false)
                // Keep the forged DDL path isolated from real-cluster metadata.
                .refresh_metadata_on_auto_schema_agreement(false)
                // Let's try more often to prevent timeouts.
                .schema_agreement_interval(Duration::from_millis(5));

            // Sub-cases: (coordinator of the DDL, paused node, should the DDL succeed).
            // Node 0 hosts the control connection, which must not affect the results.
            let cases = [
                // Paused node is the coordinator, so the agreement must fail.
                (1, 1, false),
                // Paused node is not the coordinator. Agreement only needs available nodes to agree.
                (2, 1, true),
                // The same two cases, but the paused node also hosts the control connection.
                (0, 0, false),
                (1, 0, true),
            ];

            for (coordinator, paused, should_succeed) in cases {
                info!(
                    "================= Sub test: coordinator={coordinator}, paused={paused} ================="
                );

                // A failing agreement ends only by timing out, so keep that short.
                // A successful one ends at the first check, so the timeout only bounds a broken test.
                let timeout = if should_succeed {
                    Duration::from_secs(60)
                } else {
                    Duration::from_millis(300)
                };
                let session: Session = builder
                    .clone()
                    .schema_agreement_timeout(timeout)
                    .build()
                    .await
                    .unwrap();
                let host_ids = calculate_proxy_host_ids(&proxy_uris, &translation_map, &session);

                let result = run_ddl_with_unreachable_node(
                    &session,
                    NodeIdentifier::HostId(host_ids[coordinator]),
                    paused,
                    &mut running_proxy,
                )
                .await;

                if should_succeed {
                    assert_matches!(result, Ok(_));
                } else {
                    assert_matches!(
                        result,
                        Err(ExecutionError::SchemaAgreementError(
                            SchemaAgreementError::RequestError(
                                RequestAttemptError::BrokenConnectionError(_)
                            )
                        ))
                    );
                }
            }

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

// Verifies that schema agreement process works correctly even if the first check fails.
#[tokio::test]
async fn test_schema_await_with_transient_failure() {
    setup_tracing();

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            // DB preparation phase
            let builder = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map.clone()))
                // Important in order to have a predictable amount of connections after session creation.
                // Shard connections are created asynchronously, so it's hard to predict how many will be opened
                // already when we check schema agreement.
                .pool_size(PoolSize::PerHost(1.try_into().unwrap()))
                // Let's try more often to prevent timeouts.
                .schema_agreement_interval(Duration::from_millis(30));

            let node_rules = Some(vec![RequestRule(
                Condition::not(Condition::ConnectionRegisteredAnyEvent)
                    .and(Condition::RequestOpcode(RequestOpcode::Query))
                    .and(Condition::BodyContainsCaseSensitive(Box::new(
                        *b"system.local",
                    )))
                    .and(Condition::TrueForLimitedTimes(1)),
                RequestReaction::forge_with_error(DbError::Overloaded),
            )]);

            // First, a sanity check for proxy rules.
            // If for each node first request fails (and subsequent requests succeed),
            // then first schema agreement check should return error, and second should succeed.
            // Note that this is only true because we configured the session with 1-connection-per-node.
            info!("Starting phase 1 - sanity check");
            {
                let session: Session = builder.clone().build().await.unwrap();
                running_proxy
                    .running_nodes
                    .iter_mut()
                    .for_each(|node| node.change_request_rules(node_rules.clone()));

                // The important check: first call should error out, second one should succeed.
                session.check_schema_agreement().await.unwrap_err();
                session.check_schema_agreement().await.unwrap();

                running_proxy
                    .running_nodes
                    .iter_mut()
                    .for_each(|node| node.change_request_rules(Some(vec![])));
            }

            // Now let's check that awaiting schema agreement doesn't bail on error.
            // I'll use the same proxy rules as before, so first check will error out.
            info!("Starting phase 2 - main test");
            {
                let session: Session = builder.clone().build().await.unwrap();
                running_proxy
                    .running_nodes
                    .iter_mut()
                    .for_each(|node| node.change_request_rules(node_rules.clone()));

                session.await_schema_agreement().await.unwrap();
            }

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

// Test that produces SchemaAgreementError::RequiredHostAbsent to prove that
// such condition is possible, and handled correctly.
// As in `run_ddl_with_unreachable_node`, the DDL and the schema versions are forged by the proxy.
#[tokio::test]
async fn test_schema_await_required_host_absent() {
    setup_tracing();

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            // DB preparation phase
            let session: Session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map.clone()))
                // Needed to have pools filled immediately after session creation.
                .pool_size(PoolSize::PerHost(1.try_into().unwrap()))
                // Speeds up session creation. Schema metadata is not needed in this test.
                .fetch_schema_metadata(false)
                // Avoid unnecessary warning
                .fetch_full_schema_metadata(false)
                // Keep the forged DDL path isolated from real-cluster metadata.
                .refresh_metadata_on_auto_schema_agreement(false)
                // Schema agreement will only return error after timeout,
                // so without this line the test would take over 60s.
                .schema_agreement_timeout(Duration::from_secs(1))
                .schema_agreement_interval(Duration::from_millis(50))
                .build()
                .await
                .unwrap();

            let host_ids = calculate_proxy_host_ids(&proxy_uris, &translation_map, &session);

            let (ks, request) = forged_create_keyspace(NodeIdentifier::HostId(host_ids[1]));
            let agreed_version = Uuid::new_v4();
            running_proxy.running_nodes.iter_mut().for_each(|node| {
                node.change_request_rules(Some(vec![
                    RequestRule(create_keyspace_query(), forge_keyspace_created(ks.clone())),
                    RequestRule(schema_version_query(), forge_schema_version(agreed_version)),
                ]))
            });
            running_proxy.running_nodes[1].prepend_request_rules(vec![
                // This prevents opening new connections to the node
                RequestRule(
                    Condition::RequestOpcode(RequestOpcode::Startup),
                    RequestReaction::drop_connection(),
                ),
                // This prevents schema agreement check on this node, and closes connection.
                // After some attempts, no connections will be left.
                RequestRule(schema_version_query(), RequestReaction::drop_connection()),
            ]);

            let result = session.query_unpaged(request, &[]).await;
            let Err(ExecutionError::SchemaAgreementError(
                SchemaAgreementError::RequiredHostAbsent(host),
            )) = result
            else {
                panic!("Unexpected error type: {:?}", result);
            };

            assert_eq!(host, host_ids[1]);

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

async fn run_ddl_with_failing_agreement_check<Err: std::fmt::Debug>(
    mut run_ddl: impl AsyncFnMut(Statement) -> Result<(), Err>,
    running_proxy: &mut RunningProxy,
    host_ids: &[Uuid],
) -> Result<(), Err> {
    // Schema agreement will return error from node 0, causing it
    // to fail instantly.
    let fail_schema_check_rule = RequestRule(
        Condition::not(Condition::ConnectionRegisteredAnyEvent)
            .and(Condition::RequestOpcode(RequestOpcode::Query))
            .and(Condition::BodyContainsCaseSensitive(Box::new(
                *b"system.local",
            ))),
        RequestReaction::forge_with_error(DbError::SyntaxError),
    );

    // Let's send DDL to node 1 to avoid it failing on node 0 due to proxy rule.
    let policy = SingleTargetLoadBalancingPolicy::new(NodeIdentifier::HostId(host_ids[1]), None);

    let ks = unique_keyspace_name();

    running_proxy.running_nodes[0].change_request_rules(Some(vec![fail_schema_check_rule]));

    let mut statement = Statement::new(format!(
        "CREATE KEYSPACE {ks}
    WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}"
    ));
    statement.set_load_balancing_policy(Some(Arc::clone(&policy)));
    let result = run_ddl(statement).await;

    running_proxy.running_nodes[0].change_request_rules(Some(vec![]));
    let mut drop_statement = Statement::new(format!("DROP KEYSPACE {ks}"));
    // Execute it on the same coordinator as CREATE KEYSPACE, becuse it may have
    // not propagated the schema yet.
    drop_statement.set_load_balancing_policy(Some(policy));
    run_ddl(drop_statement).await.unwrap();

    result
}

// Verifies that schema agreement is triggered on all driver APIs.
#[tokio::test]
async fn test_schema_await_with_various_apis() {
    setup_tracing();

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            // DB preparation phase
            let session: Session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map.clone()))
                .build()
                .await
                .unwrap();

            let host_ids = calculate_proxy_host_ids(&proxy_uris, &translation_map, &session);

            fn check_error(err: Result<(), ExecutionError>) {
                assert_matches!(
                    err,
                    Err(ExecutionError::SchemaAgreementError(
                        SchemaAgreementError::RequestError(RequestAttemptError::DbError(
                            DbError::SyntaxError,
                            _
                        ))
                    ))
                )
            }

            fn check_paging_error(err: Result<(), PagerExecutionError>) {
                assert_matches!(
                    err,
                    Err(PagerExecutionError::SchemaAgreementError(
                        SchemaAgreementError::RequestError(RequestAttemptError::DbError(
                            DbError::SyntaxError,
                            _
                        ))
                    ))
                )
            }

            {
                tracing::info!("================= Sub test: query_unpaged =================");

                let result = run_ddl_with_failing_agreement_check(
                    async |ddl| session.query_unpaged(ddl, &()).await.map(|_| ()),
                    &mut running_proxy,
                    &host_ids,
                )
                .await;
                check_error(result);
            }

            {
                tracing::info!("================= Sub test: query_single_page =================");

                let result = run_ddl_with_failing_agreement_check(
                    async |ddl| {
                        session
                            .query_single_page(ddl, &(), PagingState::start())
                            .await
                            .map(|_| ())
                    },
                    &mut running_proxy,
                    &host_ids,
                )
                .await;
                check_error(result);
            }

            {
                tracing::info!("================= Sub test: query_iter =================");

                let result = run_ddl_with_failing_agreement_check(
                    async |ddl| session.query_iter(ddl, &()).await.map(|_| ()),
                    &mut running_proxy,
                    &host_ids,
                )
                .await;
                check_paging_error(result);
            }

            {
                tracing::info!("================= Sub test: execute_unpaged =================");

                let result = run_ddl_with_failing_agreement_check(
                    async |ddl| {
                        let stmt = session.prepare(ddl).await?;
                        session.execute_unpaged(&stmt, &()).await.map(|_| ())
                    },
                    &mut running_proxy,
                    &host_ids,
                )
                .await;
                check_error(result);
            }

            {
                tracing::info!("================= Sub test: execute_single_page =================");

                let result = run_ddl_with_failing_agreement_check(
                    async |ddl| {
                        let stmt = session.prepare(ddl).await?;
                        session
                            .execute_single_page(&stmt, &(), PagingState::start())
                            .await
                            .map(|_| ())
                    },
                    &mut running_proxy,
                    &host_ids,
                )
                .await;
                check_error(result);
            }

            {
                tracing::info!("================= Sub test: execute_iter =================");

                let result = run_ddl_with_failing_agreement_check(
                    async |ddl| {
                        let stmt = session.prepare(ddl).await?;
                        session.execute_iter(stmt, &()).await.map(|_| ())
                    },
                    &mut running_proxy,
                    &host_ids,
                )
                .await;
                check_paging_error(result);
            }

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

// Verifies that metadata is refreshed as part of the auto schema agreement process.
#[tokio::test]
async fn test_schema_await_refreshes_metadata() {
    setup_tracing();
    let ks = unique_keyspace_name();
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "172.42.0.2:9042".to_string());
    let session: Session = SessionBuilder::new().known_node(uri).build().await.unwrap();
    session
        .ddl(format!(
            "CREATE KEYSPACE {ks}
    WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}"
        ))
        .await
        .unwrap();

    async fn run_ddl_and_inspect_schema(
        session: &Session,
        ks: &str,
        table: &str,
        mut run_ddl: impl AsyncFnMut(&Session, Statement),
    ) {
        let statement = Statement::new(format!("CREATE TABLE {ks}.{table} (id int PRIMARY KEY)"));
        run_ddl(session, statement).await;
        let cluster_state = session.get_cluster_state();
        let keyspace = cluster_state.get_keyspace(ks).unwrap();
        // This verifies that schema metadata was refreshed, because if it wasn't,
        // the table wouldn't be found and the test would panic.
        let _table = keyspace.tables.get(table).unwrap();
    }

    {
        tracing::info!("================= Sub test: query_unpaged =================");

        run_ddl_and_inspect_schema(&session, &ks, "query_unpaged", async |session, ddl| {
            session.query_unpaged(ddl, &()).await.unwrap();
        })
        .await;
    }

    {
        tracing::info!("================= Sub test: query_single_page =================");

        run_ddl_and_inspect_schema(&session, &ks, "query_single_page", async |session, ddl| {
            session
                .query_single_page(ddl, &(), PagingState::start())
                .await
                .unwrap();
        })
        .await;
    }

    {
        tracing::info!("================= Sub test: query_iter =================");

        run_ddl_and_inspect_schema(&session, &ks, "query_iter", async |session, ddl| {
            session.query_iter(ddl, &()).await.unwrap();
        })
        .await;
    }

    {
        tracing::info!("================= Sub test: execute_unpaged =================");

        run_ddl_and_inspect_schema(&session, &ks, "execute_unpaged", async |session, ddl| {
            let stmt = session.prepare(ddl).await.unwrap();
            session.execute_unpaged(&stmt, &()).await.unwrap();
        })
        .await;
    }

    {
        tracing::info!("================= Sub test: execute_single_page =================");

        run_ddl_and_inspect_schema(
            &session,
            &ks,
            "execute_single_page",
            async |session, ddl| {
                let stmt = session.prepare(ddl).await.unwrap();
                session
                    .execute_single_page(&stmt, &(), PagingState::start())
                    .await
                    .unwrap();
            },
        )
        .await;
    }

    {
        tracing::info!("================= Sub test: execute_iter =================");

        run_ddl_and_inspect_schema(&session, &ks, "execute_iter", async |session, ddl| {
            let stmt = session.prepare(ddl).await.unwrap();
            session.execute_iter(stmt, &()).await.unwrap();
        })
        .await;
    }

    let drop_statement = Statement::new(format!("DROP KEYSPACE {ks}"));
    session.ddl(drop_statement).await.unwrap();
}
