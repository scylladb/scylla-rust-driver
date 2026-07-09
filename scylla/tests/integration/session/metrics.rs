use std::sync::Arc;

use futures::StreamExt as _;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session_builder::SessionBuilder;
use scylla::observability::metrics::Metrics;
use scylla::policies::retry::FallthroughRetryPolicy;
use scylla::response::PagingState;
use scylla_proxy::{
    Condition, ProxyError, RequestOpcode, RequestReaction, RequestRule, ShardAwareness, WorkerError,
};

use crate::utils::{setup_tracing, test_with_3_node_cluster};

/// Snapshot of the six per-paging-kind request/error counters.
///
/// Call [`RequestMetricsSnapshot::capture`] to take a snapshot, then
/// [`RequestMetricsSnapshot::assert_only_incremented`] to assert that exactly
/// one specified counter incremented by 1 since the previous snapshot. The
/// method updates `self` in place, so it always reflects the last observed
/// state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RequestMetricsSnapshot {
    requests_unpaged: u64,
    requests_manually_paged: u64,
    requests_automatically_paged: u64,
    errors_unpaged: u64,
    errors_manually_paged: u64,
    errors_automatically_paged: u64,
}

/// Which of the six per-paging-kind counters is expected to increment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RequestMetricKind {
    RequestsUnpaged,
    RequestsManuallyPaged,
    RequestsAutomaticallyPaged,
    ErrorsUnpaged,
    ErrorsManuallyPaged,
    ErrorsAutomaticallyPaged,
}

impl RequestMetricsSnapshot {
    fn capture(m: &Metrics) -> Self {
        Self {
            requests_unpaged: m.get_requests_unpaged_num(),
            requests_manually_paged: m.get_requests_manually_paged_num(),
            requests_automatically_paged: m.get_requests_automatically_paged_num(),
            errors_unpaged: m.get_errors_unpaged_num(),
            errors_manually_paged: m.get_errors_manually_paged_num(),
            errors_automatically_paged: m.get_errors_automatically_paged_num(),
        }
    }

    /// Asserts that exactly the counters listed in `kinds` each incremented by
    /// 1 since `self`, and all other counters are unchanged. Then updates
    /// `self` to the current snapshot so the next call uses the fresh baseline.
    fn assert_only_incremented(&mut self, m: &Metrics, kinds: &[RequestMetricKind]) {
        let next = Self::capture(m);
        let inc = |k| kinds.contains(&k) as u64;
        let expected = RequestMetricsSnapshot {
            requests_unpaged: self.requests_unpaged + inc(RequestMetricKind::RequestsUnpaged),
            requests_manually_paged: self.requests_manually_paged
                + inc(RequestMetricKind::RequestsManuallyPaged),
            requests_automatically_paged: self.requests_automatically_paged
                + inc(RequestMetricKind::RequestsAutomaticallyPaged),
            errors_unpaged: self.errors_unpaged + inc(RequestMetricKind::ErrorsUnpaged),
            errors_manually_paged: self.errors_manually_paged
                + inc(RequestMetricKind::ErrorsManuallyPaged),
            errors_automatically_paged: self.errors_automatically_paged
                + inc(RequestMetricKind::ErrorsAutomaticallyPaged),
        };
        assert_eq!(next, expected, "wrong counters incremented for {kinds:?}");
        *self = next;
    }
}

/// Regression test for the bug where `Session::run_request` always passed
/// `RequestPaging::Unpaged` to `RequestExecutionParams`, regardless of whether
/// the caller was a manually-paged (`query_single_page`, `execute_single_page`)
/// or an unpaged (`query_unpaged`, `execute_unpaged`) method.
///
/// As a result, manually-paged requests were incorrectly counted in the
/// unpaged metrics counter (`requests_unpaged`) rather than in the
/// manually-paged counter (`requests_manually_paged`).
///
/// The test exercises all four non-iter public session methods, as well as
/// `query_iter` (without bound values) and `execute_iter`, both in the
/// successful and the failing case, and verifies that after each call exactly
/// the right per-paging-kind counter is incremented and the other five remain
/// untouched.
///
/// No DDL is needed: all statements target `system.local`, which always exists
/// and has one row.
#[tokio::test]
async fn test_paged_and_unpaged_metrics_are_counted_separately() {
    setup_tracing();

    const QUERY: &str = "SELECT host_id FROM system.local WHERE key = 'local'";

    // FallthroughRetryPolicy ensures one server error → exactly one error
    // counter increment (no retries that would inflate the count).
    let profile = ExecutionProfile::builder()
        .retry_policy(Arc::new(FallthroughRetryPolicy))
        .build();

    let query_not_control = Condition::RequestOpcode(RequestOpcode::Query)
        .and(Condition::not(Condition::ConnectionRegisteredAnyEvent))
        .and(Condition::BodyContainsCaseSensitive(Box::new(
            *b"system.local",
        )));
    let execute_not_control = Condition::RequestOpcode(RequestOpcode::Execute)
        .and(Condition::not(Condition::ConnectionRegisteredAnyEvent));

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .default_execution_profile_handle(profile.into_handle())
                .build()
                .await
                .unwrap();

            let metrics = session.get_metrics();
            let prepared = session.prepare(QUERY).await.unwrap();

            let mut snap = RequestMetricsSnapshot::capture(&metrics);

            // Successful requests: each must increment exactly its own
            // requests_* counter.

            session.query_unpaged(QUERY, ()).await.unwrap();
            snap.assert_only_incremented(&metrics, &[RequestMetricKind::RequestsUnpaged]);

            session.execute_unpaged(&prepared, ()).await.unwrap();
            snap.assert_only_incremented(&metrics, &[RequestMetricKind::RequestsUnpaged]);

            session
                .query_single_page(QUERY, (), PagingState::start())
                .await
                .unwrap();
            snap.assert_only_incremented(&metrics, &[RequestMetricKind::RequestsManuallyPaged]);

            session
                .execute_single_page(&prepared, (), PagingState::start())
                .await
                .unwrap();
            snap.assert_only_incremented(&metrics, &[RequestMetricKind::RequestsManuallyPaged]);

            // --- query_iter / execute_iter: must increment
            //     requests_automatically_paged only.
            //
            // The pager fetches the first page eagerly inside the `.await`,
            // so the counter is already updated by the time the call returns.
            // `system.local WHERE key = 'local'` returns exactly one row in
            // one page, so each call increments the counter exactly once.
            session
                .query_iter(QUERY, ())
                .await
                .unwrap()
                .rows_stream::<(uuid::Uuid,)>()
                .unwrap()
                .for_each(|_| async {})
                .await;
            snap.assert_only_incremented(
                &metrics,
                &[RequestMetricKind::RequestsAutomaticallyPaged],
            );

            session
                .execute_iter(prepared.clone(), ())
                .await
                .unwrap()
                .rows_stream::<(uuid::Uuid,)>()
                .unwrap()
                .for_each(|_| async {})
                .await;
            snap.assert_only_incremented(
                &metrics,
                &[RequestMetricKind::RequestsAutomaticallyPaged],
            );

            // Failed requests: each must increment exactly its own
            // errors_* counter (and also its requests_* counter).

            let forge_error_on_query = vec![RequestRule(
                query_not_control.clone(),
                RequestReaction::forge().server_error(),
            )];
            let forge_error_on_execute = vec![RequestRule(
                execute_not_control.clone(),
                RequestReaction::forge().server_error(),
            )];

            // Forge errors on Query opcode: covers query_unpaged,
            // query_single_page, and query_iter (empty values → Query opcode).
            running_proxy
                .running_nodes
                .iter_mut()
                .for_each(|n| n.change_request_rules(Some(forge_error_on_query.clone())));
            let _ = session.query_unpaged(QUERY, ()).await;
            snap.assert_only_incremented(
                &metrics,
                &[
                    RequestMetricKind::RequestsUnpaged,
                    RequestMetricKind::ErrorsUnpaged,
                ],
            );

            let _ = session
                .query_single_page(QUERY, (), PagingState::start())
                .await;
            snap.assert_only_incremented(
                &metrics,
                &[
                    RequestMetricKind::RequestsManuallyPaged,
                    RequestMetricKind::ErrorsManuallyPaged,
                ],
            );

            // The first page is fetched eagerly, so the error is returned
            // directly from the `.await` on the iter call itself.
            let _ = session.query_iter(QUERY, ()).await;
            snap.assert_only_incremented(
                &metrics,
                &[
                    RequestMetricKind::RequestsAutomaticallyPaged,
                    RequestMetricKind::ErrorsAutomaticallyPaged,
                ],
            );

            // Forge errors on Execute opcode: covers execute_unpaged,
            // execute_single_page, and execute_iter.
            running_proxy
                .running_nodes
                .iter_mut()
                .for_each(|n| n.change_request_rules(Some(forge_error_on_execute.clone())));
            let _ = session.execute_unpaged(&prepared, ()).await;
            snap.assert_only_incremented(
                &metrics,
                &[
                    RequestMetricKind::RequestsUnpaged,
                    RequestMetricKind::ErrorsUnpaged,
                ],
            );

            let _ = session
                .execute_single_page(&prepared, (), PagingState::start())
                .await;
            snap.assert_only_incremented(
                &metrics,
                &[
                    RequestMetricKind::RequestsManuallyPaged,
                    RequestMetricKind::ErrorsManuallyPaged,
                ],
            );

            let _ = session.execute_iter(prepared.clone(), ()).await;
            snap.assert_only_incremented(
                &metrics,
                &[
                    RequestMetricKind::RequestsAutomaticallyPaged,
                    RequestMetricKind::ErrorsAutomaticallyPaged,
                ],
            );

            running_proxy
                .running_nodes
                .iter_mut()
                .for_each(|n| n.change_request_rules(None));

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
