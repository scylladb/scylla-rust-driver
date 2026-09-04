//! Tests of what a [`BoundStatement`] adds on top of a [`PreparedStatement`](scylla::statement::prepared::PreparedStatement):
//! it carries its own serialized values.
//!
//! The behaviour that a bound statement merely inherits from the prepared
//! statement it was bound from - token computation, load balancing,
//! repreparation, result metadata, paging semantics - is covered by the
//! prepared statement tests and is not retested here.
//!
//! All the cases live in one `#[tokio::test]`, sharing a single session and -
//! for the ones that really need to write rows - a single keyspace, so that
//! the shared cluster is not burdened with a keyspace per case. The cases that
//! only read make do with system tables and need no schema at all.

use std::sync::Arc;
use std::time::Duration;

use futures::TryStreamExt as _;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::observability::history::{HistoryCollector, HistoryListener};
use scylla::policies::load_balancing::{DefaultPolicy, LoadBalancingPolicy};
use scylla::policies::retry::{FallthroughRetryPolicy, RetryPolicy};
use scylla::response::{PagingState, PagingStateResponse};
use scylla::statement::bound::BoundStatement;
use scylla::statement::{Consistency, SerialConsistency, Statement};

use crate::utils::{
    PerformDDL as _, create_new_session_builder, setup_tracing, unique_keyspace_name,
};

#[tokio::test]
async fn test_bound_statement() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    // Cases that only read - system tables suffice.
    bound_statement_inherits_prepared_statement_config(&session).await;
    bound_statement_can_be_configured_after_binding(&session).await;
    bound_statement_is_executed_with_inherited_config(&session).await;
    bound_statement_calculates_token_from_its_values(&session).await;
    bind_rejects_invalid_values(&session).await;

    // Cases that write - they need a table of their own.
    let ks = unique_keyspace_name();
    session.ddl(format!("CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE {ks}.t (a int, b int, c text, primary key (a, b))"
        ))
        .await
        .unwrap();
    bound_statement_carries_its_values(&session, &ks).await;
    bound_statements_are_type_erased(&session, &ks).await;

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

/// The values bound to a statement are the ones sent upon execution -
/// with each of the three execution methods.
async fn bound_statement_carries_its_values(session: &Session, ks: &str) {
    let insert = session
        .prepare(format!("INSERT INTO {ks}.t (a, b, c) VALUES (?, ?, ?)"))
        .await
        .unwrap();

    session
        .execute_bound_unpaged(&insert.clone().bind(&(1, 2, "unpaged")).unwrap())
        .await
        .unwrap();
    session
        .execute_bound_single_page(
            &insert.clone().bind(&(3, 4, "single_page")).unwrap(),
            PagingState::start(),
        )
        .await
        .unwrap();
    session
        .execute_bound_iter(insert.bind(&(5, 6, "iter")).unwrap())
        .await
        .unwrap();

    let select = session
        .prepare(format!("SELECT a, b, c FROM {ks}.t WHERE a = ? AND b = ?"))
        .await
        .unwrap();

    // Reading back through a bound SELECT, too: the bound values also reach
    // the server as the statement's WHERE clause arguments.
    for (a, b, c) in [(1, 2, "unpaged"), (3, 4, "single_page"), (5, 6, "iter")] {
        let bound = select.clone().bind(&(a, b)).unwrap();
        let row = session
            .execute_bound_unpaged(&bound)
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .single_row::<(i32, i32, String)>()
            .unwrap();
        assert_eq!(row, (a, b, c.to_owned()));
    }
}

/// The values are serialized - and thus type erased - upon binding.
/// Statements bound to values of unrelated types share one type, so they can
/// be stored together and executed by code that knows nothing about the values.
async fn bound_statements_are_type_erased(session: &Session, ks: &str) {
    let insert_ab = session
        .prepare(format!("INSERT INTO {ks}.t (a, b) VALUES (?, ?)"))
        .await
        .unwrap();
    let insert_abc = session
        .prepare(format!("INSERT INTO {ks}.t (a, b, c) VALUES (?, ?, ?)"))
        .await
        .unwrap();
    let delete = session
        .prepare(format!("DELETE FROM {ks}.t WHERE a = ? AND b = ?"))
        .await
        .unwrap();

    // Different statements, different value types, one collection.
    // Partition 100 is used by this case alone, so that its final contents
    // can be asserted upon regardless of what the other cases wrote.
    let statements: Vec<BoundStatement> = vec![
        insert_ab.bind(&(100, 1)).unwrap(),
        insert_abc.bind(&(100, 2, "text")).unwrap(),
        delete.bind(&(100, 1)).unwrap(),
    ];

    // A consumer that knows nothing about any of the values.
    for bound in &statements {
        session.execute_bound_unpaged(bound).await.unwrap();
    }

    let rows: Vec<(i32, i32, Option<String>)> = session
        .query_unpaged(format!("SELECT a, b, c FROM {ks}.t WHERE a = 100"), &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(i32, i32, Option<String>)>()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(rows, vec![(100, 2, Some("text".to_owned()))]);
}

/// Binding must not lose any part of the prepared statement's configuration:
/// every option settable on a [`PreparedStatement`](scylla::statement::prepared::PreparedStatement)
/// is still there, and unchanged, after binding.
///
/// This test will get even more important once BoundStatement becomes configurable itself (after creation).
async fn bound_statement_inherits_prepared_statement_config(session: &Session) {
    let mut prepared = session
        .prepare("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?")
        .await
        .unwrap();

    // Set every option to something distinguishable from its default, so that
    // an option silently dropped by `bind()` cannot pass unnoticed.
    let retry_policy: Arc<dyn RetryPolicy> = Arc::new(FallthroughRetryPolicy::new());
    let load_balancing_policy: Arc<dyn LoadBalancingPolicy> = DefaultPolicy::builder().build();
    let execution_profile_handle = ExecutionProfile::builder().build().into_handle();

    prepared.set_page_size(3);
    prepared.set_consistency(Consistency::One);
    prepared.set_serial_consistency(Some(SerialConsistency::LocalSerial));
    prepared.set_is_idempotent(true);
    prepared.set_tracing(true);
    prepared.set_use_cached_result_metadata(true);
    prepared.set_timestamp(Some(42));
    prepared.set_request_timeout(Some(Duration::from_secs(66)));
    prepared.set_retry_policy(Some(Arc::clone(&retry_policy)));
    prepared.set_load_balancing_policy(Some(Arc::clone(&load_balancing_policy)));
    prepared.set_execution_profile_handle(Some(execution_profile_handle));
    prepared.set_history_listener(Arc::new(HistoryCollector::new()));

    let bound = prepared.clone().bind(&("system_schema",)).unwrap();
    let inherited = bound.prepared();

    assert_eq!(inherited.get_page_size(), prepared.get_page_size());
    assert_eq!(inherited.get_consistency(), prepared.get_consistency());
    assert_eq!(
        inherited.get_serial_consistency(),
        prepared.get_serial_consistency()
    );
    assert_eq!(inherited.get_is_idempotent(), prepared.get_is_idempotent());
    assert_eq!(inherited.get_tracing(), prepared.get_tracing());
    assert_eq!(
        inherited.get_use_cached_result_metadata(),
        prepared.get_use_cached_result_metadata()
    );
    assert_eq!(inherited.get_timestamp(), prepared.get_timestamp());
    assert_eq!(
        inherited.get_request_timeout(),
        prepared.get_request_timeout()
    );
    assert!(Arc::ptr_eq(
        inherited.get_retry_policy().unwrap(),
        &retry_policy
    ));
    assert!(Arc::ptr_eq(
        inherited.get_load_balancing_policy().unwrap(),
        &load_balancing_policy
    ));
    // `ExecutionProfileHandle` is opaque and not comparable, so only its
    // presence can be asserted here; it being *used* is checked below.
    assert!(inherited.get_execution_profile_handle().is_some());

    // The prepared statement itself is, of course, untouched by all of this.
    assert_eq!(inherited.get_statement(), prepared.get_statement());
    assert_eq!(inherited.get_id(), prepared.get_id());
}

/// Every public configuration setter exposed by `PreparedStatement` remains
/// available after values have been bound, without exposing mutable access to
/// the prepared statement itself.
async fn bound_statement_can_be_configured_after_binding(session: &Session) {
    let mut prepared = session
        .prepare("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?")
        .await
        .unwrap();
    prepared.set_page_size(3);
    prepared.set_consistency(Consistency::One);
    prepared.set_serial_consistency(Some(SerialConsistency::LocalSerial));
    let mut bound = prepared.bind(&("system_schema",)).unwrap();

    let retry_policy: Arc<dyn RetryPolicy> = Arc::new(FallthroughRetryPolicy::new());
    let load_balancing_policy: Arc<dyn LoadBalancingPolicy> = DefaultPolicy::builder().build();
    let execution_profile_handle = ExecutionProfile::builder().build().into_handle();
    let history_listener: Arc<dyn HistoryListener> = Arc::new(HistoryCollector::new());

    bound.set_page_size(7);
    bound.set_consistency(Consistency::Two);

    // Options changed on the bound statement override the prepared statement,
    // while options not changed after binding retain their inherited value.
    assert_eq!(bound.prepared().get_page_size(), 7);
    assert_eq!(bound.prepared().get_consistency(), Some(Consistency::Two));
    assert_eq!(
        bound.prepared().get_serial_consistency(),
        Some(SerialConsistency::LocalSerial)
    );

    bound.set_serial_consistency(Some(SerialConsistency::Serial));
    bound.set_is_idempotent(true);
    bound.set_tracing(true);
    bound.set_use_cached_result_metadata(true);
    bound.set_timestamp(Some(123));
    bound.set_request_timeout(Some(Duration::from_secs(9)));
    bound.set_retry_policy(Some(Arc::clone(&retry_policy)));
    bound.set_load_balancing_policy(Some(Arc::clone(&load_balancing_policy)));
    bound.set_history_listener(history_listener.clone());
    bound.set_execution_profile_handle(Some(execution_profile_handle));

    let configured = bound.prepared();
    assert_eq!(configured.get_page_size(), 7);
    assert_eq!(configured.get_consistency(), Some(Consistency::Two));
    assert_eq!(
        configured.get_serial_consistency(),
        Some(SerialConsistency::Serial)
    );
    assert!(configured.get_is_idempotent());
    assert!(configured.get_tracing());
    assert!(configured.get_use_cached_result_metadata());
    assert_eq!(configured.get_timestamp(), Some(123));
    assert_eq!(
        configured.get_request_timeout(),
        Some(Duration::from_secs(9))
    );
    assert!(Arc::ptr_eq(
        configured.get_retry_policy().unwrap(),
        &retry_policy
    ));
    assert!(Arc::ptr_eq(
        configured.get_load_balancing_policy().unwrap(),
        &load_balancing_policy
    ));
    assert!(configured.get_execution_profile_handle().is_some());

    assert!(Arc::ptr_eq(
        &bound.remove_history_listener().unwrap(),
        &history_listener
    ));
    bound.unset_consistency();
    bound.unset_serial_consistency();
    bound.set_retry_policy(None);
    bound.set_load_balancing_policy(None);
    bound.set_execution_profile_handle(None);

    let reset = bound.prepared();
    assert_eq!(reset.get_consistency(), None);
    assert_eq!(reset.get_serial_consistency(), None);
    assert!(reset.get_retry_policy().is_none());
    assert!(reset.get_load_balancing_policy().is_none());
    assert!(reset.get_execution_profile_handle().is_none());
}

/// The inherited configuration is not merely stored in the bound statement -
/// it is applied to its executions. Checked here for the options whose effect
/// is observable: page size, tracing, and the history listener (which has no
/// getter to compare, either).
async fn bound_statement_is_executed_with_inherited_config(session: &Session) {
    // `system_schema.tables` holds many rows in the `system_schema` partition,
    // so a page size of 1 is guaranteed not to exhaust the result at once.
    let mut select = session
        .prepare(
            Statement::new("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?")
                .with_page_size(1),
        )
        .await
        .unwrap();
    select.set_tracing(true);
    let history_collector = Arc::new(HistoryCollector::new());
    select.set_history_listener(history_collector.clone());

    let bound = select.bind(&("system_schema",)).unwrap();

    // The page size set before binding is honoured, so a single page holds
    // exactly one row...
    let (result, paging_state_response) = session
        .execute_bound_single_page(&bound, PagingState::start())
        .await
        .unwrap();
    assert!(matches!(
        paging_state_response,
        PagingStateResponse::HasMorePages { .. }
    ));
    // ...tracing was requested, so the response carries a tracing id...
    assert!(result.tracing_id().is_some());
    assert_eq!(result.into_rows_result().unwrap().rows_num(), 1);

    // ...and the history listener saw the request.
    assert!(
        !history_collector
            .clone_structured_history()
            .requests
            .is_empty()
    );

    // Iterating fetches all the pages, as always.
    let all_rows: Vec<(String,)> = session
        .execute_bound_iter(bound)
        .await
        .unwrap()
        .rows_stream::<(String,)>()
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(all_rows.len() > 1);
}

/// One serialization serves both purposes: the token is computed from the
/// values the statement already holds, and the very same bytes are then sent
/// upon execution. The token must therefore be the one the prepared statement
/// would compute for those values.
async fn bound_statement_calculates_token_from_its_values(session: &Session) {
    let select = session
        .prepare("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?")
        .await
        .unwrap();

    let values = ("system_schema",);
    let bound = select.clone().bind(&values).unwrap();

    assert_eq!(
        bound.calculate_token().unwrap(),
        select.calculate_token(&values).unwrap()
    );

    // The token is a property of the bound values only - a different binding
    // of the same statement yields a different token.
    let other_bound = select.bind(&("system",)).unwrap();
    assert_ne!(
        bound.calculate_token().unwrap(),
        other_bound.calculate_token().unwrap()
    );

    // A statement whose partition key is not bound has no token.
    let no_pk = session
        .prepare("SELECT table_name FROM system_schema.tables")
        .await
        .unwrap()
        .bind(&())
        .unwrap();
    assert_eq!(no_pk.calculate_token().unwrap(), None);
}

/// Serialization happens upon binding, so that is where its errors surface -
/// before any execution is attempted.
async fn bind_rejects_invalid_values(session: &Session) {
    let select = session
        .prepare(
            "SELECT table_name FROM system_schema.tables \
             WHERE keyspace_name = ? AND table_name = ?",
        )
        .await
        .unwrap();

    // Wrong number of values.
    select.clone().bind(&("system_schema",)).unwrap_err();
    // Wrong type of a value: both columns are of type text.
    select.bind(&(1, 2)).unwrap_err();
}
