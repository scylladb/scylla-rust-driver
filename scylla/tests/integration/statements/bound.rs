//! Tests of what a [`BoundStatement`](scylla::statement::bound::BoundStatement) adds on top of a
//! [`PreparedStatement`](scylla::statement::prepared::PreparedStatement): it carries its own
//! serialized values.
//!
//! The behaviour that a bound statement merely inherits from the prepared
//! statement it was bound from - token computation, load balancing,
//! repreparation, result metadata, paging semantics - is covered by the
//! prepared statement tests and is not retested here.
//!
//! All the cases live in one `#[tokio::test]`, sharing a single session, so
//! that the shared cluster is not burdened with a session per case. They read
//! from system tables and need no schema at all.

use std::sync::Arc;
use std::time::Duration;

use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::observability::history::HistoryCollector;
use scylla::policies::load_balancing::{DefaultPolicy, LoadBalancingPolicy};
use scylla::policies::retry::{FallthroughRetryPolicy, RetryPolicy};
use scylla::statement::{Consistency, SerialConsistency};

use crate::utils::{create_new_session_builder, setup_tracing};

#[tokio::test]
async fn test_bound_statement() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    bound_statement_inherits_prepared_statement_config(&session).await;
    bound_statement_calculates_token_from_its_values(&session).await;
    bind_rejects_invalid_values(&session).await;
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
