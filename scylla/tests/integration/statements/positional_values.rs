//! Tests of the positional values sent with a statement: how a value list that
//! does not match the statement's bind markers is rejected, and that a `NULL`
//! among the values is not.
//!
//! What matters here is that every entry point behaves the same way, so each
//! case is run through all six of them, via [`EntryPoint`].
//!
//! All the cases live in one `#[tokio::test]`, sharing a single session and a
//! single keyspace, so that the shared cluster is not burdened with a keyspace
//! per case.

use assert_matches::assert_matches;
use scylla::client::session::Session;
use scylla::serialize::SerializationError;
use scylla::serialize::row::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};

use crate::entry_point::EntryPoint;

use crate::utils::{
    PerformDDL as _, create_new_session_builder, setup_tracing, unique_keyspace_name,
};

#[tokio::test]
async fn test_positional_values() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();
    session.ddl(format!("CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session.use_keyspace(&ks, false).await.unwrap();
    session
        .ddl("CREATE TABLE t (k text PRIMARY KEY, v int)")
        .await
        .unwrap();
    session.await_schema_agreement().await.unwrap();

    too_many_values_are_rejected(&session).await;
    too_few_values_are_rejected(&session).await;
    nulls_are_accepted_among_values(&session).await;

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

fn assert_wrong_column_count(err: &SerializationError, rust_cols: usize, cql_cols: usize) {
    let kind = &err.downcast_ref::<BuiltinTypeCheckError>().unwrap().kind;
    assert_matches!(
        kind,
        BuiltinTypeCheckErrorKind::WrongColumnCount {
            rust_cols: got_rust_cols,
            cql_cols: got_cql_cols,
        } if *got_rust_cols == rust_cols && *got_cql_cols == cql_cols
    );
}

/// More values than the statement has bind markers is caught while serializing
/// them, whichever entry point sends them.
async fn too_many_values_are_rejected(session: &Session) {
    const STMT: &str = "SELECT v FROM t WHERE k = ?";

    for entry_point in EntryPoint::ALL {
        let err = entry_point
            .send(session, STMT, ("key", 1))
            .await
            .unwrap_err()
            .into_serialization_error(entry_point);
        assert_wrong_column_count(&err, 2, 1);
    }
}

/// Fewer values than the statement has bind markers is caught too - but only a
/// prepared statement knows its markers up front and can catch it while
/// serializing. An unprepared one sends the request and is turned down by the
/// database.
async fn too_few_values_are_rejected(session: &Session) {
    const STMT: &str = "SELECT v FROM t WHERE k = ?";

    for entry_point in EntryPoint::ALL {
        let err = entry_point.send(session, STMT, ()).await.unwrap_err();
        if entry_point.is_prepared() {
            assert_wrong_column_count(&err.into_serialization_error(entry_point), 0, 1);
        } else {
            err.assert_is_invalid_db_error(entry_point, "Invalid amount of bind variables");
        }
    }
}

/// A `None` among the values is a value like any other - it binds the marker to
/// `NULL` rather than leaving it unbound.
async fn nulls_are_accepted_among_values(session: &Session) {
    const INSERT: &str = "INSERT INTO t (k, v) VALUES (?, ?)";

    for entry_point in EntryPoint::ALL {
        // Each entry point writes a row of its own, so that a write that never
        // happened cannot be mistaken for another entry point's.
        let key = entry_point.name();
        entry_point
            .send(session, INSERT, (key, None::<i32>))
            .await
            .unwrap();

        let row = session
            .query_unpaged("SELECT k, v FROM t WHERE k = ?", (key,))
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .single_row::<(String, Option<i32>)>()
            .unwrap();
        assert_eq!(row, (key.to_owned(), None));
    }
}
