//! Tests of the positional values sent with a statement. They check that:
//! - the database rejects a value list that does not match the bind markers,
//! - the database accepts a `NULL` in the values.
//!
//! Every entry point must behave the same way. Thus, each case runs through
//! all six entry points, with [`EntryPoint`].
//!
//! All the cases are in one `#[tokio::test]`. They share one session and one
//! keyspace. Thus, the shared cluster does not get one keyspace for each case.

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

/// Too many values: serialization fails on each entry point.
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

/// Too few values: the result depends on the entry point.
/// - A prepared statement knows its bind markers before the send. Thus,
///   serialization fails.
/// - An unprepared statement sends the request. Then the database rejects it.
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

/// A `None` in the values is a usual value. It sets the marker to `NULL`. It
/// does not leave the marker unbound.
async fn nulls_are_accepted_among_values(session: &Session) {
    const INSERT: &str = "INSERT INTO t (k, v) VALUES (?, ?)";

    for entry_point in EntryPoint::ALL {
        // Each entry point writes its own row. Thus, the test cannot mistake a
        // missing write for the write of a different entry point.
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
