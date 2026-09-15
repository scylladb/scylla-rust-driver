//! Tests of named bind markers: a statement whose markers are written `:name`
//! takes its values from something that names them - a map, or a struct
//! deriving [`SerializeRow`](scylla::SerializeRow) - rather than from a
//! positional value list.
//!
//! All the cases live in one `#[tokio::test]`, sharing a single session and a
//! single keyspace, so that the shared cluster is not burdened with a keyspace
//! per case.

use std::collections::{BTreeMap, HashMap};

use assert_matches::assert_matches;
use scylla::client::session::Session;
use scylla::errors::{BadQuery, ExecutionError};
use scylla::serialize::SerializationError;
use scylla::serialize::row::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};

use crate::entry_point::EntryPoint;
use crate::utils::{
    PerformDDL as _, create_new_session_builder, setup_tracing, unique_keyspace_name,
};

#[tokio::test]
async fn test_named_bind_markers() {
    setup_tracing();

    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .ddl(format!("CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"))
        .await
        .unwrap();
    session.use_keyspace(&ks, false).await.unwrap();

    session
        .ddl("CREATE TABLE t (pk int, ck int, v int, PRIMARY KEY (pk, ck, v))")
        .await
        .unwrap();
    session
        .ddl("CREATE TABLE t2 (k text PRIMARY KEY, v int)")
        .await
        .unwrap();

    session.await_schema_agreement().await.unwrap();

    values_are_taken_from_the_map_by_name(&session).await;
    every_marker_must_be_named(&session).await;
    a_named_value_may_be_null(&session).await;
    marker_names_are_cql_identifiers(&session).await;

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

/// The values are matched to the markers by name, not by the order they happen
/// to be in - which for a `HashMap` is no order at all.
async fn values_are_taken_from_the_map_by_name(session: &Session) {
    let prepared = session
        .prepare("INSERT INTO t (pk, ck, v) VALUES (:pk, :ck, :v)")
        .await
        .unwrap();

    let hashmap: HashMap<&str, i32> = HashMap::from([("pk", 7), ("v", 42), ("ck", 13)]);
    session.execute_unpaged(&prepared, &hashmap).await.unwrap();

    let btreemap: BTreeMap<&str, i32> = BTreeMap::from([("ck", 113), ("v", 142), ("pk", 17)]);
    session.execute_unpaged(&prepared, &btreemap).await.unwrap();

    let rows: Vec<(i32, i32, i32)> = session
        .query_unpaged("SELECT pk, ck, v FROM t", &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(i32, i32, i32)>()
        .unwrap()
        .map(|res| res.unwrap())
        .collect();

    assert_eq!(rows, vec![(7, 13, 42), (17, 113, 142)]);
}

/// A map that does not name every bind marker must be rejected, and rejected
/// for the right reason: the first column it leaves unset. Merely asserting
/// that the request errors out would be satisfied by any failure at all.
async fn every_marker_must_be_named(session: &Session) {
    let prepared = session
        .prepare("INSERT INTO t (pk, ck, v) VALUES (:pk, :ck, :v)")
        .await
        .unwrap();

    let wrongmaps: Vec<(HashMap<&str, i32>, &str)> = vec![
        // A name that no marker uses does not stand in for the missing one.
        (HashMap::from([("pk", 7), ("fefe", 42), ("ck", 13)]), "v"),
        (HashMap::from([("v", 7), ("fefe", 42), ("ck", 13)]), "pk"),
        (
            HashMap::from([("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", 7)]),
            "pk",
        ),
        (HashMap::new(), "pk"),
        (HashMap::from([("ck", 9)]), "pk"),
    ];
    for (wrongmap, missing_column) in wrongmaps {
        let err = session
            .execute_unpaged(&prepared, &wrongmap)
            .await
            .unwrap_err();
        let ExecutionError::BadQuery(BadQuery::SerializationError(err)) = err else {
            panic!("Expected a serialization error, got {err:?}");
        };
        assert_value_missing_for_column(&err, missing_column);
    }
}

/// Naming a marker and giving it `None` binds it to `NULL`; it is not the same
/// as leaving the marker unnamed, which is what `every_marker_must_be_named`
/// covers.
async fn a_named_value_may_be_null(session: &Session) {
    #[derive(scylla::SerializeRow)]
    struct Row<'a> {
        k: &'a str,
        v: Option<i32>,
    }

    for entry_point in EntryPoint::ALL {
        // Each entry point writes a row of its own, so that a write that never
        // happened cannot be mistaken for another entry point's.
        let key = entry_point.name();
        entry_point
            .send(
                session,
                "INSERT INTO t2 (k, v) VALUES (:k, :v)",
                Row { k: key, v: None },
            )
            .await
            .unwrap();

        let row = session
            .query_unpaged("SELECT k, v FROM t2 WHERE k = ?", (key,))
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .single_row::<(String, Option<i32>)>()
            .unwrap();
        assert_eq!(row, (key.to_owned(), None));
    }
}

/// A marker name is a CQL identifier, so `:theKey` names the column `thekey`
/// while `:"theKey"` names `theKey`. The map has to spell the name the way the
/// database folded it, not the way it was written in the statement.
async fn marker_names_are_cql_identifiers(session: &Session) {
    let unquoted = "SELECT v FROM t2 WHERE k = :theKey";
    let quoted = "SELECT v FROM t2 WHERE k = :\"theKey\"";

    let folded: HashMap<&str, &str> = HashMap::from([("thekey", "some key")]);
    let verbatim: HashMap<&str, &str> = HashMap::from([("theKey", "some key")]);

    for entry_point in EntryPoint::ALL {
        for (stmt, right, wrong, missing_column) in [
            (unquoted, &folded, &verbatim, "thekey"),
            (quoted, &verbatim, &folded, "theKey"),
        ] {
            entry_point.send(session, stmt, right).await.unwrap();

            let err = entry_point
                .send(session, stmt, wrong)
                .await
                .unwrap_err()
                .into_serialization_error(entry_point);
            assert_value_missing_for_column(&err, missing_column);
        }
    }
}

fn assert_value_missing_for_column(err: &SerializationError, column: &str) {
    let kind = &err.downcast_ref::<BuiltinTypeCheckError>().unwrap().kind;
    assert_matches!(
        kind,
        BuiltinTypeCheckErrorKind::ValueMissingForColumn { name } if name == column
    );
}
