//! Tests of named bind markers.
//!
//! A named bind marker has the form `:name`. A statement with named markers
//! gets its values from a source that names them:
//! - a map,
//! - a struct that derives [`SerializeRow`](scylla::SerializeRow).
//!
//! It does not get its values from a positional value list.
//!
//! All the cases are in one `#[tokio::test]`. They share one session and one
//! keyspace. Thus, the shared cluster does not get one keyspace for each case.

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

/// The values are matched to the markers by name, not by their order. A
/// `HashMap` has no fixed order.
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

/// A map that does not name every bind marker must be rejected.
///
/// The test also checks the reason: the error must name the first column that
/// the map does not set. A check for any error is not sufficient, because any
/// failure passes it.
async fn every_marker_must_be_named(session: &Session) {
    let prepared = session
        .prepare("INSERT INTO t (pk, ck, v) VALUES (:pk, :ck, :v)")
        .await
        .unwrap();

    let wrongmaps: Vec<(HashMap<&str, i32>, &str)> = vec![
        // A name that no marker uses does not replace the missing name.
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

/// A named marker with the value `None` is bound to `NULL`.
///
/// This is different from a marker that the map does not name.
/// `every_marker_must_be_named` tests that case.
async fn a_named_value_may_be_null(session: &Session) {
    #[derive(scylla::SerializeRow)]
    struct Row<'a> {
        k: &'a str,
        v: Option<i32>,
    }

    for entry_point in EntryPoint::ALL {
        // Each entry point writes its own row. Thus, the test cannot mistake a
        // missing write for the write of a different entry point.
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

/// A marker name is a CQL identifier:
/// - `:theKey` names the column `thekey`.
/// - `:"theKey"` names the column `theKey`.
///
/// The map must use the name as the database stores it. It must not use the
/// name as the statement writes it.
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
