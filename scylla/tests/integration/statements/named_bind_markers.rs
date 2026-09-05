use assert_matches::assert_matches;
use scylla::errors::{BadQuery, ExecutionError};
use scylla::serialize::SerializationError;
use scylla::serialize::row::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};

use crate::utils::{
    PerformDDL as _, create_new_session_builder, setup_tracing, unique_keyspace_name,
};
use std::collections::{BTreeMap, HashMap};
use std::vec;

#[tokio::test]
async fn test_named_bind_markers() {
    setup_tracing();

    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session
        .ddl(format!("CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"))
        .await
        .unwrap();

    session
        .query_unpaged(format!("USE {ks}"), &[])
        .await
        .unwrap();

    session
        .ddl("CREATE TABLE t (pk int, ck int, v int, PRIMARY KEY (pk, ck, v))")
        .await
        .unwrap();

    session.await_schema_agreement().await.unwrap();

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

    // A map that does not name every bind marker must be rejected, and rejected
    // for the right reason: the first column it leaves unset. Merely asserting
    // that the request errors out would be satisfied by any failure at all.
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

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

fn assert_value_missing_for_column(err: &SerializationError, column: &str) {
    let kind = &err.downcast_ref::<BuiltinTypeCheckError>().unwrap().kind;
    assert_matches!(
        kind,
        BuiltinTypeCheckErrorKind::ValueMissingForColumn { name } if name == column
    );
}
