use assert_matches::assert_matches;

use scylla::frame::response::result::CqlValue;
use scylla::frame::value::CqlDuration;
use scylla::Session;

use crate::utils::{create_new_session_builder, setup_tracing, unique_keyspace_name};

#[tokio::test]
async fn test_cqlvalue_udt() {
    setup_tracing();
    let session: Session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();
    session
        .query_unpaged(
            format!(
                "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = \
            {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}",
                ks
            ),
            &[],
        )
        .await
        .unwrap();
    session.use_keyspace(&ks, false).await.unwrap();

    session
        .query_unpaged(
            "CREATE TYPE IF NOT EXISTS cqlvalue_udt_type (int_val int, text_val text)",
            &[],
        )
        .await
        .unwrap();
    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS cqlvalue_udt_test (k int, my cqlvalue_udt_type, primary key (k))",
            &[],
        )
        .await
        .unwrap();

    let udt_cql_value = CqlValue::UserDefinedType {
        keyspace: ks,
        type_name: "cqlvalue_udt_type".to_string(),
        fields: vec![
            ("int_val".to_string(), Some(CqlValue::Int(42))),
            ("text_val".to_string(), Some(CqlValue::Text("hi".into()))),
        ],
    };

    session
        .query_unpaged(
            "INSERT INTO cqlvalue_udt_test (k, my) VALUES (5, ?)",
            (&udt_cql_value,),
        )
        .await
        .unwrap();

    let rows_result = session
        .query_unpaged("SELECT my FROM cqlvalue_udt_test", &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();

    let (received_udt_cql_value,) = rows_result.single_row::<(CqlValue,)>().unwrap();

    assert_eq!(received_udt_cql_value, udt_cql_value);
}

#[tokio::test]
async fn test_cqlvalue_duration() {
    setup_tracing();
    let session: Session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();
    session
        .query_unpaged(
            format!(
                "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = \
                {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}",
                ks
            ),
            &[],
        )
        .await
        .unwrap();
    session.use_keyspace(&ks, false).await.unwrap();

    let duration_cql_value = CqlValue::Duration(CqlDuration {
        months: 6,
        days: 9,
        nanoseconds: 21372137,
    });

    let fixture_queries = vec![
        ("CREATE TABLE IF NOT EXISTS cqlvalue_duration_test (pk int, ck int, v duration, primary key (pk, ck))", vec![],),
        ("INSERT INTO cqlvalue_duration_test (pk, ck, v) VALUES (0, 0, ?)", vec![&duration_cql_value,],),
        ("INSERT INTO cqlvalue_duration_test (pk, ck, v) VALUES (0, 1, 89h4m48s)", vec![],),
        ("INSERT INTO cqlvalue_duration_test (pk, ck, v) VALUES (0, 2, PT89H8M53S)", vec![],),
        ("INSERT INTO cqlvalue_duration_test (pk, ck, v) VALUES (0, 3, P0000-00-00T89:09:09)", vec![],),
    ];

    for query in fixture_queries {
        session.query_unpaged(query.0, query.1).await.unwrap();
    }

    let rows_result = session
        .query_unpaged(
            "SELECT v FROM cqlvalue_duration_test WHERE pk = ?",
            (CqlValue::Int(0),),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();

    let mut rows_iter = rows_result.rows::<(CqlValue,)>().unwrap();

    let (first_value,) = rows_iter.next().unwrap().unwrap();
    assert_eq!(first_value, duration_cql_value);

    let (second_value,) = rows_iter.next().unwrap().unwrap();
    assert_eq!(
        second_value,
        CqlValue::Duration(CqlDuration {
            months: 0,
            days: 0,
            nanoseconds: 320_688_000_000_000,
        })
    );

    let (third_value,) = rows_iter.next().unwrap().unwrap();
    assert_eq!(
        third_value,
        CqlValue::Duration(CqlDuration {
            months: 0,
            days: 0,
            nanoseconds: 320_933_000_000_000,
        })
    );

    let (fourth_value,) = rows_iter.next().unwrap().unwrap();
    assert_eq!(
        fourth_value,
        CqlValue::Duration(CqlDuration {
            months: 0,
            days: 0,
            nanoseconds: 320_949_000_000_000,
        })
    );

    assert_matches!(rows_iter.next(), None);
}
