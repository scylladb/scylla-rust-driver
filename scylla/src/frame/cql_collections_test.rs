use crate::{frame::response::result::CqlValue, IntoTypedRows, SessionBuilder};
use std::{collections::HashMap, env};

#[tokio::test]
async fn test_cql_collections() {
    // Create connection
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await.unwrap();

    // HashMap test

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.teams (id int, team map<int, text>, primary key (id))",
            &[],
        )
        .await
        .unwrap();

    let team1: HashMap<i32, String> = vec![(1, "Ginny".to_owned()), (2, "Cho".to_owned())]
        .into_iter()
        .collect();
    // Test CqlValue serialization too
    let team2: CqlValue = CqlValue::Map(vec![
        (CqlValue::Int(1), CqlValue::Text("John".into())),
        (CqlValue::Int(2), CqlValue::Text("Jessica".into())),
    ]);

    session
        .query("INSERT INTO ks.teams (id, team) VALUES (1, ?)", (team1,))
        .await
        .unwrap();
    session
        .query("INSERT INTO ks.teams (id, team) VALUES (2, ?)", (team2,))
        .await
        .unwrap();

    let mut received_kv_pairs: Vec<Vec<(i32, String)>> = session
        .query("SELECT team FROM ks.teams", &[])
        .await
        .unwrap()
        .rows
        .unwrap()
        .into_typed::<(HashMap<i32, String>,)>()
        .map(Result::unwrap)
        .map(|(hashmap,)| {
            let mut v: Vec<_> = hashmap.into_iter().collect();
            v.sort_unstable();
            v
        })
        .collect();
    received_kv_pairs.sort_unstable();

    assert_eq!(
        received_kv_pairs,
        vec![
            vec![(1, "Ginny".into()), (2, "Cho".into())],
            vec![(1, "John".into()), (2, "Jessica".into())]
        ]
    );

    // Set test

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.images (name text PRIMARY KEY, owner text, tags set<text>)",
            &[],
        )
        .await.unwrap();

    let tags1: Vec<String> = vec!["pet".to_owned(), "cute".to_owned()];
    let tags2: CqlValue = CqlValue::Set(vec![
        CqlValue::Text("kitten".into()),
        CqlValue::Text("cat".into()),
        CqlValue::Text("lol".into()),
    ]);

    session
        .query(
            "INSERT INTO ks.images (name, owner, tags) VALUES ('cat.jpg', 'jsmith', ?)",
            (tags1,),
        )
        .await
        .unwrap();
    session
        .query(
            "UPDATE ks.images SET tags = ? WHERE name = 'cat.jpg';",
            (tags2,),
        )
        .await
        .unwrap();

    let mut received_elements: Vec<String> = session
        .query("SELECT tags FROM ks.images", &[])
        .await
        .unwrap()
        .rows
        .unwrap()
        .into_typed::<(Vec<String>,)>()
        .map(Result::unwrap)
        .map(|(v,)| v.into_iter())
        .flatten()
        .collect();
    received_elements.sort_unstable();

    assert_eq!(
        received_elements,
        vec!["cat".to_string(), "kitten".to_string(), "lol".to_string()]
    );

    // List test

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.plays ( id text PRIMARY KEY, game text, players int, scores list<int>)",
            &[],
        )
        .await.unwrap();

    let scores1: Vec<i32> = vec![17, 4, 2];
    let scores2: CqlValue =
        CqlValue::List(vec![CqlValue::Int(3), CqlValue::Int(9), CqlValue::Int(4)]);

    session
        .query(
            "INSERT INTO ks.plays (id, game, players, scores) VALUES ('123-afde', 'quake', 3, ?)",
            (&scores1,),
        )
        .await
        .unwrap();
    session
        .query(
            "INSERT INTO ks.plays (id, game, players, scores) VALUES ('1234-qwer', 'cs', 10, ?)",
            (&scores2,),
        )
        .await
        .unwrap();

    let mut received_elements: Vec<Vec<i32>> = session
        .query("SELECT scores FROM ks.plays", &[])
        .await
        .unwrap()
        .rows
        .unwrap()
        .into_typed::<(Vec<i32>,)>()
        .map(Result::unwrap)
        .map(|(v,)| v.into_iter().collect())
        .collect();
    received_elements.sort_unstable();

    assert_eq!(received_elements, vec![vec![3, 9, 4], vec![17, 4, 2]]);

    // Tuple test

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.durations (event text, duration tuple<int, text>, primary key (event))",
            &[],
        )
        .await.unwrap();

    let duration1 = (3, "hours");
    let duration2 = CqlValue::Tuple(vec![
        Some(CqlValue::Int(2)),
        Some(CqlValue::Text("minutes".into())),
    ]);
    session
        .query(
            "INSERT INTO ks.durations (event, duration) VALUES ('ev1', ?)",
            (duration1,),
        )
        .await
        .unwrap();
    session
        .query(
            "INSERT INTO ks.durations (event, duration) VALUES ('ev2', ?)",
            (duration2,),
        )
        .await
        .unwrap();
    session
        .query(
            "INSERT INTO ks.durations (event, duration) VALUES ('ev3', (4, null))",
            &[],
        )
        .await
        .unwrap();

    let mut received_elements: Vec<(i32, Option<String>)> = session
        .query("SELECT duration FROM ks.durations", &[])
        .await
        .unwrap()
        .rows
        .unwrap()
        .into_typed::<((i32, Option<String>),)>()
        .map(Result::unwrap)
        .map(|(x,)| x)
        .collect();
    received_elements.sort_unstable();

    assert_eq!(
        received_elements,
        vec![
            (2, Some("minutes".into())),
            (3, Some("hours".into())),
            (4, None)
        ]
    );
}
