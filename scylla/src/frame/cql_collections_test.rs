// use anyhow::Result;
use crate::{IntoTypedRows, SessionBuilder};
use std::collections::HashMap;
use std::env;

#[tokio::test]
async fn test_cql_collections() {
    // Create connection
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session = SessionBuilder::new().known_node(uri).build().await.unwrap();

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await.unwrap();

    // HashMap example

    // Create the table

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

    let team2: HashMap<i32, String> = vec![(1, "John".to_owned()), (2, "Jessica".to_owned())]
        .into_iter()
        .collect();

    session
        .query("INSERT INTO ks.teams (id, team) VALUES (1, ?)", (team1,))
        .await
        .unwrap();

    session
        .query("INSERT INTO ks.teams (id, team) VALUES (2, ?)", (team2,))
        .await
        .unwrap();

    // Recieve data in singular map:

    if let Some(rows) = session
        .query("SELECT team FROM ks.teams", &[])
        .await
        .unwrap()
        .rows
    {
        for row in rows.into_typed::<(HashMap<i32, String>,)>() {
            let teams: HashMap<i32, String> = row.unwrap().0;
            for (key, value) in &teams {
                println!("{}: {}", key, value);
            }
        }
    }

    // Set example

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.images (name text PRIMARY KEY, owner text, tags set<text>)",
            &[],
        )
        .await.unwrap();

    let tags1: Vec<String> = vec!["pet".to_owned(), "cute".to_owned()];
    let tags2: Vec<String> = vec!["kitten".to_owned(), "cat".to_owned(), "lol".to_owned()];

    session
        .query(
            "INSERT INTO ks.images (name, owner, tags) VALUES ('cat.jpg', 'jsmith', ?)",
            (tags1,),
        )
        .await
        .unwrap();

    if let Some(rows) = session
        .query("SELECT tags FROM ks.images", &[])
        .await
        .unwrap()
        .rows
    {
        for row in rows.into_typed::<(Vec<String>,)>() {
            let tag: Vec<String> = row.unwrap().0;
            println!("{:?}", tag);
        }
    }

    session
        .query(
            "UPDATE ks.images SET tags = ? WHERE name = 'cat.jpg';",
            (tags2,),
        )
        .await
        .unwrap();
    if let Some(rows) = session
        .query("SELECT tags FROM ks.images", &[])
        .await
        .unwrap()
        .rows
    {
        for row in rows.into_typed::<(Vec<String>,)>() {
            let tag: Vec<String> = row.unwrap().0;
            println!("{:?}", tag);
        }
    }

    // List example

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.plays ( id text PRIMARY KEY, game text, players int, scores list<int>)",
            &[],
        )
        .await.unwrap();

    let scores1: Vec<i32> = vec![17, 4, 2];
    session
        .query(
            "INSERT INTO ks.plays (id, game, players, scores) VALUES ('123-afde', 'quake', 3, ?)",
            (&scores1,),
        )
        .await
        .unwrap();

    if let Some(rows) = session
        .query("SELECT scores FROM ks.plays", &[])
        .await
        .unwrap()
        .rows
    {
        for row in rows.into_typed::<(Vec<i32>,)>() {
            let scores: Vec<i32> = row.unwrap().0;
            println!("{:?}", scores);
        }
    }

    let scores2: Vec<i32> = vec![3, 9, 4];

    session
        .query(
            "UPDATE ks.plays SET scores = ? WHERE id = '123-afde'",
            (&scores2,),
        )
        .await
        .unwrap();

    if let Some(rows) = session
        .query("SELECT scores FROM ks.plays", &[])
        .await
        .unwrap()
        .rows
    {
        for row in rows.into_typed::<(Vec<i32>,)>() {
            let scores: Vec<i32> = row.unwrap().0;
            println!("{:?}", scores);
        }
    }

    // Tuple example

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.durations (event text, duration tuple<int, text>, primary key (event))",
            &[],
        )
        .await.unwrap();

    let duration = (3, "hours");
    session
        .query(
            "INSERT INTO ks.durations (event, duration) VALUES ('ev1', ?)",
            (duration,),
        )
        .await
        .unwrap();

    if let Some(rows) = session
        .query("SELECT duration FROM ks.durations", &[])
        .await
        .unwrap()
        .rows
    {
        for row in rows.into_typed::<((i32, String),)>() {
            let duration = row.unwrap().0;
            println!("{:?}", duration);
        }
    }
}
