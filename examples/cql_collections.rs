use anyhow::Result;
use scylla::transport::session::{IntoTypedRows, Session};
use std::collections::HashMap;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    // Create connection
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session = Session::connect(uri, None).await?;
    session.refresh_topology().await?;

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await?;

    // HashMap example

    // Create the table
    session
        .query(
            "CREATE TABLE IF NOT EXISTS teams (id int, team map<int, text>, primary key id)",
            &[],
        )
        .await?;

    let team1: HashMap<i32, String> = vec![(1, "Ginny".to_owned()), (2, "Cho".to_owned())]
        .into_iter()
        .collect();

    let team2: HashMap<i32, String> = vec![(1, "John".to_owned()), (2, "Jessica".to_owned())]
        .into_iter()
        .collect();

    session
        .query("INSERT INTO teams (id, team) VALUES (1, ?)", (team1,))
        .await?;

    session
        .query("INSERT INTO teams (id, team) VALUES (2, ?)", (team2,))
        .await?;

    // Recieve data in singular map:

    if let Some(rows) = session.query("SELECT team FROM teams", &[]).await? {
        for row in rows.into_typed::<(HashMap<i32, String>,)>() {
            let _teams: HashMap<i32, String> = row?.0;
        }
    }

    // Set example

    session
        .query(
            "CREATE TABLE images (name text PRIMARY KEY, owner text, tags set<text>)",
            &[],
        )
        .await?;

    let tags1: Vec<String> = vec!["pet".to_owned(), "cute".to_owned()];
    let tags2: Vec<String> = vec!["kitten".to_owned(), "cat".to_owned(), "lol".to_owned()];

    session
        .query(
            "INSERT INTO images (name, owner, tags) VALUES ('cat.jpg', 'jsmith', ?)",
            (tags1,),
        )
        .await?;

    if let Some(rows) = session.query("SELECT tags FROM images", &[]).await? {
        for row in rows.into_typed::<(Vec<String>,)>() {
            let tag: Vec<String> = row?.0;
            println!("{:?}", tag);
        }
    }

    session
        .query(
            "UPDATE images SET tags = ? WHERE name = 'cat.jpg';",
            (tags2,),
        )
        .await?;
    if let Some(rows) = session.query("SELECT tags FROM images", &[]).await? {
        for row in rows.into_typed::<(Vec<String>,)>() {
            let tag: Vec<String> = row?.0;
            println!("{:?}", tag);
        }
    }

    // List example

    session
        .query(
            "CREATE TABLE plays ( id text PRIMARY KEY, game text, players int, scores list<int>)",
            &[],
        )
        .await?;

    let scores1: Vec<i32> = vec![17, 4, 2];
    session
        .query(
            "INSERT INTO plays (id, game, players, scores) VALUES ('123-afde', 'quake', 3, ?)",
            &scores1,
        )
        .await?;

    if let Some(rows) = session.query("SELECT scores FROM plays", &[]).await? {
        for row in rows.into_typed::<(Vec<i32>,)>() {
            let scores: Vec<i32> = row?.0;
            println!("{:?}", scores);
        }
    }

    let scores2: Vec<i32> = vec![3, 9, 4];

    session
        .query(
            "UPDATE plays SET scores = ? WHERE id = '123-afde'",
            &scores2,
        )
        .await?;

    if let Some(rows) = session.query("SELECT scores FROM plays", &[]).await? {
        for row in rows.into_typed::<(Vec<i32>,)>() {
            let scores: Vec<i32> = row?.0;
            println!("{:?}", scores);
        }
    }

    // Tuple example

    session
        .query(
            "CREATE TABLE durations (event text, duration tuple<int, text>,)",
            &[],
        )
        .await?;

    let duration = (3, "hours");
    session
        .query(
            "INSERT INTO durations (event, duration) VALUES ('ev1', ?)",
            &duration,
        )
        .await?;

    if let Some(rows) = session.query("SELECT duration FROM durations", &[]).await? {
        for row in rows.into_typed::<((i32, String),)>() {
            let duration = row?.0;
            println!("{:?}", duration);
        }
    }

    Ok(())
}
