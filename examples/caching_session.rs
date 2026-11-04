use anyhow::Result;
use futures::TryStreamExt as _;
use scylla::client::caching_session::CachingSessionBuilder;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "172.42.0.2:9042".to_string());

    println!("Connecting to {uri} ...");

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.caching_session (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    // Wrap the session. From now on, statements executed through `caching_session`
    // are prepared on first use and reused afterwards.
    // The cache holds at most `max_capacity` statements; when it is full, an entry is evicted.
    let caching_session = CachingSessionBuilder::new(session).max_capacity(64).build();

    // The first execution of a statement string prepares it...
    caching_session
        .execute_unpaged(
            "INSERT INTO examples_ks.caching_session (a, b, c) VALUES (?, ?, ?)",
            (1_i32, 2_i32, "abc"),
        )
        .await?;

    // ...and the following ones, with the same string, reuse the cached prepared statement.
    // No `prepare()` call and no need to keep the `PreparedStatement` around.
    for i in 0..5_i32 {
        caching_session
            .execute_unpaged(
                "INSERT INTO examples_ks.caching_session (a, b, c) VALUES (?, ?, ?)",
                (i, i * 10, format!("row {i}")),
            )
            .await?;
    }

    // Paged reads work too, so does `execute_single_page` for manual paging.
    let mut rows = caching_session
        .execute_iter("SELECT a, b, c FROM examples_ks.caching_session", &[])
        .await?
        .rows_stream::<(i32, i32, String)>()?;
    while let Some((a, b, c)) = rows.try_next().await? {
        println!("a, b, c: {a}, {b}, {c}");
    }

    // The statement string is the cache key, so a different string is a different entry.
    // Keep bind markers (`?`) for the values and use the same string every time.
    println!("Cache capacity: {}", caching_session.get_max_capacity());

    Ok(())
}
