//! Shows what a `BoundStatement` is for: carrying serialized values.
//!
//! With `Session::execute_*` the values are passed at the moment of execution,
//! so they must still exist - in their Rust types - at that moment. Binding
//! them to a prepared statement instead produces a `BoundStatement`: a single
//! value that holds the statement together with its already serialized, and
//! thus type erased, values.
//!
//! That is what makes the `execute_all` function below possible. It takes
//! statements of completely different shapes - a two-column insert, a
//! three-column insert, a delete - in one collection, and executes them
//! without knowing anything about their values.
//!
//! It also means the values are serialized exactly once, no matter how many
//! things need them. Asking a `PreparedStatement` for a token and then
//! executing it serializes the same values twice; a `BoundStatement` computes
//! its token from - and is executed with - the bytes produced by the single
//! `bind()` call.

use anyhow::Result;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::routing::Token;
use scylla::statement::bound::BoundStatement;
use std::env;

/// Executes whatever it is given.
///
/// Note what this signature does *not* mention: values. A `Vec` of prepared
/// statements could not be executed like this - each of them would still need
/// its values, and those would all have to be of the same Rust type (or boxed
/// behind a trait object) to live in one collection.
async fn execute_all(session: &Session, statements: &[BoundStatement]) -> Result<()> {
    for bound in statements {
        session.execute_bound_unpaged(bound).await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "172.42.0.2:9042".to_string());

    println!("Connecting to {uri} ...");

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.bound_statement (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    // Prepare as usual: once per statement, no matter how many times it will be
    // bound and executed later.
    let insert_ab = session
        .prepare("INSERT INTO examples_ks.bound_statement (a, b) VALUES (?, ?)")
        .await?;
    let insert_abc = session
        .prepare("INSERT INTO examples_ks.bound_statement (a, b, c) VALUES (?, ?, ?)")
        .await?;
    let delete = session
        .prepare("DELETE FROM examples_ks.bound_statement WHERE a = ? AND b = ?")
        .await?;

    // Bind values to the prepared statements. `bind()` serializes the values
    // right here - the types of the tuples below do not survive into the type
    // of the `Vec`, which is why the three statements can share it.
    //
    // `bind()` consumes the prepared statement, so clone it whenever it is
    // still needed afterwards; cloning a prepared statement is cheap, while
    // preparing one is not.
    let statements: Vec<BoundStatement> = vec![
        insert_ab.bind(&(1, 2))?,
        insert_abc.clone().bind(&(1, 3, "three"))?,
        insert_abc.bind(&(2, 1, "one"))?,
        delete.bind(&(1, 2))?,
    ];

    // A bound statement also knows the token it will be routed by, without
    // being handed the values a second time.
    for bound in &statements {
        let token: Option<Token> = bound.calculate_token()?;
        println!(
            "{} -> token {:?}",
            bound.prepared().get_statement(),
            token.map(|t| t.value())
        );
    }

    // ...and then the statements are executed with those same serialized
    // values: the tokens above cost no extra serialization.
    execute_all(&session, &statements).await?;

    // What is left: (1, 3), (2, 1) and (3, 1) - (1, 2) was inserted and then deleted.
    let rows = session
        .query_unpaged("SELECT a, b, c FROM examples_ks.bound_statement", &[])
        .await?
        .into_rows_result()?;
    for row in rows.rows::<(i32, i32, Option<&str>)>()? {
        let (a, b, c) = row?;
        println!("a, b, c: {a}, {b}, {c:?}");
    }

    println!("Ok.");

    Ok(())
}
