use anyhow::Result;
use futures::TryStreamExt;
use scylla::macros::FromUserType;
use scylla::{LegacySession, SerializeValue, SessionBuilder};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session: LegacySession = SessionBuilder::new().known_node(uri).build_legacy().await?;

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query_unpaged(
            "CREATE TYPE IF NOT EXISTS examples_ks.my_type (int_val int, text_val text)",
            &[],
        )
        .await?;

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.user_defined_type_table (k int, my my_type, primary key (k))",
            &[],
        )
        .await?;

    // Define custom struct that matches User Defined Type created earlier
    // wrapping field in Option will gracefully handle null field values
    #[derive(Debug, FromUserType, SerializeValue)]
    struct MyType {
        int_val: i32,
        text_val: Option<String>,
    }

    let to_insert = MyType {
        int_val: 17,
        text_val: Some("Some string".to_string()),
    };

    // It can be inserted like a normal value
    session
        .query_unpaged(
            "INSERT INTO examples_ks.user_defined_type_table (k, my) VALUES (5, ?)",
            (to_insert,),
        )
        .await?;

    // And read like any normal value
    let mut iter = session
        .query_iter(
            "SELECT a, b, c FROM examples_ks.user_defined_type_table",
            &[],
        )
        .await?
        .into_typed::<(MyType,)>();
    while let Some((my_val,)) = iter.try_next().await? {
        println!("{:?}", my_val);
    }

    println!("Ok.");

    Ok(())
}
