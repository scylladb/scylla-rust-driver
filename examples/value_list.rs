use anyhow::Result;
use futures::StreamExt;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session: Session = SessionBuilder::new().known_node(uri).build().await.unwrap();

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.my_type (k int, my text, primary key (k))",
            &[],
        )
        .await?;

    #[derive(scylla::SerializeRow)]
    struct MyType<'a> {
        k: i32,
        my: Option<&'a str>,
    }

    let to_insert = MyType {
        k: 17,
        my: Some("Some str"),
    };

    session
        .query_unpaged(
            "INSERT INTO examples_ks.my_type (k, my) VALUES (?, ?)",
            to_insert,
        )
        .await?;

    // You can also use type generics:
    #[derive(scylla::SerializeRow)]
    struct MyTypeWithGenerics<S: scylla::serialize::value::SerializeValue> {
        k: i32,
        my: Option<S>,
    }

    let to_insert_2 = MyTypeWithGenerics {
        k: 18,
        my: Some("Some string".to_owned()),
    };

    session
        .query_unpaged(
            "INSERT INTO examples_ks.my_type (k, my) VALUES (?, ?)",
            to_insert_2,
        )
        .await?;

    let iter = session
        .query_iter("SELECT * FROM examples_ks.my_type", &[])
        .await?
        .rows_stream::<(i32, String)>()?;

    let rows = iter.collect::<Vec<_>>().await;
    println!("Q: {:?}", rows);

    Ok(())
}
