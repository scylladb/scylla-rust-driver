use anyhow::Result;
use futures::StreamExt;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::frame::response::result::ColumnType;
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::{CellWriter, WrittenCellProof};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "172.42.0.2:9042".to_string());

    println!("Connecting to {uri} ...");

    let session: Session = SessionBuilder::new().known_node(uri).build().await.unwrap();

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.custom_serialization (k int, my text, primary key (k))",
            &[],
        )
        .await?;

    // You can implement SerializeValue for your own types.
    struct MyText<'a>(&'a str);

    impl SerializeValue for MyText<'_> {
        fn serialize<'b>(
            &self,
            typ: &ColumnType,
            writer: CellWriter<'b>,
        ) -> std::result::Result<WrittenCellProof<'b>, scylla::serialize::SerializationError>
        {
            self.0.serialize(typ, writer)
        }
    }

    // SerializeRow can be derived for a struct of statement values.
    #[derive(scylla::SerializeRow)]
    struct MyRow<'a> {
        k: i32,
        my: Option<MyText<'a>>,
    }

    let to_insert = MyRow {
        k: 17,
        my: Some(MyText("Some str")),
    };

    session
        .query_unpaged(
            "INSERT INTO examples_ks.custom_serialization (k, my) VALUES (?, ?)",
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
            "INSERT INTO examples_ks.custom_serialization (k, my) VALUES (?, ?)",
            to_insert_2,
        )
        .await?;

    let iter = session
        .query_iter("SELECT * FROM examples_ks.custom_serialization", &[])
        .await?
        .rows_stream::<(i32, String)>()?;

    let rows = iter.collect::<Vec<_>>().await;
    println!("Q: {rows:?}");

    Ok(())
}
