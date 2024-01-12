use anyhow::Result;
use scylla::macros::FromUserType;
use scylla::{IntoTypedRows, SerializeCql, Session, SessionBuilder};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

    session.query("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query(
            "CREATE TYPE IF NOT EXISTS examples_ks.my_type (int_val int, text_val text)",
            &[],
        )
        .await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS examples_ks.user_defined_type_table (k int, my my_type, primary key (k))",
            &[],
        )
        .await?;

    // Define custom struct that matches User Defined Type created earlier
    // wrapping field in Option will gracefully handle null field values
    #[derive(Debug, FromUserType, SerializeCql)]
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
        .query(
            "INSERT INTO examples_ks.user_defined_type_table (k, my) VALUES (5, ?)",
            (to_insert,),
        )
        .await?;

    // And read like any normal value
    if let Some(rows) = session
        .query("SELECT my FROM examples_ks.user_defined_type_table", &[])
        .await?
        .rows
    {
        for row in rows.into_typed::<(MyType,)>() {
            let (my_type_value,): (MyType,) = row?;
            println!("{:?}", my_type_value)
        }
    }

    println!("Ok.");

    Ok(())
}
