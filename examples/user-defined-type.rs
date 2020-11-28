use anyhow::Result;
use scylla::cql_to_rust::FromCQLVal;
use scylla::macros::{FromUserType, IntoUserType};
use scylla::transport::session::{IntoTypedRows, Session};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or("127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session = Session::connect(uri, None).await?;
    session.refresh_topology().await?;

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query(
            "CREATE TYPE IF NOT EXISTS ks.my_type (int_val int, text_val text)",
            &[],
        )
        .await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.udt_tab (k int, my my_type, primary key (k))",
            &[],
        )
        .await?;

    // Define custom struct that matches User Defined Type created earlier
    // wrapping field in Option will gracefully handle null field values
    #[derive(Debug, IntoUserType, FromUserType)]
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
        .query("INSERT INTO ks.udt_tab (k, my) VALUES (5, ?)", (to_insert,))
        .await?;

    // And read like any normal value
    if let Some(rows) = session.query("SELECT my FROM ks.udt_tab", &[]).await? {
        for row in rows.into_typed::<(MyType,)>() {
            let (my_val,) = row?;
            println!("{:?}", my_val)
        }
    }

    println!("Ok.");

    Ok(())
}
