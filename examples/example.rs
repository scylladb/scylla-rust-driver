use anyhow::Result;
use scylla::transport::session::Session;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or("localhost:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session = Session::connect(uri, None).await?;

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.t (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    session
        .query(
            "INSERT INTO ks.t (a, b, c) VALUES (?, ?, ?)",
            &scylla::values!(3, 4, "def"),
        )
        .await?;

    session
        .query("INSERT INTO ks.t (a, b, c) VALUES (1, 2, 'abc')", &[])
        .await?;

    let prepared = session
        .prepare("INSERT INTO ks.t (a, b, c) VALUES (?, 7, ?)")
        .await?;
    session
        .execute(&prepared, &scylla::values!(42_i32, "I'm prepared!"))
        .await?;

    if let Some(rs) = session.query("SELECT a, b, c FROM ks.t", &[]).await? {
        for r in rs {
            let a = r.columns[0].as_ref().unwrap().as_int().unwrap();
            let b = r.columns[1].as_ref().unwrap().as_int().unwrap();
            let c = r.columns[2].as_ref().unwrap().as_text().unwrap();
            println!("a, b, c: {}, {}, {}", a, b, c);
        }
    }

    println!("Ok.");

    Ok(())
}
