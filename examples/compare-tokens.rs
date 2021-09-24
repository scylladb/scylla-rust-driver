use anyhow::Result;
use scylla::frame::value::ValueList;
use scylla::routing::murmur3_token;
use scylla::{Session, SessionBuilder};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.t (pk bigint primary key)",
            &[],
        )
        .await?;

    let prepared = session.prepare("INSERT INTO ks.t (pk) VALUES (?)").await?;

    for pk in (0..100_i64).chain(99840..99936_i64) {
        session
            .query("INSERT INTO ks.t (pk) VALUES (?)", (pk,))
            .await?;

        let serialized_pk = (pk,).serialized()?.into_owned();
        let t = murmur3_token(prepared.compute_partition_key(&serialized_pk)?).value;

        let (qt,) = session
            .query(format!("SELECT token(pk) FROM ks.t where pk = {}", pk), &[])
            .await?
            .first_row_typed::<(i64,)>()
            .unwrap();

        assert_eq!(t, qt);
        println!("token for {}: {}", pk, t);
    }

    println!("Ok.");

    Ok(())
}
