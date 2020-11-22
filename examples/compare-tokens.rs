use anyhow::Result;
use scylla::routing::murmur3_token;
use scylla::transport::session::Session;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or("127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session = Session::connect(uri, None).await?;
    session.refresh_topology().await?;

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &scylla::values!()).await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.t (pk bigint primary key)",
            &scylla::values!(),
        )
        .await?;

    let prepared = session.prepare("INSERT INTO ks.t (pk) VALUES (?)").await?;

    for pk in (0..100_i64).chain(99840..99936_i64) {
        session
            .query("INSERT INTO ks.t (pk) VALUES (?)", &scylla::values!(pk))
            .await?;

        let t = murmur3_token(prepared.compute_partition_key(&scylla::values!(pk)?)?).value;

        let qt = session
            .query(
                format!("SELECT token(pk) FROM ks.t where pk = {}", pk),
                &scylla::values!(),
            )
            .await?
            .unwrap()
            .iter()
            .next()
            .expect("token query no rows!")
            .columns[0]
            .as_ref()
            .expect("token query null value!")
            .as_bigint()
            .expect("token wrong type!");
        assert_eq!(t, qt);
        println!("token for {}: {}", pk, t);
    }

    println!("Ok.");

    Ok(())
}
