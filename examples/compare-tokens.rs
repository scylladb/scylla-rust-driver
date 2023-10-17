use anyhow::Result;
use scylla::frame::value::ValueList;
use scylla::routing::Token;
use scylla::transport::NodeAddr;
use scylla::{Session, SessionBuilder};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.compare_tokens_example (pk bigint primary key)",
            &[],
        )
        .await?;

    let prepared = session.prepare("INSERT INTO ks.compare_tokens_example (pk) VALUES (?)").await?;

    for pk in (0..100_i64).chain(99840..99936_i64) {
        session
            .query("INSERT INTO ks.compare_tokens_example (pk) VALUES (?)", (pk,))
            .await?;

        let serialized_pk = (pk,).serialized()?.into_owned();
        let t = prepared.calculate_token(&serialized_pk)?.unwrap().value;

        println!(
            "Token endpoints for query: {:?}",
            session
                .get_cluster_data()
                .get_token_endpoints("ks", Token { value: t })
                .iter()
                .map(|n| n.address)
                .collect::<Vec<NodeAddr>>()
        );

        let qt = session
            .query(format!("SELECT token(pk) FROM ks.compare_tokens_example where pk = {}", pk), &[])
            .await?
            .rows
            .unwrap()
            .get(0)
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
