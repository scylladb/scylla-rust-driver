use anyhow::Result;
use scylla::routing::Token;
use scylla::transport::NodeAddr;
use scylla::{Session, SessionBuilder};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.compare_tokens (pk bigint primary key)",
            &[],
        )
        .await?;

    let prepared = session
        .prepare("INSERT INTO examples_ks.compare_tokens (pk) VALUES (?)")
        .await?;

    for pk in (0..100_i64).chain(99840..99936_i64) {
        session
            .query_unpaged(
                "INSERT INTO examples_ks.compare_tokens (pk) VALUES (?)",
                (pk,),
            )
            .await?;

        let t = prepared.calculate_token(&(pk,))?.unwrap().value();

        println!(
            "Token endpoints for query: {:?}",
            session
                .get_cluster_data()
                .get_token_endpoints("examples_ks", "compare_tokens", Token::new(t))
                .iter()
                .map(|(node, _shard)| node.address)
                .collect::<Vec<NodeAddr>>()
        );

        let (qt,) = session
            .query_unpaged(
                "SELECT token(pk) FROM examples_ks.compare_tokens where pk = ?",
                (pk,),
            )
            .await?
            .into_rows_result()?
            .single_row()?;
        assert_eq!(t, qt);
        println!("token for {}: {}", pk, t);
    }

    println!("Ok.");

    Ok(())
}
