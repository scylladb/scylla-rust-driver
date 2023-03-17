use anyhow::Result;
use scylla::frame::value::ValueList;
use scylla::transport::partitioner::{Murmur3Partitioner, Partitioner};
use scylla::transport::NodeAddr;
use scylla::{load_balancing, Session, SessionBuilder};
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
        let t = Murmur3Partitioner::hash(prepared.compute_partition_key(&serialized_pk)?).value;

        let statement_info = load_balancing::RoutingInfo {
            token: Some(scylla::routing::Token { value: t }),
            keyspace: Some("ks"),
            is_confirmed_lwt: false,
            ..Default::default()
        };
        println!(
            "Estimated replicas for query: {:?}",
            session
                .estimate_replicas_for_query(&statement_info)
                .iter()
                .map(|n| n.address)
                .collect::<Vec<NodeAddr>>()
        );

        let qt = session
            .query(format!("SELECT token(pk) FROM ks.t where pk = {}", pk), &[])
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
