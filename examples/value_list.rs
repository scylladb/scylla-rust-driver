use scylla::{Session, SessionBuilder};
use std::env;

#[tokio::main]
async fn main() {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session: Session = SessionBuilder::new().known_node(uri).build().await.unwrap();

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await.unwrap();

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.my_type (k int, my text, primary key (k))",
            &[],
        )
        .await
        .unwrap();

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
        .await
        .unwrap();

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
        .await
        .unwrap();

    let q = session
        .query_unpaged("SELECT * FROM examples_ks.my_type", &[])
        .await
        .unwrap();

    println!("Q: {:?}", q.rows);
}
