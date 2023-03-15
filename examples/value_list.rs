use scylla::{Legacy08Session, SessionBuilder};
use std::env;

#[tokio::main]
async fn main() {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session: Legacy08Session = SessionBuilder::new().known_node(uri).build().await.unwrap();

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await.unwrap();

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.my_type (k int, my text, primary key (k))",
            &[],
        )
        .await
        .unwrap();

    #[derive(scylla::ValueList)]
    struct MyType<'a> {
        k: i32,
        my: Option<&'a str>,
    }

    let to_insert = MyType {
        k: 17,
        my: Some("Some str"),
    };

    session
        .query("INSERT INTO ks.my_type (k, my) VALUES (?, ?)", to_insert)
        .await
        .unwrap();

    // You can also use type generics:
    #[derive(scylla::ValueList)]
    struct MyTypeWithGenerics<S: scylla::frame::value::Value> {
        k: i32,
        my: Option<S>,
    }

    let to_insert_2 = MyTypeWithGenerics {
        k: 18,
        my: Some("Some string".to_owned()),
    };

    session
        .query("INSERT INTO ks.my_type (k, my) VALUES (?, ?)", to_insert_2)
        .await
        .unwrap();

    let q = session
        .query("SELECT * FROM ks.my_type", &[])
        .await
        .unwrap();

    println!("Q: {:?}", q.rows);
}
