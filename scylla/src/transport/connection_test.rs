use crate::transport::session::Session;
use fasthash::murmur3;

// TODO: Requires a running local Scylla instance
#[tokio::test]
#[ignore]
async fn test_connecting() {
    let session = Session::connect("localhost:9042", None).await.unwrap();

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await.unwrap();
    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.t (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await
        .unwrap();
    session
        .query("CREATE TABLE IF NOT EXISTS ks.complex_pk (a int, b int, c text, d int, e int, primary key ((a,b,c),d))", &[])
        .await
        .unwrap();
    session
        .query("INSERT INTO ks.t (a, b, c) VALUES (1, 2, 'abc')", &[])
        .await
        .unwrap();
    let prepared_statement = session
        .prepare("INSERT INTO ks.t (a, b, c) VALUES (?, ?, ?)")
        .await
        .unwrap();
    let prepared_complex_pk_statement = session
        .prepare("INSERT INTO ks.complex_pk (a, b, c, d) VALUES (?, ?, ?, 7)")
        .await
        .unwrap();
    println!("Prepared statement: {:?}", prepared_statement);
    println!(
        "Prepared complex pk statement: {:?}",
        prepared_complex_pk_statement
    );
    let values = values!(17_i32, 16_i32, "I'm prepared!!!");
    println!(
        "Partition key of the simple statement: {:x?}",
        prepared_statement.compute_partition_key(&values)
    );
    println!(
        "Partition key of the complex statement: {:x?}",
        prepared_complex_pk_statement.compute_partition_key(&values)
    );
    println!(
        "Token of the simple statement: {:?}",
        murmur3::hash128(&prepared_statement.compute_partition_key(&values)) as i64
    );
    println!(
        "Token of the complex statement: {:?}",
        murmur3::hash128(&prepared_complex_pk_statement.compute_partition_key(&values)) as i64
    );

    session.execute(&prepared_statement, &values).await.unwrap();

    session
        .execute(&prepared_complex_pk_statement, &values)
        .await
        .unwrap();

    // Not required, but it's a nice habit to do that
    session.close().await;
}
