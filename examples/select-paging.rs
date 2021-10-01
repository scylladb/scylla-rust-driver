use anyhow::Result;
use futures::stream::StreamExt;
use scylla::{query::Query, Session, SessionBuilder};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.t (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    for i in 0..16_i32 {
        session
            .query(
                "INSERT INTO ks.t (a, b, c) VALUES (?, ?, 'abc')",
                (i, 2 * i),
            )
            .await?;
    }

    // Iterate through select result with paging
    let mut rows_stream = session
        .query_iter("SELECT a, b, c FROM ks.t", &[])
        .await?
        .into_typed::<(i32, i32, String)>();

    while let Some(next_row_res) = rows_stream.next().await {
        let (a, b, c) = next_row_res?;
        println!("a, b, c: {}, {}, {}", a, b, c);
    }

    let paged_query = Query::new("SELECT a, b, c FROM ks.t").with_page_size(6);
    let res1 = session.query(paged_query.clone(), &[]).await?;
    println!(
        "Paging state: {:#?} ({} rows)",
        res1.paging_state,
        res1.rows.unwrap().len()
    );
    let res2 = session
        .query_paged(paged_query.clone(), &[], res1.paging_state)
        .await?;
    println!(
        "Paging state: {:#?} ({} rows)",
        res2.paging_state,
        res2.rows.unwrap().len()
    );
    let res3 = session
        .query_paged(paged_query.clone(), &[], res2.paging_state)
        .await?;
    println!(
        "Paging state: {:#?} ({} rows)",
        res3.paging_state,
        res3.rows.unwrap().len()
    );

    let paged_prepared = session
        .prepare(Query::new("SELECT a, b, c FROM ks.t").with_page_size(7))
        .await?;
    let res4 = session.execute(&paged_prepared, &[]).await?;
    println!(
        "Paging state from the prepared statement execution: {:#?} ({} rows)",
        res4.paging_state,
        res4.rows.unwrap().len()
    );
    let res5 = session
        .execute_paged(&paged_prepared, &[], res4.paging_state)
        .await?;
    println!(
        "Paging state from the second prepared statement execution: {:#?} ({} rows)",
        res5.paging_state,
        res5.rows.unwrap().len()
    );
    let res6 = session
        .execute_paged(&paged_prepared, &[], res5.paging_state)
        .await?;
    println!(
        "Paging state from the third prepared statement execution: {:#?} ({} rows)",
        res6.paging_state,
        res6.rows.unwrap().len()
    );
    println!("Ok.");

    Ok(())
}
