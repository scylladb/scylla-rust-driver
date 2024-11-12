use anyhow::Result;
use futures::StreamExt as _;
use scylla::statement::PagingState;
use scylla::{query::Query, Session, SessionBuilder};
use std::env;
use std::ops::ControlFlow;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.select_paging (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    for i in 0..16_i32 {
        session
            .query_unpaged(
                "INSERT INTO examples_ks.select_paging (a, b, c) VALUES (?, ?, 'abc')",
                (i, 2 * i),
            )
            .await?;
    }

    // Iterate through select result with paging
    let mut rows_stream = session
        .query_iter("SELECT a, b, c FROM examples_ks.select_paging", &[])
        .await?
        .rows_stream::<(i32, i32, String)>()?;

    while let Some(next_row_res) = rows_stream.next().await {
        let (a, b, c) = next_row_res?;
        println!("a, b, c: {}, {}, {}", a, b, c);
    }

    let paged_query = Query::new("SELECT a, b, c FROM examples_ks.select_paging").with_page_size(6);

    // Manual paging in a loop, unprepared statement.
    let mut paging_state = PagingState::start();
    loop {
        let (res, paging_state_response) = session
            .query_single_page(paged_query.clone(), &[], paging_state)
            .await?;

        let res = res
            .into_rows_result()?
            .expect("Got result different than Rows");

        println!(
            "Paging state: {:#?} ({} rows)",
            paging_state_response,
            res.rows_num(),
        );

        match paging_state_response.into_paging_control_flow() {
            ControlFlow::Break(()) => {
                // No more pages to be fetched.
                break;
            }
            ControlFlow::Continue(new_paging_state) => {
                // Update paging paging state from the response, so that query
                // will be resumed from where it ended the last time.
                paging_state = new_paging_state;
            }
        }
    }

    let paged_prepared = session
        .prepare(Query::new("SELECT a, b, c FROM examples_ks.select_paging").with_page_size(7))
        .await?;

    // Manual paging in a loop, prepared statement.
    let mut paging_state = PagingState::default();
    loop {
        let (res, paging_state_response) = session
            .execute_single_page(&paged_prepared, &[], paging_state)
            .await?;

        let res = res
            .into_rows_result()?
            .expect("Got result different than Rows");

        println!(
            "Paging state from the prepared statement execution: {:#?} ({} rows)",
            paging_state_response,
            res.rows_num(),
        );

        match paging_state_response.into_paging_control_flow() {
            ControlFlow::Break(()) => {
                // No more pages to be fetched.
                break;
            }
            ControlFlow::Continue(new_paging_state) => {
                // Update paging paging state from the response, so that query
                // will be resumed from where it ended the last time.
                paging_state = new_paging_state;
            }
        }
    }

    println!("Ok.");

    Ok(())
}
