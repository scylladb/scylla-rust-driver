use scylla::{response::PagingState, statement::Statement};
use std::{env, ops::ControlFlow, sync::Arc};
use uuid::Uuid;

use crate::common::SIMPLE_INSERT_QUERY;

mod common;

const CONCURRENCY_LEVEL: usize = 20;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n: i32 = env::var("CNT")
        .ok()
        .and_then(|s: String| s.parse::<i32>().ok())
        .expect("CNT parameter is required.");

    let session = Arc::new(common::init_simple_table_caching().await?);

    let insert_query = SIMPLE_INSERT_QUERY;
    for _ in 0..50 {
        let statement: Statement = insert_query.into();
        let prepared = session.add_prepared_statement(&statement).await?;

        let id = Uuid::new_v4();
        session
            .get_session()
            .execute_unpaged(&prepared, (id, 10))
            .await?;
    }
    let mut tasks = vec![];
    for _ in 0..(CONCURRENCY_LEVEL) {
        let session = session.clone();
        tasks.push(tokio::task::spawn(async move {
            let mut select_query = Statement::new("SELECT * FROM benchmarks.basic");
            select_query.set_page_size(1);
            let prepared = session.add_prepared_statement(&select_query).await.unwrap();

            for _ in 0..n {
                let mut state = PagingState::start();

                let mut sm = 0;
                loop {
                    let (res, next) = session
                        .get_session()
                        .execute_single_page(&prepared, &[], state)
                        .await
                        .unwrap();
                    res.into_rows_result()
                        .unwrap()
                        .rows::<(Uuid, i32)>()
                        .unwrap()
                        .for_each(|r| {
                            sm += r.unwrap().1;
                        });
                    if let ControlFlow::Continue(ps) = next.into_paging_control_flow() {
                        state = ps;
                    } else {
                        break;
                    }
                }
                assert_eq!(sm, 500);
            }
        }));
    }

    for t in tasks {
        t.await?;
    }

    Ok(())
}
