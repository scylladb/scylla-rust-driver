use scylla::{response::PagingState, statement::Statement};
use std::ops::ControlFlow;
use uuid::Uuid;

mod common;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n: i32 = common::get_cnt();

    let session = common::init_simple_table_caching().await?;

    let insert_query = "INSERT INTO benchmarks.basic (id, val) VALUES (?, ?)";
    for _ in 0..50 {
        let statement: Statement = insert_query.into();
        let prepared = session.add_prepared_statement(&statement).await?;

        let id = Uuid::new_v4();
        session
            .get_session()
            .execute_unpaged(&prepared, (id, 10))
            .await?;
    }

    let mut select_query = Statement::new("SELECT * FROM benchmarks.basic");
    select_query.set_page_size(1);
    let prepared = session.add_prepared_statement(&select_query).await?;

    for _ in 0..n {
        let mut state = PagingState::start();

        let mut sm = 0;
        loop {
            let (res, next) = session
                .get_session()
                .execute_single_page(&prepared, &[], state)
                .await?;
            res.into_rows_result()?
                .rows::<(Uuid, i32)>()?
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

    Ok(())
}
