use scylla::statement::batch::{Batch, BatchStatement};
use std::cmp::min;
use uuid::Uuid;

use crate::common::SIMPLE_INSERT_QUERY;

mod common;

// Empirically determined max batch size, that doesn't cause database error.
const STEP: i32 = 3971;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n: i32 = common::get_cnt();

    let session = common::init_simple_table_caching().await?;

    let insert_query = SIMPLE_INSERT_QUERY;

    for i in 0..((n + STEP - 1) / STEP) {
        let c_len = min(n - (i * STEP), STEP);
        let insert_vec = vec![
            BatchStatement::Query(insert_query.into());
            // Always using small enough values so it doesn't overflow.
            c_len.try_into().unwrap()
        ];

        let statement: Batch =
            Batch::new_with_statements(scylla::statement::batch::BatchType::Logged, insert_vec);

        let mut params_vec = vec![];
        for _ in 0..c_len {
            params_vec.push((Uuid::new_v4(), 1));
        }
        session.batch(&statement, params_vec).await?;
    }

    let select_query = "SELECT COUNT(1) FROM benchmarks.basic USING TIMEOUT 120s;";

    assert!(
        session
            .get_session()
            .query_unpaged(select_query, &[])
            .await?
            .into_rows_result()?
            .first_row::<(i64,)>()?
            .0
            == n.into()
    );

    Ok(())
}
