use scylla::{client::caching_session::CachingSession, statement::Statement, value::Row};

use crate::common::DESER_INSERT_QUERY;

mod common;

const DEFAULT_CACHE_SIZE: u32 = 512;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n: i32 = common::get_cnt();

    let session = common::init_deser_table().await?;

    let session: CachingSession = CachingSession::from(session, DEFAULT_CACHE_SIZE as usize);

    let insert_query = DESER_INSERT_QUERY;

    for _ in 0..n {
        let statement: Statement = insert_query.into();
        let prepared = session.add_prepared_statement(&statement).await?;

        session
            .get_session()
            .execute_unpaged(&prepared, common::get_deser_data())
            .await?;
    }

    let select_query = "SELECT * FROM benchmarks.basic";
    for _ in 0..n {
        let r = session
            .execute_unpaged(select_query, &[])
            .await?
            .into_rows_result()?
            .rows::<Row>()?
            .collect::<Vec<_>>();
        assert_eq!(r.len() as i32, n);
    }

    Ok(())
}
