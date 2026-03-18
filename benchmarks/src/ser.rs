use scylla::{client::caching_session::CachingSession, statement::Statement};

use crate::common::DESER_INSERT_QUERY;

mod common;

const DEFAULT_CACHE_SIZE: u32 = 512;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n: i32 = common::get_cnt();

    let session = common::init_deser_table().await?;

    let session: CachingSession = CachingSession::from(session, DEFAULT_CACHE_SIZE as usize);

    let insert_query = DESER_INSERT_QUERY;

    for _ in 0..n * n {
        let statement: Statement = insert_query.into();
        let prepared = session.add_prepared_statement(&statement).await?;

        session
            .get_session()
            .execute_unpaged(&prepared, common::get_deser_data())
            .await?;
    }

    common::check_row_cnt(session.get_session(), n * n).await?;
    Ok(())
}
