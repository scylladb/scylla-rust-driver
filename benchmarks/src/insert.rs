use scylla::statement::Statement;
use uuid::Uuid;

use crate::common::SIMPLE_INSERT_QUERY;

mod common;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n: i32 = common::get_cnt();

    let session = common::init_simple_table_caching().await?;

    let insert_query = SIMPLE_INSERT_QUERY;

    for _ in 0..n {
        let statement: Statement = insert_query.into();
        let prepared = session.add_prepared_statement(&statement).await?;

        let id = Uuid::new_v4();
        session
            .get_session()
            .execute_unpaged(&prepared, (id, 100))
            .await?;
    }

    common::check_row_cnt(session.get_session(), n).await?;

    Ok(())
}
