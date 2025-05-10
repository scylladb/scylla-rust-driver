use scylla::statement::Statement;

use crate::utils::{create_new_session_builder, setup_tracing};

/// Tests that PreparedStatement inherits the StatementConfig from Statement
/// that it originates from.
// Commit message:
//
// statement: preserve query configuration during prepare()
// When preparing a statement based on a Query instance, we should preserve the whole
// configuration. It’s especially important for CachingSession users, because prepare()
// happens implicitly in this case, so there’s no other way to customize the configuration
// for a particular query.
// Fixes #340
#[tokio::test]
async fn test_prepared_config() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let mut query = Statement::new("SELECT * FROM system_schema.tables");
    query.set_is_idempotent(true);
    query.set_page_size(42);

    let prepared_statement = session.prepare(query).await.unwrap();

    assert!(prepared_statement.get_is_idempotent());
    assert_eq!(prepared_statement.get_page_size(), 42);
}
