use scylla::errors::{ExecutionError, RequestAttemptError};
use scylla::frame::frame_errors::BatchSerializationError;
use scylla::frame::frame_errors::CqlRequestSerializationError;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::query::Query;

use crate::utils::create_new_session_builder;
use crate::utils::setup_tracing;
use crate::utils::unique_keyspace_name;
use crate::utils::PerformDDL;

use assert_matches::assert_matches;

#[tokio::test]
#[ntest::timeout(60000)]
async fn batch_statements_and_values_mismatch_detected() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}", ks)).await.unwrap();
    session.use_keyspace(ks, false).await.unwrap();
    session
        .ddl("CREATE TABLE IF NOT EXISTS batch_serialization_test (p int PRIMARY KEY, val int)")
        .await
        .unwrap();

    let mut batch = Batch::new(BatchType::Logged);
    let stmt = session
        .prepare("INSERT INTO batch_serialization_test (p, val) VALUES (?, ?)")
        .await
        .unwrap();
    batch.append_statement(stmt.clone());
    batch.append_statement(Query::new(
        "INSERT INTO batch_serialization_test (p, val) VALUES (3, 4)",
    ));
    batch.append_statement(stmt);

    // Subtest 1: counts are correct
    {
        session.batch(&batch, &((1, 2), (), (5, 6))).await.unwrap();
    }

    // Subtest 2: not enough values
    {
        let err = session.batch(&batch, &((1, 2), ())).await.unwrap_err();
        assert_matches!(
            err,
            ExecutionError::LastAttemptError(RequestAttemptError::CqlRequestSerialization(
                CqlRequestSerializationError::BatchSerialization(
                    BatchSerializationError::ValuesAndStatementsLengthMismatch {
                        n_value_lists: 2,
                        n_statements: 3
                    }
                )
            ))
        )
    }

    // Subtest 3: too many values
    {
        let err = session
            .batch(&batch, &((1, 2), (), (5, 6), (7, 8)))
            .await
            .unwrap_err();
        assert_matches!(
            err,
            ExecutionError::LastAttemptError(RequestAttemptError::CqlRequestSerialization(
                CqlRequestSerializationError::BatchSerialization(
                    BatchSerializationError::ValuesAndStatementsLengthMismatch {
                        n_value_lists: 4,
                        n_statements: 3
                    }
                )
            ))
        )
    }
}
