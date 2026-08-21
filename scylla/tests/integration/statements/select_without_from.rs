use crate::utils::{create_new_session_builder, setup_tracing};
use scylla::errors::{DbError, ExecutionError, RequestAttemptError};
use scylla_cql::value::CqlTimeuuid;

fn is_select_without_from_unsupported(err: &ExecutionError) -> bool {
    matches!(
        err,
        ExecutionError::LastAttemptError(RequestAttemptError::DbError(
            DbError::SyntaxError | DbError::Invalid,
            _
        ))
    )
}

#[tokio::test]
async fn test_select_without_from() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let rows = match session.query_unpaged("SELECT 1", &[]).await {
        Ok(result) => result.into_rows_result().unwrap(),
        Err(err) if is_select_without_from_unsupported(&err) => return,
        Err(err) => panic!("SELECT without FROM failed unexpectedly: {err}"),
    };

    assert_eq!(rows.column_specs().len(), 1);
    assert_eq!(rows.column_specs().get_by_index(0).unwrap().name(), "1");
    let (value,): (i32,) = rows.single_row::<(i32,)>().unwrap();
    assert_eq!(value, 1);

    let rows = session
        .query_unpaged("SELECT now()", &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();
    assert_eq!(rows.column_specs().len(), 1);
    assert_eq!(rows.column_specs().get_by_index(0).unwrap().name(), "now()");
    let (_value,): (CqlTimeuuid,) = rows.single_row::<(CqlTimeuuid,)>().unwrap();

    let prepared = session.prepare("SELECT 1").await.unwrap();
    let rows = session
        .execute_unpaged(&prepared, &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();
    assert_eq!(rows.column_specs().len(), 1);
    assert_eq!(rows.column_specs().get_by_index(0).unwrap().name(), "1");
    let (value,): (i32,) = rows.single_row::<(i32,)>().unwrap();
    assert_eq!(value, 1);

    let prepared = session.prepare("SELECT now()").await.unwrap();
    let rows = session
        .execute_unpaged(&prepared, &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();
    assert_eq!(rows.column_specs().len(), 1);
    assert_eq!(rows.column_specs().get_by_index(0).unwrap().name(), "now()");
    let (_value,): (CqlTimeuuid,) = rows.single_row::<(CqlTimeuuid,)>().unwrap();
}
