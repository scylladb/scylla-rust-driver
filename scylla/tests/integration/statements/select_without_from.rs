use crate::utils::{create_new_session_builder, setup_tracing};
use scylla::errors::{DbError, ExecutionError, RequestAttemptError};
use scylla::frame::response::result::{ColumnType, NativeType};
use scylla::value::CqlTimeuuid;

#[tokio::test]
async fn test_select_without_from() {
    setup_tracing();
    // The test touches no user schema, so schema metadata is dead weight.
    let session = create_new_session_builder()
        .fetch_schema_metadata(false)
        .fetch_full_schema_metadata(false)
        .build()
        .await
        .unwrap();

    // A cluster without the extension rejects the statement in the grammar, before any
    // semantic check runs, so `SyntaxError` - and only `SyntaxError` - means "unsupported".
    // `Invalid` must not be accepted here: it is what a *supporting* cluster returns for
    // statements it parsed but refused, so treating it
    // as "unsupported" would silently skip every assertion below.
    let result = match session.query_unpaged("SELECT 1", &[]).await {
        Ok(result) => result,
        Err(ExecutionError::LastAttemptError(RequestAttemptError::DbError(
            DbError::SyntaxError,
            _,
        ))) => {
            println!("Skipping because the cluster doesn't support SELECT without FROM");
            return;
        }
        Err(err) => panic!("SELECT without FROM failed unexpectedly: {err}"),
    };

    let rows = result.into_rows_result().unwrap();
    let spec = rows.column_specs().get_by_index(0).unwrap();
    assert_eq!(rows.column_specs().len(), 1);
    assert_eq!(spec.name(), "1");
    assert_eq!(rows.single_row::<(i32,)>().unwrap(), (1,));

    // Selector naming: an unaliased selector is named after its expression text, with
    // function names keyspace-qualified, and `AS` overrides that.
    let rows = session
        .query_unpaged("SELECT 1, 'hi' AS greeting, now()", &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();
    let names: Vec<&str> = rows.column_specs().iter().map(|spec| spec.name()).collect();
    assert_eq!(names, ["1", "greeting", "system.now()"]);

    let (one, greeting, _now) = rows
        .single_row::<(i32, &str, CqlTimeuuid)>()
        .unwrap_or_else(|err| panic!("failed to deserialize: {err}"));
    assert_eq!((one, greeting), (1, "hi"));

    // Preparing the statement yields the same result metadata.
    let prepared = session.prepare("SELECT 1").await.unwrap();
    let rows = session
        .execute_unpaged(&prepared, &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();
    let spec = rows.column_specs().get_by_index(0).unwrap();
    assert_eq!(rows.column_specs().len(), 1);
    assert_eq!(spec.name(), "1");
    assert_eq!(rows.single_row::<(i32,)>().unwrap(), (1,));

    // A bind marker is only allowed where a type can be inferred for it. There are no
    // columns in scope, so the type has to come from a function signature or from a clause
    // with a fixed type, such as LIMIT.
    //
    // Only the marker types are asserted. The table spec of a marker typed by a function
    // argument is `(<session keyspace>, "one_row")`, because the server resolves function
    // names against the session keyspace; that pair is meaningless and not worth pinning.
    let prepared = session
        .prepare("SELECT intAsBlob(?) AS b LIMIT ?")
        .await
        .unwrap();
    let variable_types: Vec<&ColumnType> = prepared
        .get_variable_col_specs()
        .iter()
        .map(|spec| spec.typ())
        .collect();
    assert_eq!(
        variable_types,
        [
            &ColumnType::Native(NativeType::Int),
            &ColumnType::Native(NativeType::Int)
        ]
    );
    let rows = session
        .execute_unpaged(&prepared, (42_i32, 1_i32))
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();
    let spec = rows.column_specs().get_by_index(0).unwrap();
    assert_eq!(spec.name(), "b");
    assert_eq!(
        rows.single_row::<(Vec<u8>,)>().unwrap(),
        (42_i32.to_be_bytes().to_vec(),)
    );
}
