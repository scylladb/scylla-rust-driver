use std::time::Duration;

use assert_matches::assert_matches;
use scylla::{
    client::execution_profile::ExecutionProfile, errors::ExecutionError, statement::Statement,
};

use crate::utils::{create_new_session_builder, setup_tracing};

#[ignore = "works on remote Scylla instances only (local ones are too fast)"]
#[tokio::test]
async fn test_request_timeout() {
    setup_tracing();

    let fast_timeouting_profile_handle = ExecutionProfile::builder()
        .request_timeout(Some(Duration::from_millis(1)))
        .build()
        .into_handle();

    {
        let session = create_new_session_builder().build().await.unwrap();

        let mut query: Statement = Statement::new("SELECT * FROM system_schema.tables");
        query.set_request_timeout(Some(Duration::from_millis(1)));
        match session.query_unpaged(query, &[]).await {
            Ok(_) => panic!("the query should have failed due to a client-side timeout"),
            Err(e) => assert_matches!(e, ExecutionError::RequestTimeout(_)),
        }

        let mut prepared = session
            .prepare("SELECT * FROM system_schema.tables")
            .await
            .unwrap();

        prepared.set_request_timeout(Some(Duration::from_millis(1)));
        match session.execute_unpaged(&prepared, &[]).await {
            Ok(_) => panic!("the prepared query should have failed due to a client-side timeout"),
            Err(e) => assert_matches!(e, ExecutionError::RequestTimeout(_)),
        };
    }
    {
        let timeouting_session = create_new_session_builder()
            .default_execution_profile_handle(fast_timeouting_profile_handle)
            .build()
            .await
            .unwrap();

        let mut query = Statement::new("SELECT * FROM system_schema.tables");

        match timeouting_session.query_unpaged(query.clone(), &[]).await {
            Ok(_) => panic!("the query should have failed due to a client-side timeout"),
            Err(e) => assert_matches!(e, ExecutionError::RequestTimeout(_)),
        };

        query.set_request_timeout(Some(Duration::from_secs(10000)));

        timeouting_session.query_unpaged(query, &[]).await.expect(
            "the query should have not failed, because no client-side timeout was specified",
        );

        let mut prepared = timeouting_session
            .prepare("SELECT * FROM system_schema.tables")
            .await
            .unwrap();

        match timeouting_session.execute_unpaged(&prepared, &[]).await {
            Ok(_) => panic!("the prepared query should have failed due to a client-side timeout"),
            Err(e) => assert_matches!(e, ExecutionError::RequestTimeout(_)),
        };

        prepared.set_request_timeout(Some(Duration::from_secs(10000)));

        timeouting_session.execute_unpaged(&prepared, &[]).await.expect("the prepared query should have not failed, because no client-side timeout was specified");
    }
}
