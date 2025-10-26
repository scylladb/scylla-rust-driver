use std::{sync::Arc, time::Duration};

use assert_matches::assert_matches;
use scylla::{
    client::{execution_profile::ExecutionProfile, session_builder::SessionBuilder},
    errors::ExecutionError,
    statement::{
        Statement,
        batch::{Batch, BatchType},
    },
};
use scylla_proxy::{
    Condition, ProxyError, Reaction as _, RequestOpcode, RequestReaction, RequestRule, WorkerError,
};

use crate::utils::{setup_tracing, test_with_3_node_cluster};

#[cfg_attr(scylla_cloud_tests, ignore)]
#[tokio::test]
async fn test_request_timeout() {
    setup_tracing();

    let res = test_with_3_node_cluster(
        scylla_proxy::ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let short_timeout = Duration::from_millis(1);
            let very_long_timeout = Duration::from_secs(10000);
            let query_str = "SELECT host_id FROM system.local WHERE key='local'";

            let create_session_builder = || {
                SessionBuilder::new()
                    .known_node(proxy_uris[0].as_str())
                    .address_translator(Arc::new(translation_map.clone()))
            };

            running_proxy.running_nodes.iter_mut().for_each(|node| {
                node.change_request_rules(Some(vec![
                    RequestRule(
                        Condition::any([
                            Condition::RequestOpcode(RequestOpcode::Query),
                            Condition::RequestOpcode(RequestOpcode::Execute),
                            Condition::RequestOpcode(RequestOpcode::Batch),
                        ])
                            .and(Condition::not(Condition::ConnectionRegisteredAnyEvent)),
                        RequestReaction::delay(10 * short_timeout)
                    )
                ]));
            });

            let session = create_session_builder()
                .build()
                .await
                .unwrap();

            // Case 1: per-statement timeouts.
            {
                let mut query: Statement = Statement::new(query_str);
                query.set_request_timeout(Some(short_timeout));
                match session.query_unpaged(query, &[]).await {
                    Ok(_) => panic!("the query should have failed due to a client-side timeout"),
                    Err(e) => assert_matches!(e, ExecutionError::RequestTimeout(_)),
                }

                let mut batch: Batch = Batch::new(BatchType::Logged);
                batch.set_request_timeout(Some(short_timeout));
                match session.batch(&batch, &[][..] as &[()]).await {
                    Ok(_) => panic!("the batch should have failed due to a client-side timeout"),
                    Err(e) => assert_matches!(e, ExecutionError::RequestTimeout(_)),
                }

                let mut prepared = session
                    .prepare(query_str)
                    .await
                    .unwrap();

                prepared.set_request_timeout(Some(short_timeout));
                match session.execute_unpaged(&prepared, &[]).await {
                    Ok(_) => {
                        panic!("the prepared query should have failed due to a client-side timeout")
                    }
                    Err(e) => assert_matches!(e, ExecutionError::RequestTimeout(_)),
                };
            }

            // Case 2: tight session-level timeout, overridden by per-statement long timeouts.
            {
                let fast_timeouting_profile = ExecutionProfile::builder()
                    .request_timeout(Some(short_timeout))
                    .build();

                // Although this `clone()` looks suspicious, it is necessary to get an owned handle to the profile
                // in order to call `map_to_another_profile()`, which requires a mutable reference.
                // The profile handle itself is just a reference-counted pointer, so cloning it is cheap,
                // and the remapping affects the session too.
                session.get_default_execution_profile_handle().clone().map_to_another_profile(fast_timeouting_profile);

                let mut query = Statement::new(query_str);

                match session.query_unpaged(query.clone(), &[]).await {
                    Ok(_) => panic!("the query should have failed due to a client-side timeout"),
                    Err(e) => assert_matches!(e, ExecutionError::RequestTimeout(_)),
                };

                query.set_request_timeout(Some(very_long_timeout));

                session.query_unpaged(query, &[]).await.expect(
                    "the query should have not failed, because no client-side timeout was specified",
                );

                let mut batch: Batch = Batch::new(BatchType::Logged);
                match session.batch(&batch, &[][..] as &[()]).await {
                    Ok(_) => panic!("the batch should have failed due to a client-side timeout"),
                    Err(e) => assert_matches!(e, ExecutionError::RequestTimeout(_)),
                }

                batch.set_request_timeout(Some(very_long_timeout));
                session.batch(&batch, &[][..] as &[()]).await.expect("the batch should have not failed, because no client-side timeout was specified");

                let mut prepared = session
                    .prepare(query_str)
                    .await
                    .unwrap();

                match session.execute_unpaged(&prepared, &[]).await {
                    Ok(_) => {
                        panic!("the prepared query should have failed due to a client-side timeout")
                    }
                    Err(e) => assert_matches!(e, ExecutionError::RequestTimeout(_)),
                };

                prepared.set_request_timeout(Some(very_long_timeout));

                session.execute_unpaged(&prepared, &[]).await.expect("the prepared query should have not failed, because no client-side timeout was specified");
            }
            running_proxy
        }
    ).await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
