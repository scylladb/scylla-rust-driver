use scylla::client::execution_profile::ExecutionProfile;
use scylla::policies::load_balancing::{DefaultPolicy, LatencyAwarenessBuilder};

use crate::utils::{create_new_session_builder, setup_tracing};

// This is a regression test for #696.
#[tokio::test]
#[ntest::timeout(1000)]
async fn latency_aware_query_completes() {
    setup_tracing();
    let policy = DefaultPolicy::builder()
        .latency_awareness(LatencyAwarenessBuilder::default())
        .build();
    let handle = ExecutionProfile::builder()
        .load_balancing_policy(policy)
        .build()
        .into_handle();

    let session = create_new_session_builder()
        .default_execution_profile_handle(handle)
        .build()
        .await
        .unwrap();

    session.query_unpaged("whatever", ()).await.unwrap_err();
}
