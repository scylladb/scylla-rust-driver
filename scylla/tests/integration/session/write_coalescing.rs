use std::num::NonZeroU64;
use std::sync::Arc;

use scylla::client::{PoolSize, WriteCoalescingDelay};
use scylla::policies::host_filter::AllowListHostFilter;

use crate::utils::{PerformDDL, create_new_session_builder, setup_tracing, unique_keyspace_name};

/// It's difficult to write a reliable test that checks whether coalescing
/// works like intended or not. Instead, this is a smoke test which is supposed
/// to trigger the coalescing logic and check that everything works fine
/// no matter whether coalescing is enabled or not.
#[tokio::test]
async fn test_coalescing() {
    setup_tracing();

    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();
    session
        .ddl(format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"
        ))
        .await
        .unwrap();
    session
        .ddl(format!(
            "CREATE TABLE {ks}.t (case_id int, p int, v blob, PRIMARY KEY (case_id, p))"
        ))
        .await
        .unwrap();

    // Non-deterministic sub-millisecond delay
    run_coalescing_case(&ks, 0, Some(WriteCoalescingDelay::SmallNondeterministic)).await;
    // 1ms delay
    run_coalescing_case(
        &ks,
        1,
        Some(WriteCoalescingDelay::Milliseconds(
            NonZeroU64::new(1).unwrap(),
        )),
    )
    .await;
    // No delay - coalescing disabled
    run_coalescing_case(&ks, 2, None).await;

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

/// Writes rows with more and more concurrent requests and reads them back.
/// Each case writes to its own partition.
async fn run_coalescing_case(
    ks: &str,
    case_id: i32,
    write_coalescing_delay: Option<WriteCoalescingDelay>,
) {
    // A single connection to the contact point only, so that concurrent requests share it.
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "172.42.0.2:9042".to_string());
    let builder = create_new_session_builder()
        .host_filter(Arc::new(AllowListHostFilter::new([uri]).unwrap()))
        .pool_size(PoolSize::PerHost(1.try_into().unwrap()))
        .fetch_schema_metadata(false)
        .write_coalescing(write_coalescing_delay.is_some());
    let builder = match write_coalescing_delay {
        Some(delay) => builder.write_coalescing_delay(delay),
        None => builder,
    };
    let session = Arc::new(builder.build().await.unwrap());
    session.use_keyspace(ks, false).await.unwrap();

    let mut futs = Vec::new();

    const NUM_BATCHES: i32 = 10;

    for batch_size in 0..NUM_BATCHES {
        // Each future should issue more and more queries in the first poll
        let base = arithmetic_sequence_sum(batch_size);
        let session = Arc::clone(&session);
        futs.push(tokio::task::spawn(async move {
            let futs = (base..base + batch_size).map(|j| {
                let session = &session;
                async move {
                    let prepared = session
                        .prepare("INSERT INTO t (case_id, p, v) VALUES (?, ?, ?)")
                        .await
                        .unwrap();
                    session
                        .execute_unpaged(&prepared, (case_id, j, vec![j as u8; j as usize]))
                        .await
                        .unwrap();
                }
            });
            let _joined: Vec<()> = futures::future::join_all(futs).await;
        }));

        tokio::task::yield_now().await;
    }

    let _joined: Vec<()> = futures::future::try_join_all(futs).await.unwrap();

    // Check that everything was written properly
    let range_end = arithmetic_sequence_sum(NUM_BATCHES);
    let mut results = session
        .query_unpaged("SELECT p, v FROM t WHERE case_id = ?", (case_id,))
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(i32, Vec<u8>)>()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    results.sort();

    let expected = (0..range_end)
        .map(|i| (i, vec![i as u8; i as usize]))
        .collect::<Vec<_>>();

    assert_eq!(results, expected);
}

// Returns the sum of integral numbers in the range [0..n)
fn arithmetic_sequence_sum(n: i32) -> i32 {
    n * (n - 1) / 2
}
