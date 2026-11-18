//! Tests of how the response to a statement is split into pages. They check
//! that:
//! - a statement that returns several pages gives all the rows, in order,
//! - a single page call gives only the first page, and says that more follow,
//! - a statement that returns no rows gives no rows.
//!
//! Every entry point must behave the same way. Thus, each case runs through
//! all six entry points, with [`EntryPoint`].
//!
//! All the cases are in one `#[tokio::test]`. They share one session, one
//! keyspace and one table. Thus, the shared cluster does not get one keyspace
//! for each case.

use scylla::client::session::Session;
use scylla::statement::Statement;
use scylla::statement::batch::{Batch, BatchType};

use crate::entry_point::{EntryPoint, Rows};
use crate::utils::{
    PerformDDL as _, create_new_session_builder, setup_tracing, unique_keyspace_name,
};

/// The rows in one page. Each partition has several times more rows. Thus, a
/// partition spans several pages.
const PAGE_SIZE: i32 = 20;
const ROWS_PER_PARTITION: i32 = 100;
const PARTITIONS: [&str; 2] = ["a", "b"];

type Row = (String, i32, i32);

#[tokio::test]
async fn test_paging() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();
    session.ddl(format!("CREATE KEYSPACE {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session.use_keyspace(&ks, false).await.unwrap();
    session
        .ddl("CREATE TABLE t (k text, c int, v int, PRIMARY KEY (k, c))")
        .await
        .unwrap();
    session.await_schema_agreement().await.unwrap();

    insert_rows(&session).await;

    a_partition_is_returned_in_pages(&session).await;
    all_partitions_are_returned_in_pages(&session).await;
    no_rows_are_returned_as_no_rows(&session).await;

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

/// The rows of a partition, in the clustering order. The value `v` differs
/// between partitions. Thus, a row from the wrong partition does not match.
fn rows_of_partition(partition: usize) -> Vec<Row> {
    (0..ROWS_PER_PARTITION)
        .map(|c| {
            (
                PARTITIONS[partition].to_owned(),
                c,
                c + 1000 * partition as i32,
            )
        })
        .collect()
}

async fn insert_rows(session: &Session) {
    let insert = session
        .prepare("INSERT INTO t (k, c, v) VALUES (?, ?, ?)")
        .await
        .unwrap();
    for partition in 0..PARTITIONS.len() {
        // A batch to one partition is atomic and cheap.
        let mut batch = Batch::new(BatchType::Unlogged);
        let rows = rows_of_partition(partition);
        for _ in &rows {
            batch.append_statement(insert.clone());
        }
        session.batch(&batch, &rows).await.unwrap();
    }
}

fn select(query: &str) -> Statement {
    Statement::new(query).with_page_size(PAGE_SIZE)
}

/// Checks the rows of a response that came from one partition.
///
/// The all-pages entry points return the whole partition. The single page
/// entry points return only the first page, and `has_more_pages` is set.
fn assert_rows_of_partition(entry_point: EntryPoint, partition: usize, fetched: Rows<Row>) {
    let expected = rows_of_partition(partition);
    if entry_point.fetches_all_pages() {
        assert_eq!(fetched.rows, expected, "{}", entry_point.name());
        assert!(!fetched.has_more_pages, "{}", entry_point.name());
    } else {
        assert!(
            !fetched.rows.is_empty() && fetched.rows.len() <= PAGE_SIZE as usize,
            "{} returned {} rows, but the page size is {PAGE_SIZE}",
            entry_point.name(),
            fetched.rows.len()
        );
        assert_eq!(
            fetched.rows,
            expected[..fetched.rows.len()],
            "{}",
            entry_point.name()
        );
        assert!(
            fetched.has_more_pages,
            "{} did not report more pages",
            entry_point.name()
        );
    }
}

/// A partition has more rows than a page. Thus, the response has several
/// pages.
async fn a_partition_is_returned_in_pages(session: &Session) {
    for entry_point in EntryPoint::ALL {
        let fetched = entry_point
            .fetch(session, select("SELECT k, c, v FROM t WHERE k = ?"), ("a",))
            .await
            .unwrap()
            .into_rows::<Row>()
            .await;
        assert_rows_of_partition(entry_point, 0, fetched);
    }
}

/// A scan reads the partitions one after another. The order of the partitions
/// is the order of their tokens. The test does not depend on it.
async fn all_partitions_are_returned_in_pages(session: &Session) {
    for entry_point in EntryPoint::ALL {
        let fetched = entry_point
            .fetch(session, select("SELECT k, c, v FROM t"), ())
            .await
            .unwrap()
            .into_rows::<Row>()
            .await;

        let first_partition = PARTITIONS
            .iter()
            .position(|partition| *partition == fetched.rows[0].0)
            .expect("The first row is from an unknown partition");
        if entry_point.fetches_all_pages() {
            assert!(!fetched.has_more_pages, "{}", entry_point.name());
            let all_rows = PARTITIONS.len() * ROWS_PER_PARTITION as usize;
            assert_eq!(fetched.rows.len(), all_rows, "{}", entry_point.name());
            for (partition, name) in PARTITIONS.iter().enumerate() {
                let rows_in_partition: Vec<Row> = fetched
                    .rows
                    .iter()
                    .filter(|row| row.0 == *name)
                    .cloned()
                    .collect();
                assert_eq!(
                    rows_in_partition,
                    rows_of_partition(partition),
                    "{}",
                    entry_point.name()
                );
            }
        } else {
            // A page is smaller than a partition. Thus, it holds the first
            // rows of the first partition only.
            assert_rows_of_partition(entry_point, first_partition, fetched);
        }
    }
}

async fn no_rows_are_returned_as_no_rows(session: &Session) {
    for entry_point in EntryPoint::ALL {
        let fetched = entry_point
            .fetch(
                session,
                select("SELECT k, c, v FROM t WHERE k = ?"),
                ("absent",),
            )
            .await
            .unwrap()
            .into_rows::<Row>()
            .await;
        assert!(fetched.rows.is_empty(), "{}", entry_point.name());
        assert!(!fetched.has_more_pages, "{}", entry_point.name());
    }
}
