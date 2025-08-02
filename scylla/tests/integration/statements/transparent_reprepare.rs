use scylla::{
    client::{caching_session::CachingSession, session::Session},
    statement::batch::Batch,
};

use crate::utils::{
    create_new_session_builder, setup_tracing, unique_keyspace_name, PerformDDL as _,
};

async fn rename(session: &Session, rename_str: &str) {
    session
        .ddl(format!("ALTER TABLE tab RENAME {rename_str}"))
        .await
        .unwrap();
}

async fn rename_caching(session: &CachingSession, rename_str: &str) {
    session
        .ddl(format!("ALTER TABLE tab RENAME {rename_str}"))
        .await
        .unwrap();
}

// A tests which checks that Session::execute automatically reprepares PreparedStatemtns if they become unprepared.
// Doing an ALTER TABLE statement clears prepared statement cache and all prepared statements need
// to be prepared again.
// To verify that this indeed happens you can run:
// RUST_LOG=debug cargo test test_unprepared_reprepare_in_execute -- --nocapture
// And look for this line in the logs:
// Connection::execute: Got DbError::Unprepared - repreparing statement with id ...
#[tokio::test]
async fn test_unprepared_reprepare_in_execute() {
    setup_tracing();

    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session.use_keyspace(&ks, false).await.unwrap();

    session
        .ddl("CREATE TABLE IF NOT EXISTS tab (a int, b int, c int, primary key (a, b, c))")
        .await
        .unwrap();

    let insert_a_b_c = session
        .prepare("INSERT INTO tab (a, b, c) VALUES (?, ?, ?)")
        .await
        .unwrap();

    session
        .execute_unpaged(&insert_a_b_c, (1, 2, 3))
        .await
        .unwrap();

    // Swap names of columns b and c
    rename(&session, "b TO tmp_name").await;

    // During rename the query should fail
    assert!(session
        .execute_unpaged(&insert_a_b_c, (1, 2, 3))
        .await
        .is_err());
    rename(&session, "c TO b").await;
    assert!(session
        .execute_unpaged(&insert_a_b_c, (1, 2, 3))
        .await
        .is_err());
    rename(&session, "tmp_name TO c").await;

    // Insert values again (b and c are swapped so those are different inserts)
    session
        .execute_unpaged(&insert_a_b_c, (1, 2, 3))
        .await
        .unwrap();

    let mut all_rows: Vec<(i32, i32, i32)> = session
        .query_unpaged("SELECT a, b, c FROM tab", ())
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(i32, i32, i32)>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    all_rows.sort_unstable();
    assert_eq!(all_rows, vec![(1, 2, 3), (1, 3, 2)]);

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

// A tests which checks that Session::batch automatically reprepares PreparedStatemtns if they become unprepared.
// Doing an ALTER TABLE statement clears prepared statement cache and all prepared statements need
// to be prepared again.
// To verify that this indeed happens you can run:
// RUST_LOG=debug cargo test test_unprepared_reprepare_in_batch -- --nocapture
// And look for this line in the logs:
// Connection::batch: got DbError::Unprepared - repreparing statement with id ...
#[tokio::test]
async fn test_unprepared_reprepare_in_batch() {
    setup_tracing();

    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session.use_keyspace(&ks, false).await.unwrap();

    session
        .ddl("CREATE TABLE IF NOT EXISTS tab (a int, b int, c int, primary key (a, b, c))")
        .await
        .unwrap();

    let insert_a_b_c = session
        .prepare("INSERT INTO tab (a, b, c) VALUES (?, ?, ?)")
        .await
        .unwrap();
    let insert_a_b_6 = session
        .prepare("INSERT INTO tab (a, b, c) VALUES (?, ?, 6)")
        .await
        .unwrap();

    let mut batch: Batch = Default::default();
    batch.append_statement(insert_a_b_c);
    batch.append_statement(insert_a_b_6);

    session.batch(&batch, ((1, 2, 3), (4, 5))).await.unwrap();

    // Swap names of columns b and c
    rename(&session, "b TO tmp_name").await;

    // During rename the query should fail
    assert!(session.batch(&batch, ((1, 2, 3), (4, 5))).await.is_err());
    rename(&session, "c TO b").await;
    assert!(session.batch(&batch, ((1, 2, 3), (4, 5))).await.is_err());
    rename(&session, "tmp_name TO c").await;

    // Insert values again (b and c are swapped so those are different inserts)
    session.batch(&batch, ((1, 2, 3), (4, 5))).await.unwrap();

    let mut all_rows: Vec<(i32, i32, i32)> = session
        .query_unpaged("SELECT a, b, c FROM tab", ())
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(i32, i32, i32)>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    all_rows.sort_unstable();
    assert_eq!(all_rows, vec![(1, 2, 3), (1, 3, 2), (4, 5, 6), (4, 6, 5)]);

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

// A tests which checks that Session::execute automatically reprepares PreparedStatemtns if they become unprepared.
// Doing an ALTER TABLE statement clears prepared statement cache and all prepared statements need
// to be prepared again.
// To verify that this indeed happens you can run:
// RUST_LOG=debug cargo test test_unprepared_reprepare_in_caching_session_execute -- --nocapture
// And look for this line in the logs:
// Connection::execute: Got DbError::Unprepared - repreparing statement with id ...
#[tokio::test]
async fn test_unprepared_reprepare_in_caching_session_execute() {
    setup_tracing();

    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session.use_keyspace(&ks, false).await.unwrap();

    let caching_session: CachingSession = CachingSession::from(session, 64);

    caching_session
        .ddl("CREATE TABLE IF NOT EXISTS tab (a int, b int, c int, primary key (a, b, c))")
        .await
        .unwrap();

    let insert_a_b_c = "INSERT INTO tab (a, b, c) VALUES (?, ?, ?)";

    caching_session
        .execute_unpaged(insert_a_b_c, &(1, 2, 3))
        .await
        .unwrap();

    // Swap names of columns b and c
    rename_caching(&caching_session, "b TO tmp_name").await;

    // During rename the query should fail
    assert!(caching_session
        .execute_unpaged(insert_a_b_c, &(1, 2, 3))
        .await
        .is_err());
    rename_caching(&caching_session, "c TO b").await;
    assert!(caching_session
        .execute_unpaged(insert_a_b_c, &(1, 2, 3))
        .await
        .is_err());
    rename_caching(&caching_session, "tmp_name TO c").await;

    // Insert values again (b and c are swapped so those are different inserts)
    caching_session
        .execute_unpaged(insert_a_b_c, &(1, 2, 3))
        .await
        .unwrap();

    let mut all_rows: Vec<(i32, i32, i32)> = caching_session
        .execute_unpaged("SELECT a, b, c FROM tab", &())
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(i32, i32, i32)>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    all_rows.sort_unstable();
    assert_eq!(all_rows, vec![(1, 2, 3), (1, 3, 2)]);

    caching_session
        .ddl(format!("DROP KEYSPACE {ks}"))
        .await
        .unwrap();
}
