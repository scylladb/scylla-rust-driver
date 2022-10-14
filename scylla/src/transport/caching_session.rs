use crate::batch::{Batch, BatchStatement};
use crate::frame::value::{BatchValues, ValueList};
use crate::prepared_statement::PreparedStatement;
use crate::query::Query;
use crate::transport::errors::QueryError;
use crate::transport::iterator::RowIterator;
use crate::{QueryResult, Session};
use bytes::Bytes;
use dashmap::DashMap;
use futures::future::try_join_all;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;

/// Provides auto caching while executing queries
#[derive(Debug)]
pub struct CachingSession<S = RandomState>
where
    S: Clone + BuildHasher,
{
    pub session: Session,
    /// The prepared statement cache size
    /// If a prepared statement is added while the limit is reached, the oldest prepared statement
    /// is removed from the cache
    pub max_capacity: usize,
    pub cache: DashMap<String, PreparedStatement, S>,
}

impl<S> CachingSession<S>
where
    S: Default + BuildHasher + Clone,
{
    pub fn from(session: Session, cache_size: usize) -> Self {
        Self {
            session,
            max_capacity: cache_size,
            cache: Default::default(),
        }
    }
}

impl<S> CachingSession<S>
where
    S: BuildHasher + Clone,
{
    /// Builds a [`CachingCaching::session`] from a [`Session`] and a cache size,
    /// using a customer hasher.
    pub fn with_hasher(session: Session, cache_size: usize, hasher: S) -> Self {
        Self {
            session,
            max_capacity: cache_size,
            cache: DashMap::with_hasher(hasher),
        }
    }

    /// Does the same thing as [`Session::execute`] but uses the prepared statement cache
    pub async fn execute(
        &self,
        query: impl Into<Query>,
        values: impl ValueList,
    ) -> Result<QueryResult, QueryError> {
        let query = query.into();
        let prepared = self.add_prepared_statement(&query).await?;
        let values = values.serialized()?;
        self.session.execute(&prepared, values.clone()).await
    }

    /// Does the same thing as [`Session::execute_iter`] but uses the prepared statement cache
    pub async fn execute_iter(
        &self,
        query: impl Into<Query>,
        values: impl ValueList,
    ) -> Result<RowIterator, QueryError> {
        let query = query.into();
        let prepared = self.add_prepared_statement(&query).await?;
        let values = values.serialized()?;
        self.session.execute_iter(prepared, values.clone()).await
    }

    /// Does the same thing as [`Session::execute_paged`] but uses the prepared statement cache
    pub async fn execute_paged(
        &self,
        query: impl Into<Query>,
        values: impl ValueList,
        paging_state: Option<Bytes>,
    ) -> Result<QueryResult, QueryError> {
        let query = query.into();
        let prepared = self.add_prepared_statement(&query).await?;
        let values = values.serialized()?;
        self.session
            .execute_paged(&prepared, values.clone(), paging_state.clone())
            .await
    }

    /// Does the same thing as [`Session::batch`] but uses the prepared statement cache\
    /// Prepares batch using CachingSession::prepare_batch if needed and then executes it
    pub async fn batch(
        &self,
        batch: &Batch,
        values: impl BatchValues,
    ) -> Result<QueryResult, QueryError> {
        let all_prepared: bool = batch
            .statements
            .iter()
            .all(|stmt| matches!(stmt, BatchStatement::PreparedStatement(_)));

        if all_prepared {
            self.session.batch(batch, &values).await
        } else {
            let prepared_batch: Batch = self.prepare_batch(batch).await?;

            self.session.batch(&prepared_batch, &values).await
        }
    }

    /// Prepares all statements within the batch and returns a new batch where every
    /// statement is prepared.
    /// Uses the prepared statements cache.
    pub async fn prepare_batch(&self, batch: &Batch) -> Result<Batch, QueryError> {
        let mut prepared_batch = batch.clone();

        try_join_all(
            prepared_batch
                .statements
                .iter_mut()
                .map(|statement| async move {
                    if let BatchStatement::Query(query) = statement {
                        let prepared = self.add_prepared_statement(&*query).await?;
                        *statement = BatchStatement::PreparedStatement(prepared);
                    }
                    Ok::<(), QueryError>(())
                }),
        )
        .await?;

        Ok(prepared_batch)
    }

    /// Adds a prepared statement to the cache
    pub async fn add_prepared_statement(
        &self,
        query: impl Into<&Query>,
    ) -> Result<PreparedStatement, QueryError> {
        let query = query.into();

        if let Some(prepared) = self.cache.get(&query.contents) {
            // Clone, because else the value is mutably borrowed and the execute method gives a compile error
            Ok(prepared.clone())
        } else {
            let prepared = self.session.prepare(query.clone()).await?;

            if self.max_capacity == self.cache.len() {
                // Cache is full, remove the first entry
                // Don't hold a reference into the map (that's why the to_string() is called)
                // This is because the documentation of the remove fn tells us that it may deadlock
                // when holding some sort of reference into the map
                let query = self.cache.iter().next().map(|c| c.key().to_string());

                // Don't inline this: https://stackoverflow.com/questions/69873846/an-owned-value-is-still-references-somehow
                if let Some(q) = query {
                    self.cache.remove(&q);
                }
            }

            self.cache.insert(query.contents.clone(), prepared.clone());

            Ok(prepared)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::test_utils::unique_keyspace_name;
    use crate::{
        batch::{Batch, BatchStatement},
        prepared_statement::PreparedStatement,
        CachingSession, Session, SessionBuilder,
    };
    use futures::TryStreamExt;
    use std::collections::BTreeSet;

    async fn new_for_test() -> Session {
        let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
        let session = SessionBuilder::new()
            .known_node(uri)
            .build()
            .await
            .expect("Could not create session");
        let ks = unique_keyspace_name();

        session
            .query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}}", ks), &[])
            .await
            .expect("Could not create keyspace");

        session
            .query(
                format!(
                    "CREATE TABLE IF NOT EXISTS {}.test_table (a int primary key, b int)",
                    ks
                ),
                &[],
            )
            .await
            .expect("Could not create table");

        session
            .use_keyspace(ks, false)
            .await
            .expect("Could not set keyspace");

        session
    }

    async fn create_caching_session() -> CachingSession {
        let session = CachingSession::from(new_for_test().await, 2);

        // Add a row, this makes it easier to check if the caching works combined with the regular execute fn on Session
        session
            .execute("insert into test_table(a, b) values (1, 2)", &[])
            .await
            .unwrap();

        // Clear the cache because it now contains an insert
        assert_eq!(session.cache.len(), 1);

        session.cache.clear();

        session
    }

    /// Test that when the cache is full and a different query comes in, that query will be added
    /// to the cache and a random query is removed
    #[tokio::test]
    async fn test_full() {
        let session = create_caching_session().await;

        let first_query = "select * from test_table";
        let middle_query = "insert into test_table(a, b) values (?, ?)";
        let last_query = "update test_table set b = ? where a = 1";

        session
            .add_prepared_statement(&first_query.into())
            .await
            .unwrap();
        session
            .add_prepared_statement(&middle_query.into())
            .await
            .unwrap();
        session
            .add_prepared_statement(&last_query.into())
            .await
            .unwrap();

        assert_eq!(2, session.cache.len());

        // This query should be in the cache
        assert!(session.cache.get(last_query).is_some());

        // Either the first or middle query should be removed
        let first_query_removed = session.cache.get(first_query).is_none();
        let middle_query_removed = session.cache.get(middle_query).is_none();

        assert!(first_query_removed || middle_query_removed);
    }

    /// Checks that the same prepared statement is reused when executing the same query twice
    #[tokio::test]
    async fn test_execute_cached() {
        let session = create_caching_session().await;
        let result = session
            .execute("select * from test_table", &[])
            .await
            .unwrap();

        assert_eq!(1, session.cache.len());
        assert_eq!(1, result.rows.unwrap().len());

        let result = session
            .execute("select * from test_table", &[])
            .await
            .unwrap();

        assert_eq!(1, session.cache.len());
        assert_eq!(1, result.rows.unwrap().len());
    }

    /// Checks that caching works with execute_iter
    #[tokio::test]
    async fn test_execute_iter_cached() {
        let session = create_caching_session().await;

        assert!(session.cache.is_empty());

        let iter = session
            .execute_iter("select * from test_table", &[])
            .await
            .unwrap();

        let rows = iter.try_collect::<Vec<_>>().await.unwrap().len();

        assert_eq!(1, rows);
        assert_eq!(1, session.cache.len());
    }

    /// Checks that caching works with execute_paged
    #[tokio::test]
    async fn test_execute_paged_cached() {
        let session = create_caching_session().await;

        assert!(session.cache.is_empty());

        let result = session
            .execute_paged("select * from test_table", &[], None)
            .await
            .unwrap();

        assert_eq!(1, session.cache.len());
        assert_eq!(1, result.rows.unwrap().len());
    }

    async fn assert_test_batch_table_rows_contain(
        sess: &CachingSession,
        expected_rows: &[(i32, i32)],
    ) {
        let selected_rows: BTreeSet<(i32, i32)> = sess
            .execute("SELECT a, b FROM test_batch_table", ())
            .await
            .unwrap()
            .rows_typed::<(i32, i32)>()
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        for expected_row in expected_rows.iter() {
            if !selected_rows.contains(expected_row) {
                panic!(
                    "Expected {:?} to contain row: {:?}, but they didnt",
                    selected_rows, expected_row
                );
            }
        }
    }

    /// This test checks that we can construct a CachingSession with custom HashBuilder implementations
    #[tokio::test]
    async fn test_custom_hasher() {
        let _session: CachingSession<std::collections::hash_map::RandomState> =
            CachingSession::from(new_for_test().await, 2);
        let _session: CachingSession<ahash::RandomState> =
            CachingSession::from(new_for_test().await, 2);
        let _session: CachingSession<ahash::RandomState> =
            CachingSession::with_hasher(new_for_test().await, 2, Default::default());
    }

    #[tokio::test]
    async fn test_batch() {
        let session: CachingSession = create_caching_session().await;

        session
            .execute(
                "CREATE TABLE IF NOT EXISTS test_batch_table (a int, b int, primary key (a, b))",
                (),
            )
            .await
            .unwrap();

        let unprepared_insert_a_b: &str = "insert into test_batch_table (a, b) values (?, ?)";
        let unprepared_insert_a_7: &str = "insert into test_batch_table (a, b) values (?, 7)";
        let unprepared_insert_8_b: &str = "insert into test_batch_table (a, b) values (8, ?)";
        let prepared_insert_a_b: PreparedStatement = session
            .add_prepared_statement(&unprepared_insert_a_b.into())
            .await
            .unwrap();
        let prepared_insert_a_7: PreparedStatement = session
            .add_prepared_statement(&unprepared_insert_a_7.into())
            .await
            .unwrap();
        let prepared_insert_8_b: PreparedStatement = session
            .add_prepared_statement(&unprepared_insert_8_b.into())
            .await
            .unwrap();

        let assert_batch_prepared = |b: &Batch| {
            for stmt in &b.statements {
                match stmt {
                    BatchStatement::PreparedStatement(_) => {}
                    _ => panic!("Unprepared statement in prepared batch!"),
                }
            }
        };

        {
            let mut unprepared_batch: Batch = Default::default();
            unprepared_batch.append_statement(unprepared_insert_a_b);
            unprepared_batch.append_statement(unprepared_insert_a_7);
            unprepared_batch.append_statement(unprepared_insert_8_b);

            session
                .batch(&unprepared_batch, ((10, 20), (10,), (20,)))
                .await
                .unwrap();
            assert_test_batch_table_rows_contain(&session, &[(10, 20), (10, 7), (8, 20)]).await;

            let prepared_batch: Batch = session.prepare_batch(&unprepared_batch).await.unwrap();
            assert_batch_prepared(&prepared_batch);

            session
                .batch(&prepared_batch, ((15, 25), (15,), (25,)))
                .await
                .unwrap();
            assert_test_batch_table_rows_contain(&session, &[(15, 25), (15, 7), (8, 25)]).await;
        }

        {
            let mut partially_prepared_batch: Batch = Default::default();
            partially_prepared_batch.append_statement(unprepared_insert_a_b);
            partially_prepared_batch.append_statement(prepared_insert_a_7.clone());
            partially_prepared_batch.append_statement(unprepared_insert_8_b);

            session
                .batch(&partially_prepared_batch, ((30, 40), (30,), (40,)))
                .await
                .unwrap();
            assert_test_batch_table_rows_contain(&session, &[(30, 40), (30, 7), (8, 40)]).await;

            let prepared_batch: Batch = session
                .prepare_batch(&partially_prepared_batch)
                .await
                .unwrap();
            assert_batch_prepared(&prepared_batch);

            session
                .batch(&prepared_batch, ((35, 45), (35,), (45,)))
                .await
                .unwrap();
            assert_test_batch_table_rows_contain(&session, &[(35, 45), (35, 7), (8, 45)]).await;
        }

        {
            let mut fully_prepared_batch: Batch = Default::default();
            fully_prepared_batch.append_statement(prepared_insert_a_b);
            fully_prepared_batch.append_statement(prepared_insert_a_7);
            fully_prepared_batch.append_statement(prepared_insert_8_b);

            session
                .batch(&fully_prepared_batch, ((50, 60), (50,), (60,)))
                .await
                .unwrap();
            assert_test_batch_table_rows_contain(&session, &[(50, 60), (50, 7), (8, 60)]).await;

            let prepared_batch: Batch = session.prepare_batch(&fully_prepared_batch).await.unwrap();
            assert_batch_prepared(&prepared_batch);

            session
                .batch(&prepared_batch, ((55, 65), (55,), (65,)))
                .await
                .unwrap();

            assert_test_batch_table_rows_contain(&session, &[(55, 65), (55, 7), (8, 65)]).await;
        }

        {
            let mut bad_batch: Batch = Default::default();
            bad_batch.append_statement(unprepared_insert_a_b);
            bad_batch.append_statement("This isnt even CQL");
            bad_batch.append_statement(unprepared_insert_8_b);

            assert!(session.batch(&bad_batch, ((1, 2), (), (2,))).await.is_err());
            assert!(session.prepare_batch(&bad_batch).await.is_err());
        }
    }
}
