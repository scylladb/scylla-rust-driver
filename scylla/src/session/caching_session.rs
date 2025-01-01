use crate::batch::{Batch, BatchStatement};
use crate::execution::errors::QueryError;
#[allow(deprecated)]
use crate::execution::pager::LegacyRowIterator;
use crate::prepared_statement::PreparedStatement;
use crate::query::Query;
use crate::routing::partitioner::PartitionerName;
use crate::statement::{PagingState, PagingStateResponse};
#[allow(deprecated)]
use crate::LegacyQueryResult;
use crate::QueryResult;
use bytes::Bytes;
use dashmap::DashMap;
use futures::future::try_join_all;
use scylla_cql::frame::response::result::{PreparedMetadata, ResultMetadata};
use scylla_cql::types::serialize::batch::BatchValues;
use scylla_cql::types::serialize::row::SerializeRow;
use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::BuildHasher;
use std::sync::Arc;

use crate::execution::pager::QueryPager;
#[allow(deprecated)]
use crate::session::{
    CurrentDeserializationApi, DeserializationApiKind, GenericSession, LegacyDeserializationApi,
};

/// Contains just the parts of a prepared statement that were returned
/// from the database. All remaining parts (query string, page size,
/// consistency, etc.) are taken from the Query passed
/// to the `CachingSession::execute` family of methods.
#[derive(Debug)]
struct RawPreparedStatementData {
    id: Bytes,
    is_confirmed_lwt: bool,
    metadata: PreparedMetadata,
    result_metadata: Arc<ResultMetadata<'static>>,
    partitioner_name: PartitionerName,
}

/// Provides auto caching while executing queries
pub struct GenericCachingSession<DeserializationApi, S = RandomState>
where
    S: Clone + BuildHasher,
    DeserializationApi: DeserializationApiKind,
{
    session: GenericSession<DeserializationApi>,
    /// The prepared statement cache size
    /// If a prepared statement is added while the limit is reached, the oldest prepared statement
    /// is removed from the cache
    max_capacity: usize,
    cache: DashMap<String, RawPreparedStatementData, S>,
}

impl<DeserializationApi, S> fmt::Debug for GenericCachingSession<DeserializationApi, S>
where
    S: Clone + BuildHasher,
    DeserializationApi: DeserializationApiKind,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GenericCachingSession")
            .field("session", &self.session)
            .field("max_capacity", &self.max_capacity)
            .field("cache", &self.cache)
            .finish()
    }
}

pub type CachingSession<S = RandomState> = GenericCachingSession<CurrentDeserializationApi, S>;

#[deprecated(
    since = "0.15.0",
    note = "Legacy deserialization API is inefficient and is going to be removed soon"
)]
#[allow(deprecated)]
pub type LegacyCachingSession<S = RandomState> = GenericCachingSession<LegacyDeserializationApi, S>;

impl<DeserApi, S> GenericCachingSession<DeserApi, S>
where
    S: Default + BuildHasher + Clone,
    DeserApi: DeserializationApiKind,
{
    pub fn from(session: GenericSession<DeserApi>, cache_size: usize) -> Self {
        Self {
            session,
            max_capacity: cache_size,
            cache: Default::default(),
        }
    }
}

impl<DeserApi, S> GenericCachingSession<DeserApi, S>
where
    S: BuildHasher + Clone,
    DeserApi: DeserializationApiKind,
{
    /// Builds a [`CachingSession`] from a [`Session`](GenericSession), a cache size,
    /// and a [`BuildHasher`], using a customer hasher.
    pub fn with_hasher(session: GenericSession<DeserApi>, cache_size: usize, hasher: S) -> Self {
        Self {
            session,
            max_capacity: cache_size,
            cache: DashMap::with_hasher(hasher),
        }
    }
}

impl<S> GenericCachingSession<CurrentDeserializationApi, S>
where
    S: BuildHasher + Clone,
{
    /// Does the same thing as [`Session::execute_unpaged`](GenericSession::execute_unpaged)
    /// but uses the prepared statement cache.
    pub async fn execute_unpaged(
        &self,
        query: impl Into<Query>,
        values: impl SerializeRow,
    ) -> Result<QueryResult, QueryError> {
        let query = query.into();
        let prepared = self.add_prepared_statement_owned(query).await?;
        self.session.execute_unpaged(&prepared, values).await
    }

    /// Does the same thing as [`Session::execute_iter`](GenericSession::execute_iter)
    /// but uses the prepared statement cache.
    pub async fn execute_iter(
        &self,
        query: impl Into<Query>,
        values: impl SerializeRow,
    ) -> Result<QueryPager, QueryError> {
        let query = query.into();
        let prepared = self.add_prepared_statement_owned(query).await?;
        self.session.execute_iter(prepared, values).await
    }

    /// Does the same thing as [`Session::execute_single_page`](GenericSession::execute_single_page)
    /// but uses the prepared statement cache.
    pub async fn execute_single_page(
        &self,
        query: impl Into<Query>,
        values: impl SerializeRow,
        paging_state: PagingState,
    ) -> Result<(QueryResult, PagingStateResponse), QueryError> {
        let query = query.into();
        let prepared = self.add_prepared_statement_owned(query).await?;
        self.session
            .execute_single_page(&prepared, values, paging_state)
            .await
    }

    /// Does the same thing as [`Session::batch`](GenericSession::batch) but uses the
    /// prepared statement cache.\
    /// Prepares batch using [`CachingSession::prepare_batch`](GenericCachingSession::prepare_batch)
    /// if needed and then executes it.
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
}

#[deprecated(
    since = "0.15.0",
    note = "Legacy deserialization API is inefficient and is going to be removed soon"
)]
#[allow(deprecated)]
impl<S> GenericCachingSession<LegacyDeserializationApi, S>
where
    S: BuildHasher + Clone,
{
    /// Does the same thing as [`Session::execute_unpaged`](GenericSession::execute_unpaged)
    /// but uses the prepared statement cache.
    pub async fn execute_unpaged(
        &self,
        query: impl Into<Query>,
        values: impl SerializeRow,
    ) -> Result<LegacyQueryResult, QueryError> {
        let query = query.into();
        let prepared = self.add_prepared_statement_owned(query).await?;
        self.session.execute_unpaged(&prepared, values).await
    }

    /// Does the same thing as [`Session::execute_iter`](GenericSession::execute_iter)
    /// but uses the prepared statement cache.
    pub async fn execute_iter(
        &self,
        query: impl Into<Query>,
        values: impl SerializeRow,
    ) -> Result<LegacyRowIterator, QueryError> {
        let query = query.into();
        let prepared = self.add_prepared_statement_owned(query).await?;
        self.session.execute_iter(prepared, values).await
    }

    /// Does the same thing as [`Session::execute_single_page`](GenericSession::execute_single_page)
    /// but uses the prepared statement cache.
    pub async fn execute_single_page(
        &self,
        query: impl Into<Query>,
        values: impl SerializeRow,
        paging_state: PagingState,
    ) -> Result<(LegacyQueryResult, PagingStateResponse), QueryError> {
        let query = query.into();
        let prepared = self.add_prepared_statement_owned(query).await?;
        self.session
            .execute_single_page(&prepared, values, paging_state)
            .await
    }

    /// Does the same thing as [`Session::batch`](GenericSession::batch) but uses
    /// the prepared statement cache.\
    /// Prepares batch using CachingSession::prepare_batch if needed and then executes it.
    pub async fn batch(
        &self,
        batch: &Batch,
        values: impl BatchValues,
    ) -> Result<LegacyQueryResult, QueryError> {
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
}

impl<DeserApi, S> GenericCachingSession<DeserApi, S>
where
    S: BuildHasher + Clone,
    DeserApi: DeserializationApiKind,
{
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
        self.add_prepared_statement_owned(query.into().clone())
            .await
    }

    async fn add_prepared_statement_owned(
        &self,
        query: impl Into<Query>,
    ) -> Result<PreparedStatement, QueryError> {
        let query = query.into();

        if let Some(raw) = self.cache.get(&query.contents) {
            let page_size = query.get_validated_page_size();
            let mut stmt = PreparedStatement::new(
                raw.id.clone(),
                raw.is_confirmed_lwt,
                raw.metadata.clone(),
                raw.result_metadata.clone(),
                query.contents,
                page_size,
                query.config,
            );
            stmt.set_partitioner_name(raw.partitioner_name.clone());
            Ok(stmt)
        } else {
            let query_contents = query.contents.clone();
            let prepared = self.session.prepare(query).await?;

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

            let raw = RawPreparedStatementData {
                id: prepared.get_id().clone(),
                is_confirmed_lwt: prepared.is_confirmed_lwt(),
                metadata: prepared.get_prepared_metadata().clone(),
                result_metadata: prepared.get_result_metadata().clone(),
                partitioner_name: prepared.get_partitioner_name().clone(),
            };
            self.cache.insert(query_contents, raw);

            Ok(prepared)
        }
    }

    pub fn get_max_capacity(&self) -> usize {
        self.max_capacity
    }

    pub fn get_session(&self) -> &GenericSession<DeserApi> {
        &self.session
    }
}

#[cfg(test)]
mod tests {
    use crate::query::Query;
    use crate::routing::partitioner::PartitionerName;
    use crate::session::Session;
    use crate::statement::PagingState;
    use crate::test_utils::{
        create_new_session_builder, scylla_supports_tablets, setup_tracing, PerformDDL,
    };
    use crate::utils::test_utils::unique_keyspace_name;
    #[allow(deprecated)]
    use crate::LegacyCachingSession;
    use crate::{
        batch::{Batch, BatchStatement},
        prepared_statement::PreparedStatement,
        CachingSession,
    };
    use futures::TryStreamExt;
    use scylla_cql::frame::response::result::Row;
    use std::collections::BTreeSet;

    async fn new_for_test(with_tablet_support: bool) -> Session {
        let session = create_new_session_builder()
            .build()
            .await
            .expect("Could not create session");
        let ks = unique_keyspace_name();

        let mut create_ks = format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks}
        WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"
        );
        if !with_tablet_support && scylla_supports_tablets(&session).await {
            create_ks += " AND TABLETS = {'enabled': false}";
        }

        session
            .ddl(create_ks)
            .await
            .expect("Could not create keyspace");

        session
            .ddl(format!(
                "CREATE TABLE IF NOT EXISTS {}.test_table (a int primary key, b int)",
                ks
            ))
            .await
            .expect("Could not create table");

        session
            .use_keyspace(ks, false)
            .await
            .expect("Could not set keyspace");

        session
    }

    async fn create_caching_session() -> CachingSession {
        let session = CachingSession::from(new_for_test(true).await, 2);

        // Add a row, this makes it easier to check if the caching works combined with the regular execute fn on Session
        session
            .execute_unpaged("insert into test_table(a, b) values (1, 2)", &[])
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
        setup_tracing();
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
    async fn test_execute_unpaged_cached() {
        setup_tracing();
        let session = create_caching_session().await;
        let result = session
            .execute_unpaged("select * from test_table", &[])
            .await
            .unwrap();
        let result_rows = result.into_rows_result().unwrap();

        assert_eq!(1, session.cache.len());
        assert_eq!(1, result_rows.rows_num());

        let result = session
            .execute_unpaged("select * from test_table", &[])
            .await
            .unwrap();

        let result_rows = result.into_rows_result().unwrap();

        assert_eq!(1, session.cache.len());
        assert_eq!(1, result_rows.rows_num());
    }

    /// Checks that caching works with execute_iter
    #[tokio::test]
    async fn test_execute_iter_cached() {
        setup_tracing();
        let session = create_caching_session().await;

        assert!(session.cache.is_empty());

        let iter = session
            .execute_iter("select * from test_table", &[])
            .await
            .unwrap()
            .rows_stream::<Row>()
            .unwrap()
            .into_stream();

        let rows = iter
            .into_stream()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len();

        assert_eq!(1, rows);
        assert_eq!(1, session.cache.len());
    }

    /// Checks that caching works with execute_single_page
    #[tokio::test]
    async fn test_execute_single_page_cached() {
        setup_tracing();
        let session = create_caching_session().await;

        assert!(session.cache.is_empty());

        let (result, _paging_state) = session
            .execute_single_page("select * from test_table", &[], PagingState::start())
            .await
            .unwrap();

        assert_eq!(1, session.cache.len());
        assert_eq!(1, result.into_rows_result().unwrap().rows_num());
    }

    async fn assert_test_batch_table_rows_contain(
        sess: &CachingSession,
        expected_rows: &[(i32, i32)],
    ) {
        let selected_rows: BTreeSet<(i32, i32)> = sess
            .execute_unpaged("SELECT a, b FROM test_batch_table", ())
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .rows::<(i32, i32)>()
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        for expected_row in expected_rows.iter() {
            if !selected_rows.contains(expected_row) {
                panic!(
                    "Expected {:?} to contain row: {:?}, but they didn't",
                    selected_rows, expected_row
                );
            }
        }
    }

    /// This test checks that we can construct a CachingSession with custom HashBuilder implementations
    #[tokio::test]
    async fn test_custom_hasher() {
        setup_tracing();
        #[derive(Default, Clone)]
        struct CustomBuildHasher;
        impl std::hash::BuildHasher for CustomBuildHasher {
            type Hasher = CustomHasher;
            fn build_hasher(&self) -> Self::Hasher {
                CustomHasher(0)
            }
        }

        struct CustomHasher(u8);
        impl std::hash::Hasher for CustomHasher {
            fn write(&mut self, bytes: &[u8]) {
                for b in bytes {
                    self.0 ^= *b;
                }
            }
            fn finish(&self) -> u64 {
                self.0 as u64
            }
        }

        let _session: CachingSession<std::collections::hash_map::RandomState> =
            CachingSession::from(new_for_test(true).await, 2);
        let _session: CachingSession<CustomBuildHasher> =
            CachingSession::from(new_for_test(true).await, 2);
        let _session: CachingSession<CustomBuildHasher> =
            CachingSession::with_hasher(new_for_test(true).await, 2, Default::default());
    }

    #[tokio::test]
    async fn test_batch() {
        setup_tracing();
        let session: CachingSession = create_caching_session().await;

        session
            .ddl("CREATE TABLE IF NOT EXISTS test_batch_table (a int, b int, primary key (a, b))")
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

    // The CachingSession::execute and friends should have the same StatementConfig
    // and the page size as the Query provided as a parameter. It must not cache
    // those parameters internally.
    // Reproduces #597
    #[tokio::test]
    async fn test_parameters_caching() {
        setup_tracing();
        let session: CachingSession = CachingSession::from(new_for_test(true).await, 100);

        session
            .ddl("CREATE TABLE tbl (a int PRIMARY KEY, b int)")
            .await
            .unwrap();

        let q = Query::new("INSERT INTO tbl (a, b) VALUES (?, ?)");

        // Insert one row with timestamp 1000
        let mut q1 = q.clone();
        q1.set_timestamp(Some(1000));

        session
            .execute_unpaged(q1, (1, 1))
            .await
            .unwrap()
            .result_not_rows()
            .unwrap();

        // Insert another row with timestamp 2000
        let mut q2 = q.clone();
        q2.set_timestamp(Some(2000));

        session
            .execute_unpaged(q2, (2, 2))
            .await
            .unwrap()
            .result_not_rows()
            .unwrap();

        // Fetch both rows with their timestamps
        let mut rows = session
            .execute_unpaged("SELECT b, WRITETIME(b) FROM tbl", ())
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .rows::<(i32, i64)>()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        rows.sort_unstable();
        assert_eq!(rows, vec![(1, 1000), (2, 2000)]);
    }

    // Checks whether the PartitionerName is cached properly.
    #[tokio::test]
    async fn test_partitioner_name_caching() {
        setup_tracing();
        if option_env!("CDC") == Some("disabled") {
            return;
        }

        // This test uses CDC which is not yet compatible with Scylla's tablets.
        let session: CachingSession = CachingSession::from(new_for_test(false).await, 100);

        session
            .ddl("CREATE TABLE tbl (a int PRIMARY KEY) with cdc = {'enabled': true}")
            .await
            .unwrap();

        session
            .get_session()
            .await_schema_agreement()
            .await
            .unwrap();

        // This creates a query with default partitioner name (murmur hash),
        // but after adding the statement it should be changed to the cdc
        // partitioner. It should happen when the query is prepared
        // and after it is fetched from the cache.
        let verify_partitioner = || async {
            let query = Query::new("SELECT * FROM tbl_scylla_cdc_log WHERE \"cdc$stream_id\" = ?");
            let prepared = session.add_prepared_statement(&query).await.unwrap();
            assert_eq!(prepared.get_partitioner_name(), &PartitionerName::CDC);
        };

        // Using a closure here instead of a loop so that, when the test fails,
        // one can see which case failed by looking at the full backtrace
        verify_partitioner().await;
        verify_partitioner().await;
    }

    // NOTE: intentionally no `#[test]`: this is a compile-time test
    fn _caching_session_impls_debug() {
        fn assert_debug<T: std::fmt::Debug>() {}
        assert_debug::<CachingSession>();
        #[allow(deprecated)]
        assert_debug::<LegacyCachingSession>();
    }
}
