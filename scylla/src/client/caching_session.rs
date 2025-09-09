//! Provides a convenient wrapper over the [`Session`] that caches
//! prepared statements automatically and reuses them when possible.

use crate::errors::{ExecutionError, PagerExecutionError, PrepareError};
use crate::response::query_result::QueryResult;
use crate::response::{PagingState, PagingStateResponse};
use crate::routing::partitioner::PartitionerName;
use crate::statement::batch::{Batch, BatchStatement};
use crate::statement::prepared::PreparedStatement;
use crate::statement::unprepared::Statement;
use bytes::Bytes;
use dashmap::DashMap;
use futures::future::try_join_all;
use scylla_cql::frame::response::result::{PreparedMetadata, ResultMetadata};
use scylla_cql::serialize::batch::BatchValues;
use scylla_cql::serialize::row::SerializeRow;
use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::BuildHasher;
use std::sync::Arc;

use crate::client::pager::QueryPager;
use crate::client::session::Session;

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
pub struct CachingSession<S = RandomState>
where
    S: Clone + BuildHasher,
{
    session: Arc<Session>,
    /// The prepared statement cache size
    /// If a prepared statement is added while the limit is reached, the oldest prepared statement
    /// is removed from the cache
    max_capacity: usize,
    cache: DashMap<String, RawPreparedStatementData, S>,
    use_cached_metadata: bool,
}

impl<S> fmt::Debug for CachingSession<S>
where
    S: Clone + BuildHasher,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GenericCachingSession")
            .field("session", &self.session)
            .field("max_capacity", &self.max_capacity)
            .field("cache", &self.cache)
            .finish()
    }
}

impl<S> CachingSession<S>
where
    S: Default + BuildHasher + Clone,
{
    /// Builds a [`CachingSession`] from a [`Session`] and a cache size.
    pub fn from(session: Session, cache_size: usize) -> Self {
        Self {
            session: Arc::new(session),
            max_capacity: cache_size,
            cache: Default::default(),
            use_cached_metadata: false,
        }
    }
}

impl<S> CachingSession<S>
where
    S: BuildHasher + Clone,
{
    /// Builds a [`CachingSession`] from a [`Session`], a cache size,
    /// and a [`BuildHasher`], using a customer hasher.
    pub fn with_hasher(session: Session, cache_size: usize, hasher: S) -> Self {
        Self {
            session: Arc::new(session),
            max_capacity: cache_size,
            cache: DashMap::with_hasher(hasher),
            use_cached_metadata: false,
        }
    }
}

impl<S> CachingSession<S>
where
    S: BuildHasher + Clone,
{
    /// Does the same thing as [`Session::execute_unpaged`]
    /// but uses the prepared statement cache.
    pub async fn execute_unpaged(
        &self,
        query: impl Into<Statement>,
        values: impl SerializeRow,
    ) -> Result<QueryResult, ExecutionError> {
        let query = query.into();
        let prepared = self.add_prepared_statement_owned(query).await?;
        self.session.execute_unpaged(&prepared, values).await
    }

    /// Does the same thing as [`Session::execute_iter`]
    /// but uses the prepared statement cache.
    pub async fn execute_iter(
        &self,
        query: impl Into<Statement>,
        values: impl SerializeRow,
    ) -> Result<QueryPager, PagerExecutionError> {
        let query = query.into();
        let prepared = self.add_prepared_statement_owned(query).await?;
        self.session.execute_iter(prepared, values).await
    }

    /// Does the same thing as [`Session::execute_single_page`]
    /// but uses the prepared statement cache.
    pub async fn execute_single_page(
        &self,
        query: impl Into<Statement>,
        values: impl SerializeRow,
        paging_state: PagingState,
    ) -> Result<(QueryResult, PagingStateResponse), ExecutionError> {
        let query = query.into();
        let prepared = self.add_prepared_statement_owned(query).await?;
        self.session
            .execute_single_page(&prepared, values, paging_state)
            .await
    }

    /// Does the same thing as [`Session::batch`] but uses the
    /// prepared statement cache.\
    /// Prepares batch using [`CachingSession::prepare_batch`]
    /// if needed and then executes it.
    pub async fn batch(
        &self,
        batch: &Batch,
        values: impl BatchValues,
    ) -> Result<QueryResult, ExecutionError> {
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

impl<S> CachingSession<S>
where
    S: BuildHasher + Clone,
{
    /// Prepares all statements within the batch and returns a new batch where every
    /// statement is prepared.
    /// Uses the prepared statements cache.
    pub async fn prepare_batch(&self, batch: &Batch) -> Result<Batch, ExecutionError> {
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
                    Ok::<(), ExecutionError>(())
                }),
        )
        .await?;

        Ok(prepared_batch)
    }

    /// Adds a prepared statement to the cache
    pub async fn add_prepared_statement(
        &self,
        query: impl Into<&Statement>,
    ) -> Result<PreparedStatement, PrepareError> {
        self.add_prepared_statement_owned(query.into().clone())
            .await
    }

    async fn add_prepared_statement_owned(
        &self,
        query: impl Into<Statement>,
    ) -> Result<PreparedStatement, PrepareError> {
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
            stmt.set_use_cached_result_metadata(self.use_cached_metadata);
            Ok(stmt)
        } else {
            let query_contents = query.contents.clone();
            let prepared = {
                let mut stmt = self.session.prepare(query).await?;
                stmt.set_use_cached_result_metadata(self.use_cached_metadata);
                stmt
            };

            // This loop was added to prevent a race condition (+ memory leak).
            // When 2 threads enter this because cache is full, they may remove the same element,
            // but add different ones. Then we get cache overflow.
            // If we don't have a loop here, then this overflow would never disappear during typical
            // operation of caching session.
            // The loop has downsides: it could evict more entries than strictly necessary, or starve
            // some thread for a bit. If this becomes a problem then maybe we should research how
            // some more robust caching crates are implemented?
            while self.max_capacity <= self.cache.len() {
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

    /// Retrieves the maximum capacity of the prepared statements cache.
    pub fn get_max_capacity(&self) -> usize {
        self.max_capacity
    }

    /// Retrieves the underlying [Session] instance.
    pub fn get_session(&self) -> &Session {
        &self.session
    }
}

/// The default cache capacity set on the [CachingSessionBuilder].
/// Can be changed using [CachingSessionBuilder::max_capacity].
pub const DEFAULT_MAX_CAPACITY: usize = 128;

/// [CachingSessionBuilder] is used to create new [CachingSession] instances.
///
/// **NOTE:** The builder specifies a default capacity of the prepared statement cache
/// that may be too low for use cases running lots of different prepared statements.
/// If you expect to run a large number of different prepared statements (more than
/// [DEFAULT_MAX_CAPACITY]), consider increasing the capacity with
/// [CachingSessionBuilder::max_capacity].
///
/// # Example
///
/// ```
/// # use scylla::client::session::Session;
/// # use scylla::client::session_builder::SessionBuilder;
/// # use scylla::client::caching_session::{CachingSession, CachingSessionBuilder};
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let session: Session = SessionBuilder::new()
///     .known_node("127.0.0.1:9042")
///     .build()
///     .await?;
/// let caching_session: CachingSession = CachingSessionBuilder::new(session)
///     .max_capacity(2137)
///     .build();
/// # Ok(())
/// # }
/// ```
pub struct CachingSessionBuilder<S = RandomState>
where
    S: Clone + BuildHasher,
{
    session: Arc<Session>,
    max_capacity: usize,
    hasher: S,
    use_cached_metadata: bool,
}

impl CachingSessionBuilder<RandomState> {
    /// Wraps a [Session] and creates a new [CachingSessionBuilder] instance,
    /// which can be used to create a new [CachingSession].
    pub fn new(session: Session) -> Self {
        Self::new_shared(Arc::new(session))
    }

    /// Wraps an Arc<[Session]> and creates a new [CachingSessionBuilder] instance,
    /// which can be used to create a new [CachingSession].
    pub fn new_shared(session: Arc<Session>) -> Self {
        Self {
            session,
            max_capacity: DEFAULT_MAX_CAPACITY,
            hasher: RandomState::default(),
            use_cached_metadata: false,
        }
    }
}

impl<S> CachingSessionBuilder<S>
where
    S: Clone + BuildHasher,
{
    /// Configures maximum capacity of the prepared statements cache.
    pub fn max_capacity(mut self, max_capacity: usize) -> Self {
        self.max_capacity = max_capacity;
        self
    }

    /// Make use of cached metadata to decode results
    /// of the statement's execution.
    ///
    /// If true, the driver will request the server not to
    /// attach the result metadata in response to the statement execution.
    ///
    /// The driver will cache the result metadata received from the server
    /// after statement preparation and will use it
    /// to deserialize the results of statement execution.
    ///
    /// See documentation of [`PreparedStatement`] for more details on limitations
    /// of this functionality.
    ///
    /// This option is false by default.
    pub fn use_cached_result_metadata(mut self, use_cached_metadata: bool) -> Self {
        self.use_cached_metadata = use_cached_metadata;
        self
    }

    /// Finishes configuration of [CachingSession].
    pub fn build(self) -> CachingSession<S> {
        CachingSession {
            session: self.session,
            max_capacity: self.max_capacity,
            cache: DashMap::with_hasher(self.hasher),
            use_cached_metadata: self.use_cached_metadata,
        }
    }
}

impl<S> CachingSessionBuilder<S>
where
    S: Clone + BuildHasher,
{
    /// Provides a custom hasher for the prepared statement cache.
    ///
    /// # Example
    ///
    /// ```
    /// # use scylla::client::session::Session;
    /// # use scylla::client::session_builder::SessionBuilder;
    /// # use scylla::client::caching_session::{CachingSession, CachingSessionBuilder};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// #[derive(Default, Clone)]
    /// struct CustomBuildHasher;
    /// impl std::hash::BuildHasher for CustomBuildHasher {
    ///     // Custom hasher implementation goes here
    /// #    type Hasher = CustomHasher;
    /// #    fn build_hasher(&self) -> Self::Hasher {
    /// #        CustomHasher(0)
    /// #    }
    /// }
    ///
    /// # struct CustomHasher(u8);
    /// # impl std::hash::Hasher for CustomHasher {
    /// #     fn write(&mut self, bytes: &[u8]) {
    /// #         for b in bytes {
    /// #             self.0 ^= *b;
    /// #         }
    /// #     }
    /// #     fn finish(&self) -> u64 {
    /// #         self.0 as u64
    /// #     }
    /// # }
    ///
    /// let session: Session = SessionBuilder::new()
    ///     .known_node("127.0.0.1:9042")
    ///     .build()
    ///     .await?;
    /// let caching_session: CachingSession<CustomBuildHasher> = CachingSessionBuilder::new(session)
    ///     .hasher(CustomBuildHasher::default())
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn hasher<S2: Clone + BuildHasher>(self, hasher: S2) -> CachingSessionBuilder<S2> {
        let Self {
            session,
            max_capacity,
            hasher: _,
            use_cached_metadata,
        } = self;
        CachingSessionBuilder {
            session,
            max_capacity,
            hasher,
            use_cached_metadata,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::client::caching_session::{CachingSessionBuilder, DEFAULT_MAX_CAPACITY};
    use crate::client::session::Session;
    use crate::client::session_builder::SessionBuilder;
    use crate::response::PagingState;
    use crate::routing::partitioner::PartitionerName;
    use crate::statement::batch::{Batch, BatchStatement};
    use crate::statement::prepared::PreparedStatement;
    use crate::statement::unprepared::Statement;
    use crate::test_utils::{
        PerformDDL, create_new_session_builder, scylla_supports_tablets, setup_tracing,
    };
    use crate::utils::test_utils::unique_keyspace_name;
    use crate::value::Row;
    use futures::TryStreamExt;
    use scylla_proxy::{
        Condition, Proxy, Reaction as _, RequestFrame, RequestOpcode, RequestReaction, RequestRule,
        ResponseFrame,
    };
    use std::collections::{BTreeSet, HashMap};
    use std::hash::{BuildHasher, RandomState};
    use std::net::SocketAddr;
    use std::sync::Arc;

    use super::CachingSession;

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
                "CREATE TABLE IF NOT EXISTS {ks}.test_table (a int primary key, b int)"
            ))
            .await
            .expect("Could not create table");

        session
            .use_keyspace(ks, false)
            .await
            .expect("Could not set keyspace");

        session
    }

    async fn teardown_keyspace(session: &Session) {
        let ks = session.get_keyspace().unwrap();
        session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
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

        teardown_keyspace(session.get_session()).await;
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

        teardown_keyspace(session.get_session()).await;
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

        teardown_keyspace(session.get_session()).await;
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

        teardown_keyspace(session.get_session()).await;
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
                    "Expected {selected_rows:?} to contain row: {expected_row:?}, but they didn't"
                );
            }
        }
    }

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

    /// This test checks that we can construct a CachingSession with custom HashBuilder implementations
    #[tokio::test]
    async fn test_custom_hasher() {
        setup_tracing();

        let session: CachingSession<std::collections::hash_map::RandomState> =
            CachingSession::from(new_for_test(true).await, 2);
        teardown_keyspace(session.get_session()).await;
        let session: CachingSession<CustomBuildHasher> =
            CachingSession::from(new_for_test(true).await, 2);
        teardown_keyspace(session.get_session()).await;
        let session: CachingSession<CustomBuildHasher> =
            CachingSession::with_hasher(new_for_test(true).await, 2, Default::default());
        teardown_keyspace(session.get_session()).await;
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

        teardown_keyspace(session.get_session()).await;
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

        let q = Statement::new("INSERT INTO tbl (a, b) VALUES (?, ?)");

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

        teardown_keyspace(session.get_session()).await;
    }

    // Checks whether the PartitionerName is cached properly.
    #[tokio::test]
    #[cfg_attr(cassandra_tests, ignore)]
    async fn test_partitioner_name_caching() {
        setup_tracing();

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
            let query =
                Statement::new("SELECT * FROM tbl_scylla_cdc_log WHERE \"cdc$stream_id\" = ?");
            let prepared = session.add_prepared_statement(&query).await.unwrap();
            assert_eq!(prepared.get_partitioner_name(), &PartitionerName::CDC);
        };

        // Using a closure here instead of a loop so that, when the test fails,
        // one can see which case failed by looking at the full backtrace
        verify_partitioner().await;
        verify_partitioner().await;

        teardown_keyspace(session.get_session()).await;
    }

    // NOTE: intentionally no `#[test]`: this is a compile-time test
    fn _caching_session_impls_debug() {
        fn assert_debug<T: std::fmt::Debug>() {}
        assert_debug::<CachingSession>();
    }

    fn assert_hashers_equal(h1: &impl BuildHasher, h2: &impl BuildHasher) {
        const TO_BE_HASHED: &[u8] = "Rzułty".as_bytes();
        assert_eq!(h1.hash_one(TO_BE_HASHED), h2.hash_one(TO_BE_HASHED));
    }

    /// Tests that [CachingSessionBuilder] passes its config options to the built [CachingSession].
    #[tokio::test]
    async fn test_builder() {
        setup_tracing();

        let proxy_addr = SocketAddr::new(scylla_proxy::get_exclusive_local_address(), 9042);

        // A proxy that allows finishing creation of a Session.
        // It performs the whole handshake on all connections, but responds to all
        // QUERY, PREPARE and EXECUTE requests with an error.
        let proxy_rules = vec![
            // OPTIONS -> SUPPORTED rule
            RequestRule(
                Condition::RequestOpcode(RequestOpcode::Options),
                RequestReaction::forge_response(Arc::new(move |frame: RequestFrame| {
                    ResponseFrame::forged_supported(frame.params, &HashMap::default()).unwrap()
                })),
            ),
            // STARTUP -> READY rule
            // REGISTER -> READY rule
            RequestRule(
                Condition::or(
                    Condition::RequestOpcode(RequestOpcode::Startup),
                    Condition::RequestOpcode(RequestOpcode::Register),
                ),
                RequestReaction::forge_response(Arc::new(move |frame: RequestFrame| {
                    ResponseFrame::forged_ready(frame.params)
                })),
            ),
            // QUERY, PREPARE, EXECUTE -> ERROR rule
            RequestRule(
                Condition::any([
                    Condition::RequestOpcode(RequestOpcode::Query),
                    Condition::RequestOpcode(RequestOpcode::Prepare),
                    Condition::RequestOpcode(RequestOpcode::Execute),
                ]),
                RequestReaction::forge().server_error(),
            ),
        ];

        let proxy = Proxy::builder()
            .with_node(
                scylla_proxy::Node::builder()
                    .proxy_address(proxy_addr)
                    .request_rules(proxy_rules)
                    .build_dry_mode(),
            )
            .build()
            .run()
            .await
            .unwrap();

        let create_session = || async {
            SessionBuilder::new()
                .known_node_addr(proxy_addr)
                .build()
                .await
                .unwrap()
        };

        // Default hasher and max_capacity.
        {
            const MAX_CAPACITY: usize = 42;
            let session = create_session().await;
            let mut builder = CachingSessionBuilder::new(session);
            builder = builder.max_capacity(MAX_CAPACITY);
            let caching_session: CachingSession = builder.build();

            assert_eq!(caching_session.max_capacity, MAX_CAPACITY);
            // We cannot compare hashers, because we have no access to the default-constructed RandomState.
            // Each RandomState::new() seeds it with another thread-local seed, so this is not feasible.
        }

        // Default hasher type with custom construction of it.
        {
            let session = create_session().await;
            let hasher = RandomState::new();
            let caching_session = CachingSessionBuilder::new(session)
                .hasher(hasher.clone())
                .build();

            assert_eq!(caching_session.max_capacity, DEFAULT_MAX_CAPACITY);
            assert_hashers_equal(caching_session.cache.hasher(), &hasher);
        }

        // Custom hasher.
        {
            let session = create_session().await;
            let caching_session = CachingSessionBuilder::new(session)
                .hasher(CustomBuildHasher)
                .build();

            assert_eq!(caching_session.max_capacity, DEFAULT_MAX_CAPACITY);
            assert_hashers_equal(caching_session.cache.hasher(), &CustomBuildHasher);
        }

        let _ = proxy.finish().await;
    }
}
