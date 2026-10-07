//! Provides a convenient wrapper over the [`Session`] that caches
//! prepared statements automatically and reuses them when possible.

use crate::errors::{ExecutionError, PagerExecutionError, PrepareError};
use crate::response::query_result::QueryResult;
use crate::response::{PagingState, PagingStateResponse};
use crate::serialize::batch::BatchValues;
use crate::serialize::row::SerializeRow;
use crate::statement::batch::{Batch, BatchStatement};
use crate::statement::prepared::{PreparedStatement, UnconfiguredPreparedStatement};
use crate::statement::unprepared::Statement;
use dashmap::DashMap;
use futures::future::try_join_all;
use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::BuildHasher;
use std::sync::Arc;

use crate::client::pager::QueryPager;
use crate::client::session::Session;

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
    cache: DashMap<String, UnconfiguredPreparedStatement, S>,
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
    ///
    /// # Panics
    ///
    /// Panics if `cache_size` is 0.
    pub fn from(session: Session, cache_size: usize) -> Self {
        assert!(
            cache_size > 0,
            "prepared statement cache capacity must be greater than 0"
        );
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
    ///
    /// # Panics
    ///
    /// Panics if `cache_size` is 0.
    pub fn with_hasher(session: Session, cache_size: usize, hasher: S) -> Self {
        assert!(
            cache_size > 0,
            "prepared statement cache capacity must be greater than 0"
        );
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
    /// prepared statement cache.
    ///
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
            let mut stmt = raw.make_configured_handle(query.config, page_size);
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

            let raw = prepared.make_unconfigured_handle();
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
    ///
    /// # Panics
    ///
    /// Panics if the configured maximum capacity is 0.
    pub fn max_capacity(mut self, max_capacity: usize) -> Self {
        assert!(
            max_capacity > 0,
            "prepared statement cache capacity must be greater than 0"
        );
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
    use crate::frame::protocol_features::ProtocolFeatures;
    use crate::frame::request::RequestV2;
    use crate::frame::request::execute::ExecuteV2;
    use crate::frame::response::result::{ColumnSpec, ColumnType, NativeType, TableSpec};
    use crate::response::PagingState;
    use crate::routing::partitioner::PartitionerName;
    use crate::statement::unprepared::Statement;
    use crate::statement::{Consistency, SerialConsistency};
    use crate::test_utils::{
        PerformDDL, create_new_session_builder, disable_tablets_unless_supported,
        dry_mode_handshake_rules, setup_tracing,
    };
    use crate::utils::test_utils::unique_keyspace_name;
    use crate::value::Row;
    use futures::TryStreamExt;
    use scylla_proxy::{
        Condition, Proxy, Reaction as _, RequestFrame, RequestOpcode, RequestReaction, RequestRule,
        ResponseFrame, RunningProxy,
    };
    use std::hash::{BuildHasher, RandomState};
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    use super::CachingSession;

    /// Creates a session with a fresh keyspace.
    ///
    /// If `required_tablet_feature` is `Some`, tablets are disabled in that keyspace
    /// unless the cluster supports the given feature - see
    /// [`disable_tablets_unless_supported`].
    async fn new_for_test(required_tablet_feature: Option<&str>) -> Session {
        let session = create_new_session_builder()
            .build()
            .await
            .expect("Could not create session");
        let ks = unique_keyspace_name();

        let mut create_ks = format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks}
        WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}"
        );
        if let Some(feature) = required_tablet_feature {
            create_ks += disable_tablets_unless_supported(&session, feature).await;
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

    /// Test that when the cache is full and a different query comes in, that query will be added
    /// to the cache and a random query is removed
    #[tokio::test]
    async fn test_full() {
        setup_tracing();
        let (proxy_addr, proxy, _prepares) = make_test_table_proxy().await;
        let session: CachingSession = CachingSession::from(connect(proxy_addr).await, 2);

        let first_query = "SELECT * FROM test_table";
        let middle_query = "INSERT INTO test_table(a, b) VALUES (?, ?)";
        let last_query = "UPDATE test_table SET b = ? WHERE a = 1";

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

        let _ = proxy.finish().await;
    }

    /// Checks that executing a statement twice prepares it only once,
    /// with every method that uses the cache.
    #[tokio::test]
    async fn test_execute_cached() {
        setup_tracing();
        let (proxy_addr, proxy, mut prepares) = make_test_table_proxy().await;
        let session: CachingSession = CachingSession::from(connect(proxy_addr).await, 2);

        let mut assert_prepared_once = || {
            prepares.try_recv().unwrap();
            prepares.try_recv().unwrap_err();
            assert_eq!(1, session.cache.len());
            session.cache.clear();
        };

        for _ in 0..2 {
            let result = session
                .execute_unpaged(SELECT_FROM_TEST_TABLE, &[])
                .await
                .unwrap();
            assert_eq!(1, result.into_rows_result().unwrap().rows_num());
        }
        assert_prepared_once();

        for _ in 0..2 {
            let rows = session
                .execute_iter(SELECT_FROM_TEST_TABLE, &[])
                .await
                .unwrap()
                .rows_stream::<Row>()
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(1, rows.len());
        }
        assert_prepared_once();

        for _ in 0..2 {
            let (result, _paging_state) = session
                .execute_single_page(SELECT_FROM_TEST_TABLE, &[], PagingState::start())
                .await
                .unwrap();
            assert_eq!(1, result.into_rows_result().unwrap().rows_num());
        }
        assert_prepared_once();

        let _ = proxy.finish().await;
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
        let (proxy_addr, proxy) = make_minimal_proxy().await;

        let _: CachingSession<std::collections::hash_map::RandomState> =
            CachingSession::from(connect(proxy_addr).await, 2);
        let _: CachingSession<CustomBuildHasher> =
            CachingSession::from(connect(proxy_addr).await, 2);
        let _: CachingSession<CustomBuildHasher> =
            CachingSession::with_hasher(connect(proxy_addr).await, 2, Default::default());

        let _ = proxy.finish().await;
    }

    // The CachingSession::execute and friends should have the same StatementConfig
    // and the page size as the Query provided as a parameter. It must not cache
    // those parameters internally.
    // Reproduces #597
    #[tokio::test]
    async fn test_parameters_caching() {
        setup_tracing();

        fn col_spec(name: &'static str) -> ColumnSpec<'static> {
            ColumnSpec::borrowed(
                name,
                ColumnType::Native(NativeType::Int),
                TableSpec::borrowed("ks", "test_table"),
            )
        }

        let (prepare_tx, mut prepares) = mpsc::unbounded_channel();
        let (execute_tx, mut executes) = mpsc::unbounded_channel();
        let (proxy_addr, proxy) = make_proxy(vec![
            RequestRule(
                Condition::RequestOpcode(RequestOpcode::Prepare).and(
                    Condition::BodyContainsCaseSensitive(b"test_table"[..].into()),
                ),
                RequestReaction::forge_response(Arc::new(|frame: RequestFrame| {
                    ResponseFrame::forged_prepared(
                        frame.params,
                        b"select_b",
                        &[col_spec("a")],
                        &[col_spec("b")],
                    )
                    .unwrap()
                }))
                .with_feedback_when_performed(prepare_tx),
            ),
            RequestRule(
                Condition::RequestOpcode(RequestOpcode::Execute),
                RequestReaction::forge_response(Arc::new(|frame: RequestFrame| {
                    ResponseFrame::forged_rows(frame.params, &[col_spec("b")], [(2,)], None)
                        .unwrap()
                }))
                .with_feedback_when_performed(execute_tx),
            ),
        ])
        .await;
        let session: CachingSession = CachingSession::from(connect(proxy_addr).await, 100);

        let cases = [
            (Consistency::One, SerialConsistency::Serial, 1000, 10),
            (
                Consistency::Quorum,
                SerialConsistency::LocalSerial,
                2000,
                20,
            ),
        ];
        for (consistency, serial_consistency, timestamp, page_size) in cases {
            let mut statement = Statement::new("SELECT b FROM test_table WHERE a = ?");
            statement.set_consistency(consistency);
            statement.set_serial_consistency(Some(serial_consistency));
            statement.set_timestamp(Some(timestamp));
            statement.set_page_size(page_size);

            session
                .execute_single_page(statement, (1,), PagingState::start())
                .await
                .unwrap();

            let (frame, _) = executes.try_recv().unwrap();
            let RequestV2::Execute(ExecuteV2 { parameters, .. }) =
                frame.deserialize(&ProtocolFeatures::default()).unwrap()
            else {
                panic!("Expected an EXECUTE request");
            };
            assert_eq!(parameters.consistency, consistency);
            assert_eq!(parameters.serial_consistency, Some(serial_consistency));
            assert_eq!(parameters.timestamp, Some(timestamp));
            assert_eq!(parameters.page_size, Some(page_size));
        }

        // The second statement must have been served from the cache.
        prepares.try_recv().unwrap();
        prepares.try_recv().unwrap_err();

        let _ = proxy.finish().await;
    }

    // Checks whether the PartitionerName is cached properly.
    #[tokio::test]
    #[cfg_attr(cassandra_tests, ignore)]
    async fn test_partitioner_name_caching() {
        setup_tracing();

        let session: CachingSession =
            CachingSession::from(new_for_test(Some("CDC_WITH_TABLETS")).await, 100);

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

    /// Starts a dry-mode proxy that allows finishing creation of a Session.
    /// It performs the whole handshake on all connections, applies `rules`, and
    /// responds to all other QUERY, PREPARE and EXECUTE requests with an error.
    async fn make_proxy(rules: Vec<RequestRule>) -> (SocketAddr, RunningProxy) {
        let proxy_addr = SocketAddr::new(scylla_proxy::get_exclusive_local_address(), 9042);

        let mut proxy_rules = dry_mode_handshake_rules();
        proxy_rules.extend(rules);
        proxy_rules.push(RequestRule(
            Condition::any([
                Condition::RequestOpcode(RequestOpcode::Query),
                Condition::RequestOpcode(RequestOpcode::Prepare),
                Condition::RequestOpcode(RequestOpcode::Execute),
            ]),
            RequestReaction::forge().server_error(),
        ));

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

        (proxy_addr, proxy)
    }

    async fn make_minimal_proxy() -> (SocketAddr, RunningProxy) {
        make_proxy(Vec::new()).await
    }

    const SELECT_FROM_TEST_TABLE: &str = "SELECT * FROM test_table";

    fn test_table_col_specs() -> [ColumnSpec<'static>; 2] {
        let table_spec = TableSpec::borrowed("ks", "test_table");
        [
            ColumnSpec::borrowed("a", ColumnType::Native(NativeType::Int), table_spec.clone()),
            ColumnSpec::borrowed("b", ColumnType::Native(NativeType::Int), table_spec),
        ]
    }

    /// Starts a dry-mode proxy pretending that `test_table` holds a single row.
    ///
    /// It prepares any statement on `test_table`, reporting each PREPARE to the returned
    /// receiver, and answers every EXECUTE with that row.
    async fn make_test_table_proxy() -> (
        SocketAddr,
        RunningProxy,
        mpsc::UnboundedReceiver<(RequestFrame, Option<u16>)>,
    ) {
        let (prepare_tx, prepare_rx) = mpsc::unbounded_channel();
        let rules = vec![
            RequestRule(
                Condition::RequestOpcode(RequestOpcode::Prepare).and(
                    Condition::BodyContainsCaseSensitive(b"test_table"[..].into()),
                ),
                RequestReaction::forge_response(Arc::new(|frame: RequestFrame| {
                    ResponseFrame::forged_prepared(
                        frame.params,
                        b"test_table_statement",
                        &[],
                        &test_table_col_specs(),
                    )
                    .unwrap()
                }))
                .with_feedback_when_performed(prepare_tx),
            ),
            RequestRule(
                Condition::RequestOpcode(RequestOpcode::Execute),
                RequestReaction::forge_response(Arc::new(|frame: RequestFrame| {
                    ResponseFrame::forged_rows(
                        frame.params,
                        &test_table_col_specs(),
                        [(1, 2)],
                        None,
                    )
                    .unwrap()
                })),
            ),
        ];
        let (proxy_addr, proxy) = make_proxy(rules).await;
        (proxy_addr, proxy, prepare_rx)
    }

    async fn connect(proxy_addr: SocketAddr) -> Session {
        SessionBuilder::new()
            .known_node_addr(proxy_addr)
            .build()
            .await
            .unwrap()
    }

    /// Tests that [CachingSessionBuilder] passes its config options to the built [CachingSession].
    #[tokio::test]
    async fn test_builder() {
        setup_tracing();
        let (proxy_addr, proxy) = make_minimal_proxy().await;

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

    #[tokio::test]
    #[should_panic(expected = "prepared statement cache capacity must be greater than 0")]
    async fn test_builder_zero_capacity_panics() {
        setup_tracing();
        let (proxy_addr, proxy) = make_minimal_proxy().await;
        let session = SessionBuilder::new()
            .known_node_addr(proxy_addr)
            .build()
            .await
            .unwrap();
        let _ = CachingSessionBuilder::new(session).max_capacity(0);
        let _ = proxy.finish().await;
    }

    #[tokio::test]
    #[should_panic(expected = "prepared statement cache capacity must be greater than 0")]
    async fn test_from_zero_capacity_panics() {
        setup_tracing();
        let (proxy_addr, proxy) = make_minimal_proxy().await;
        let session = SessionBuilder::new()
            .known_node_addr(proxy_addr)
            .build()
            .await
            .unwrap();
        let _: CachingSession = CachingSession::from(session, 0);
        let _ = proxy.finish().await;
    }

    #[tokio::test]
    #[should_panic(expected = "prepared statement cache capacity must be greater than 0")]
    async fn test_with_hasher_zero_capacity_panics() {
        setup_tracing();
        let (proxy_addr, proxy) = make_minimal_proxy().await;
        let session = SessionBuilder::new()
            .known_node_addr(proxy_addr)
            .build()
            .await
            .unwrap();
        let _ = CachingSession::with_hasher(session, 0, RandomState::new());
        let _ = proxy.finish().await;
    }
}
