//! Driver benchmark scenarios.
//!
//! The scenarios cover:
//! - unpaged `SELECT` via [`Session::execute_unpaged`],
//! - `INSERT` via [`Session::execute_unpaged`],
//! - `BATCH` of a configurable number of statements via [`Session::batch`],
//! - auto-paged `SELECT` via [`Session::execute_iter`],
//! - learning the table's tablets from routing feedback, driven by unpaged
//!   `SELECT`s on a session that has not learned them yet.
//!
//! The actual measurement (separating connection setup from the measured
//! request loop) is handled by the benchmark harness; this crate only provides
//! the reusable building blocks.

use std::env;
use std::hint::black_box;
use std::time::Duration;

use anyhow::Result;
use futures::StreamExt as _;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::prepared::PreparedStatement;

/// Default contact point, matching the address used by the repository's
/// docker-compose test cluster (see `test/cluster/docker-compose.yml`).
pub const DEFAULT_NODE: &str = "172.42.0.2:9042";

/// Keyspace used by all benchmark scenarios.
pub const KEYSPACE: &str = "benchmarks_ks";

/// Table used by all benchmark scenarios.
pub const TABLE: &str = "t";

/// Number of tablets of the benchmark table.
///
/// It is what ScyllaDB picks by default for the repository's docker-compose
/// cluster (3 nodes, 2 shards each), but the table definition also pins it as
/// the minimum, so that tablet merges on the tiny table cannot change it and
/// the cost of learning the table's tablets stays the same across runs.
pub const TABLET_COUNT: usize = 64;

/// Number of distinct-partition requests issued during context construction to
/// warm up tablet routing, so that the measured loops are not dominated by the
/// driver relearning tablets (see the warmup loop in `BenchContext::build`).
/// Chosen comfortably above [`TABLET_COUNT`] so that every tablet is learned
/// during the warmup.
const TABLET_WARMUP_REQUESTS: usize = 8 * TABLET_COUNT;

/// Interval of the driver's periodic background work (keepalives, metadata
/// refresh) in benchmark sessions. See the session construction in
/// `BenchContext::build` for why it is pushed this far.
const BACKGROUND_WORK_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Returns the contact point to connect to, taken from the `SCYLLA_URI`
/// environment variable and falling back to [`DEFAULT_NODE`].
pub fn node_address() -> String {
    env::var("SCYLLA_URI").unwrap_or_else(|_| DEFAULT_NODE.to_string())
}

/// Tunable sizes for the benchmark scenarios.
#[derive(Clone, Copy, Debug)]
pub struct ScenarioConfig {
    pub prefilled_partitions: usize,
    pub prefilled_rows_per_partition: usize,
    /// Number of statements packed into the `BATCH` scenario's batch.
    pub batch_size: usize,
    /// Page size used by the auto-paged `SELECT` scenario. Chosen smaller than
    /// `paged_rows` so that multiple pages are actually fetched.
    pub page_size: i32,
    /// Whether to learn every tablet of the table during context construction
    /// (see the warmup loop in `BenchContext::build`). Disabled only by the
    /// `tablet_learning` scenario, which measures that learning itself.
    pub warm_up_tablets: bool,
}

impl Default for ScenarioConfig {
    fn default() -> Self {
        Self {
            prefilled_partitions: 100,
            prefilled_rows_per_partition: 10,
            batch_size: 8,
            page_size: 20,
            warm_up_tablets: true,
        }
    }
}

/// A connected session together with everything the scenarios need.
///
/// Construction (connecting, creating the schema, preparing statements,
/// pre-populating data and warming up tablet routing) is intentionally
/// separated from the per-request work so that the benchmark harness can
/// exclude it from measurements. Each `run_*` method performs only the work
/// that should be measured.
pub struct BenchContext {
    runtime: tokio::runtime::Runtime,
    session: Session,
    prepared_insert: PreparedStatement,
    prepared_select: PreparedStatement,
    prepared_select_all: PreparedStatement,
    batch: Batch,
    batch_values: Vec<(i32, i32)>,
}

impl BenchContext {
    /// Connects to `node`, (re)creates the benchmark schema, prepares the
    /// statements used by the scenarios, pre-populates the data needed by the
    /// auto-paged `SELECT` scenario and warms up tablet routing.
    pub fn new(node: &str, config: ScenarioConfig) -> Result<Self> {
        Self::build(node, config)
    }

    fn build(node: &str, config: ScenarioConfig) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let (session, prepared_insert, prepared_select, prepared_select_all, batch, batch_values) =
            runtime.block_on(async {
                // Use the driver defaults, in particular one connection per shard.
                //
                // Except for the periodic background work: by default the driver
                // sends a keepalive on every connection every 30 s and refreshes
                // the cluster metadata every 60 s. Under Valgrind a scenario
                // easily runs for that long, so a timer firing inside the
                // measured loop would add a random burst of allocations and
                // instructions. Push both far past any scenario's duration.
                let builder = SessionBuilder::new()
                    .known_node(node)
                    .keepalive_interval(BACKGROUND_WORK_INTERVAL)
                    .cluster_metadata_refresh_interval(BACKGROUND_WORK_INTERVAL);
                let session: Session = builder.build().await?;

                session
                    .query_unpaged(
                        format!(
                            "CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} WITH REPLICATION = \
                         {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}"
                        ),
                        &[],
                    )
                    .await?;

                session
                    .query_unpaged(
                        format!(
                            "CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE} \
                         (a int, b int, c text, primary key (a, b)) \
                         WITH tablets = {{'min_tablet_count': {TABLET_COUNT}}}"
                        ),
                        &[],
                    )
                    .await?;

                // Make sure that there are no rows - test should be more stable thanks to that.
                session
                    .query_unpaged(format!("TRUNCATE {KEYSPACE}.{TABLE}"), &[])
                    .await?;

                let prepared_insert = session
                    .prepare(format!(
                        "INSERT INTO {KEYSPACE}.{TABLE} (a, b, c) VALUES (?, ?, 'abc')"
                    ))
                    .await?;

                let prepared_select = session
                    .prepare(format!(
                        "SELECT a, b, c FROM {KEYSPACE}.{TABLE} WHERE a = ?"
                    ))
                    .await?;

                let mut prepared_select_all = session
                    .prepare(format!("SELECT a, b, c FROM {KEYSPACE}.{TABLE}"))
                    .await?;
                prepared_select_all.set_page_size(config.page_size);

                // Build the batch once so that the measured loop only executes it.
                let mut batch = Batch::new(BatchType::Unlogged);
                let mut batch_values = Vec::with_capacity(config.batch_size);
                for i in 0..config.batch_size {
                    batch.append_statement(prepared_insert.clone());
                    batch_values.push((i as i32, (2 * i) as i32));
                }

                // Pre-populate rows for the SELECT scenarios.
                futures::stream::iter(0..config.prefilled_partitions)
                    .for_each_concurrent(16, async |partition| {
                        futures::stream::iter(0..config.prefilled_rows_per_partition)
                            .for_each_concurrent(16, async |row| {
                                session
                                    .execute_unpaged(
                                        &prepared_insert,
                                        (partition as i32, row as i32),
                                    )
                                    .await
                                    .unwrap();
                            })
                            .await;
                    })
                    .await;

                // Warm up tablet routing before the measured loop runs.
                //
                // ScyllaDB tables use tablets, and when the driver routes a
                // request to a node that is not the tablet's replica the server
                // replies with tablet-routing feedback. The cluster worker then
                // rebuilds cluster metadata to record it -- it clones the whole
                // `ClusterState` (every keyspace and table). Until the driver
                // has learned every tablet this happens on almost every request,
                // dominating the measured allocations (hundreds per request) and
                // hiding the request path's own cost.
                //
                // Reads (not writes) are used so that the warmup does not insert
                // rows, which would otherwise inflate the auto-paged `SELECT`
                // scenario's full-table scan; tablet learning is driven by where
                // a request is routed, so a read of a (possibly absent) row
                // teaches the driver the tablet just as a write would.
                if config.warm_up_tablets {
                    futures::stream::iter(0..TABLET_WARMUP_REQUESTS as i32)
                        .for_each_concurrent(64, async |i| {
                            session
                                .execute_unpaged(&prepared_select, (i,))
                                .await
                                .unwrap();
                        })
                        .await;
                }

                session.refresh_metadata().await?;

                Ok::<_, anyhow::Error>((
                    session,
                    prepared_insert,
                    prepared_select,
                    prepared_select_all,
                    batch,
                    batch_values,
                ))
            })?;

        Ok(Self {
            runtime,
            session,
            prepared_insert,
            prepared_select,
            prepared_select_all,
            batch,
            batch_values,
        })
    }

    /// Runs `n` `INSERT`s via [`Session::execute_unpaged`].
    pub fn run_inserts(&self, n: usize) {
        self.runtime.block_on(async {
            for i in 0..n as i32 {
                let result = self
                    .session
                    .execute_unpaged(&self.prepared_insert, (i, 2 * i))
                    .await
                    .unwrap();
                black_box(result);
            }
        })
    }

    /// Runs `n` unpaged `SELECT`s via [`Session::execute_unpaged`].
    pub fn run_unpaged_selects(&self, n: usize) {
        self.runtime.block_on(async {
            for i in 0..n as i32 {
                let result = self
                    .session
                    .execute_unpaged(&self.prepared_select, (i,))
                    .await
                    .unwrap();
                black_box(result);
            }
        })
    }

    /// Learns the table's tablets: runs `n` unpaged `SELECT`s of distinct
    /// partitions via [`Session::execute_unpaged`] on a session that has not
    /// warmed up tablet routing (see [`ScenarioConfig::warm_up_tablets`]), so
    /// that the measurement covers the tablet-routing feedback the requests
    /// trigger and the cluster metadata updates it causes.
    ///
    /// The loop is made deterministic, so that the allocation count can be
    /// compared exactly between runs:
    ///
    /// - The requests are sequential. Concurrent requests to a not yet learned
    ///   tablet each trigger feedback for it, and how many do depends on timing.
    /// - After each request the task yields once. The cluster worker, which
    ///   applies the feedback, runs on this same single-threaded runtime and only
    ///   gets to run when this task yields; without the yield the next request
    ///   would be routed using the state from before the feedback and could
    ///   trigger it again for the same tablet. With the yield every tablet
    ///   triggers feedback exactly once, so the number of metadata updates is
    ///   the tablet count, whichever requests happen to hit unlearned tablets.
    /// - `n` is many times [`TABLET_COUNT`], so that every tablet is hit enough
    ///   times to be learned even though an individual request to an unlearned
    ///   tablet is not guaranteed to trigger feedback (it does not when it happens
    ///   to be routed to the right node and shard).
    pub fn run_tablet_learning(&self, n: usize) {
        self.runtime.block_on(async {
            for i in 0..n as i32 {
                let result = self
                    .session
                    .execute_unpaged(&self.prepared_select, (i,))
                    .await
                    .unwrap();
                black_box(result);
                tokio::task::yield_now().await;
            }
        })
    }

    /// Runs `n` `BATCH`es, each containing [`ScenarioConfig::batch_size`]
    /// prepared `INSERT`s, via [`Session::batch`].
    pub fn run_batches(&self, n: usize) {
        self.runtime.block_on(async {
            for _ in 0..n as i32 {
                let result = self
                    .session
                    .batch(&self.batch, &self.batch_values)
                    .await
                    .unwrap();
                black_box(result);
            }
        })
    }

    /// Runs `n` auto-paged `SELECT`s via [`Session::execute_iter`], draining
    /// every page of every result.
    pub fn run_paged_selects(&self, n: usize) {
        self.runtime.block_on(async {
            for _ in 0..n as i32 {
                let mut stream = self
                    .session
                    .execute_iter(self.prepared_select_all.clone(), &[])
                    .await
                    .unwrap()
                    .rows_stream::<(i32, i32, String)>()
                    .unwrap();
                while let Some(row) = stream.next().await {
                    black_box(row.unwrap());
                }
            }
        })
    }
}
