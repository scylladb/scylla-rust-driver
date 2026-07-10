//! Driver micro-benchmark scenarios.
//!
//! The scenarios cover:
//! - unpaged `SELECT` via [`Session::execute_unpaged`],
//! - `INSERT` via [`Session::execute_unpaged`],
//! - auto-paged `SELECT` via [`Session::execute_iter`],
//!
//! The actual measurement (separating connection setup from the measured
//! request loop) is handled by the benchmark harness; this crate only provides
//! the reusable building blocks.

use std::env;
use std::hint::black_box;

use anyhow::Result;
use futures::StreamExt as _;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::prepared::PreparedStatement;

/// Default contact point, matching the address used by the repository's
/// docker-compose test cluster (see `test/cluster/docker-compose.yml`).
pub const DEFAULT_NODE: &str = "172.42.0.2:9042";

/// Keyspace used by all benchmark scenarios.
pub const KEYSPACE: &str = "benchmarks_ks";

/// Table used by all benchmark scenarios.
pub const TABLE: &str = "t";

/// Number of distinct-partition requests issued during context construction to
/// warm up tablet routing, so that the measured loops are not dominated by the
/// driver relearning tablets (see the warmup loop in [`BenchContext::build`]).
/// Chosen comfortably above the table's tablet count so that every tablet is
/// learned during the warmup.
const TABLET_WARMUP_REQUESTS: usize = 512;

/// Returns the contact point to connect to, taken from the `SCYLLA_URI`
/// environment variable and falling back to [`DEFAULT_NODE`].
pub fn node_address() -> String {
    env::var("SCYLLA_URI").unwrap_or_else(|_| DEFAULT_NODE.to_string())
}

/// Tunable sizes for the benchmark scenarios.
#[derive(Clone, Copy, Debug)]
pub struct ScenarioConfig {
    /// Number of statements packed into the `BATCH` scenario's batch.
    pub batch_size: usize,
    /// Number of rows pre-populated in the table and streamed by the auto-paged
    /// `SELECT` scenario.
    pub paged_rows: usize,
    /// Page size used by the auto-paged `SELECT` scenario. Chosen smaller than
    /// `paged_rows` so that multiple pages are actually fetched.
    pub page_size: i32,
}

impl Default for ScenarioConfig {
    fn default() -> Self {
        Self {
            batch_size: 8,
            paged_rows: 200,
            page_size: 20,
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

        let (session, prepared_insert, prepared_select, prepared_select_all) =
            runtime.block_on(async {
                // Use the driver defaults, in particular one connection per shard.
                let builder = SessionBuilder::new().known_node(node);
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
                         (a int, b int, c text, primary key (a, b))"
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
                        "SELECT a, b, c FROM {KEYSPACE}.{TABLE} WHERE a = ? AND b = ?"
                    ))
                    .await?;

                let mut prepared_select_all = session
                    .prepare(format!("SELECT a, b, c FROM {KEYSPACE}.{TABLE}"))
                    .await?;
                prepared_select_all.set_page_size(config.page_size);

                // Pre-populate rows for the auto-paged SELECT scenario.
                for i in 0..config.paged_rows as i32 {
                    session.execute_unpaged(&prepared_insert, (0i32, i)).await?;
                }

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
                futures::stream::iter(0..TABLET_WARMUP_REQUESTS as i32)
                    .for_each_concurrent(64, async |i| {
                        session
                            .execute_unpaged(&prepared_select, (i, 2 * i))
                            .await
                            .unwrap();
                    })
                    .await;

                session.refresh_metadata().await?;

                Ok::<_, anyhow::Error>((
                    session,
                    prepared_insert,
                    prepared_select,
                    prepared_select_all,
                ))
            })?;

        Ok(Self {
            runtime,
            session,
            prepared_insert,
            prepared_select,
            prepared_select_all,
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
                    .execute_unpaged(&self.prepared_select, (i, 2 * i))
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
