use std::sync::Arc;

use crate::utils::{
    PerformDDL, execute_prepared_statement_everywhere, execute_unprepared_statement_everywhere,
    scylla_supports_tablets, setup_tracing, supports_feature, test_with_3_node_cluster,
    unique_keyspace_name,
};

use futures::TryStreamExt;
use futures::future::try_join_all;
use itertools::Itertools;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::cluster::Node;
use scylla::errors::{DbError, RequestAttemptError, WriteType};
use scylla::policies::retry::{DefaultRetrySession, RetryDecision, RetryPolicy, RetrySession};
use scylla::routing::Shard;
use scylla::routing::partitioner::PartitionerName;
use scylla::serialize::row::SerializeRow;
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::unprepared::Statement;

use scylla_proxy::{
    Condition, ProxyError, Reaction, ResponseFrame, ResponseOpcode, ResponseReaction, ResponseRule,
    RunningProxy, ShardAwareness, TargetShard, WorkerError,
};

use std::future::Future;

use tokio::sync::mpsc::{self, UnboundedReceiver};
use tracing::info;
use uuid::Uuid;

/// RetryPolicy that works around Scylla LWT bug: https://scylladb.atlassian.net/browse/SCYLLADB-3194
/// The symptom of the bug is the request failing with error message like below:
///
/// thread 'load_balancing::tablets::test_tablets' (3651327) panicked at scylla/tests/integration/load_balancing/tablets.rs:602:21:
/// Test attempt failed despite no migration. Error: case QueryDescriptor { op: LwtUpdateIf, ks_presence: WithKs, data_form: RegularPrepared } failed: QueryDescriptor { op: LwtUpdateIf, ks_presence: WithKs, data_form: RegularPrepared }: execution failed: Database returned an error: A non-timeout error during a write request (consistency: LocalSerial, received: 1, required: 2, numfailures: 1, write_type: Cas, Error message: Failed to prepare ballot 2766fdcc-7b7f-11f1-abb6-46f4f195de97 for test_rust_675cf549299940bba12bd8786ceaad6d.t. Replica errors: host_id 826bb99e-6317-448c-a2f1-02b41fefbb1b -> seastar::rpc::remote_verb_error (stale topology exception, caller version 1470, callee fence version 1476);
#[derive(Debug)]
struct LwtBallotBugRetryPolicy;
struct LwtBallotBugRetryPolicySession {
    retries_left: usize,
    inner: DefaultRetrySession,
}

impl RetryPolicy for LwtBallotBugRetryPolicy {
    fn new_session(&self) -> Box<dyn RetrySession> {
        Box::new(LwtBallotBugRetryPolicySession {
            retries_left: 20,
            inner: DefaultRetrySession::new(),
        })
    }
}
impl RetrySession for LwtBallotBugRetryPolicySession {
    fn decide_should_retry(
        &mut self,
        request_info: scylla::policies::retry::RequestInfo,
    ) -> scylla::policies::retry::RetryDecision {
        if let RequestAttemptError::DbError(
            DbError::WriteFailure {
                write_type: WriteType::Cas,
                consistency: Consistency::LocalSerial,
                ..
            },
            message,
        ) = request_info.error
            && message.contains("Failed to prepare ballot")
        {
            if self.retries_left > 0 {
                self.retries_left -= 1;
            } else {
                // Panic explicitly so that we know that retry policy worked, but
                // error kept happening.
                panic!(
                    "LWT ballot error happened 20 times in a row! e: {}",
                    request_info.error
                );
            }

            return RetryDecision::RetrySameTarget(None);
        }

        self.inner.decide_should_retry(request_info)
    }

    fn reset(&mut self) {
        self.retries_left = 20;
        self.inner.reset();
    }
}

#[derive(scylla::DeserializeRow)]
struct SelectedTablet {
    last_token: i64,
    replicas: Vec<(Uuid, i32)>,
}

#[derive(Eq, PartialEq)]
struct Tablet {
    first_token: i64,
    last_token: i64,
    replicas: Vec<(Arc<Node>, i32)>,
}

async fn get_tablets(session: &Session, ks: &str, table: &str) -> Vec<Tablet> {
    let cluster_state = session.get_cluster_state();
    let endpoints = cluster_state.get_nodes_info();
    for endpoint in endpoints.iter() {
        info!(
            "Endpoint id: {}, address: {}",
            endpoint.host_id,
            endpoint.address.ip()
        );
    }

    let selected_tablets_response = session.query_iter(
        "SELECT last_token, replicas FROM system.tablets WHERE keyspace_name = ? AND table_name = ? ALLOW FILTERING",
        &(ks, table)).await.unwrap();

    let mut selected_tablets: Vec<SelectedTablet> = selected_tablets_response
        .rows_stream::<SelectedTablet>()
        .unwrap()
        .into_stream()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    selected_tablets.sort_unstable_by_key(|a| a.last_token);

    let (tablets, _) = selected_tablets.iter().fold(
        (Vec::new(), i64::MIN),
        |(mut tablets, first_token), tablet| {
            let replicas = tablet
                .replicas
                .iter()
                .map(|(uuid, shard)| {
                    (
                        Arc::clone(
                            endpoints
                                .get(endpoints.iter().position(|e| e.host_id == *uuid).unwrap())
                                .unwrap(),
                        ),
                        *shard,
                    )
                })
                .collect();
            let raw_tablet = Tablet {
                first_token,
                last_token: tablet.last_token,
                replicas,
            };

            tablets.push(raw_tablet);
            (tablets, tablet.last_token.wrapping_add(1))
        },
    );

    // Print information about tablets, useful for debugging
    for tablet in tablets.iter() {
        info!(
            "Tablet: [{}, {}]: {:?}",
            tablet.first_token,
            tablet.last_token,
            tablet
                .replicas
                .iter()
                .map(|(replica, shard)| { (replica.address.ip(), shard) })
                .collect::<Vec<_>>()
        );
    }

    tablets
}

// Finds an example partition key for each tablet in the table.
//
// `make_key` turns a probe integer into a candidate key (whatever shape the
// `prepared` statement's partition key has, e.g. `(i, 1)` for a `(pk, ck)` table
// or `(i,)` for a single-column partition key). We probe increasing integers,
// compute each candidate's token with `prepared`, and keep the first key that
// falls into each tablet, so the returned vector has exactly one key per tablet.
fn calculate_key_per_tablet<K: SerializeRow>(
    tablets: &[Tablet],
    prepared: &PreparedStatement,
    make_key: impl Fn(i32) -> K,
) -> Vec<K> {
    let mut present_tablets = vec![false; tablets.len()];
    let mut keys = Vec::new();
    for i in 0..1000 {
        let key = make_key(i);
        let token_value = prepared.calculate_token(&key).unwrap().unwrap().value();
        let tablet_idx = tablets
            .iter()
            .position(|tablet| {
                tablet.first_token <= token_value && token_value <= tablet.last_token
            })
            .unwrap();
        if !present_tablets[tablet_idx] {
            keys.push(key);
            present_tablets[tablet_idx] = true;
        }
    }

    // This function tries 1000 keys and assumes that it is enough to cover all
    // tablets. This is just a random number that seems to work in the tests,
    // so the assert is here to catch the problem early if 1000 stops being enough
    // for some reason.
    assert!(present_tablets.iter().all(|x| *x));

    keys
}

// Builds a `cdc$stream_id` blob (the partition key of a CDC log table) landing
// in each tablet. The CDC partitioner uses the first 8 bytes of the blob,
// interpreted as a big-endian i64, directly as the token, so a blob whose
// leading 8 bytes encode a token inside a tablet's range routes to that tablet.
// Real stream ids are 16 bytes, so we pad to that length.
fn cdc_stream_id_key_per_tablet(tablets: &[Tablet]) -> Vec<Vec<u8>> {
    tablets
        .iter()
        .map(|tablet| {
            // `last_token` is the inclusive upper bound of the tablet's range,
            // so it belongs to this tablet.
            let mut blob = vec![0u8; 16];
            blob[..8].copy_from_slice(&tablet.last_token.to_be_bytes());
            blob
        })
        .collect()
}

fn frame_has_tablet_feedback(frame: ResponseFrame) -> bool {
    let response =
        scylla_cql::frame::parse_response_body_extensions(frame.params.flags, None, frame.body)
            .unwrap();
    match response.custom_payload {
        Some(map) => map.contains_key("tablets-routing-v1"),
        None => false,
    }
}

fn count_tablet_feedbacks(
    rx: &mut mpsc::UnboundedReceiver<(ResponseFrame, Option<TargetShard>)>,
) -> usize {
    std::iter::from_fn(|| rx.try_recv().ok())
        .map(|(frame, _shard)| frame_has_tablet_feedback(frame))
        .filter(|b| *b)
        .count()
}

async fn prepare_schema(session: &Session, ks: &str, table: &str, tablet_count: usize) {
    let supports_table_tablet_options = supports_feature(session, "TABLET_OPTIONS").await;
    let (keyspace_tablet_opts, table_tablet_opts) = if supports_table_tablet_options {
        (
            "AND tablets = { 'enabled': true }".to_string(),
            format!("WITH tablets = {{ 'min_tablet_count': {tablet_count} }}"),
        )
    } else {
        (
            format!("AND tablets = {{ 'initial': {tablet_count} }}"),
            String::new(),
        )
    };

    session
        .ddl(format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks}
            WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 2}}
            {keyspace_tablet_opts}"
        ))
        .await
        .unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {ks}.{table} (a int, b int, c text, primary key (a, b))
            {table_tablet_opts}"
        ))
        .await
        .unwrap();
}

/// Learns the tablet info for a set of keys by executing each key's prepared
/// statement on every shard of every node: the non-replica targets answer with
/// tablet feedback that teaches the driver where each tablet lives.
///
/// Also asserts that every key produced at least one feedback, which confirms
/// the `TABLETS_ROUTING_V1` extension was negotiated and feedback is flowing --
/// otherwise the later verification phase, which relies on the *absence* of
/// feedback, would pass vacuously.
async fn learn_tablet_info(
    session: &Session,
    per_key: &[PreparedForKey],
    feedback_rxs: &mut [UnboundedReceiver<(ResponseFrame, Option<TargetShard>)>],
) -> Result<(), String> {
    drain_feedbacks(feedback_rxs);
    let mut keys_with_feedback = 0;
    for (prepared, values) in per_key {
        execute_prepared_statement_everywhere(
            session,
            session.get_cluster_state().as_ref(),
            prepared,
            &**values,
        )
        .await
        .map_err(|e| format!("learning execution failed: {e}"))?;
        let feedbacks: usize = feedback_rxs.iter_mut().map(count_tablet_feedbacks).sum();
        if feedbacks > 0 {
            keys_with_feedback += 1;
        }
    }

    if keys_with_feedback == per_key.len() {
        Ok(())
    } else {
        Err(format!(
            "Expected tablet feedback for all {} keys, got it for {}",
            per_key.len(),
            keys_with_feedback
        ))
    }
}

/// Drains all pending feedback frames from every receiver.
///
/// A subtest learns tablet info in one phase (which produces feedback) and then
/// asserts on the feedback from a later phase, so we drain in between to keep
/// the learning-phase feedback from leaking into the assertion.
fn drain_feedbacks(feedback_rxs: &mut [UnboundedReceiver<(ResponseFrame, Option<TargetShard>)>]) {
    for rx in feedback_rxs.iter_mut() {
        while rx.try_recv().is_ok() {}
    }
}

/// The CQL operation exercised by a subtest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    Select,
    Update,
    Insert,
    Delete,
    /// `UPDATE ... IF c != null`.
    LwtUpdateIf,
    /// `UPDATE ... IF EXISTS`.
    LwtUpdateIfExists,
    /// `INSERT ... IF NOT EXISTS`.
    LwtInsertIfNotExists,
    /// A plain `SELECT` executed with `Consistency::Serial`, i.e. a Paxos read.
    ///
    /// This is the only way to force LWT routing without a conditional `IF`
    /// clause: the server rejects `SERIAL` consistency on writes ("You must use
    /// conditional updates for serializable writes"), so there is no
    /// `LwtManual{Update,Insert,Delete}` counterpart.
    LwtManualSelect,
}

/// The underlying CQL verb of an [`Op`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verb {
    Select,
    Update,
    Insert,
    Delete,
}

/// The LWT condition appended to a statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LwtCondition {
    None,
    IfValueNotNull, // ... IF c != null
    IfExists,       // ... IF EXISTS
    IfNotExists,    // ... IF NOT EXISTS
}

impl Op {
    fn verb(self) -> Verb {
        match self {
            Op::Select | Op::LwtManualSelect => Verb::Select,
            Op::Update | Op::LwtUpdateIf | Op::LwtUpdateIfExists => Verb::Update,
            Op::Insert | Op::LwtInsertIfNotExists => Verb::Insert,
            Op::Delete => Verb::Delete,
        }
    }

    fn condition(self) -> LwtCondition {
        match self {
            Op::LwtUpdateIf => LwtCondition::IfValueNotNull,
            Op::LwtUpdateIfExists => LwtCondition::IfExists,
            Op::LwtInsertIfNotExists => LwtCondition::IfNotExists,
            _ => LwtCondition::None,
        }
    }

    /// Whether the op is forced to be an LWT by requesting SERIAL consistency
    /// (rather than by using a conditional `IF ...` clause).
    fn is_manual_lwt(self) -> bool {
        matches!(self, Op::LwtManualSelect)
    }

    /// Whether the op is an LWT, either conditional or forced via SERIAL.
    fn is_lwt(self) -> bool {
        self.is_manual_lwt() || self.condition() != LwtCondition::None
    }
}

/// Whether the keyspace is spelled out in the query (`ks.t`) or not (`t`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KsPresence {
    WithKs,
    WithoutKs,
}

/// How the partition key, clustering key and value are expressed in the query.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DataForm {
    /// All values are literals, executed as an unprepared statement:
    /// `... WHERE a = 1 AND b = 1`.
    ConcreteUnprepared,
    /// All values are literals, executed as a prepared statement.
    ConcretePrepared,
    /// Partition and clustering keys are bind markers: `... WHERE a = ? AND b = ?`.
    RegularPrepared,
    /// Partition key is a literal, clustering key is a bind marker:
    /// `... WHERE a = 1 AND b = ?`.
    ConcretePreparedMixed,
}

impl DataForm {
    fn is_prepared(self) -> bool {
        !matches!(self, DataForm::ConcreteUnprepared)
    }
    fn pk_is_marker(self) -> bool {
        matches!(self, DataForm::RegularPrepared)
    }
    fn ck_is_marker(self) -> bool {
        matches!(
            self,
            DataForm::RegularPrepared | DataForm::ConcretePreparedMixed
        )
    }
    fn value_is_marker(self) -> bool {
        matches!(
            self,
            DataForm::RegularPrepared | DataForm::ConcretePreparedMixed
        )
    }
    /// Whether the CQL text is independent of the concrete key, so a single
    /// prepared statement can be reused for every key. This holds only when both
    /// key components are bind markers (no literal key is embedded in the text).
    fn text_is_key_independent(self) -> bool {
        self.pk_is_marker() && self.ck_is_marker()
    }
}

/// Fully describes a single query variant tested as a subtest.
#[derive(Clone, Copy, Debug)]
struct QueryDescriptor {
    op: Op,
    ks_presence: KsPresence,
    data_form: DataForm,
}

impl QueryDescriptor {
    /// Whether the driver can compute the token (and thus route in a tablet-aware
    /// way) for this query. Only prepared statements that bind the full partition
    /// key as markers are token-aware.
    fn is_token_aware(self) -> bool {
        self.data_form.is_prepared() && self.data_form.pk_is_marker()
    }
}

const TEXT_VALUE: &str = "abc";

/// Renders an `int` position in the query text: a bind marker (`?`) or a
/// literal, depending on `is_marker`.
fn render_int(is_marker: bool, value: i32) -> String {
    if is_marker {
        "?".to_string()
    } else {
        value.to_string()
    }
}

/// Renders a `text` position in the query text: a bind marker (`?`) or a quoted
/// literal, depending on `is_marker`.
fn render_text(is_marker: bool, value: &str) -> String {
    if is_marker {
        "?".to_string()
    } else {
        format!("'{value}'")
    }
}

/// Builds the row of values to bind for a given key, as a single boxed
/// `SerializeRow` (one allocation, unlike a `Vec<Box<dyn SerializeValue>>`).
///
/// The bound values must appear in the same order as the corresponding bind
/// markers in the text produced by [`build_cql`]. Which columns are markers is
/// fully determined by the data form, so we can enumerate the concrete tuples:
/// `SELECT`/`DELETE` bind `(pk?, ck?)`, `UPDATE` binds `(value?, pk?, ck?)` and
/// `INSERT` binds `(pk?, ck?, value?)`, keeping only the marked positions.
fn build_values(descriptor: QueryDescriptor, a: i32, b: i32) -> Box<dyn SerializeRow> {
    match (descriptor.data_form, descriptor.op.verb()) {
        // No markers: every value is a literal (or the statement is unprepared).
        (DataForm::ConcreteUnprepared | DataForm::ConcretePrepared, _) => Box::new(()),
        // Fully bound partition and clustering key.
        (DataForm::RegularPrepared, Verb::Select | Verb::Delete) => Box::new((a, b)),
        (DataForm::RegularPrepared, Verb::Update) => Box::new((TEXT_VALUE, a, b)),
        (DataForm::RegularPrepared, Verb::Insert) => Box::new((a, b, TEXT_VALUE)),
        // Literal partition key, bound clustering key (and value).
        (DataForm::ConcretePreparedMixed, Verb::Select | Verb::Delete) => Box::new((b,)),
        (DataForm::ConcretePreparedMixed, Verb::Update) => Box::new((TEXT_VALUE, b)),
        (DataForm::ConcretePreparedMixed, Verb::Insert) => Box::new((b, TEXT_VALUE)),
    }
}

/// Builds the CQL text and the row of bound values for a given key, according to
/// the descriptor.
fn build_cql(
    descriptor: QueryDescriptor,
    ks: &str,
    a: i32,
    b: i32,
) -> (String, Box<dyn SerializeRow>) {
    let table = match descriptor.ks_presence {
        KsPresence::WithKs => format!("{ks}.t"),
        KsPresence::WithoutKs => "t".to_string(),
    };
    let df = descriptor.data_form;

    let text = match descriptor.op.verb() {
        Verb::Select => {
            let pk = render_int(df.pk_is_marker(), a);
            let ck = render_int(df.ck_is_marker(), b);
            format!("SELECT a, b, c FROM {table} WHERE a = {pk} AND b = {ck}")
        }
        Verb::Delete => {
            let pk = render_int(df.pk_is_marker(), a);
            let ck = render_int(df.ck_is_marker(), b);
            format!("DELETE FROM {table} WHERE a = {pk} AND b = {ck}")
        }
        Verb::Update => {
            let val = render_text(df.value_is_marker(), TEXT_VALUE);
            let pk = render_int(df.pk_is_marker(), a);
            let ck = render_int(df.ck_is_marker(), b);
            let cond = match descriptor.op.condition() {
                LwtCondition::IfValueNotNull => " IF c != null",
                LwtCondition::IfExists => " IF EXISTS",
                _ => "",
            };
            format!("UPDATE {table} SET c = {val} WHERE a = {pk} AND b = {ck}{cond}")
        }
        Verb::Insert => {
            let pk = render_int(df.pk_is_marker(), a);
            let ck = render_int(df.ck_is_marker(), b);
            let val = render_text(df.value_is_marker(), TEXT_VALUE);
            let cond = match descriptor.op.condition() {
                LwtCondition::IfNotExists => " IF NOT EXISTS",
                _ => "",
            };
            format!("INSERT INTO {table} (a, b, c) VALUES ({pk}, {ck}, {val}){cond}")
        }
    };

    (text, build_values(descriptor, a, b))
}

/// Number of times each query is executed per tablet key when checking routing.
const VERIFY_QUERY_COUNT: usize = 30;

/// A prepared statement paired with the values to bind for one key.
type PreparedForKey = (PreparedStatement, Box<dyn SerializeRow>);

/// Pairs a single prepared statement with each of `keys`, cloning the statement
/// for every key (`PreparedStatement` is cheap to clone). Used when the same
/// statement is reused for every key.
fn prepared_for_keys<K: SerializeRow + 'static>(
    prepared: &PreparedStatement,
    keys: impl IntoIterator<Item = K>,
) -> Vec<PreparedForKey> {
    keys.into_iter()
        .map(|key| (prepared.clone(), Box::new(key) as Box<dyn SerializeRow>))
        .collect()
}

/// Whether a phase is expected to produce tablet feedback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FeedbackExpectation {
    /// The driver already knows the tablets, so it routes straight to a replica
    /// and the server sends no feedback. This is the correct behavior.
    Zero,
    /// The driver mis-routes and the server keeps re-teaching it via feedback.
    /// Currently only used to pin the materialized-view bug (see
    /// [`verify_materialized_view_tablet_bug`]).
    NonZero,
}

/// Runs each key's prepared statement [`VERIFY_QUERY_COUNT`] times and checks the
/// resulting tablet-routing behavior. Assumes the driver already has the relevant
/// tablet info populated locally (see [`learn_tablet_info`]).
///
/// Every execution reports the node+shard that served it via its result's
/// `QueryResult::request_coordinator`, so we can attribute each response to a
/// key without isolating the shared feedback channels. This lets all keys (and
/// repetitions) run concurrently.
///
/// When `require_single_coordinator` is set (token-aware LWTs, which are pinned
/// to a single replica to avoid Paxos contention), every execution for a given
/// key must hit exactly one node+shard.
async fn verify_feedback_for_keys(
    session: &Session,
    per_key: &[PreparedForKey],
    expected: FeedbackExpectation,
    require_single_coordinator: bool,
    feedback_rxs: &mut [UnboundedReceiver<(ResponseFrame, Option<TargetShard>)>],
) -> Result<(), String> {
    drain_feedbacks(feedback_rxs);

    // Run every key (and repetition) concurrently, collecting the coordinator
    // (node host id + shard) of each execution, grouped by key.
    let coordinators_per_key: Vec<Vec<(Uuid, Option<Shard>)>> =
        try_join_all(per_key.iter().map(|(prepared, values)| async move {
            try_join_all((0..VERIFY_QUERY_COUNT).map(|_| async move {
                let result = session
                    .execute_unpaged(prepared, values)
                    .await
                    .map_err(|e| format!("execution failed: {e}"))?;
                let coordinator = result.request_coordinator();
                Ok::<_, String>((coordinator.node().host_id, coordinator.shard()))
            }))
            .await
        }))
        .await?;

    if require_single_coordinator {
        for coordinators in &coordinators_per_key {
            let distinct = coordinators.iter().unique().count();
            if distinct != 1 {
                return Err(format!(
                    "Expected all queries for a key to hit a single replica+shard, \
                     got {distinct} distinct coordinators"
                ));
            }
        }
    }

    let feedbacks: usize = feedback_rxs.iter_mut().map(count_tablet_feedbacks).sum();
    let ok = match expected {
        FeedbackExpectation::Zero => feedbacks == 0,
        FeedbackExpectation::NonZero => feedbacks > 0,
    };
    if !ok {
        return Err(format!(
            "Expected {expected:?} tablet feedback in verify phase, received {feedbacks}"
        ));
    }
    Ok(())
}

/// Runs `attempt` (which reads the given tablets, learns their info, and
/// verifies routing), retrying while tablet migrations invalidate the driver's
/// cache mid-run.
///
/// Tablet info for `base_table` is read before each attempt and passed in. On
/// failure we re-read it: if the tablets are unchanged the failure is real and
/// we panic; if they changed a migration raced the test and we retry.
async fn with_migration_retry<F>(session: &Session, ks: &str, base_table: &str, mut attempt: F)
where
    F: AsyncFnMut(&[Tablet]) -> Result<(), String>,
{
    let mut last_error = None;
    for _ in 0..5 {
        let tablets = get_tablets(session, ks, base_table).await;
        match attempt(&tablets).await {
            Ok(()) => return,
            Err(e) => {
                let new_tablets = get_tablets(session, ks, base_table).await;
                if tablets == new_tablets {
                    // We failed, but there was no migration.
                    panic!("Test attempt failed despite no migration. Error: {e}");
                }
                last_error = Some(e);
                // There was a migration, let's try again.
            }
        }
    }
    panic!(
        "There was a tablet migration during each attempt! Last error: {}",
        last_error.unwrap()
    );
}

/// Prepares the statement(s) needed to run `descriptor` for every key, pairing
/// each key with its prepared statement and bound values.
///
/// When the CQL text does not depend on the key (all key components are bind
/// markers) a single statement is prepared and cloned for each key
/// (`PreparedStatement` is intentionally cheap to clone). Otherwise the text
/// embeds a literal key, so one statement is prepared per key -- and those
/// preparations are done concurrently.
async fn prepare_statements_per_key(
    session: &Session,
    descriptor: QueryDescriptor,
    ks: &str,
    keys: &[(i32, i32)],
) -> Result<Vec<PreparedForKey>, String> {
    let set_manual_lwt = descriptor.op.is_manual_lwt();

    if descriptor.data_form.text_is_key_independent() {
        // The text is the same for every key, so prepare it once and reuse it.
        let (text, _) = build_cql(descriptor, ks, keys[0].0, keys[0].1);
        let mut prepared = session
            .prepare(text)
            .await
            .map_err(|e| format!("prepare failed: {e}"))?;
        if set_manual_lwt {
            prepared.set_consistency(Consistency::Serial);
        }
        Ok(keys
            .iter()
            .map(|&(a, b)| {
                let (_, values) = build_cql(descriptor, ks, a, b);
                (prepared.clone(), values)
            })
            .collect())
    } else {
        // Each key produces a different text, so prepare them concurrently.
        try_join_all(keys.iter().map(|&(a, b)| async move {
            let (text, values) = build_cql(descriptor, ks, a, b);
            let mut prepared = session
                .prepare(text)
                .await
                .map_err(|e| format!("prepare failed: {e}"))?;
            if set_manual_lwt {
                prepared.set_consistency(Consistency::Serial);
            }
            Ok::<_, String>((prepared, values))
        }))
        .await
    }
}

/// Runs a single query variant (subtest) for every tablet key and checks the
/// expected tablet-routing behavior.
///
/// This assumes the driver already has all tablet info populated locally (see
/// [`learn_tablet_info`]).
async fn verify_case(
    session: &Session,
    descriptor: QueryDescriptor,
    ks: &str,
    keys: &[(i32, i32)],
    feedback_rxs: &mut [UnboundedReceiver<(ResponseFrame, Option<TargetShard>)>],
) -> Result<(), String> {
    if !descriptor.data_form.is_prepared() {
        drain_feedbacks(feedback_rxs);
        // Unprepared queries: the server never sends tablet feedback for them,
        // regardless of routing (the driver could not use it anyway). We send to
        // every shard of every node and assert that none produced feedback. The
        // per-key executions are independent, so we run them concurrently.
        try_join_all(keys.iter().map(|&(a, b)| async move {
            let (text, values) = build_cql(descriptor, ks, a, b);
            let mut statement = Statement::new(text);
            if descriptor.op.is_manual_lwt() {
                statement.set_consistency(Consistency::Serial);
            }
            execute_unprepared_statement_everywhere(
                session,
                session.get_cluster_state().as_ref(),
                &statement,
                &*values,
            )
            .await
            .map_err(|e| format!("unprepared execution failed: {e}"))
        }))
        .await?;
        let feedbacks: usize = feedback_rxs.iter_mut().map(count_tablet_feedbacks).sum();
        if feedbacks != 0 {
            return Err(format!(
                "Expected 0 feedbacks for unprepared query, received {feedbacks}"
            ));
        }
        return Ok(());
    }

    let per_key = prepare_statements_per_key(session, descriptor, ks, keys).await?;

    // The driver's own notion of token-awareness must match our expectation. All
    // per-key statements share the same shape, so checking the first is enough.
    let is_token_aware = per_key[0].0.is_token_aware();
    if is_token_aware != descriptor.is_token_aware() {
        return Err(format!(
            "Expected is_token_aware()={}, got {is_token_aware} for {descriptor:?}",
            descriptor.is_token_aware(),
        ));
    }

    // Token-aware queries already have full tablet info, so they route straight
    // to a replica and get no feedback. Non-token-aware prepared queries can't be
    // routed, but the server doesn't bother teaching the driver (the feedback
    // would be useless), so they too get no feedback -- just like unprepared ones.
    //
    // Token-aware LWTs additionally use the LWT optimization: every query for a
    // given key is pinned to a single replica+shard to avoid Paxos contention.
    let require_single_coordinator = descriptor.is_token_aware() && descriptor.op.is_lwt();
    verify_feedback_for_keys(
        session,
        &per_key,
        FeedbackExpectation::Zero,
        require_single_coordinator,
        feedback_rxs,
    )
    .await
    .map_err(|e| format!("{descriptor:?}: {e}"))
}

/// Runs the whole set of query-variant subtests against a freshly populated
/// driver tablet cache. Tablet migrations can invalidate the cache mid-run, so
/// [`with_migration_retry`] re-checks whether a migration happened on failure
/// and, if so, retries the whole attempt.
async fn run_tablet_cases(
    session: &Session,
    ks: &str,
    populate_prepared: &PreparedStatement,
    cases: &[QueryDescriptor],
    feedback_rxs: &mut [UnboundedReceiver<(ResponseFrame, Option<TargetShard>)>],
) {
    with_migration_retry(session, ks, "t", async |tablets| {
        let value_per_tablet = calculate_key_per_tablet(tablets, populate_prepared, |i| (i, 1));

        let per_key = prepared_for_keys(populate_prepared, value_per_tablet.iter().copied());
        learn_tablet_info(session, &per_key, feedback_rxs).await?;

        // Refresh the whole metadata to make sure that the tablet info the
        // driver learned above survives a metadata refresh (which happens
        // periodically and during tablet maintenance). If the refresh dropped
        // the tablet mapping, the token-aware cases below would start
        // receiving feedback again.
        session
            .refresh_metadata()
            .await
            .map_err(|e| format!("refresh_metadata failed: {e}"))?;

        for case in cases {
            verify_case(session, *case, ks, &value_per_tablet, feedback_rxs)
                .await
                .map_err(|e| format!("case {case:?} failed: {e}"))?;
        }
        Ok(())
    })
    .await;
}

/// Exercises tablet routing for a materialized view, asserting the driver's
/// *current, buggy* behavior.
///
/// The tablet cache is keyed by table name, and the maintenance step that runs
/// on every metadata refresh drops any cached tablet whose table is not in the
/// keyspace's `tables` map. Materialized views live in a separate `views` map,
/// so their tablet info is wiped on every refresh, after which the driver falls
/// back to (non-tablet) vnode routing and the server has to keep re-teaching it
/// via tablet feedback.
///
/// This subtest learns the view's tablet info, forces a metadata refresh, and
/// then asserts that token-aware queries against the view *still* receive tablet
/// feedback -- unlike the base-table cases, which receive none.
///
/// TODO: once the driver maintains tablet info for materialized views across
/// metadata refreshes, this subtest should assert ZERO feedback after the
/// refresh, exactly like the base-table cases.
async fn verify_materialized_view_tablet_bug(
    session: &Session,
    feedback_rxs: &mut [UnboundedReceiver<(ResponseFrame, Option<TargetShard>)>],
) {
    // Materialized views on tablet keyspaces require the keyspace to stay
    // RF-rack-valid (RF <= rack count). The CI cluster has a single rack, so we
    // use RF=1 here, unlike the RF=2 base keyspace used elsewhere.
    let ks = unique_keyspace_name();
    session
        .ddl(format!(
            "CREATE KEYSPACE {ks}
            WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}
            AND tablets = {{'enabled': true}}"
        ))
        .await
        .unwrap();
    session
        .ddl(format!(
            "CREATE TABLE {ks}.t (a int, b int, c text, primary key (a, b))"
        ))
        .await
        .unwrap();
    // The view's partition key is `b` (a single column of the base primary key).
    session
        .ddl(format!(
            "CREATE MATERIALIZED VIEW {ks}.mv AS SELECT a, b, c FROM {ks}.t
            WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b, a)"
        ))
        .await
        .unwrap();

    let mv_select = session
        .prepare(format!("SELECT a, b, c FROM {ks}.mv WHERE b = ?"))
        .await
        .unwrap();
    // The driver computes the token from the view's partition key, so the query
    // is token-aware even though the routing itself is currently broken.
    assert!(mv_select.is_token_aware());

    with_migration_retry(session, &ks, "mv", async |tablets| {
        // The view has many tablets; sampling a handful of them (each with a
        // distinct partition-key value) is enough to observe the mis-routing,
        // and keeps the subtest fast.
        const MV_SAMPLED_TABLETS: usize = 8;
        let mv_keys: Vec<(i32,)> = calculate_key_per_tablet(tablets, &mv_select, |i| (i,))
            .into_iter()
            .take(MV_SAMPLED_TABLETS)
            .collect();
        let per_key = prepared_for_keys(&mv_select, mv_keys);

        learn_tablet_info(session, &per_key, feedback_rxs).await?;

        // Refreshing the metadata wipes the view's tablet info (the bug).
        session
            .refresh_metadata()
            .await
            .map_err(|e| format!("refresh_metadata failed: {e}"))?;

        // With correct behavior the driver would now route straight to each
        // tablet's replica and receive no feedback. Because the view's tablet
        // info was dropped, the driver mis-routes and the server teaches it again.
        //
        // TODO: change to `FeedbackExpectation::Zero` once materialized-view
        // tablet routing survives metadata refreshes.
        verify_feedback_for_keys(
            session,
            &per_key,
            FeedbackExpectation::NonZero,
            false,
            feedback_rxs,
        )
        .await
    })
    .await;

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

/// Exercises tablet routing for a CDC log table (`<table>_scylla_cdc_log`).
///
/// Unlike materialized views, CDC log tables are ordinary entries in
/// `system_schema.tables`, so they live in the keyspace's `tables` map and
/// their cached tablet info survives metadata refreshes. Their partition key is
/// the single `blob` column `cdc$stream_id`, routed with the CDC partitioner.
///
/// This subtest learns the log table's tablet info, refreshes the metadata, and
/// asserts that token-aware queries against the log table then receive no
/// tablet feedback -- i.e. routing works correctly, just like for a base table.
async fn verify_cdc_log_tablet_routing(
    session: &Session,
    feedback_rxs: &mut [UnboundedReceiver<(ResponseFrame, Option<TargetShard>)>],
) {
    let ks = unique_keyspace_name();
    session
        .ddl(format!(
            "CREATE KEYSPACE {ks}
            WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}}
            AND tablets = {{'enabled': true}}"
        ))
        .await
        .unwrap();
    session
        .ddl(format!(
            "CREATE TABLE {ks}.t (a int, b int, c text, primary key (a, b))
            WITH cdc = {{'enabled': true}}"
        ))
        .await
        .unwrap();

    let cdc_select = session
        .prepare(format!(
            "SELECT * FROM {ks}.t_scylla_cdc_log WHERE \"cdc$stream_id\" = ?"
        ))
        .await
        .unwrap();
    assert!(cdc_select.is_token_aware());
    assert_eq!(cdc_select.get_partitioner_name(), &PartitionerName::CDC);

    with_migration_retry(session, &ks, "t", async |tablets| {
        // CDC log tables are co-located with their base table: they share the
        // base table's tablets, and a log entry's `cdc$stream_id` is built so
        // that its CDC-partitioner token matches the base write's token. The log
        // table therefore has no tablet rows of its own in `system.tablets`, so
        // we derive the tablet ranges from the base table and build a stream id
        // whose token lands in each one.
        //
        // The base table has many tablets; sampling a handful keeps the subtest
        // fast while still exercising several distinct tablets.
        const CDC_SAMPLED_TABLETS: usize = 8;
        let sampled = &tablets[..CDC_SAMPLED_TABLETS.min(tablets.len())];
        let per_key = prepared_for_keys(
            &cdc_select,
            cdc_stream_id_key_per_tablet(sampled)
                .into_iter()
                .map(|key| (key,)),
        );

        learn_tablet_info(session, &per_key, feedback_rxs).await?;

        // The cached tablet info must survive a metadata refresh (CDC log
        // tables are regular tables, so unlike views they are not dropped by
        // the tablet-cache maintenance step).
        session
            .refresh_metadata()
            .await
            .map_err(|e| format!("refresh_metadata failed: {e}"))?;

        // Token-aware queries should now route straight to a replica without
        // any feedback.
        verify_feedback_for_keys(
            session,
            &per_key,
            FeedbackExpectation::Zero,
            false,
            feedback_rxs,
        )
        .await
    })
    .await;

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

/// Installs a response rule on every proxy node that forwards each `Result`
/// response frame (except event registrations) into a per-node channel, and
/// returns the receiving ends. The subtests inspect these to count how many
/// responses carried tablet-routing feedback.
fn install_feedback_channels(
    running_proxy: &mut RunningProxy,
) -> Vec<UnboundedReceiver<(ResponseFrame, Option<TargetShard>)>> {
    let (feedback_txs, feedback_rxs): (Vec<_>, Vec<_>) = (0..3)
        .map(|_| mpsc::unbounded_channel::<(ResponseFrame, Option<TargetShard>)>())
        .unzip();
    for (i, tx) in feedback_txs.into_iter().enumerate() {
        running_proxy.running_nodes[i].change_response_rules(Some(vec![ResponseRule(
            Condition::ResponseOpcode(ResponseOpcode::Result)
                .and(Condition::not(Condition::ConnectionRegisteredAnyEvent)),
            ResponseReaction::noop().with_feedback_when_performed(tx),
        )]));
    }
    feedback_rxs
}

/// Shared harness for the tablet subtests.
///
/// Spins up a 3-node proxied cluster, builds a session, skips the test if the
/// server doesn't support tablets, installs the feedback channels, and finally
/// runs `test_body` with the session and the feedback receivers. The proxy
/// bookkeeping (including tolerating the expected disconnect at shutdown) is
/// handled here so each subtest can focus on its own schema and assertions.
async fn with_tablet_session<F, Fut>(test_body: F)
where
    F: FnOnce(Session, Vec<UnboundedReceiver<(ResponseFrame, Option<TargetShard>)>>) -> Fut,
    Fut: Future<Output = ()>,
{
    setup_tracing();

    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let session = scylla::client::session_builder::SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .default_execution_profile_handle(
                    ExecutionProfile::builder()
                        .retry_policy(Arc::new(LwtBallotBugRetryPolicy))
                        .build()
                        .into_handle(),
                )
                .build()
                .await
                .unwrap();

            if !scylla_supports_tablets(&session).await {
                tracing::warn!("Skipping test because this Scylla version doesn't support tablets");
                return running_proxy;
            }

            let feedback_rxs = install_feedback_channels(&mut running_proxy);

            test_body(session, feedback_rxs).await;

            running_proxy
        },
    )
    .await;
    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}

/// Exercises the driver's tablet awareness for a base table across many query
/// variants (the full [`QueryDescriptor`] matrix), all sharing a single keyspace
/// and table because creating keyspaces/tables is expensive.
#[tokio::test]
async fn test_tablets() {
    const TABLET_COUNT: usize = 16;

    with_tablet_session(|session, mut feedback_rxs| async move {
        let ks = unique_keyspace_name();

        /* Prepare schema */
        prepare_schema(&session, &ks, "t", TABLET_COUNT).await;

        // Enter the keyspace so that unqualified `t` (KsPresence::WithoutKs)
        // resolves to the test table.
        session.use_keyspace(&ks, false).await.unwrap();

        // Token-aware INSERT used to populate the driver's tablet cache and to
        // compute a representative key per tablet.
        let prepared_insert = session
            .prepare(format!("INSERT INTO {ks}.t (a, b, c) VALUES (?, ?, 'abc')"))
            .await
            .unwrap();

        let mut cases = vec![
            // Default policy is tablet-aware: token-aware INSERT gets no
            // feedback once the driver has all tablet info.
            QueryDescriptor {
                op: Op::Insert,
                ks_presence: KsPresence::WithKs,
                data_form: DataForm::RegularPrepared,
            },
            // The same token-aware, no-feedback expectation holds for every
            // plain (non-LWT) CQL operation.
            QueryDescriptor {
                op: Op::Select,
                ks_presence: KsPresence::WithKs,
                data_form: DataForm::RegularPrepared,
            },
            QueryDescriptor {
                op: Op::Update,
                ks_presence: KsPresence::WithKs,
                data_form: DataForm::RegularPrepared,
            },
            QueryDescriptor {
                op: Op::Delete,
                ks_presence: KsPresence::WithKs,
                data_form: DataForm::RegularPrepared,
            },
            // Scylla never sends tablet feedback for unprepared queries.
            QueryDescriptor {
                op: Op::Insert,
                ks_presence: KsPresence::WithKs,
                data_form: DataForm::ConcreteUnprepared,
            },
            // A prepared statement whose values are all literals is not
            // token-aware (no partition-key marker), so the driver cannot
            // route it. The server does not send tablet feedback for such
            // queries either (it would be useless), just like for unprepared
            // ones, so no feedback is expected.
            QueryDescriptor {
                op: Op::Select,
                ks_presence: KsPresence::WithKs,
                data_form: DataForm::ConcretePrepared,
            },
            // Likewise when only the clustering key is a marker but the
            // partition key is a literal: still not token-aware, still no
            // feedback.
            QueryDescriptor {
                op: Op::Select,
                ks_presence: KsPresence::WithKs,
                data_form: DataForm::ConcretePreparedMixed,
            },
            // The keyspace does not have to be spelled out in the query; an
            // unqualified table name (resolved via the session keyspace) is
            // routed the same way.
            QueryDescriptor {
                op: Op::Select,
                ks_presence: KsPresence::WithoutKs,
                data_form: DataForm::RegularPrepared,
            },
            QueryDescriptor {
                op: Op::Insert,
                ks_presence: KsPresence::WithoutKs,
                data_form: DataForm::ConcreteUnprepared,
            },
        ];

        // LWT statements are routed to a single replica in a fixed order to
        // avoid Paxos contention. This optimization only applies to
        // token-aware (fully bound partition key) statements. We exercise
        // every flavor of LWT: the three conditional forms plus a "manual"
        // Paxos read forced via SERIAL consistency. Gated on the server
        // supporting LWTs on tablet tables.
        if supports_feature(&session, "LWT_WITH_TABLETS").await {
            cases.extend([
                QueryDescriptor {
                    op: Op::LwtUpdateIf,
                    ks_presence: KsPresence::WithKs,
                    data_form: DataForm::RegularPrepared,
                },
                QueryDescriptor {
                    op: Op::LwtUpdateIfExists,
                    ks_presence: KsPresence::WithKs,
                    data_form: DataForm::RegularPrepared,
                },
                QueryDescriptor {
                    op: Op::LwtInsertIfNotExists,
                    ks_presence: KsPresence::WithKs,
                    data_form: DataForm::RegularPrepared,
                },
                QueryDescriptor {
                    op: Op::LwtManualSelect,
                    ks_presence: KsPresence::WithKs,
                    data_form: DataForm::RegularPrepared,
                },
            ]);
        }

        run_tablet_cases(&session, &ks, &prepared_insert, &cases, &mut feedback_rxs).await;

        session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
    })
    .await;
}

/// Exercises tablet routing for a materialized view, asserting the driver's
/// *current, buggy* behavior. Runs in its own keyspace (see
/// [`verify_materialized_view_tablet_bug`] for the details of the bug and why
/// the keyspace must use RF=1).
#[tokio::test]
async fn test_tablets_materialized_view() {
    with_tablet_session(|session, mut feedback_rxs| async move {
        // Gated on the server supporting materialized views on tablet tables.
        if !supports_feature(&session, "VIEWS_WITH_TABLETS").await {
            tracing::warn!(
                "Skipping materialized-view subtest because this Scylla version \
                 doesn't support views on tablet tables"
            );
            return;
        }

        verify_materialized_view_tablet_bug(&session, &mut feedback_rxs).await;
    })
    .await;
}

/// Exercises tablet routing for a CDC log table. Runs in its own keyspace, since
/// enabling CDC is a per-keyspace/table property (see
/// [`verify_cdc_log_tablet_routing`] for details).
#[tokio::test]
async fn test_tablets_cdc_log() {
    with_tablet_session(|session, mut feedback_rxs| async move {
        // Gated on the server supporting CDC on tablet tables.
        if !supports_feature(&session, "CDC_WITH_TABLETS").await {
            tracing::warn!(
                "Skipping CDC subtest because this Scylla version doesn't support \
                 CDC on tablet tables"
            );
            return;
        }

        verify_cdc_log_tablet_routing(&session, &mut feedback_rxs).await;
    })
    .await;
}
