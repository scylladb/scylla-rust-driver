//! Shows how to group data-modifying statements into a single batch.
//!
//! A batch is an atomicity and grouping tool, not a throughput optimization.
//! Sending N statements as one batch is usually *slower* than sending them as N
//! concurrent requests: the whole batch goes to one coordinator, which then has
//! to fan the statements out itself, where N separate requests would each go
//! straight to a replica of their own partition. A multi-partition `LOGGED`
//! batch additionally pays for a batchlog write. If what you want is
//! throughput, see the `parallel` example instead.
//!
//! The one coordinator is not picked blindly: when the batch's *first*
//! statement is prepared and its values bind the partition key, the driver
//! routes the batch to a replica — and the right shard — of that statement's
//! partition. Grouping a batch by partition is therefore what makes it cheaper.
//! A batch whose first statement is unprepared, or whose partition key is
//! written as a literal rather than a bind marker, lands on an arbitrary node.
//!
//! WARNING: Do not use unprepared statements with values in batches. Before
//! sending a batch, driver will need to prepare them all.
//!
//! What a batch does buy you is that the statements it contains are applied
//! together — but note that isolation only holds *within a single partition*.
//! Statements touching several partitions may be observed partially applied by
//! a concurrent reader, even in a `LOGGED` batch; the batchlog only guarantees
//! that they all eventually complete.
//!
//! Only `INSERT`, `UPDATE` and `DELETE` statements are allowed in a batch.

use anyhow::Result;
use futures::TryStreamExt as _;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::Consistency;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::unprepared::Statement;
use std::env;

/// All rows written by this example share one partition key, so the batches
/// below really are single-partition ones.
const ACCOUNT_ID: i32 = 1;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "172.42.0.2:9042".to_string());

    println!("Connecting to {uri} ...");

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    // `account_id` is the partition key and `entry_id` the clustering key, so
    // all entries of one account live in the same partition.
    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.batch_ledger (account_id int, entry_id int, description text, primary key (account_id, entry_id))",
            &[],
        )
        .await?;

    // ---------------------------------------------------------------------
    // 1. Building a batch out of statements of every supported kind.
    // ---------------------------------------------------------------------

    // The default batch type is `Logged`.
    let mut batch: Batch = Default::default();

    // An unprepared statement given as plain text.
    batch.append_statement(
        "INSERT INTO examples_ks.batch_ledger (account_id, entry_id, description) VALUES (1, 1, 'from statement text')",
    );

    // An unprepared statement built by hand, which lets you configure it.
    let unprepared: Statement = Statement::new(
        "INSERT INTO examples_ks.batch_ledger (account_id, entry_id, description) VALUES (1, 2, 'from a Statement')",
    );
    batch.append_statement(unprepared);

    // A prepared statement. This is the kind you want: the driver already knows
    // its metadata, so nothing extra has to happen when the batch is sent.
    let prepared = session
        .prepare(
            "INSERT INTO examples_ks.batch_ledger (account_id, entry_id, description) VALUES (?, ?, ?)",
        )
        .await?;
    batch.append_statement(prepared.clone());

    // Batch values are a tuple with exactly one entry per statement, in order.
    // Statements without bind markers still need an entry — the empty tuple.
    //
    // The two unprepared statements above carry no values, which is what keeps
    // this batch cheap: an unprepared statement with a *non-empty* value list
    // forces the driver to prepare it first, once per `Session::batch` call,
    // sequentially, with no caching in between. See part 2 for the fix.
    let batch_values = ((), (), (ACCOUNT_ID, 3, "from a prepared statement"));

    session.batch(&batch, batch_values).await?;

    // ---------------------------------------------------------------------
    // 2. `Session::prepare_batch` — preparing a whole batch in one go.
    // ---------------------------------------------------------------------

    let mut unprepared_batch: Batch = Default::default();
    unprepared_batch.append_statement(
        "INSERT INTO examples_ks.batch_ledger (account_id, entry_id, description) VALUES (?, ?, ?)",
    );
    unprepared_batch.append_statement(
        "UPDATE examples_ks.batch_ledger SET description = ? WHERE account_id = ? AND entry_id = ?",
    );

    // Prepares every statement of the batch concurrently and returns a batch
    // that keeps the original batch's type and options. Do this once and reuse
    // the result, exactly like you would reuse a `PreparedStatement`.
    let prepared_batch: Batch = session.prepare_batch(&unprepared_batch).await?;

    session
        .batch(
            &prepared_batch,
            (
                (ACCOUNT_ID, 4, "inserted by a prepared batch"),
                ("updated by a prepared batch", ACCOUNT_ID, 2),
            ),
        )
        .await?;

    // ---------------------------------------------------------------------
    // 3. Batch type and batch options.
    // ---------------------------------------------------------------------

    // `Logged` (the default) makes the cluster write the batch to a batchlog
    // first, so that a coordinator failure cannot leave the mutations half
    // applied forever. That costs extra round trips, and it buys nothing when
    // the batch stays inside one partition — such a batch is applied atomically
    // anyway, and the server downgrades a single-partition `Logged` batch to
    // `Unlogged` as an optimization. Saying `Unlogged` here does not change the
    // outcome, then; it records that we know this batch does not need the
    // batchlog, rather than leaning on the server to notice. Reach for it
    // deliberately whenever you do not need that guarantee. (`Counter` is the
    // third type, required for — and only usable with — batched counter
    // updates.)
    //
    // `Batch::new_with_statements` builds the batch and its contents at once;
    // `Batch::new(BatchType::Unlogged)` would give the same empty batch to fill
    // in with `append_statement`.
    let mut batch = Batch::new_with_statements(
        BatchType::Unlogged,
        vec![prepared.clone().into(), prepared.into()],
    );

    // Options are set on the `Batch` itself and apply to the whole batch.
    let consistency = Consistency::One;
    batch.set_consistency(consistency);

    println!(
        "Running an {:?} batch at consistency {consistency:?} ...",
        batch.get_type()
    );

    session
        .batch(
            &batch,
            (
                (ACCOUNT_ID, 5, "unlogged, single partition"),
                (ACCOUNT_ID, 6, "unlogged, single partition"),
            ),
        )
        .await?;

    // ---------------------------------------------------------------------
    // Read back what the batches wrote.
    // ---------------------------------------------------------------------

    let mut rows = session
        .query_iter(
            "SELECT entry_id, description FROM examples_ks.batch_ledger WHERE account_id = ?",
            (ACCOUNT_ID,),
        )
        .await?
        .rows_stream::<(i32, String)>()?;

    while let Some((entry_id, description)) = rows.try_next().await? {
        println!("entry {entry_id}: {description}");
    }

    println!("Ok.");

    Ok(())
}
