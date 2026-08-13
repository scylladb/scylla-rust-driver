//! Compile-time guard on how deeply the driver's public futures are nested.
//!
//! Awaiting an `async fn` embeds the callee's anonymous future type as a field
//! of the caller's, so a chain of N awaits produces a type nested N deep, and
//! `rustc` must recurse just as deep to compute its layout. That recursion is
//! bounded by `recursion_limit`, and - this is the part that makes it our
//! problem rather than a private one - the limit that applies is the one of
//! *the crate being compiled*, not of the crate that defines the futures.
//!
//! So when `SessionBuilder::build()`'s future gets too deep, it is not this
//! crate that stops compiling: it is every user crate that awaits it, with
//!
//! ```text
//! error: queries overflow the depth limit!
//!   = help: consider increasing the recursion limit by adding a
//!           `#![recursion_limit = "256"]` attribute to your crate
//! ```
//!
//! and nothing the user can do about it short of that attribute. Our doctests
//! and integration tests are separate crates too, so they hit it first - but
//! only as a puzzling CI failure whose obvious "fix" is to paste the attribute
//! everywhere and let the real depth go on growing.
//!
//! Hence this file. It awaits the deepest chains we expose - session
//! establishment and request execution - under a `recursion_limit` far below the
//! 128 that `rustc` defaults to, so that growing those chains past the budget
//! breaks this test, with the note below, instead of breaking users.
//!
//! At the time of writing the deepest of them - request execution - compiles at
//! a limit of 68 but not at 64, against the budget of 80 set here and the 128 a
//! user crate has. Session establishment, the chain this guard was written for,
//! needs under 40.
//!
//! # If this test stops compiling
//!
//! Do not raise the limit here - the number is the point. Find the `.await`
//! chain that grew and break it with a `Box::pin`, the way `Cluster::new` and
//! `MetadataWorker::fetch_on_candidate` do: `Pin<Box<_>>` is a pointer, and the
//! layout computation does not descend through it. Raise the budget only
//! deliberately, knowing that it is spent out of a user's 128.
#![recursion_limit = "80"]
#![allow(missing_docs)]

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::{ExecutionError, NewSessionError};
use scylla::response::query_result::QueryResult;

/// Session establishment: control connection, initial metadata fetch, pools.
async fn _establish() -> Result<Session, NewSessionError> {
    SessionBuilder::new()
        .known_node("127.0.0.1:9042")
        .build()
        .await
}

/// Request execution: the other chain that user code awaits directly.
async fn _execute(session: &Session) -> Result<QueryResult, ExecutionError> {
    session
        .query_unpaged("SELECT * FROM system.local", &[])
        .await
}

/// Nothing to run: the test is that this file compiles under the
/// `recursion_limit` set above.
#[test]
fn futures_are_shallow_enough_for_user_crates() {}
