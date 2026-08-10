// This hopefully fixes the following error that occurs in CI:
// error: queries overflow the depth limit!
//   |
//   = help: consider increasing the recursion limit by adding a `#![recursion_limit = "256"]` attribute to your crate (`integration`)
//   = note: query depth increased by 130 when computing layout of `{async block@scylla/tests/integration/ccm/authenticate.rs:39:1: 39:15}`
#![recursion_limit = "256"]
// Rigorous documentation is not necessary for integration tests.
#![allow(missing_docs)]

pub(crate) mod ccm;
mod load_balancing;
mod macros;
mod metadata;
mod session;
mod statements;
mod types;
pub(crate) mod utils;
