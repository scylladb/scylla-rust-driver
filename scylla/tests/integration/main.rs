// Rigorous documentation is not necessary for integration tests.
#![allow(missing_docs)]
// Tests spawn tasks that belong to no session.
#![allow(clippy::disallowed_methods)]

pub(crate) mod ccm;
pub(crate) mod entry_point;
mod load_balancing;
mod macros;
mod metadata;
mod session;
mod statements;
mod types;
pub(crate) mod utils;
