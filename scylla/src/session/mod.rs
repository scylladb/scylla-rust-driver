mod caching_session;

// The purpose of session::session module is to not have any complex logic in mod.rs.
// No code external to this module will ever see this awkward path, because the inner
// session module is pub(self), and its items are only accessible through the below
// glob re-export.
#[allow(clippy::module_inception)]
mod session;
pub use session::*;

pub mod session_builder;
#[cfg(test)]
mod session_test;

#[allow(deprecated)]
pub use caching_session::{CachingSession, GenericCachingSession, LegacyCachingSession};
#[cfg(feature = "cloud")]
pub use session_builder::CloudSessionBuilder;
pub use session_builder::SessionBuilder;
