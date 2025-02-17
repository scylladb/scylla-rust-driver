mod actions;
mod errors;
mod frame;
mod proxy;

pub type TargetShard = u16;

pub use actions::{
    example_db_errors, Action, Condition, ConditionHandler, Reaction, RequestReaction, RequestRule,
    ResponseReaction, ResponseRule,
};
pub use errors::{DoorkeeperError, ProxyError, WorkerError};
pub use frame::{FrameOpcode, RequestFrame, RequestOpcode, ResponseFrame, ResponseOpcode};
pub use proxy::{Node, Proxy, RunningProxy, ShardAwareness};

pub use proxy::get_exclusive_local_address;

#[cfg(test)]
pub(crate) fn setup_tracing() {
    let _ = tracing_subscriber::fmt::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(tracing_subscriber::fmt::TestWriter::new())
        .try_init();
}
