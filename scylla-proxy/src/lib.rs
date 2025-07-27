mod actions;
mod errors;
mod frame;
mod proxy;

pub type TargetShard = u16;

pub use actions::{
    example_db_errors, Action, Condition, Reaction, RequestReaction, RequestRule, ResponseReaction,
    ResponseRule,
};
pub use errors::{DoorkeeperError, ProxyError, WorkerError};
pub use frame::{RequestFrame, RequestOpcode, ResponseFrame, ResponseOpcode};
pub use proxy::{Node, Proxy, RunningProxy, ShardAwareness};

pub use proxy::get_exclusive_local_address;

#[cfg(test)]
pub(crate) fn setup_tracing() {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::Layer;

    let testing_layer = tracing_subscriber::fmt::layer()
        .with_test_writer()
        .with_filter(tracing_subscriber::EnvFilter::from_default_env());
    let noop_layer = tracing_subscriber::fmt::layer().with_writer(std::io::sink);
    let _ = tracing_subscriber::registry()
        .with(testing_layer)
        .with(noop_layer)
        .try_init();
}
