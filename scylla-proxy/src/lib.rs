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
