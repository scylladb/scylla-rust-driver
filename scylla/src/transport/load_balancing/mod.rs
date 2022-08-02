//! Load balancing configurations\
//! `Session` can use any load balancing policy which implements the `LoadBalancingPolicy` trait\
//! Policies which implement the `ChildLoadBalancingPolicy` can be wrapped in some other policies\
//! See [the book](https://rust-driver.docs.scylladb.com/stable/load-balancing/load-balancing.html) for more information

use super::{cluster::ClusterData, node::Node};
use crate::routing::Token;

use std::borrow::Cow;
use std::sync::{Arc, Mutex};

mod dumb;
pub mod load_balancing_data;
pub mod precomputed_replicas;
pub mod random_order_iter;
pub mod token_ring;
pub mod tried_nodes_set;

pub use dumb::{DumbPlan, DumbPolicy};
pub use load_balancing_data::LoadBalancingData;
pub use precomputed_replicas::PrecomputedReplicas;
pub use random_order_iter::RandomOrderIter;
pub use token_ring::TokenRing;
pub use tried_nodes_set::TriedNodesSet;

/// Represents info about statement that can be used by load balancing policies.
#[derive(Default)]
pub struct StatementInfo<'a> {
    pub token: Option<Token>,
    pub keyspace: Option<&'a str>,
}

/// Policy that decides which nodes to contact for each query
pub trait LoadBalancingPolicy: Send + Sync {
    /// It is used for each query to find which nodes to query.\
    /// The generated plan can borrow any of the arguments for the whole query.
    fn plan<'a>(
        &'a self,
        statement_info: &'a StatementInfo,
        cluster: &'a ClusterData,
    ) -> LBPlan<'a>;

    /// Returns name of this load balancing policy
    fn name(&self) -> Cow<'_, str>;
}

/// Represents a load balancing plan generated by a load balancing policy.\
/// Common policies have their plan added as an enum to avoid needless allocations.\
/// In case of a custom policy it has to allocate a Box with its plan inside.
pub enum LBPlan<'a> {
    Dumb(DumbPlan<'a>),
    Custom(Box<dyn LoadBalancingPlan<'a> + Send + 'a>),
}

/// Each load balancing plan implements this trait.\
/// A plan is almost like an Iterator<Item = &Arc<Node>> but the lifetimes must be correct.\
pub trait LoadBalancingPlan<'a> {
    fn next(&mut self) -> Option<&'a Arc<Node>>;
}

impl<'a> LoadBalancingPlan<'a> for LBPlan<'a> {
    fn next(&mut self) -> Option<&'a Arc<Node>> {
        match self {
            LBPlan::Dumb(dumb_plan) => dumb_plan.next(),
            LBPlan::Custom(custom_plan) => custom_plan.next(),
        }
    }
}

/// Sometimes a load balancing plan needs to be shared between multiple threads.
/// This happens e.g. in case of speculative execution where multiple query attempts
/// try to use consecutive nodes from a single plan. This is a wrapper over LBPlan which
/// converts it into a thread safe version. Then LoadBalancingPlan is implemented
/// for &SharedLBPlan which can be passed to other functions like any other plan.
/// TODO: It could be implemented in a more efficient way, for example it could use
/// AtomicUsize in round robin instead of using Mutex.
pub struct SharedLBPlan<'a> {
    plan: Mutex<LBPlan<'a>>,
}

impl<'a> From<LBPlan<'a>> for SharedLBPlan<'a> {
    fn from(plan: LBPlan<'a>) -> SharedLBPlan<'a> {
        SharedLBPlan {
            plan: Mutex::new(plan),
        }
    }
}

impl<'a> LoadBalancingPlan<'a> for &SharedLBPlan<'a> {
    fn next(&mut self) -> Option<&'a Arc<Node>> {
        let mut_ref = &mut self.plan.lock().unwrap();
        mut_ref.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    #[test]
    fn test_custom_policy_compiles() {
        struct CustomPolicy;
        impl LoadBalancingPolicy for CustomPolicy {
            fn plan<'a>(
                &'a self,
                statement_info: &'a StatementInfo,
                cluster: &'a ClusterData,
            ) -> LBPlan<'a> {
                LBPlan::Custom(Box::new(DumbPolicy.plan(statement_info, cluster)))
            }

            fn name(&self) -> Cow<'_, str> {
                Cow::Borrowed("Custom")
            }
        }
    }
}
