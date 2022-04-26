use super::{LoadBalancingPolicy, Plan, QueryInfo};
use crate::transport::cluster::ClusterData;

pub struct DefaultPolicy {}

impl DefaultPolicy {
    pub fn new() -> Self {
        Self {}
    }
}

impl LoadBalancingPolicy for DefaultPolicy {
    fn plan<'a>(&self, _info: &QueryInfo, _cluster: &'a ClusterData) -> Plan<'a> {
        todo!();
    }
}
