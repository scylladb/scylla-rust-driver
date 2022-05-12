use super::{Event, LoadBalancingPolicy, Plan, QueryInfo};
use crate::transport::cluster::{ClusterData, EventConsumer};

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

impl EventConsumer for DefaultPolicy {
    fn consume(&self, _events: &[Event], _cluster: &ClusterData) {
        todo!();
    }
}
