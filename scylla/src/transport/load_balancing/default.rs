use super::{LoadBalancingPolicy, Plan, Statement};
use crate::transport::cluster::ClusterData;

pub struct DefaultPolicy {}

impl DefaultPolicy {
    pub fn new() -> Self {
        Self {}
    }
}

impl LoadBalancingPolicy for DefaultPolicy {
    fn plan<'a>(&self, _statement: &Statement, _cluster: &'a ClusterData) -> Plan<'a> {
        todo!();
    }

    fn name(&self) -> String {
        "DefaultPolicy".to_string()
    }
}
