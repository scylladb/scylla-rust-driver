use super::{LoadBalancingPolicy, Plan, Statement};
use crate::transport::cluster::ClusterData;

#[derive(Debug, Default)]
pub struct DefaultPolicy {}

impl DefaultPolicy {
    pub fn new() -> Self {
        Self {}
    }
}

impl LoadBalancingPolicy for DefaultPolicy {
    fn plan<'a>(&self, _statement: &Statement, cluster: &'a ClusterData) -> Plan<'a> {
        Box::new(cluster.known_peers.values().cloned())
    }

    fn name(&self) -> String {
        "DefaultPolicy".to_string()
    }
}
