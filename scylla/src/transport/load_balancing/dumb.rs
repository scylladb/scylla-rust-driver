use super::{LoadBalancingPolicy, Plan, Statement};
use crate::transport::cluster::ClusterData;

pub struct DumbPolicy;

impl LoadBalancingPolicy for DumbPolicy {
    fn plan<'a>(&self, _statement: &Statement, cluster: &'a ClusterData) -> Plan<'a> {
        Box::new(cluster.all_nodes.iter().cloned())
    }

    fn name(&self) -> String {
        "DumbPolicy".to_string()
    }
}
