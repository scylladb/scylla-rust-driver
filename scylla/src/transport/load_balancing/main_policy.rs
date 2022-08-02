use super::{
    LBPlan, LoadBalancingPolicy, NetworkStrategyPlan, NoTokenNetworkStrategyPlan,
    SimpleStrategyPlan, StatementInfo,
};
use crate::routing::Token;

use crate::transport::{cluster::ClusterData, topology::Strategy};
use rand::seq::SliceRandom;
use smallvec::SmallVec;
use std::borrow::Cow;
use std::collections::HashMap;

pub struct MainPolicy {
    // TODO: We could detect local_dc by latency
    local_dc: Option<String>,
    dummy_network_strategy_dc_repfactors: HashMap<String, usize>,
}

impl MainPolicy {
    pub fn new(local_dc: Option<String>) -> MainPolicy {
        let dummy_network_strategy_dc_name: String = match &local_dc {
            Some(local_dc_name) => local_dc_name.to_string(),
            None => "dummy_dc_name".to_string(),
        };

        MainPolicy {
            local_dc,
            dummy_network_strategy_dc_repfactors: [(dummy_network_strategy_dc_name, 3)]
                .into_iter()
                .collect(),
        }
    }

    fn plan_simple_strategy<'a>(
        &'a self,
        token_opt: &Option<Token>,
        replication_factor: usize,
        cluster: &'a ClusterData,
    ) -> LBPlan<'a> {
        LBPlan::SimpleStrategyPlan(SimpleStrategyPlan::new(
            token_opt,
            replication_factor,
            cluster,
        ))
    }

    fn plan_network_strategy<'a>(
        &'a self,
        token: &Option<Token>,
        local_dc_name: &'a str,
        datacenter_repfactors: &'a HashMap<String, usize>,
        cluster: &'a ClusterData,
    ) -> LBPlan<'a> {
        match token {
            Some(token) => LBPlan::NetworkStrategyPlan(NetworkStrategyPlan::new(
                *token,
                datacenter_repfactors,
                local_dc_name,
                cluster,
            )),
            None => LBPlan::NoTokenNetworkStrategyPlan(NoTokenNetworkStrategyPlan::new(
                local_dc_name,
                cluster,
            )),
        }
    }

    fn plan_with_keyspace_strategy<'a>(
        &'a self,
        token: &Option<Token>,
        strategy: &'a Strategy,
        cluster: &'a ClusterData,
    ) -> LBPlan<'a> {
        match strategy {
            Strategy::SimpleStrategy { replication_factor } => {
                self.plan_simple_strategy(token, *replication_factor, cluster)
            }
            Strategy::LocalStrategy => self.plan_simple_strategy(token, 1, cluster),
            Strategy::NetworkTopologyStrategy {
                datacenter_repfactors,
            } => {
                let local_dc_name: &str = match &self.local_dc {
                    Some(dc_name) => dc_name,
                    None => {
                        // Choose random DC and pretend this one is our own
                        let dc_names: SmallVec<[&str; 8]> =
                            datacenter_repfactors.keys().map(|s| s.as_str()).collect();
                        dc_names
                            .choose(&mut rand::thread_rng())
                            .copied()
                            .unwrap_or("no_dcs_dc")
                    }
                };

                self.plan_network_strategy(token, local_dc_name, datacenter_repfactors, cluster)
            }
            Strategy::Other { .. } => self.plan_without_keyspace_strategy(token, cluster),
        }
    }

    fn plan_without_keyspace_strategy<'a>(
        &'a self,
        token: &Option<Token>,
        cluster: &'a ClusterData,
    ) -> LBPlan<'a> {
        match &self.local_dc {
            Some(local_dc_name) => self.plan_network_strategy(
                token,
                local_dc_name,
                &self.dummy_network_strategy_dc_repfactors,
                cluster,
            ),
            None => self.plan_simple_strategy(token, 3, cluster),
        }
    }
}

impl LoadBalancingPolicy for MainPolicy {
    fn plan<'a>(
        &'a self,
        statement_info: &'a StatementInfo,
        cluster: &'a ClusterData,
    ) -> LBPlan<'a> {
        if let Some(ks_name) = &statement_info.keyspace {
            if let Some(keyspace) = cluster.get_keyspace_info().get(*ks_name) {
                return self.plan_with_keyspace_strategy(
                    &statement_info.token,
                    &keyspace.strategy,
                    cluster,
                );
            }
        }

        self.plan_without_keyspace_strategy(&statement_info.token, cluster)
    }

    fn name(&self) -> Cow<'_, str> {
        Cow::Owned(format!("MainPolicy {{ local_dc = {:?} }}", self.local_dc))
    }
}
