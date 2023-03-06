mod utils;

use std::ops::Deref;
use std::sync::Arc;

use assert_matches::assert_matches;
use scylla::batch::BatchStatement;
use scylla::batch::{Batch, BatchType};
use scylla::query::Query;
use scylla::statement::SerialConsistency;
use scylla::{
    frame::types::LegacyConsistency,
    load_balancing::{LoadBalancingPolicy, Plan, RoutingInfo},
    retry_policy::{RetryPolicy, RetrySession},
    speculative_execution::SpeculativeExecutionPolicy,
    test_utils::unique_keyspace_name,
    transport::ClusterData,
    ExecutionProfile, SessionBuilder,
};
use scylla_cql::Consistency;
use tokio::sync::mpsc;
use utils::test_with_3_node_cluster;

use scylla_proxy::{
    Condition, ProxyError, RequestReaction, RequestRule, ShardAwareness, WorkerError,
};

#[derive(Debug)]
enum Report {
    LoadBalancing,
    RetryPolicy,
    SpeculativeExecution,
}

#[derive(Debug, Clone)]
struct BoundToPredefinedNodePolicy<const NODE: u8> {
    profile_reporter: mpsc::UnboundedSender<(Report, u8)>,
    consistency_reporter: mpsc::UnboundedSender<LegacyConsistency>,
}

impl<const NODE: u8> BoundToPredefinedNodePolicy<NODE> {
    fn report_node(&self, report: Report) {
        self.profile_reporter.send((report, NODE)).unwrap();
    }
    fn report_consistency(&self, c: LegacyConsistency) {
        self.consistency_reporter.send(c).unwrap();
    }
}

impl<const NODE: u8> LoadBalancingPolicy for BoundToPredefinedNodePolicy<NODE> {
    fn plan<'a>(&self, _: &RoutingInfo, cluster: &'a ClusterData) -> Plan<'a> {
        self.report_node(Report::LoadBalancing);
        let node = cluster.get_nodes_info().iter().next().unwrap();
        Box::new(std::iter::once(node.clone()))
    }

    fn name(&self) -> String {
        "BoundToPredefinedNodePolicy".to_owned()
    }
}

impl<const NODE: u8> RetryPolicy for BoundToPredefinedNodePolicy<NODE> {
    fn new_session(&self) -> Box<dyn scylla::retry_policy::RetrySession> {
        self.report_node(Report::RetryPolicy);
        Box::new(self.clone())
    }

    fn clone_boxed(&self) -> Box<dyn RetryPolicy> {
        Box::new(self.clone())
    }
}

impl<const NODE: u8> RetrySession for BoundToPredefinedNodePolicy<NODE> {
    fn decide_should_retry(
        &mut self,
        query_info: scylla::retry_policy::QueryInfo,
    ) -> scylla::retry_policy::RetryDecision {
        self.report_consistency(query_info.consistency);
        scylla::retry_policy::RetryDecision::DontRetry
    }

    fn reset(&mut self) {}
}

impl<const NODE: u8> SpeculativeExecutionPolicy for BoundToPredefinedNodePolicy<NODE> {
    fn max_retry_count(&self, _: &scylla::speculative_execution::Context) -> usize {
        1
    }

    fn retry_interval(&self, _: &scylla::speculative_execution::Context) -> std::time::Duration {
        self.report_node(Report::SpeculativeExecution);
        std::time::Duration::from_millis(200)
    }
}

#[tokio::test]
#[ntest::timeout(20000)]
async fn test_execution_profiles() {
    let res = test_with_3_node_cluster(ShardAwareness::QueryNode, |proxy_uris, translation_map, mut running_proxy| async move {

        let (routing_tx, mut profile_rx) = mpsc::unbounded_channel();
        let (consistency_tx, mut consistency_rx) = mpsc::unbounded_channel();

        let policy1 = Arc::new(BoundToPredefinedNodePolicy::<1> {
            profile_reporter: routing_tx.clone(),
            consistency_reporter: consistency_tx.clone(),
        });
        let policy2 = Arc::new(BoundToPredefinedNodePolicy::<2> {
            profile_reporter: routing_tx.clone(),
            consistency_reporter: consistency_tx.clone(),
        });

        let profile1 = ExecutionProfile::builder()
            .load_balancing_policy(policy1.clone())
            .retry_policy(Box::new(policy1.deref().clone()))
            .consistency(Consistency::One)
            .serial_consistency(None)
            .speculative_execution_policy(None)
            .build();

        let profile2 = ExecutionProfile::builder()
            .load_balancing_policy(policy2.clone())
            .retry_policy(Box::new(policy2.deref().clone()))
            .consistency(Consistency::Two)
            .serial_consistency(Some(SerialConsistency::LocalSerial))
            .speculative_execution_policy(Some(policy2))
            .build();

        let session = SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .address_translator(Arc::new(translation_map))
            .default_execution_profile_handle(profile1.into_handle())
            .build()
            .await
            .unwrap();
        let ks = unique_keyspace_name();

        /* Prepare schema */
        session.query(format!("CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 3}}", ks), &[]).await.unwrap();
        session
            .query(
                format!(
                    "CREATE TABLE IF NOT EXISTS {}.t (a int, b int, c text, primary key (a, b))",
                    ks
                ),
                &[],
            )
            .await
            .unwrap();

        let mut query = Query::from(format!("INSERT INTO {}.t (a, b, c) VALUES (1, 2, 'abc')", ks));
        let mut prepared = session.prepare(format!("INSERT INTO {}.t (a, b, c) VALUES (1, 2, 'abc')", ks)).await.unwrap();
        let mut batch = Batch::new_with_statements(BatchType::Unlogged, vec![BatchStatement::Query(query.clone())]);

        while profile_rx.try_recv().is_ok() {}
        consistency_rx.try_recv().unwrap_err();


        /* Test load balancing and retry policy */

        // Run on default per-session execution profile
        session.query(query.clone(), &[]).await.unwrap();
        let report1 = profile_rx.recv().await.unwrap();
        let report2 = profile_rx.recv().await.unwrap();
        assert_matches!((report1, report2), ((Report::LoadBalancing, 1), (Report::RetryPolicy, 1)) | ((Report::RetryPolicy, 1), (Report::LoadBalancing, 1)));
        profile_rx.try_recv().unwrap_err();

        session.execute(&prepared, &[]).await.unwrap();
        let report1 = profile_rx.recv().await.unwrap();
        let report2 = profile_rx.recv().await.unwrap();
        assert_matches!((report1, report2), ((Report::LoadBalancing, 1), (Report::RetryPolicy, 1)) | ((Report::RetryPolicy, 1), (Report::LoadBalancing, 1)));
        profile_rx.try_recv().unwrap_err();

        session.batch(&batch, ((),)).await.unwrap();
        let report1 = profile_rx.recv().await.unwrap();
        let report2 = profile_rx.recv().await.unwrap();
        assert_matches!((report1, report2), ((Report::LoadBalancing, 1), (Report::RetryPolicy, 1)) | ((Report::RetryPolicy, 1), (Report::LoadBalancing, 1)));
        profile_rx.try_recv().unwrap_err();

        // Run on query-specific execution profile
        query.set_execution_profile_handle(Some(profile2.clone().into_handle()));
        session.query(query.clone(), &[]).await.unwrap();
        let report1 = profile_rx.recv().await.unwrap();
        let report2 = profile_rx.recv().await.unwrap();
        assert_matches!((report1, report2), ((Report::LoadBalancing, 2), (Report::RetryPolicy, 2)) | ((Report::RetryPolicy, 2), (Report::LoadBalancing, 2)));
        profile_rx.try_recv().unwrap_err();

        prepared.set_execution_profile_handle(Some(profile2.clone().into_handle()));
        session.execute(&prepared, &[]).await.unwrap();
        let report1 = profile_rx.recv().await.unwrap();
        let report2 = profile_rx.recv().await.unwrap();
        assert_matches!((report1, report2), ((Report::LoadBalancing, 2), (Report::RetryPolicy, 2)) | ((Report::RetryPolicy, 2), (Report::LoadBalancing, 2)));
        profile_rx.try_recv().unwrap_err();

        batch.set_execution_profile_handle(Some(profile2.clone().into_handle()));
        session.batch(&batch, ((),)).await.unwrap();
        let report1 = profile_rx.recv().await.unwrap();
        let report2 = profile_rx.recv().await.unwrap();
        assert_matches!((report1, report2), ((Report::LoadBalancing, 2), (Report::RetryPolicy, 2)) | ((Report::RetryPolicy, 2), (Report::LoadBalancing, 2)));
        profile_rx.try_recv().unwrap_err();

        // Run again on default per-session execution profile
        query.set_execution_profile_handle(None);
        session.query(query.clone(), &[]).await.unwrap();
        let report1 = profile_rx.recv().await.unwrap();
        let report2 = profile_rx.recv().await.unwrap();
        assert_matches!((report1, report2), ((Report::LoadBalancing, 1), (Report::RetryPolicy, 1)) | ((Report::RetryPolicy, 1), (Report::LoadBalancing, 1)));
        profile_rx.try_recv().unwrap_err();

        prepared.set_execution_profile_handle(None);
        session.execute(&prepared, &[]).await.unwrap();
        let report1 = profile_rx.recv().await.unwrap();
        let report2 = profile_rx.recv().await.unwrap();
        assert_matches!((report1, report2), ((Report::LoadBalancing, 1), (Report::RetryPolicy, 1)) | ((Report::RetryPolicy, 1), (Report::LoadBalancing, 1)));
        profile_rx.try_recv().unwrap_err();

        batch.set_execution_profile_handle(None);
        session.batch(&batch, ((),)).await.unwrap();
        let report1 = profile_rx.recv().await.unwrap();
        let report2 = profile_rx.recv().await.unwrap();
        assert_matches!((report1, report2), ((Report::LoadBalancing, 1), (Report::RetryPolicy, 1)) | ((Report::RetryPolicy, 1), (Report::LoadBalancing, 1)));
        profile_rx.try_recv().unwrap_err();


        /* Test consistencies */
        let rule_overloaded = RequestRule(
            Condition::True,
            RequestReaction::forge().overloaded()
        );
        for i in 0..=2 {
            running_proxy.running_nodes[i].change_request_rules(Some(vec![rule_overloaded.clone()]));
        }

        profile_rx.try_recv().unwrap_err();

        // Run non-LWT on default per-session execution profile
        session.query(query.clone(), &[]).await.unwrap_err();
        let report_consistency = consistency_rx.recv().await.unwrap();
        assert_matches!(report_consistency, LegacyConsistency::Regular(Consistency::One));
        consistency_rx.try_recv().unwrap_err();

        session.execute(&prepared, &[]).await.unwrap_err();
        let report_consistency = consistency_rx.recv().await.unwrap();
        assert_matches!(report_consistency, LegacyConsistency::Regular(Consistency::One));
        consistency_rx.try_recv().unwrap_err();

        session.batch(&batch, ((),)).await.unwrap_err();
        let report_consistency = consistency_rx.recv().await.unwrap();
        assert_matches!(report_consistency, LegacyConsistency::Regular(Consistency::One));
        consistency_rx.try_recv().unwrap_err();

        // Run on statement-specific execution profile
        query.set_execution_profile_handle(Some(profile2.clone().into_handle()));
        session.query(query.clone(), &[]).await.unwrap_err();
        let report_consistency = consistency_rx.recv().await.unwrap();
        assert_matches!(report_consistency, LegacyConsistency::Regular(Consistency::Two));
        consistency_rx.try_recv().unwrap_err();

        prepared.set_execution_profile_handle(Some(profile2.clone().into_handle()));
        session.execute(&prepared, &[]).await.unwrap_err();
        let report_consistency = consistency_rx.recv().await.unwrap();
        assert_matches!(report_consistency, LegacyConsistency::Regular(Consistency::Two));
        consistency_rx.try_recv().unwrap_err();

        batch.set_execution_profile_handle(Some(profile2.clone().into_handle()));
        session.batch(&batch, ((),)).await.unwrap_err();
        let report_consistency = consistency_rx.recv().await.unwrap();
        assert_matches!(report_consistency, LegacyConsistency::Regular(Consistency::Two));
        consistency_rx.try_recv().unwrap_err();

        // Run with statement-set specific options
        query.set_consistency(Consistency::Three);
        session.query(query.clone(), &[]).await.unwrap_err();
        let report_consistency = consistency_rx.recv().await.unwrap();
        assert_matches!(report_consistency, LegacyConsistency::Regular(Consistency::Three));
        consistency_rx.try_recv().unwrap_err();

        prepared.set_consistency(Consistency::Three);
        session.execute(&prepared, &[]).await.unwrap_err();
        let report_consistency = consistency_rx.recv().await.unwrap();
        assert_matches!(report_consistency, LegacyConsistency::Regular(Consistency::Three));
        consistency_rx.try_recv().unwrap_err();

        batch.set_consistency(Consistency::Three);
        session.batch(&batch, ((),)).await.unwrap_err();
        let report_consistency = consistency_rx.recv().await.unwrap();
        assert_matches!(report_consistency, LegacyConsistency::Regular(Consistency::Three));
        consistency_rx.try_recv().unwrap_err();

        for i in 0..=2 {
            running_proxy.running_nodes[i].change_request_rules(None);
        }

        running_proxy
    }).await;
    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
