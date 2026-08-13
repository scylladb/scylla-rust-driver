use std::ops::Deref;
use std::sync::Arc;

use crate::utils::{PerformDDL, setup_tracing, test_with_3_node_cluster, unique_keyspace_name};
use assert_matches::assert_matches;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session_builder::SessionBuilder;
use scylla::cluster::ClusterState;
use scylla::cluster::NodeRef;
use scylla::policies::load_balancing::{LoadBalancingPolicy, RoutingInfo};
use scylla::policies::retry::{RetryPolicy, RetrySession};
use scylla::policies::speculative_execution::SpeculativeExecutionPolicy;
use scylla::routing::Shard;
use scylla::statement::SerialConsistency;
use scylla::statement::batch::{Batch, BatchStatement, BatchType};
use scylla::statement::unprepared::Statement;
use scylla_cql::Consistency;
use tokio::sync::mpsc;

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
    consistency_reporter: mpsc::UnboundedSender<Consistency>,
}

impl<const NODE: u8> BoundToPredefinedNodePolicy<NODE> {
    fn report_node(&self, report: Report) {
        self.profile_reporter.send((report, NODE)).unwrap();
    }
    fn report_consistency(&self, c: Consistency) {
        self.consistency_reporter.send(c).unwrap();
    }
}

impl<const NODE: u8> LoadBalancingPolicy for BoundToPredefinedNodePolicy<NODE> {
    fn pick<'a>(
        &'a self,
        _info: &'a RoutingInfo,
        cluster: &'a ClusterState,
    ) -> Option<(NodeRef<'a>, Option<Shard>)> {
        self.report_node(Report::LoadBalancing);
        cluster
            .get_nodes_info()
            .iter()
            .next()
            .map(|node| (node, None))
    }

    fn fallback<'a>(
        &'a self,
        _info: &'a RoutingInfo,
        _cluster: &'a ClusterState,
    ) -> scylla::policies::load_balancing::FallbackPlan<'a> {
        Box::new(std::iter::empty())
    }

    fn on_request_success(
        &self,
        _query: &RoutingInfo,
        _latency: std::time::Duration,
        _node: NodeRef<'_>,
    ) {
    }

    fn on_request_failure(
        &self,
        _query: &RoutingInfo,
        _latency: std::time::Duration,
        _node: NodeRef<'_>,
        _error: &scylla::errors::RequestAttemptError,
    ) {
    }

    fn name(&self) -> String {
        "BoundToPredefinedNodePolicy".to_owned()
    }
}

impl<const NODE: u8> RetryPolicy for BoundToPredefinedNodePolicy<NODE> {
    fn new_session(&self) -> Box<dyn scylla::policies::retry::RetrySession> {
        self.report_node(Report::RetryPolicy);
        Box::new(self.clone())
    }
}

impl<const NODE: u8> RetrySession for BoundToPredefinedNodePolicy<NODE> {
    fn decide_should_retry(
        &mut self,
        request_info: scylla::policies::retry::RequestInfo,
    ) -> scylla::policies::retry::RetryDecision {
        self.report_consistency(request_info.consistency);
        scylla::policies::retry::RetryDecision::DontRetry
    }

    fn reset(&mut self) {}
}

impl<const NODE: u8> SpeculativeExecutionPolicy for BoundToPredefinedNodePolicy<NODE> {
    fn max_retry_count(&self, _: &scylla::policies::speculative_execution::Context) -> usize {
        1
    }

    fn retry_interval(
        &self,
        _: &scylla::policies::speculative_execution::Context,
    ) -> std::time::Duration {
        self.report_node(Report::SpeculativeExecution);
        std::time::Duration::from_millis(200)
    }
}

#[tokio::test]
async fn test_execution_profiles() {
    setup_tracing();
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
            .retry_policy(Arc::new(policy1.deref().clone()))
            .consistency(Consistency::One)
            .serial_consistency(None)
            .speculative_execution_policy(None)
            .build();

        let profile2 = ExecutionProfile::builder()
            .load_balancing_policy(policy2.clone())
            .retry_policy(Arc::new(policy2.deref().clone()))
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
        session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3}}")).await.unwrap();
        session
            .ddl(
                format!(
                    "CREATE TABLE IF NOT EXISTS {ks}.t (a int, b int, c text, primary key (a, b))"
                ),
            )
            .await
            .unwrap();

        let mut query = Statement::from(format!("INSERT INTO {ks}.t (a, b, c) VALUES (1, 2, 'abc')"));
        let mut prepared = session.prepare(format!("INSERT INTO {ks}.t (a, b, c) VALUES (1, 2, 'abc')")).await.unwrap();
        let mut batch = Batch::new_with_statements(BatchType::Unlogged, vec![BatchStatement::Query(query.clone())]);

        while profile_rx.try_recv().is_ok() {}
        consistency_rx.try_recv().unwrap_err();

        // On the happy path (no attempt fails), the driver never creates a RetrySession
        // (it is instantiated lazily, upon the first attempt error), so only the load
        // balancing policy reports itself.
        async fn assert_lb_report(
            profile_rx: &mut mpsc::UnboundedReceiver<(Report, u8)>,
            expected_node: u8,
        ) {
            let report = profile_rx.recv().await.unwrap();
            assert_matches!(report, (Report::LoadBalancing, node) if node == expected_node);
            profile_rx.try_recv().unwrap_err();
        }

        // Once an attempt fails, the RetrySession is created, so both policies report themselves.
        async fn assert_lb_and_retry_reports(
            profile_rx: &mut mpsc::UnboundedReceiver<(Report, u8)>,
            expected_node: u8,
        ) {
            let report1 = profile_rx.recv().await.unwrap();
            let report2 = profile_rx.recv().await.unwrap();
            assert_matches!(
                (report1, report2),
                ((Report::LoadBalancing, n1), (Report::RetryPolicy, n2))
                    | ((Report::RetryPolicy, n1), (Report::LoadBalancing, n2))
                    if n1 == expected_node && n2 == expected_node
            );
            profile_rx.try_recv().unwrap_err();
        }


        /* Test load balancing policy */

        // Run on default per-session execution profile
        session.query_unpaged(query.clone(), &[]).await.unwrap();
        assert_lb_report(&mut profile_rx, 1).await;

        session.execute_unpaged(&prepared, &[]).await.unwrap();
        assert_lb_report(&mut profile_rx, 1).await;

        session.batch(&batch, ((),)).await.unwrap();
        assert_lb_report(&mut profile_rx, 1).await;

        // Run on query-specific execution profile
        query.set_execution_profile_handle(Some(profile2.clone().into_handle()));
        session.query_unpaged(query.clone(), &[]).await.unwrap();
        assert_lb_report(&mut profile_rx, 2).await;

        prepared.set_execution_profile_handle(Some(profile2.clone().into_handle()));
        session.execute_unpaged(&prepared, &[]).await.unwrap();
        assert_lb_report(&mut profile_rx, 2).await;

        batch.set_execution_profile_handle(Some(profile2.clone().into_handle()));
        session.batch(&batch, ((),)).await.unwrap();
        assert_lb_report(&mut profile_rx, 2).await;

        // Run again on default per-session execution profile
        query.set_execution_profile_handle(None);
        session.query_unpaged(query.clone(), &[]).await.unwrap();
        assert_lb_report(&mut profile_rx, 1).await;

        prepared.set_execution_profile_handle(None);
        session.execute_unpaged(&prepared, &[]).await.unwrap();
        assert_lb_report(&mut profile_rx, 1).await;

        batch.set_execution_profile_handle(None);
        session.batch(&batch, ((),)).await.unwrap();
        assert_lb_report(&mut profile_rx, 1).await;


        /* Test retry policy and consistencies */
        let rule_overloaded = RequestRule(
            Condition::True,
            RequestReaction::forge().overloaded()
        );
        for i in 0..=2 {
            running_proxy.running_nodes[i].change_request_rules(Some(vec![rule_overloaded.clone()]));
        }

        profile_rx.try_recv().unwrap_err();

        // Run non-LWT on default per-session execution profile
        session.query_unpaged(query.clone(), &[]).await.unwrap_err();
        let report_consistency = consistency_rx.recv().await.unwrap();
        assert_matches!(report_consistency, Consistency::One);
        consistency_rx.try_recv().unwrap_err();
        assert_lb_and_retry_reports(&mut profile_rx, 1).await;

        session.execute_unpaged(&prepared, &[]).await.unwrap_err();
        let report_consistency = consistency_rx.recv().await.unwrap();
        assert_matches!(report_consistency, Consistency::One);
        consistency_rx.try_recv().unwrap_err();
        assert_lb_and_retry_reports(&mut profile_rx, 1).await;

        session.batch(&batch, ((),)).await.unwrap_err();
        let report_consistency = consistency_rx.recv().await.unwrap();
        assert_matches!(report_consistency, Consistency::One);
        consistency_rx.try_recv().unwrap_err();
        assert_lb_and_retry_reports(&mut profile_rx, 1).await;

        // Run on statement-specific execution profile
        query.set_execution_profile_handle(Some(profile2.clone().into_handle()));
        session.query_unpaged(query.clone(), &[]).await.unwrap_err();
        let report_consistency = consistency_rx.recv().await.unwrap();
        assert_matches!(report_consistency, Consistency::Two);
        consistency_rx.try_recv().unwrap_err();
        assert_lb_and_retry_reports(&mut profile_rx, 2).await;

        prepared.set_execution_profile_handle(Some(profile2.clone().into_handle()));
        session.execute_unpaged(&prepared, &[]).await.unwrap_err();
        let report_consistency = consistency_rx.recv().await.unwrap();
        assert_matches!(report_consistency, Consistency::Two);
        consistency_rx.try_recv().unwrap_err();
        assert_lb_and_retry_reports(&mut profile_rx, 2).await;

        batch.set_execution_profile_handle(Some(profile2.clone().into_handle()));
        session.batch(&batch, ((),)).await.unwrap_err();
        let report_consistency = consistency_rx.recv().await.unwrap();
        assert_matches!(report_consistency, Consistency::Two);
        consistency_rx.try_recv().unwrap_err();
        assert_lb_and_retry_reports(&mut profile_rx, 2).await;

        // Run with statement-set specific options
        query.set_consistency(Consistency::Three);
        session.query_unpaged(query.clone(), &[]).await.unwrap_err();
        let report_consistency = consistency_rx.recv().await.unwrap();
        assert_matches!(report_consistency, Consistency::Three);
        consistency_rx.try_recv().unwrap_err();
        assert_lb_and_retry_reports(&mut profile_rx, 2).await;

        prepared.set_consistency(Consistency::Three);
        session.execute_unpaged(&prepared, &[]).await.unwrap_err();
        let report_consistency = consistency_rx.recv().await.unwrap();
        assert_matches!(report_consistency, Consistency::Three);
        consistency_rx.try_recv().unwrap_err();
        assert_lb_and_retry_reports(&mut profile_rx, 2).await;

        batch.set_consistency(Consistency::Three);
        session.batch(&batch, ((),)).await.unwrap_err();
        let report_consistency = consistency_rx.recv().await.unwrap();
        assert_matches!(report_consistency, Consistency::Three);
        consistency_rx.try_recv().unwrap_err();
        assert_lb_and_retry_reports(&mut profile_rx, 2).await;

        for i in 0..=2 {
            running_proxy.running_nodes[i].change_request_rules(None);
        }

        session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();

        running_proxy
    }).await;
    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
