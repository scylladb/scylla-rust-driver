use futures::Future;
use itertools::Itertools;
use scylla::load_balancing::LoadBalancingPolicy;
use scylla::query::Query;
use scylla::retry_policy::FallthroughRetryPolicy;
use scylla::speculative_execution::SimpleSpeculativeExecutionPolicy;
use scylla::transport::session::Session;
use scylla::SessionBuilder;
use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, instrument::WithSubscriber};

use scylla_proxy::{
    Condition, Node, Proxy, ProxyError, Reaction, RequestOpcode, RequestReaction, RequestRule,
    RunningProxy, ShardAwareness, WorkerError,
};

fn init_logger() {
    let _ = tracing_subscriber::fmt::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .without_time()
        .try_init();
}

#[derive(Debug)]
struct FixedOrderLoadBalancer;
impl LoadBalancingPolicy for FixedOrderLoadBalancer {
    fn plan<'a>(
        &self,
        _statement: &scylla::load_balancing::Statement,
        cluster: &'a scylla::transport::ClusterData,
    ) -> scylla::load_balancing::Plan<'a> {
        Box::new(
            cluster
                .get_nodes_info()
                .clone()
                .into_iter()
                .sorted_by(|node1, node2| Ord::cmp(&node1.address, &node2.address)),
        )
    }

    fn name(&self) -> String {
        "FixedOrderLoadBalancer".to_string()
    }
}

async fn test_with_3_node_cluster<F, Fut>(
    first_proxy_node_addr_last_octet: u16,
    test: F,
) -> Result<(), ProxyError>
where
    F: FnOnce([String; 3], HashMap<SocketAddr, SocketAddr>, RunningProxy) -> Fut,
    Fut: Future<Output = RunningProxy>,
{
    init_logger();
    let real1_uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let proxy1_uri = format!("127.0.0.{}:9042", first_proxy_node_addr_last_octet);
    let real2_uri = env::var("SCYLLA_URI2").unwrap_or_else(|_| "127.0.0.2:9042".to_string());
    let proxy2_uri = format!("127.0.0.{}:9042", first_proxy_node_addr_last_octet + 1);
    let real3_uri = env::var("SCYLLA_URI3").unwrap_or_else(|_| "127.0.0.3:9042".to_string());
    let proxy3_uri = format!("127.0.0.{}:9042", first_proxy_node_addr_last_octet + 2);

    let real1_addr = SocketAddr::from_str(real1_uri.as_str()).unwrap();
    let proxy1_addr = SocketAddr::from_str(proxy1_uri.as_str()).unwrap();
    let real2_addr = SocketAddr::from_str(real2_uri.as_str()).unwrap();
    let proxy2_addr = SocketAddr::from_str(proxy2_uri.as_str()).unwrap();
    let real3_addr = SocketAddr::from_str(real3_uri.as_str()).unwrap();
    let proxy3_addr = SocketAddr::from_str(proxy3_uri.as_str()).unwrap();

    let proxy = Proxy::new(
        [
            (proxy1_addr, real1_addr),
            (proxy2_addr, real2_addr),
            (proxy3_addr, real3_addr),
        ]
        .map(|(proxy_addr, real_addr)| {
            Node::builder()
                .real_address(real_addr)
                .proxy_address(proxy_addr)
                .shard_awareness(ShardAwareness::QueryNode)
                .build()
        }),
    );

    let translation_map = proxy.translation_map();
    let running_proxy = proxy.run().with_current_subscriber().await.unwrap();

    let running_proxy = test(
        [proxy1_uri, proxy2_uri, proxy3_uri],
        translation_map,
        running_proxy,
    )
    .await;

    running_proxy.finish().await
}

#[tokio::test]
#[ntest::timeout(5000)]
async fn speculative_execution_is_fired() {
    const TIMEOUT_PER_REQUEST: Duration = Duration::from_millis(1000);
    let res = test_with_3_node_cluster(217, |proxy_uris, translation_map, mut running_proxy| async move {
        // DB preparation phase
        let session: Session = SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .speculative_execution(Arc::new(SimpleSpeculativeExecutionPolicy {
                max_retry_count: 2,
                retry_interval: Duration::from_millis(10),
            }))
            .retry_policy(Box::new(FallthroughRetryPolicy))
            .address_translator(Arc::new(translation_map))
            .build()
            .await
            .unwrap();

        session
            .query("DROP KEYSPACE IF EXISTS ks2", &[])
            .await
            .unwrap();
        session.query("CREATE KEYSPACE IF NOT EXISTS ks2 WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3}", &[]).await.unwrap();
        session
            .query("CREATE TABLE ks2.t (a int primary key)", &[])
            .await
            .unwrap();

        let mut q = Query::from("INSERT INTO ks2.t (a) VALUES (?)");
        q.set_is_idempotent(true); // this is to allow speculative execution to fire

        let drop_frame_rule = RequestRule(
            Condition::RequestOpcode(RequestOpcode::Query)
                .and(Condition::BodyContainsCaseSensitive(Box::new(*b"ks2.t"))),
            RequestReaction::drop_frame(),
        );

        info!("--------------------- BEGINNING main test part ----------------");

        info!("--------------------- first query - no rules  ----------------");
        // first run before any rules
        session.query(q.clone(), (3,)).await.unwrap();

        info!("--------------------- second query - 0 and 2 nodes not responding  ----------------");
        running_proxy.running_nodes[0]
            .change_request_rules(Some(vec![drop_frame_rule.clone()]))
            .await;
        running_proxy.running_nodes[2]
            .change_request_rules(Some(vec![drop_frame_rule.clone()]))
            .await;

        session.query(q.clone(), (2,)).await.unwrap();

        info!("--------------------- third query - 0 and 1 nodes not responding  ----------------");
        running_proxy.running_nodes[2]
            .change_request_rules(None)
            .await;
        running_proxy.running_nodes[1]
            .change_request_rules(Some(vec![drop_frame_rule.clone()]))
            .await;

        session.query(q.clone(), (1,)).await.unwrap();


        info!("--------------------- fourth query - all nodes not responding  ----------------");
        running_proxy.running_nodes[2]
        .change_request_rules(Some(vec![drop_frame_rule]))
        .await;

        tokio::select! {
            res = session.query(q, (0,)) => panic!("Rules did not work: received response {:?}", res),
            _ = tokio::time::sleep(TIMEOUT_PER_REQUEST) => (),
        };

        info!("--------------------- FINISHING main test part ----------------");

        running_proxy
    }).await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}

#[tokio::test]
#[ntest::timeout(10000)]
async fn retries_occur() {
    let res = test_with_3_node_cluster(210, |proxy_uris, translation_map, mut running_proxy| async move {

        // DB preparation phase
        let session: Session = SessionBuilder::new()
            .known_node(proxy_uris[0].as_str())
            .address_translator(Arc::new(translation_map))
            .build()
            .await
            .unwrap();

        session
            .query("DROP KEYSPACE IF EXISTS ks1", &[])
            .await
            .unwrap();
        session.query("CREATE KEYSPACE IF NOT EXISTS ks1 WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3}", &[]).await.unwrap();
        session
            .query("CREATE TABLE ks1.t (a int primary key)", &[])
            .await
            .unwrap();

        let mut q = Query::from("INSERT INTO ks1.t (a) VALUES (?)");
        q.set_is_idempotent(true); // this is to allow retry to fire

        let forge_error_rule = RequestRule(
            Condition::RequestOpcode(RequestOpcode::Query)
                .and(Condition::BodyContainsCaseSensitive(Box::new(*b"INTO ks1.t"))),
            RequestReaction::forge().server_error(),
        );

        info!("--------------------- BEGINNING main test part ----------------");

        info!("--------------------- first query - no rules  ----------------");
        session.query(q.clone(), (3,)).await.unwrap();

        info!("--------------------- second query - 0 and 2 nodes not responding  ----------------");
        running_proxy.running_nodes[0]
            .change_request_rules(Some(vec![forge_error_rule.clone()]))
            .await;
        running_proxy.running_nodes[2]
            .change_request_rules(Some(vec![forge_error_rule.clone()]))
            .await;

        session.query(q.clone(), (2,)).await.unwrap();

        info!("--------------------- third query - all nodes not responding  ----------------");
        running_proxy.running_nodes[1]
            .change_request_rules(Some(vec![forge_error_rule]))
            .await;

        session.query(q.clone(), (1,)).await.unwrap_err();

        info!("--------------------- fourth query - 0 and 1 nodes not responding  ----------------");
        running_proxy.running_nodes[2]
        .change_request_rules(None)
        .await;

        session.query(q, (1,)).await.unwrap();

        info!("--------------------- FINISHING main test part ----------------");

        running_proxy
    }).await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}
