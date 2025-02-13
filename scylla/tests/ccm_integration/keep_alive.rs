use crate::common::utils::test_with_3_node_cluster;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

use scylla_proxy::{
    Condition, ProxyError, Reaction, RequestOpcode, RequestReaction, RequestRule, ShardAwareness,
    WorkerError,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio::time::sleep;

/// Waits for a specified number of keep-alive messages to be received within a given timeout period.
///
/// # Arguments
///
/// * `request_rx` - An `UnboundedReceiver` that receives the keep-alive messages.
/// * `expected_number_of_keep_alive` - The number of keep-alive messages expected to be received.
/// * `timeout` - The maximum duration to wait for the expected number of keep-alive messages.
///
/// # Panics
///
/// This function will panic if the number of received keep-alive messages does not match the
/// expected number within the timeout period.
///
async fn wait_for_keep_alive<T>(
    mut request_rx: UnboundedReceiver<T>,
    expected_number_of_keep_alive: u32,
    timeout: Duration,
) {
    let mut total_keep_alives: u32 = 0;
    let start = tokio::time::Instant::now();
    while total_keep_alives < expected_number_of_keep_alive && start.elapsed() < timeout {
        if let Ok(_t) = request_rx.try_recv() {
            total_keep_alives += 1;
        }
        sleep(Duration::from_millis(1)).await;
    }
    println!("Total keep alive: {}", total_keep_alives);
    assert_eq!(total_keep_alives, expected_number_of_keep_alive);
}

#[tokio::test]
#[cfg(not(scylla_cloud_tests))]
async fn node_should_go_down_gracefully_when_connection_closed_during_heartbeat() {
    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let _session: Session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .keepalive_interval(Duration::from_millis(20))
                .build()
                .await
                .unwrap();

            // TODO: No way to get node status, no as in java-driver

            // Stop listening for new connections (so it can't reconnect)

            let request_rule = |tx| {
                vec![RequestRule(
                    Condition::RequestOpcode(RequestOpcode::Options),
                    RequestReaction::drop_connection().with_feedback_when_performed(tx),
                )]
            };
            let (request_tx, request_rx) = mpsc::unbounded_channel();
            running_proxy.running_nodes[0]
                .change_request_rules(Some(request_rule(request_tx.clone())));

            // Wait for heartbeat and for node to subsequently close its connection.

            wait_for_keep_alive(request_rx, 1, Duration::from_secs(1)).await;

            //int heartbeatCount = getHeartbeatsForNode().size();
            // When node receives a heartbeat, close the connection.
            //simulacronNode.prime(
            //    when(Options.INSTANCE)
            //        .then(closeConnection(DisconnectAction.Scope.CONNECTION, CloseType.DISCONNECT)));

            // Wait for heartbeat and for node to subsequently close its connection.
            //waitForDown(node);

            // Should have been a heartbeat received since that's what caused the disconnect.
            // assertThat(getHeartbeatsForNode().size()).isGreaterThan(heartbeatCount);

            running_proxy
        },
    )
    .await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}

#[tokio::test]
#[cfg(not(scylla_cloud_tests))]
async fn should_not_send_heartbeat_during_protocol_initialization() {
    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let request_rule = |tx| {
                vec![RequestRule(
                    Condition::RequestOpcode(RequestOpcode::Options),
                    RequestReaction::drop_connection().with_feedback_when_performed(tx),
                )]
            };
            let (request_tx, request_rx) = mpsc::unbounded_channel();
            running_proxy.running_nodes[0]
                .change_request_rules(Some(request_rule(request_tx.clone())));

            let result = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .keepalive_interval(Duration::from_millis(20))
                .build()
                .await;
            assert!(result.is_err());

            wait_for_keep_alive(request_rx, 1, Duration::from_secs(1)).await;

            running_proxy
        },
    )
    .await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}

#[tokio::test]
#[cfg(not(scylla_cloud_tests))]
async fn should_send_heartbeat_on_control_connection() {
    let res = test_with_3_node_cluster(
        ShardAwareness::QueryNode,
        |proxy_uris, translation_map, mut running_proxy| async move {
            let request_rule = |tx| {
                vec![RequestRule(
                    Condition::RequestOpcode(RequestOpcode::Options)
                        .and(Condition::ConnectionRegisteredAnyEvent),
                    RequestReaction::noop().with_feedback_when_performed(tx),
                )]
            };

            let (request_tx, request_rx) = mpsc::unbounded_channel();
            running_proxy.running_nodes[0]
                .change_request_rules(Some(request_rule(request_tx.clone())));

            let _session = SessionBuilder::new()
                .known_node(proxy_uris[0].as_str())
                .address_translator(Arc::new(translation_map))
                .keepalive_interval(Duration::from_millis(20))
                //.tcp_keepalive_interval(Duration::from_millis(20))
                .build()
                .await
                .unwrap();

            wait_for_keep_alive(request_rx, 1, Duration::from_secs(1)).await;

            running_proxy
        },
    )
    .await;

    match res {
        Ok(()) => (),
        Err(ProxyError::Worker(WorkerError::DriverDisconnected(_))) => (),
        Err(err) => panic!("{}", err),
    }
}

#[tokio::test]
#[cfg(not(scylla_cloud_tests))]
async fn should_send_heartbeat_on_regular_connection() {
    /*
        // Prime a simple query so we get at least some results
    simulacronNode.prime(when(QUERY).then(PrimeDsl.rows().row("column1", "1", "column2", "2")));

    try (CqlSession session = newSession()) {
      // Make a bunch of queries over two seconds.  This should preempt any heartbeats.
      assertThat(session.execute(QUERY)).hasSize(1);
      final AtomicInteger nonControlHeartbeats = countHeartbeatsOnRegularConnection();
      for (int i = 0; i < 20; i++) {
        assertThat(session.execute(QUERY)).hasSize(1);
        MILLISECONDS.sleep(100);
      }

      // No heartbeats should be sent, except those on the control connection.
      assertThat(nonControlHeartbeats.get()).isZero();

      // Stop querying, heartbeats should be sent again
      await()
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .atMost(60, TimeUnit.SECONDS)
          .until(() -> nonControlHeartbeats.get() >= 1);
    }
     */
}

#[tokio::test]
#[cfg(not(scylla_cloud_tests))]
async fn should_send_heartbeat_when_requests_being_written_but_nothing_received() {
    /*
    // Prime a query that will never return a response.
    String noResponseQueryStr = "delay";
    SIMULACRON_RULE.cluster().prime(when(noResponseQueryStr).then(noResult()));

    try (CqlSession session = newSession()) {
      AtomicInteger heartbeats = countHeartbeatsOnRegularConnection();

      for (int i = 0; i < 25; i++) {
        session.executeAsync(noResponseQueryStr);
        session.executeAsync(noResponseQueryStr);
        MILLISECONDS.sleep(100);
      }

      // We should expect at least 2 heartbeats
      assertThat(heartbeats.get()).isGreaterThanOrEqualTo(2);
    }
    */
}

#[tokio::test]
#[cfg(not(scylla_cloud_tests))]
async fn should_close_connection_when_heartbeat_times_out() {
    /*
    try (CqlSession session = newSession()) {
        Node node = session.getMetadata().getNodes().values().iterator().next();
        assertThat(node.getState()).isEqualTo(NodeState.UP);

        // Ensure we get some heartbeats and the node remains up.
        AtomicInteger heartbeats = new AtomicInteger();
        simulacronNode.registerQueryListener(
            (n, l) -> heartbeats.incrementAndGet(), true, this::isOptionRequest);

        await()
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .atMost(60, TimeUnit.SECONDS)
            .until(() -> heartbeats.get() >= 2);
        assertThat(node.getState()).isEqualTo(NodeState.UP);

        // configure node to not respond to options request, which should cause a timeout.
        simulacronNode.prime(when(Options.INSTANCE).then(noResult()));
        heartbeats.set(0);

        // wait for heartbeat to be sent.
        await()
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .atMost(60, TimeUnit.SECONDS)
            .until(() -> heartbeats.get() >= 1);
        heartbeats.set(0);

        // node should go down because heartbeat was unanswered.
        waitForDown(node);

        // clear prime so now responds to options request again.
        simulacronNode.clearPrimes();

        // wait for node to come up again and ensure heartbeats are successful and node remains up.
        waitForUp(node);

        await()
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .atMost(60, TimeUnit.SECONDS)
            .until(() -> heartbeats.get() >= 2);
        assertThat(node.getState()).isEqualTo(NodeState.UP);
      }
    */
}
