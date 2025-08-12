//! Implements integration with cluster-side tracing mechanism.
//!
//! The mechanism is used to trace queries and their execution
//! across the cluster, providing insights into query performance and execution flow.
//!
//! If user requests tracing for a query, the cluster will
//! record the statement execution details in `system_traces.sessions` and `system_traces.events`,
//! as well as return a tracing ID in the response, which can be used to query the tracing
//! info later.

use crate::DeserializeRow;
use crate::value::CqlTimestamp;
use itertools::Itertools;
use scylla_cql::value::CqlTimeuuid;
use std::collections::HashMap;
use std::net::IpAddr;

/// Tracing info retrieved from `system_traces.sessions`
/// with all events from `system_traces.events`
#[derive(Debug, DeserializeRow, Clone, PartialEq, Eq)]
#[scylla(crate = "crate")]
pub struct TracingInfo {
    /// Address of the client that requested execution of the query.
    pub client: Option<IpAddr>,

    /// Kind of a command that was executed, textually described.
    ///
    /// Example from the ScyllaDB documentation:
    /// `QUERY`
    pub command: Option<String>,

    /// Address of the coordinator node that executed the query.
    pub coordinator: Option<IpAddr>,

    /// Duration of the query execution in microseconds.
    pub duration: Option<i32>,

    /// Various parameters of the query execution.
    ///
    /// Example from the ScyllaDB documentation:
    /// ```json
    /// {
    ///     'consistency_level': 'ONE',
    ///     'page_size': '100',
    ///     'query': 'INSERT into keyspace1.standard1 (key, "C0") VALUES (0x12345679, bigintAsBlob(123456));',
    ///     'serial_consistency_level': 'SERIAL',
    ///     'user_timestamp': '1469091441238107'
    /// }
    /// ```
    pub parameters: Option<HashMap<String, String>>,

    /// Kind of the request, textually described.
    ///
    /// Example from the ScyllaDB documentation:
    /// `Execute CQL3 query`
    pub request: Option<String>,

    /// Point in time when the execution started - time since unix epoch.
    pub started_at: Option<CqlTimestamp>,

    /// Events that happened during the traced execution.
    #[scylla(skip)]
    pub events: Vec<TracingEvent>,
}

/// A single event happening during a traced query
#[derive(Debug, DeserializeRow, Clone, PartialEq, Eq)]
#[scylla(crate = "crate")]
pub struct TracingEvent {
    /// Unique identifier of the event.
    pub event_id: CqlTimeuuid,

    /// Description of the event.
    /// Examples from the ScyllaDB documentation:
    /// ```notrust
    /// - Execute CQL3 query
    /// - Parsing a statement [shard 1]
    /// - Sending a mutation to /127.0.0.1 [shard 1]
    /// - Request complete
    /// ```
    pub activity: Option<String>,

    /// Address of the node that generated the event.
    pub source: Option<IpAddr>,

    /// Number of microseconds elapsed since the start of the query execution
    /// on the node that generated the event.
    pub source_elapsed: Option<i32>,

    /// Thread on which the event was generated.
    /// Example from the ScyllaDB tracing:
    /// `shard 0`
    pub thread: Option<String>,
}

impl TracingInfo {
    /// Returns a list of unique nodes involved in the query
    pub fn nodes(&self) -> Vec<IpAddr> {
        self.events
            .iter()
            .filter_map(|e| e.source)
            .unique()
            .collect()
    }
}

// A query used to query TracingInfo from system_traces.sessions
pub(crate) const TRACES_SESSION_QUERY_STR: &str = "SELECT client, command, coordinator, duration, parameters, request, started_at \
    FROM system_traces.sessions WHERE session_id = ?";

// A query used to query TracingEvent from system_traces.events
pub(crate) const TRACES_EVENTS_QUERY_STR: &str = "SELECT event_id, activity, source, source_elapsed, thread \
    FROM system_traces.events WHERE session_id = ?";
