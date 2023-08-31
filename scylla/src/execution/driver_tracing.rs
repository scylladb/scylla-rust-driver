use std::{
    borrow::Borrow,
    fmt::Display,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use itertools::Either;
use scylla_cql::frame::response::result::{deser_cql_value, ColumnSpec, Rows};
use tracing::trace_span;

use crate::{
    cluster::Node,
    connection::Connection,
    routing::Token,
    utils::pretty::{CommaSeparatedDisplayer, CqlValueDisplayer},
    QueryResult,
};

pub(crate) struct RequestSpan {
    span: tracing::Span,
    speculative_executions: AtomicUsize,
}

impl RequestSpan {
    pub(crate) fn new_query(contents: &str, request_size: usize) -> Self {
        use tracing::field::Empty;

        let span = trace_span!(
            "Request",
            kind = "unprepared",
            contents = contents,
            //
            request_size = request_size,
            result_size = Empty,
            result_rows = Empty,
            replicas = Empty,
            shard = Empty,
            speculative_executions = Empty,
        );

        Self {
            span,
            speculative_executions: 0.into(),
        }
    }

    pub(crate) fn new_prepared<'ps>(
        partition_key: Option<impl Iterator<Item = (&'ps [u8], &'ps ColumnSpec)> + Clone>,
        token: Option<Token>,
        request_size: usize,
    ) -> Self {
        use tracing::field::Empty;

        let span = trace_span!(
            "Request",
            kind = "prepared",
            partition_key = Empty,
            token = Empty,
            //
            request_size = request_size,
            result_size = Empty,
            result_rows = Empty,
            replicas = Empty,
            shard = Empty,
            speculative_executions = Empty,
        );

        if let Some(partition_key) = partition_key {
            span.record(
                "partition_key",
                tracing::field::display(
                    format_args!("{}", partition_key_displayer(partition_key),),
                ),
            );
        }
        if let Some(token) = token {
            span.record("token", token.value);
        }

        Self {
            span,
            speculative_executions: 0.into(),
        }
    }

    pub(crate) fn new_batch() -> Self {
        use tracing::field::Empty;

        let span = trace_span!(
            "Request",
            kind = "batch",
            //
            request_size = Empty,
            result_size = Empty,
            result_rows = Empty,
            replicas = Empty,
            shard = Empty,
            speculative_executions = Empty,
        );

        Self {
            span,
            speculative_executions: 0.into(),
        }
    }

    pub(crate) fn record_shard_id(&self, conn: &Connection) {
        if let Some(info) = conn.get_shard_info() {
            self.span.record("shard", info.shard);
        }
    }

    pub(crate) fn record_result_fields(&self, result: &QueryResult) {
        self.span.record("result_size", result.serialized_size);
        if let Some(rows) = result.rows.as_ref() {
            self.span.record("result_rows", rows.len());
        }
    }

    pub(crate) fn record_rows_fields(&self, rows: &Rows) {
        self.span.record("result_size", rows.serialized_size);
        self.span.record("result_rows", rows.rows.len());
    }

    pub(crate) fn record_replicas<'a>(&'a self, replicas: &'a [impl Borrow<Arc<Node>>]) {
        struct ReplicaIps<'a, N>(&'a [N]);
        impl<'a, N> Display for ReplicaIps<'a, N>
        where
            N: Borrow<Arc<Node>>,
        {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let mut nodes = self.0.iter();
                if let Some(node) = nodes.next() {
                    write!(f, "{}", node.borrow().address.ip())?;

                    for node in nodes {
                        write!(f, ",{}", node.borrow().address.ip())?;
                    }
                }
                Ok(())
            }
        }
        self.span
            .record("replicas", tracing::field::display(&ReplicaIps(replicas)));
    }

    pub(crate) fn inc_speculative_executions(&self) {
        self.speculative_executions.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn span(&self) -> &tracing::Span {
        &self.span
    }
}

impl Drop for RequestSpan {
    fn drop(&mut self) {
        self.span.record(
            "speculative_executions",
            self.speculative_executions.load(Ordering::Relaxed),
        );
    }
}

fn partition_key_displayer<'ps, 'res>(
    mut pk_values_iter: impl Iterator<Item = (&'ps [u8], &'ps ColumnSpec)> + 'res + Clone,
) -> impl Display + 'res {
    CommaSeparatedDisplayer(
        std::iter::from_fn(move || {
            pk_values_iter
                .next()
                .map(|(mut cell, spec)| deser_cql_value(&spec.typ, &mut cell))
        })
        .map(|c| match c {
            Ok(c) => Either::Left(CqlValueDisplayer(c)),
            Err(_) => Either::Right("<decoding error>"),
        }),
    )
}
