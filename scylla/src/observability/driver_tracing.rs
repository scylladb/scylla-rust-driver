use crate::cluster::node::Node;
use crate::network::Connection;
use crate::response::query_result::QueryResult;
use crate::routing::{Shard, Token};
use crate::utils::safe_format::IteratorSafeFormatExt;
use itertools::Either;
use scylla_cql::frame::response::result::ColumnSpec;
use scylla_cql::frame::response::result::RawMetadataAndRawRows;
use scylla_cql::value::deser_cql_value;
use std::borrow::Borrow;
use std::fmt::Display;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::trace_span;

pub(crate) struct RequestSpan {
    span: tracing::Span,
    speculative_executions: AtomicUsize,
}

impl RequestSpan {
    pub(crate) fn new_query(contents: &str) -> Self {
        use tracing::field::Empty;

        let span = trace_span!(
            "Request",
            kind = "unprepared",
            contents = contents,
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

    pub(crate) fn new_prepared<'ps, 'spec: 'ps>(
        partition_key: Option<impl Iterator<Item = (&'ps [u8], &'ps ColumnSpec<'spec>)> + Clone>,
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
            span.record("token", token.value());
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

    pub(crate) fn record_raw_rows_fields(&self, raw_rows: &RawMetadataAndRawRows) {
        self.span
            .record("raw_result_size", raw_rows.metadata_and_rows_bytes_size());
    }

    pub(crate) fn record_result_fields(&self, query_result: &QueryResult) {
        if let Some(raw_metadata_and_rows) = query_result.raw_metadata_and_rows() {
            self.record_raw_rows_fields(raw_metadata_and_rows);
        }
    }

    pub(crate) fn record_replicas<'a>(
        &'a self,
        replicas: impl Iterator<Item = (impl Borrow<Arc<Node>> + 'a, Shard)> + Clone,
    ) {
        struct Replica<N>(N, Shard);
        impl<N> Display for Replica<N>
        where
            N: Borrow<Arc<Node>>,
        {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let Self(node, shard) = self;
                write!(f, "{}-shard{}", node.borrow().address.ip(), shard)
            }
        }
        self.span.record(
            "replicas",
            tracing::field::display(
                replicas
                    .map(|(node, shard)| Replica(node, shard))
                    .safe_format(", "),
            ),
        );
    }

    pub(crate) fn record_request_size(&self, size: usize) {
        self.span.record("request_size", size);
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

fn partition_key_displayer<'ps, 'res, 'spec: 'ps>(
    mut pk_values_iter: impl Iterator<Item = (&'ps [u8], &'ps ColumnSpec<'spec>)> + 'res + Clone,
) -> impl Display + 'res {
    std::iter::from_fn(move || {
        pk_values_iter
            .next()
            .map(|(mut cell, spec)| deser_cql_value(spec.typ(), &mut cell))
    })
    .map(|c| match c {
        Ok(c) => Either::Left(c),
        Err(_) => Either::Right("<decoding error>"),
    })
    .safe_format(", ")
}
