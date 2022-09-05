pub(crate) mod parse;

pub mod test_utils;

use std::error::Error;

use tracing::{error, field::display, Span};

/// Constructs an OpenTelemetry span at the trace level.
///
/// Basically does the same as [tracing::trace_span!], only adding two (empty) fields:
/// `otel.status_code` and `otel.status_message`.
#[macro_export]
macro_rules! otel_span {
    (parent: $parent:expr, $name:expr, $($field:tt)*) => {
        tracing::trace_span!(
            target: module_path!(),
            parent: $parent,
            $name,
            $($field)*
            otel.status_code = tracing::field::Empty,
            otel.status_message = tracing::field::Empty,
        )
    };
    ($name:expr, $($field:tt)*) => {
        tracing::trace_span!(
            target: module_path!(),
            $name,
            $($field)*
            otel.status_code = tracing::field::Empty,
            otel.status_message = tracing::field::Empty,
        )
    };
    (parent: $parent:expr, $name:expr) => {
        $crate::otel_span!(parent: $parent, $name,)
    };
    ($name:expr) => {
        $crate::otel_span!($name,)
    };
}

pub(crate) trait RecordError {
    fn record_error(self, span: &Span) -> Self;
}

impl<T, E> RecordError for Result<T, E>
where
    E: Error,
{
    fn record_error(self, span: &Span) -> Self {
        if let Err(ref err) = self {
            span.record_error(err);
        }
        self
    }
}

pub(crate) trait SpanExt {
    fn record_error(&self, error: &impl Error);

    fn record_none_explicitly<Q, V>(&self, field: &Q, value: Option<V>)
    where
        Q: ?Sized + tracing::field::AsField,
        V: tracing::field::Value;
}

impl SpanExt for Span {
    fn record_error(&self, error: &impl Error) {
        self.record("otel.status_code", "ERROR");
        self.record("otel.status_message", display(error));
        self.in_scope(|| error!("{}", "Finished with error"));
    }

    fn record_none_explicitly<Q, V>(&self, field: &Q, value: Option<V>)
    where
        Q: ?Sized + tracing::field::AsField,
        V: tracing::field::Value,
    {
        match &value {
            Some(v) => self.record(field, v),
            None => self.record(field, "<None>"),
        };
    }
}

#[macro_export]
macro_rules! scylladb_tag {
    ($suffix:literal) => {
        concat!("db.scylladb.", $suffix)
    };
}
