use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use futures::Stream;
use std::result::Result as StdResult;
use thiserror::Error;
use tokio::sync::mpsc;

use super::errors::QueryError;
use crate::cql_to_rust::{FromRow, FromRowError};

use crate::frame::{
    response::{
        result::{Result, Row, Rows},
        Response,
    },
    value::SerializedValues,
};
use crate::statement::{prepared_statement::PreparedStatement, query::Query};
use crate::transport::connection::Connection;
use crate::transport::metrics::Metrics;

pub struct RowIterator {
    current_row_idx: usize,
    current_page: Rows,
    page_receiver: mpsc::Receiver<StdResult<Rows, QueryError>>,
}

impl Stream for RowIterator {
    type Item = StdResult<Row, QueryError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut s = self.as_mut();

        if s.is_current_page_exhausted() {
            match Pin::new(&mut s.page_receiver).poll_recv(cx) {
                Poll::Ready(Some(Ok(rows))) => {
                    s.current_page = rows;
                    s.current_row_idx = 0;
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }

        let idx = s.current_row_idx;
        if idx < s.current_page.rows.len() {
            let row = mem::take(&mut s.current_page.rows[idx]);
            s.current_row_idx += 1;
            return Poll::Ready(Some(Ok(row)));
        }

        // We probably got a zero-sized page
        // Yield, but tell that we are ready
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl RowIterator {
    pub fn into_typed<RowT: FromRow>(self) -> TypedRowIterator<RowT> {
        TypedRowIterator {
            row_iterator: self,
            phantom_data: Default::default(),
        }
    }

    pub(crate) fn new_for_query(
        conn: Arc<Connection>,
        query: Query,
        values: SerializedValues,
        metrics: Arc<Metrics>,
    ) -> RowIterator {
        let metrics_copy = metrics.clone();
        Self::new_with_worker(
            |mut helper| async move {
                let mut paging_state = None;
                loop {
                    let now = Instant::now();
                    metrics_copy.inc_total_paged_queries();
                    let rows = conn.query(&query, &values, paging_state).await;
                    let _ = metrics_copy.log_query_latency(now.elapsed().as_millis() as u64);
                    paging_state = helper.handle_response(rows).await;
                    if paging_state.is_none() {
                        break;
                    }
                }
            },
            metrics,
        )
    }

    pub(crate) fn new_for_prepared_statement(
        conn: Arc<Connection>,
        prepared_statement: PreparedStatement,
        values: SerializedValues,
        metrics: Arc<Metrics>,
    ) -> RowIterator {
        let metrics_copy = metrics.clone();
        Self::new_with_worker(
            |mut helper| async move {
                let mut paging_state = None;
                loop {
                    let now = Instant::now();
                    metrics_copy.inc_total_paged_queries();
                    let rows = conn
                        .execute(&prepared_statement, &values, paging_state)
                        .await;
                    let _ = metrics_copy.log_query_latency(now.elapsed().as_millis() as u64);
                    paging_state = helper.handle_response(rows).await;
                    if paging_state.is_none() {
                        break;
                    }
                }
            },
            metrics,
        )
    }

    fn new_with_worker<F, G>(worker: F, metrics: Arc<Metrics>) -> RowIterator
    where
        F: FnOnce(WorkerHelper) -> G,
        G: Future<Output = ()> + Send + 'static,
    {
        // TODO: How many pages in flight do we allow?
        let (sender, receiver) = mpsc::channel(1);
        let helper = WorkerHelper::new(sender, metrics);

        tokio::task::spawn(worker(helper));

        RowIterator {
            current_row_idx: 0,
            current_page: Default::default(),
            page_receiver: receiver,
        }
    }

    fn is_current_page_exhausted(&self) -> bool {
        self.current_row_idx >= self.current_page.rows.len()
    }
}
struct WorkerHelper {
    sender: mpsc::Sender<StdResult<Rows, QueryError>>,
    metrics: Arc<Metrics>,
}

impl WorkerHelper {
    fn new(sender: mpsc::Sender<StdResult<Rows, QueryError>>, metrics: Arc<Metrics>) -> Self {
        Self { sender, metrics }
    }

    async fn handle_response(
        &mut self,
        response: StdResult<Response, QueryError>,
    ) -> Option<Bytes> {
        match response {
            Ok(Response::Result(Result::Rows(rows))) => {
                let paging_state = rows.metadata.paging_state.clone();
                if self.sender.send(Ok(rows)).await.is_err() {
                    // TODO: Log error
                    None
                } else {
                    paging_state
                }
            }
            Ok(Response::Error(err)) => {
                self.metrics.inc_failed_paged_queries();
                let _ = self.sender.send(Err(err.into())).await;
                None
            }
            Ok(_) => {
                self.metrics.inc_failed_paged_queries();
                let _ = self
                    .sender
                    .send(Err(QueryError::ProtocolError(
                        "Unexpected response to next page query",
                    )))
                    .await;
                None
            }
            Err(err) => {
                self.metrics.inc_failed_paged_queries();
                let _ = self.sender.send(Err(err)).await;
                None
            }
        }
    }
}

pub struct TypedRowIterator<RowT> {
    row_iterator: RowIterator,
    phantom_data: std::marker::PhantomData<RowT>,
}

/// Couldn't get next typed row from the iterator
#[derive(Error, Debug, Clone)]
pub enum NextRowError {
    /// Query to fetch next page has failed
    #[error(transparent)]
    QueryError(#[from] QueryError),

    /// Parsing values in row as given types failed
    #[error(transparent)]
    FromRowError(#[from] FromRowError),
}

impl<RowT: FromRow> Stream for TypedRowIterator<RowT> {
    type Item = StdResult<RowT, NextRowError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut s = self.as_mut();

        let next_elem: Option<StdResult<Row, QueryError>> =
            match Pin::new(&mut s.row_iterator).poll_next(cx) {
                Poll::Ready(next_elem) => next_elem,
                Poll::Pending => return Poll::Pending,
            };

        let next_ready: Option<Self::Item> = match next_elem {
            Some(Ok(next_row)) => Some(RowT::from_row(next_row).map_err(|e| e.into())),
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        };

        Poll::Ready(next_ready)
    }
}

// TypedRowIterator can be moved freely for any RowT so it's Unpin
impl<RowT> Unpin for TypedRowIterator<RowT> {}
