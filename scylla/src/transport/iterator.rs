use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Result as AResult;
use bytes::Bytes;
use futures::Stream;
use tokio::sync::mpsc;

use crate::frame::{
    response::{
        result::{Result, Row, Rows},
        Response,
    },
    value::Value,
};
use crate::statement::{prepared_statement::PreparedStatement, query::Query};
use crate::transport::connection::Connection;

pub struct RowIterator {
    current_row_idx: usize,
    current_page: Rows,
    page_receiver: mpsc::Receiver<AResult<Rows>>,
}

impl Stream for RowIterator {
    type Item = AResult<Row>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut s = self.as_mut();

        if s.is_current_page_exhausted() {
            match Pin::new(&mut s.page_receiver).poll_next(cx) {
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
    pub(crate) fn new_for_query(
        conn: Arc<Connection>,
        query: Query,
        values: Vec<Value>,
    ) -> RowIterator {
        Self::new_for_page_fetcher(move |paging_state| {
            // TODO: We should avoid copying here, but making it
            // working without copying is really hard
            let conn = conn.clone();
            let query = query.clone();
            let values = values.clone();
            async move { conn.query(&query, &values, paging_state).await }
        })
    }

    pub(crate) fn new_for_prepared_statement(
        conn: Arc<Connection>,
        prepared_statement: PreparedStatement,
        values: Vec<Value>,
    ) -> RowIterator {
        Self::new_for_page_fetcher(move |paging_state| {
            // TODO: We should avoid copying here, but making it
            // working without copying is really hard
            let conn = conn.clone();
            let prepared_statement = prepared_statement.clone();
            let values = values.clone();
            async move {
                conn.execute(&prepared_statement, &values, paging_state)
                    .await
            }
        })
    }

    fn new_for_page_fetcher<F, G>(page_fetcher: F) -> RowIterator
    where
        F: Fn(Option<Bytes>) -> G + Send + Sync + 'static,
        G: Future<Output = AResult<Response>> + Send + 'static,
    {
        // TODO: How many pages in flight do we allow?
        let (sender, receiver) = mpsc::channel(1);

        tokio::task::spawn(Self::worker(sender, page_fetcher));

        RowIterator {
            current_row_idx: 0,
            current_page: Default::default(),
            page_receiver: receiver,
        }
    }

    async fn worker<F, G>(page_sender: mpsc::Sender<AResult<Rows>>, page_fetcher: F)
    where
        F: Fn(Option<Bytes>) -> G + Send + Sync,
        G: Future<Output = AResult<Response>> + Send,
    {
        let mut last_response = page_fetcher(None).await;
        loop {
            match last_response {
                Ok(Response::Result(Result::Rows(rows))) => {
                    let paging_state = rows.metadata.paging_state.clone();
                    if page_sender.send(Ok(rows)).await.is_err() {
                        // TODO: Log error
                        return;
                    }

                    if let Some(paging_state) = paging_state {
                        last_response = page_fetcher(Some(paging_state)).await;
                    } else {
                        return;
                    }
                }
                Ok(Response::Error(err)) => {
                    let _ = page_sender.send(Err(err.into()));
                    return;
                }
                Ok(resp) => {
                    let _ = page_sender.send(Err(anyhow!("Unexpected response: {:?}", resp)));
                    return;
                }
                Err(err) => {
                    let _ = page_sender.send(Err(err));
                    return;
                }
            }
        }
    }

    fn is_current_page_exhausted(&self) -> bool {
        self.current_row_idx >= self.current_page.rows.len()
    }
}
