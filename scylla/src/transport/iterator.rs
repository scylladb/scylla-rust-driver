use std::cell::RefCell;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Result as AResult;
use bytes::Bytes;
use futures::Stream;
use tokio::sync::mpsc;

use crate::cql_to_rust::FromRow;
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
    pub fn into_typed<RowT: FromRow>(self) -> TypedRowIterator<RowT> {
        TypedRowIterator {
            row_iterator: RefCell::new(self),
            phantom_data: Default::default(),
        }
    }

    pub(crate) fn new_for_query(
        conn: Arc<Connection>,
        query: Query,
        values: Vec<Value>,
    ) -> RowIterator {
        Self::new_with_worker(|mut helper| async move {
            let mut paging_state = None;
            loop {
                let rows = conn.query(&query, &values, paging_state).await;
                paging_state = helper.handle_response(rows).await;
                if paging_state.is_none() {
                    break;
                }
            }
        })
    }

    pub(crate) fn new_for_prepared_statement(
        conn: Arc<Connection>,
        prepared_statement: PreparedStatement,
        values: Vec<Value>,
    ) -> RowIterator {
        Self::new_with_worker(|mut helper| async move {
            let mut paging_state = None;
            loop {
                let rows = conn
                    .execute(&prepared_statement, &values, paging_state)
                    .await;
                paging_state = helper.handle_response(rows).await;
                if paging_state.is_none() {
                    break;
                }
            }
        })
    }

    fn new_with_worker<F, G>(worker: F) -> RowIterator
    where
        F: FnOnce(WorkerHelper) -> G,
        G: Future<Output = ()> + Send + 'static,
    {
        // TODO: How many pages in flight do we allow?
        let (sender, receiver) = mpsc::channel(1);
        let helper = WorkerHelper::new(sender);

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
    sender: mpsc::Sender<AResult<Rows>>,
}

impl WorkerHelper {
    fn new(sender: mpsc::Sender<AResult<Rows>>) -> Self {
        Self { sender }
    }

    async fn handle_response(&mut self, response: AResult<Response>) -> Option<Bytes> {
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
                let _ = self.sender.send(Err(err.into())).await;
                None
            }
            Ok(resp) => {
                let _ = self
                    .sender
                    .send(Err(anyhow!("Unexpected response: {:?}", resp)))
                    .await;
                None
            }
            Err(err) => {
                let _ = self.sender.send(Err(err)).await;
                None
            }
        }
    }
}

pub struct TypedRowIterator<RowT> {
    row_iterator: RefCell<RowIterator>,
    phantom_data: std::marker::PhantomData<RowT>,
}

impl<RowT: FromRow> Stream for TypedRowIterator<RowT> {
    type Item = AResult<RowT>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let my_row_iterator: &mut RowIterator = &mut *self.row_iterator.borrow_mut();

        let next_elem: Option<AResult<Row>> = match Pin::new(my_row_iterator).poll_next(cx) {
            Poll::Ready(next_elem) => next_elem,
            Poll::Pending => return Poll::Pending,
        };

        let next_ready: Option<Self::Item> = match next_elem {
            Some(Ok(next_row)) => Some(RowT::from_row(next_row).map_err(anyhow::Error::from)),
            Some(Err(e)) => Some(Err(e)),
            None => None,
        };

        return Poll::Ready(next_ready);
    }
}
