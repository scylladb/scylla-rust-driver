use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::frame::ResponseOpcode;

/// The `kind` of a RESULT response carrying a prepared statement id.
const RESULT_KIND_PREPARED: i32 = 4;

/// Remembers the query text of the prepared statements, so that EXECUTE requests,
/// which carry only the statement id, can be matched against the query.
///
/// Shared by the whole proxy, because a statement prepared by one connection (or session,
/// or on one node) can be executed by another without preparing it.
/// Connections use it through [`ConnectionPreparedStatements`].
#[derive(Clone, Default)]
pub(crate) struct PreparedStatements {
    /// Prepared statement id -> query text.
    queries: Arc<Mutex<HashMap<Vec<u8>, Arc<str>>>>,
}

impl PreparedStatements {
    pub(crate) fn for_new_connection(&self) -> ConnectionPreparedStatements {
        ConnectionPreparedStatements {
            all: self.clone(),
            pending: Arc::default(),
        }
    }
}

/// [`PreparedStatements`] as seen by one connection. Cloning gives another handle to
/// the same connection, for its request and response processors.
#[derive(Clone)]
pub(crate) struct ConnectionPreparedStatements {
    all: PreparedStatements,
    /// Queries of this connection's PREPARE requests that have not been answered yet,
    /// by stream (stream ids are only unique within a connection).
    pending: Arc<Mutex<HashMap<i16, Arc<str>>>>,
}

impl ConnectionPreparedStatements {
    /// Notes the query of a PREPARE request. A body that cannot be parsed is ignored.
    pub(crate) fn on_prepare_request(&self, stream: i16, body: &[u8]) {
        let Some((len, rest)) = body.split_first_chunk::<4>() else {
            return;
        };
        let Ok(len) = usize::try_from(i32::from_be_bytes(*len)) else {
            return;
        };
        let Some(query) = rest.get(..len).and_then(|q| std::str::from_utf8(q).ok()) else {
            return;
        };
        self.pending.lock().unwrap().insert(stream, query.into());
    }

    /// Notes the statement id from the response to a PREPARE request.
    pub(crate) fn on_response(&self, stream: i16, opcode: ResponseOpcode, body: &[u8]) {
        let Some(query) = self.pending.lock().unwrap().remove(&stream) else {
            return;
        };
        if opcode != ResponseOpcode::Result {
            return;
        }
        let Some((kind, rest)) = body.split_first_chunk::<4>() else {
            return;
        };
        if i32::from_be_bytes(*kind) != RESULT_KIND_PREPARED {
            return;
        }
        if let Some(id) = Self::read_short_bytes(rest) {
            self.all.queries.lock().unwrap().insert(id.to_vec(), query);
        }
    }

    /// The query of the statement an EXECUTE request refers to, if it was prepared
    /// through the proxy.
    pub(crate) fn query_of_execute(&self, body: &[u8]) -> Option<Arc<str>> {
        let id = Self::read_short_bytes(body)?;
        self.all.queries.lock().unwrap().get(id).cloned()
    }

    fn read_short_bytes(buf: &[u8]) -> Option<&[u8]> {
        let (len, rest) = buf.split_first_chunk::<2>()?;
        rest.get(..usize::from(u16::from_be_bytes(*len)))
    }
}
