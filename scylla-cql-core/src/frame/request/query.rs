//! Paging-related types for CQL queries.

use std::{ops::ControlFlow, sync::Arc};

/// A response containing the paging state of a paged query,
/// i.e. whether there are more pages to fetch or not, and if so,
/// what is the state to use for resuming the query from the next page.
#[derive(Debug, Clone)]
pub enum PagingStateResponse {
    /// Indicates that there are more pages to fetch, and provides the
    /// [PagingState] to use for resuming the query from the next page.
    HasMorePages {
        /// The paging state to use for resuming the query from the next page.
        state: PagingState,
    },

    /// Indicates that there are no more pages to fetch, and the query has finished.
    NoMorePages,
}

impl PagingStateResponse {
    /// Determines if the query has finished or it should be resumed with given
    /// [PagingState] in order to fetch next pages.
    #[inline]
    pub fn finished(&self) -> bool {
        matches!(*self, Self::NoMorePages)
    }

    pub fn new_from_raw_bytes(raw_paging_state: Option<&[u8]>) -> Self {
        match raw_paging_state {
            Some(raw_bytes) => Self::HasMorePages {
                state: PagingState::new_from_raw_bytes(raw_bytes),
            },
            None => Self::NoMorePages,
        }
    }

    /// Converts the response into [ControlFlow], signalling whether the query has finished
    /// or it should be resumed with given [PagingState] in order to fetch next pages.
    #[inline]
    pub fn into_paging_control_flow(self) -> ControlFlow<(), PagingState> {
        match self {
            Self::HasMorePages {
                state: next_page_handle,
            } => ControlFlow::Continue(next_page_handle),
            Self::NoMorePages => ControlFlow::Break(()),
        }
    }
}

/// The state of a paged query, i.e. where to resume fetching result rows
/// upon next request.
///
/// Cheaply clonable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PagingState(Option<Arc<[u8]>>);

impl PagingState {
    /// A start state - the state of a not-yet-started paged query.
    #[inline]
    pub fn start() -> Self {
        Self(None)
    }

    /// Returns the inner representation of [PagingState].
    /// One can use this to store paging state for a longer time,
    /// and later restore it using [Self::new_from_raw_bytes].
    /// In case None is returned, this signifies
    /// [PagingState::start()] being underneath.
    #[inline]
    pub fn as_bytes_slice(&self) -> Option<&Arc<[u8]>> {
        self.0.as_ref()
    }

    /// Creates PagingState from its inner representation.
    /// One can use this to restore paging state after longer time,
    /// having previously stored it using [Self::as_bytes_slice].
    #[inline]
    pub fn new_from_raw_bytes(raw_bytes: impl Into<Arc<[u8]>>) -> Self {
        Self(Some(raw_bytes.into()))
    }
}

impl Default for PagingState {
    fn default() -> Self {
        Self::start()
    }
}
