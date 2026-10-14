//! Stream ids of a connection, and the slots through which responses reach the
//! requests waiting for them.
//!
//! Slot states are private to this module, so the rules that tie them to the
//! stream id bitmap are kept here.

use super::{OLD_AGE_ORPHAN_THRESHOLD, TaskResponse};
use crate::errors::BrokenConnectionError;
use crate::frame::SerializedRequest;
use std::collections::{BTreeSet, HashMap};
use std::pin::Pin;
use std::sync::Mutex as StdMutex;
use std::sync::OnceLock;
use std::sync::atomic::{self, AtomicU64};
use std::task::{Context, Poll, Waker};
use tokio::sync::mpsc;
use tokio::time::Instant;

/// Number of stream ids usable on one connection, i.e. the maximum number of
/// requests in flight on it. Every id in `0..MAX_IN_FLIGHT_REQUESTS` is
/// allocatable; none is reserved.
pub(crate) const MAX_IN_FLIGHT_REQUESTS: usize = i16::MAX as usize + 1;

/// Number of stream ids in one block of the `used` bitmap.
const BLOCK_BITS: usize = u64::BITS as usize;

// Slots are allocated in segments: the first one has `FIRST_SEGMENT_SIZE`
// slots, and each next one doubles the total. This way a connection only
// allocates slots for as many requests as it had in flight at once.
const FIRST_SEGMENT_SIZE: usize = 64;
const SEGMENT_COUNT: usize = (MAX_IN_FLIGHT_REQUESTS / FIRST_SEGMENT_SIZE).ilog2() as usize + 1;

// Each stream id must have exactly one bit and one slot.
const _: () = {
    // `allocate` casts stream ids from `usize` to `i16`.
    assert!(MAX_IN_FLIGHT_REQUESTS <= i16::MAX as usize + 1);
    assert!(MAX_IN_FLIGHT_REQUESTS.is_multiple_of(BLOCK_BITS));
    assert!(FIRST_SEGMENT_SIZE << (SEGMENT_COUNT - 1) == MAX_IN_FLIGHT_REQUESTS);
};

/// Stream ids of a connection, and the slots through which responses reach the
/// requests waiting for them.
///
/// Requests allocate stream ids themselves, so handing a response over needs no
/// per-request allocation.
pub(super) struct StreamSlots {
    /// Bit `i % BLOCK_BITS` of block `i / BLOCK_BITS` is set if stream id `i`
    /// is allocated.
    // Only changed by read-modify-write operations, which keeps a release of
    // a bit paired with its next claim, whatever other bits change in between.
    used: Box<[AtomicU64]>,
    /// Segment 0 holds the slots of stream ids `0..FIRST_SEGMENT_SIZE`, and
    /// segment `k > 0` those of `(FIRST_SEGMENT_SIZE << (k - 1))..(FIRST_SEGMENT_SIZE << k)`.
    segments: [OnceLock<Box<[StdMutex<SlotState>]>>; SEGMENT_COUNT],
    // Locked only while holding a slot lock, never the other way around.
    orphans: StdMutex<OrphanageTracker>,
}

#[derive(Default)]
enum SlotState {
    /// The stream id is not allocated.
    #[default]
    Free,
    /// A request waits for the response.
    Waiting(Option<Waker>),
    /// The response arrived, but the request has not taken it yet.
    Ready(Result<TaskResponse, BrokenConnectionError>),
    /// The request stopped waiting before the response arrived.
    Orphaned,
}

impl StreamSlots {
    pub(super) fn new() -> Self {
        Self {
            used: (0..MAX_IN_FLIGHT_REQUESTS / BLOCK_BITS)
                .map(|_| AtomicU64::new(0))
                .collect(),
            segments: Default::default(),
            orphans: StdMutex::new(OrphanageTracker::new()),
        }
    }

    /// Allocates a stream id for `request` and sends it, or returns `None` if
    /// all stream ids are in use.
    // `#[inline]`, like `ResponseFuture::poll`, so that the request path in
    // `connection` can inline it from another codegen unit.
    #[inline]
    pub(super) fn submit<'a>(
        &'a self,
        permit: mpsc::Permit<'_, SerializedRequest>,
        mut request: SerializedRequest,
    ) -> Option<ResponseFuture<'a>> {
        let stream = self.allocate()?;
        request.set_stream(stream.id);
        permit.send(request);
        Some(ResponseFuture {
            stream: Some(stream),
        })
    }

    /// Allocates a stream id, or returns `None` if all are in use.
    fn allocate(&self) -> Option<AllocatedStream<'_>> {
        for (block_index, block) in self.used.iter().enumerate() {
            let mut used = block.load(atomic::Ordering::Relaxed);
            while used != u64::MAX {
                let bit = 1 << used.trailing_ones();
                // `Acquire` pairs with the `Release` in `release`, so the previous
                // owner's reset of the slot happens before this request uses it.
                used = block.fetch_or(bit, atomic::Ordering::Acquire);
                if used & bit == 0 {
                    let id = (block_index * BLOCK_BITS) as i16 + bit.trailing_zeros() as i16;
                    let mut state = self.slot(id).lock().unwrap();
                    debug_assert!(
                        matches!(*state, SlotState::Free),
                        "stream id {id} allocated while its slot is in use"
                    );
                    *state = SlotState::Waiting(None);
                    drop(state);
                    return Some(AllocatedStream { streams: self, id });
                }
            }
        }
        None
    }

    /// Frees stream `id`. Its slot must already be `Free`.
    fn release(&self, id: i16) {
        let id = id as usize;
        // `Release` makes the slot's reset to `Free` visible to the next owner of
        // the id, see `allocate`.
        self.used[id / BLOCK_BITS].fetch_and(!(1 << (id % BLOCK_BITS)), atomic::Ordering::Release);
    }

    /// Returns the segment holding the slot of stream `id`, and the slot's index in it.
    fn locate(id: i16) -> (usize, usize) {
        let id = id as usize;
        match (id / FIRST_SEGMENT_SIZE).checked_ilog2() {
            None => (0, id),
            Some(log) => (log as usize + 1, id - (FIRST_SEGMENT_SIZE << log)),
        }
    }

    fn slot(&self, id: i16) -> &StdMutex<SlotState> {
        let (segment, index) = Self::locate(id);
        let segment_size = if segment == 0 {
            FIRST_SEGMENT_SIZE
        } else {
            FIRST_SEGMENT_SIZE << (segment - 1)
        };
        &self.segments[segment]
            .get_or_init(|| (0..segment_size).map(|_| Default::default()).collect())[index]
    }

    /// Hands the response over to the request that was sent on stream `id`.
    /// Returns `false` if there is no such request.
    pub(super) fn complete(&self, id: i16, response: TaskResponse) -> bool {
        let (segment, index) = Self::locate(id);
        let Some(slot) = self.segments[segment].get().map(|slots| &slots[index]) else {
            return false;
        };
        let mut state = slot.lock().unwrap();
        match std::mem::take(&mut *state) {
            SlotState::Waiting(waker) => {
                *state = SlotState::Ready(Ok(response));
                drop(state);
                if let Some(waker) = waker {
                    waker.wake();
                }
                true
            }
            SlotState::Orphaned => {
                self.orphans.lock().unwrap().remove(id);
                drop(state);
                self.release(id);
                true
            }
            unexpected @ (SlotState::Free | SlotState::Ready(_)) => {
                *state = unexpected;
                false
            }
        }
    }

    /// Fails all requests that wait for a response.
    pub(super) fn fail_all(&self, error: &BrokenConnectionError) {
        let slots = self
            .segments
            .iter()
            .filter_map(OnceLock::get)
            .flat_map(|slots| slots.iter());
        for slot in slots {
            let mut state = slot.lock().unwrap();
            if let SlotState::Waiting(waker) = &mut *state {
                let waker = waker.take();
                *state = SlotState::Ready(Err(error.clone()));
                drop(state);
                if let Some(waker) = waker {
                    waker.wake();
                }
            }
        }
    }

    pub(super) fn old_orphans_count(&self) -> usize {
        self.orphans
            .lock()
            .unwrap()
            .orphans_older_than(OLD_AGE_ORPHAN_THRESHOLD)
    }
}

/// Waits for the response to a request sent by [`StreamSlots::submit`].
///
/// Dropping it before the response is taken (e.g. when the request is
/// cancelled) marks the stream id as orphaned if the response may still
/// arrive, or releases it.
#[must_use = "`submit` sent the request already, and dropping this future discards its response"]
pub(super) struct ResponseFuture<'a> {
    /// `None` once the response is taken, which releases the stream id.
    stream: Option<AllocatedStream<'a>>,
}

impl Future for ResponseFuture<'_> {
    type Output = Result<TaskResponse, BrokenConnectionError>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let stream = self
            .stream
            .take()
            .expect("response polled after completion");
        let response = stream.poll_response(cx);
        if response.is_pending() {
            self.stream = Some(stream);
        } else {
            stream.release();
        }
        response
    }
}

/// A stream id allocated for a request.
///
/// Dropping it before the response is taken marks the stream id as orphaned if
/// the response may still arrive, or releases it.
struct AllocatedStream<'a> {
    streams: &'a StreamSlots,
    id: i16,
}

impl AllocatedStream<'_> {
    /// Takes the response if it arrived. Otherwise, the response wakes `cx`.
    fn poll_response(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<TaskResponse, BrokenConnectionError>> {
        let mut state = self.streams.slot(self.id).lock().unwrap();
        if let SlotState::Waiting(waker) = &mut *state {
            match waker {
                Some(waker) => waker.clone_from(cx.waker()),
                None => *waker = Some(cx.waker().clone()),
            }
            return Poll::Pending;
        }
        // This stream owns the slot. Only it can make the slot `Free` or
        // `Orphaned`, so the state is `Ready` if it is not `Waiting`.
        let SlotState::Ready(response) = std::mem::take(&mut *state) else {
            unreachable!("stream slot polled while not allocated");
        };
        Poll::Ready(response)
    }

    /// Releases the stream id once its response is taken.
    fn release(self) {
        let (streams, id) = (self.streams, self.id);
        // The slot is `Free` already, so `Drop` doesn't need to lock it.
        std::mem::forget(self);
        streams.release(id);
    }
}

impl Drop for AllocatedStream<'_> {
    fn drop(&mut self) {
        let mut state = self.streams.slot(self.id).lock().unwrap();
        if matches!(*state, SlotState::Waiting(_)) {
            // The reader releases the stream id when the response arrives.
            *state = SlotState::Orphaned;
            self.streams.orphans.lock().unwrap().insert(self.id);
        } else {
            *state = SlotState::Free;
            drop(state);
            self.streams.release(self.id);
        }
    }
}

struct OrphanageTracker {
    orphans: HashMap<i16, Instant>,
    by_orphaning_times: BTreeSet<(Instant, i16)>,
}

impl OrphanageTracker {
    fn new() -> Self {
        Self {
            orphans: HashMap::new(),
            by_orphaning_times: BTreeSet::new(),
        }
    }

    fn insert(&mut self, stream_id: i16) {
        let now = Instant::now();
        self.orphans.insert(stream_id, now);
        self.by_orphaning_times.insert((now, stream_id));
    }

    fn remove(&mut self, stream_id: i16) {
        if let Some(time) = self.orphans.remove(&stream_id) {
            self.by_orphaning_times.remove(&(time, stream_id));
        }
    }

    fn orphans_older_than(&self, age: std::time::Duration) -> usize {
        let minimal_age = Instant::now() - age;
        self.by_orphaning_times
            .range(..(minimal_age, i16::MAX))
            .count() // This has linear time complexity, but in terms of
        // the number of old orphans. Healthy connection - one
        // that does not have old orphaned stream ids, will
        // calculate this function quickly.
    }
}

#[cfg(test)]
mod tests {
    use super::{ResponseFuture, StreamSlots, TaskResponse};
    use crate::errors::BrokenConnectionErrorKind;
    use crate::frame::request::options::Options;
    use crate::frame::response::ResponseOpcode;
    use crate::frame::{FrameParams, SerializedRequest};
    use crate::test_utils::setup_tracing;
    use bytes::Bytes;
    use tokio::sync::mpsc;

    fn task_response(body: Bytes) -> TaskResponse {
        TaskResponse {
            params: FrameParams::default(),
            opcode: ResponseOpcode::Result,
            body,
        }
    }

    /// Takes the place of the router's submit channel.
    struct SubmitChannel {
        sender: mpsc::Sender<SerializedRequest>,
        receiver: mpsc::Receiver<SerializedRequest>,
    }

    impl SubmitChannel {
        fn new() -> Self {
            let (sender, receiver) = mpsc::channel(1);
            Self { sender, receiver }
        }

        /// Submits a request, and checks that it was sent on stream `id`.
        fn submit<'a>(&mut self, streams: &'a StreamSlots, id: i16) -> ResponseFuture<'a> {
            let request = SerializedRequest::make(&Options, None, false).unwrap();
            let response = streams
                .submit(self.sender.try_reserve().unwrap(), request)
                .unwrap();
            let frame = self.receiver.try_recv().unwrap();
            assert_eq!(frame.get_data()[2..4], id.to_be_bytes());
            response
        }
    }

    #[tokio::test]
    async fn stream_slots_hand_over_responses() {
        setup_tracing();
        let streams = StreamSlots::new();
        let mut channel = SubmitChannel::new();
        let id_body = |id: i16| Bytes::copy_from_slice(&id.to_be_bytes());

        // 200 streams span the first three slot segments.
        let responses: Vec<_> = (0..200).map(|id| channel.submit(&streams, id)).collect();
        for id in (0..200).rev() {
            assert!(streams.complete(id, task_response(id_body(id))));
        }
        for (id, response) in (0..).zip(responses) {
            assert_eq!(response.await.unwrap().body, id_body(id));
        }

        // All stream ids were released.
        assert!(!streams.complete(0, task_response(Bytes::new())));
        assert!(!streams.complete(1000, task_response(Bytes::new())));
        let _response = channel.submit(&streams, 0);
    }

    #[tokio::test]
    async fn stream_slots_release_cancelled_and_failed_streams() {
        setup_tracing();
        let streams = StreamSlots::new();
        let mut channel = SubmitChannel::new();
        let orphan_count = || streams.orphans.lock().unwrap().orphans.len();
        let empty_response = || task_response(Bytes::new());

        // A cancelled request's stream stays allocated, as an orphan, until
        // its response arrives.
        let mut response = channel.submit(&streams, 0);
        assert!(futures::poll!(&mut response).is_pending());
        drop(response);
        assert_eq!(orphan_count(), 1);
        let other = channel.submit(&streams, 1);
        assert!(streams.complete(0, empty_response()));
        assert_eq!(orphan_count(), 0);
        assert!(streams.complete(1, empty_response()));
        other.await.unwrap();

        // A response that arrived but was not taken.
        let response = channel.submit(&streams, 0);
        assert!(streams.complete(0, empty_response()));
        drop(response);
        assert_eq!(orphan_count(), 0);

        // A request waiting when the connection breaks.
        let mut response = channel.submit(&streams, 0);
        assert!(futures::poll!(&mut response).is_pending());
        streams.fail_all(&BrokenConnectionErrorKind::ChannelError.into());
        assert!(response.await.is_err());
        let _response = channel.submit(&streams, 0);
    }

    #[tokio::test]
    #[should_panic(expected = "response polled after completion")]
    async fn stream_slots_reject_poll_after_completion() {
        setup_tracing();
        let streams = StreamSlots::new();
        let mut channel = SubmitChannel::new();
        let mut response = channel.submit(&streams, 0);
        assert!(streams.complete(0, task_response(Bytes::new())));
        assert!(futures::poll!(&mut response).is_ready());
        // The stream id is released, and another request can own it now.
        let _other = channel.submit(&streams, 0);
        let _ = futures::poll!(&mut response);
    }
}
