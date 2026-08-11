use std::collections::HashSet;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::time::Instant;
use tracing::{debug, error};
use uuid::Uuid;

use crate::cluster::control_connection::{
    ControlConnection, ControlConnectionEvent, ControlConnectionEvents,
};
use crate::cluster::metadata::update::{MetadataUpdate, RefreshRequest};
use crate::cluster::metadata::{ClientRoutesUpdate, Metadata};
use crate::errors::MetadataError;
use crate::frame::response::event::ClientRoutesChangeEvent;
use crate::frame::response::event::EventV2 as Event;
use crate::frame::response::event::StatusChangeEvent;

use super::merge_channel;
use super::reader::{MetadataReader, TopologyUpdateGuard};

/// How often the worker attempts to establish a control connection while it has none.
const CONTROL_CONNECTION_REPAIR_INTERVAL: Duration = Duration::from_secs(1);

/// Works in the background to keep learning about the cluster: fetches metadata
/// periodically and on request, and drains CQL server events from the control
/// connection. All of its output is put into the `merge_channel` that the
/// caller of `new` provides.
///
/// The worker alternates between two loops:
/// - [`work_on_cc`](Self::work_on_cc), which serves a specific control connection
///   until it breaks, and
/// - [`work_without_cc`](Self::work_without_cc), which establishes a new one.
///
/// The control connection is therefore not a field, but a value owned by the
/// currently running loop.
pub(in super::super) struct MetadataWorker {
    /// Establishes control connections (fetching metadata in the process) and
    /// keeps the known peers to establish them to.
    metadata_reader: MetadataReader,

    // This value determines how frequently the metadata
    // worker will refresh the cluster metadata
    cluster_metadata_refresh_interval: Duration,

    // To listen for refresh requests
    refresh_channel: tokio::sync::mpsc::Receiver<RefreshRequest>,

    /// Produces updates for the cluster worker.
    updates: merge_channel::Sender<MetadataUpdate>,

    /// A received refresh request whose refresh has not been performed yet.
    ///
    /// This is worker state rather than a local of either loop: a request may
    /// arrive on a control connection that then turns out to be defunct, in which
    /// case it must survive the switch to [`work_without_cc`](Self::work_without_cc),
    /// which retries the refresh and ultimately answers it.
    ///
    /// Invariant: at most one request is pending at a time - both loops take it
    /// (answering it, directly or through the cluster worker) before awaiting
    /// the next one.
    ///
    /// Within [`work_on_cc`](Self::work_on_cc) a stronger invariant holds: while
    /// a request is pending, the full fetch in flight was started after (and
    /// because of) its receipt - see the `refresh_channel` branch there for how
    /// its guard upholds this.
    pending_request: Option<RefreshRequest>,
}

/// What a server event obliges the worker to fetch, as classified by
/// [`MetadataWorker::handle_server_event`].
enum EventAction {
    /// Nothing to fetch - the event was fully handled synchronously.
    None,
    /// Fetch full metadata: the event describes a change (e.g. in topology)
    /// that only a full fetch picks up.
    FetchFull,
    /// Fetch the client routes for these (connection id, host id) pairs, as
    /// listed in a CLIENT_ROUTES_CHANGE:UPDATE_NODES event.
    FetchClientRoutes { pairs: HashSet<(String, Uuid)> },
}

/// The fetch work that [`MetadataWorker::work_on_cc`] owes but has not started
/// yet, because the fetch that must precede it is still in flight.
///
/// The starter step at the top of the loop drains this plan into
/// [`PendingFetches`] as soon as the blocking fetch completes, so whenever the
/// loop awaits, everything still recorded here is genuinely blocked.
///
/// There is deliberately no state for "a full fetch and partial work owed at
/// once": a due full fetch subsumes all partial work (see
/// [`note_full_needed`](Self::note_full_needed)), so recording both would keep
/// data only to throw it away.
#[derive(Default)]
enum FetchPlan {
    /// Nothing is owed.
    #[default]
    Idle,
    /// A full metadata fetch is owed: requested explicitly, implied by a
    /// server event, or owed because a partial fetch failed.
    Full,
    /// Partial fetch work is owed, at most one entry per partial fetch type.
    /// Currently client routes is the only such type; when more are added,
    /// the payload becomes a struct with one optional entry per type (like
    /// `PartialMetadataChanges`).
    Partial(ClientRoutesFetchRequest),
}

impl FetchPlan {
    /// Records that a full fetch is owed.
    ///
    /// Any partial work recorded so far is dropped: the full fetch starts no
    /// earlier than now - hence after the events that made the partial work
    /// due - and reads everything a partial fetch would, so it subsumes that
    /// work.
    fn note_full_needed(&mut self) {
        *self = FetchPlan::Full;
    }

    /// Records client-routes work, merging it into the work already owed.
    ///
    /// If a full fetch is already owed, the work is dropped - subsumed, by the
    /// argument in [`note_full_needed`](Self::note_full_needed).
    fn note_client_routes(&mut self, request: ClientRoutesFetchRequest) {
        match self {
            FetchPlan::Idle => *self = FetchPlan::Partial(request),
            FetchPlan::Partial(pending) => pending.merge(request),
            FetchPlan::Full => (),
        }
    }
}

/// The (connection id, host id) pairs accumulated from CLIENT_ROUTES_CHANGE
/// events, to be resolved by one partial client-routes fetch.
///
/// A set: merging the work of several events is a plain union, and identical
/// pairs listed repeatedly are deduplicated for free.
struct ClientRoutesFetchRequest {
    pairs: HashSet<(String, Uuid)>,
}

impl ClientRoutesFetchRequest {
    /// Merges another event's pairs in.
    fn merge(&mut self, newer: Self) {
        self.pairs.extend(newer.pairs);
    }

    /// Performs the partial fetch for the accumulated pairs.
    ///
    /// Takes `self` by value so that the returned future borrows only the
    /// control connection - it is stored in [`PendingFetches`] across
    /// `select!` iterations.
    async fn fetch_on(
        self,
        cc: &ControlConnection,
    ) -> Result<Option<ClientRoutesUpdate>, MetadataError> {
        cc.fetch_client_routes_update(&self.pairs).await
    }
}

/// A full-metadata fetch in flight.
///
/// Boxed to give the anonymous `async fn` future a nameable type, which is
/// what lets [`PendingFetches`] offer
/// [`start_due_fetches`](PendingFetches::start_due_fetches) as a plain method.
/// The allocation cost is negligible: a fetch starts at most once per refresh
/// interval, server event or refresh request.
type FullFetch<'cc> =
    Pin<Box<dyn Future<Output = Result<TopologyUpdateGuard, MetadataError>> + Send + 'cc>>;

/// A partial client-routes fetch in flight; boxed for the same reason as
/// [`FullFetch`].
type ClientRoutesFetch<'cc> =
    Pin<Box<dyn Future<Output = Result<Option<ClientRoutesUpdate>, MetadataError>> + Send + 'cc>>;

/// The fetches of [`MetadataWorker::work_on_cc`] currently in flight, owning
/// their futures - which borrow the control connection, hence the lifetime.
///
/// The shape makes the scheduling rules structural:
/// - a full fetch never has anything running beside it (it subsumes and
///   preempts all partial work), and
/// - per partial fetch type, at most one fetch runs at a time. Currently
///   client routes is the only such type; when more are added, `Partial`
///   becomes a struct variant with one optional future per type, which is
///   also how partial fetches of different types get to run concurrently.
///
/// [`start_due_fetches`](Self::start_due_fetches) starts the fetches that the
/// [`FetchPlan`] owes. As a [`Future`], `PendingFetches` resolves to the
/// [`FetchOutcome`] of the in-flight fetch once it completes, emptying `self`
/// back to [`Idle`](Self::Idle) - so a completed fetch is never polled again.
/// While `Idle`, polling pends forever *without registering a waker*: an idle
/// value alone never wakes the worker. This is sound in `work_on_cc` because
/// fetches are only started between `select!`s, never while one is being
/// awaited, and the `select!` always has branches that do register wakers
/// (e.g. event draining).
///
/// Cancel-safe: the futures live in this value, not in the `select!` branch
/// polling it, so losing the `select!` race leaves the in-flight fetch intact.
enum PendingFetches<'cc> {
    /// No fetch is in flight.
    Idle,
    /// A full metadata fetch is in flight.
    Full { fetch: FullFetch<'cc> },
    /// A partial client-routes fetch is in flight.
    Partial {
        client_routes_fetch: ClientRoutesFetch<'cc>,
    },
}

impl<'cc> PendingFetches<'cc> {
    /// The starter step of [`MetadataWorker::work_on_cc`]: starts every fetch
    /// that is due per `plan` (or per the periodic refresh schedule) and not
    /// blocked, so that the worker never awaits while startable work is owed.
    fn start_due_fetches(
        &mut self,
        plan: &mut FetchPlan,
        next_refresh_deadline: &mut Instant,
        refresh_interval: Duration,
        cc: &'cc ControlConnection,
    ) {
        // A full fetch is due when a server event or a refresh request
        // demanded one, or when the periodic deadline has passed.
        if !matches!(self, PendingFetches::Full { .. })
            && (matches!(plan, FetchPlan::Full) || Instant::now() >= *next_refresh_deadline)
        {
            // Starting the full fetch drops both the partial work owed by the
            // plan and any running partial fetch: the full fetch reads all of
            // that data, and reads it after the events that made the partial
            // work due, so both are subsumed.
            *plan = FetchPlan::Idle;
            *next_refresh_deadline = deadline_after(Instant::now(), refresh_interval);

            debug!("Requesting metadata refresh");
            *self = PendingFetches::Full {
                fetch: Box::pin(cc.query_metadata()),
            };
            return;
        }

        // Partial client-routes work starts only when nothing at all is in flight.
        if matches!(self, PendingFetches::Idle) {
            let request = match std::mem::take(plan) {
                FetchPlan::Idle => return, // Nothing to do.
                FetchPlan::Partial(request) => request,
                FetchPlan::Full => unreachable!("Covered by the previous block"),
            };
            *self = PendingFetches::Partial {
                client_routes_fetch: Box::pin(request.fetch_on(cc)),
            };
        }
    }
}

/// What [`PendingFetches`] resolves to: the output of whichever in-flight
/// fetch completed, tagged with its fetch type.
enum FetchOutcome {
    Full(Result<TopologyUpdateGuard, MetadataError>),
    ClientRoutes(Result<Option<ClientRoutesUpdate>, MetadataError>),
}

impl Future for PendingFetches<'_> {
    type Output = FetchOutcome;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // `PendingFetches` is `Unpin` (the fetch futures are heap-pinned), so
        // it can be worked on through a plain `&mut`.
        let this = self.get_mut();
        let outcome = match this {
            PendingFetches::Idle => return Poll::Pending,
            PendingFetches::Full { fetch } => match fetch.as_mut().poll(cx) {
                Poll::Ready(result) => FetchOutcome::Full(result),
                Poll::Pending => return Poll::Pending,
            },
            PendingFetches::Partial {
                client_routes_fetch,
            } => match client_routes_fetch.as_mut().poll(cx) {
                Poll::Ready(result) => FetchOutcome::ClientRoutes(result),
                Poll::Pending => return Poll::Pending,
            },
        };

        // The completed fetch has been consumed; drop its future.
        *this = PendingFetches::Idle;
        Poll::Ready(outcome)
    }
}

impl MetadataWorker {
    pub(in super::super) fn new(
        metadata_reader: MetadataReader,
        cluster_metadata_refresh_interval: Duration,
        refresh_channel: tokio::sync::mpsc::Receiver<RefreshRequest>,
        updates: merge_channel::Sender<MetadataUpdate>,
    ) -> Self {
        Self {
            metadata_reader,
            cluster_metadata_refresh_interval,
            refresh_channel,
            updates,
            pending_request: None,
        }
    }

    /// Runs the worker until the cluster (or the runtime) is gone.
    ///
    /// `initial_cc` is the control connection that the initial metadata was fetched
    /// on; `None` means none could be kept, in which case one is established first.
    pub(in super::super) async fn work(
        mut self,
        initial_cc: Option<(ControlConnection, ControlConnectionEvents)>,
    ) {
        let mut next_cc = initial_cc;

        loop {
            let (cc, cc_events) = match next_cc.take() {
                Some(cc) => cc,
                None => match self.work_without_cc().await {
                    ControlFlow::Break(()) => return,
                    ControlFlow::Continue(cc) => cc,
                },
            };

            match self.work_on_cc(cc, cc_events).await {
                ControlFlow::Break(()) => return,
                // The control connection is defunct - establish a new one.
                ControlFlow::Continue(()) => (),
            }
        }
    }

    /// Attempts to establish a control connection until one is available.
    ///
    /// The first attempt is made immediately; further ones are spaced by
    /// [`CONTROL_CONNECTION_REPAIR_INTERVAL`], with an incoming refresh request
    /// triggering an attempt right away.
    ///
    /// Each attempt fetches metadata, too, so a successful one both satisfies the
    /// pending refresh request (through the cluster worker) and publishes an update.
    ///
    /// Returns [`ControlFlow::Break`] if the worker should stop.
    async fn work_without_cc(
        &mut self,
    ) -> ControlFlow<(), (ControlConnection, ControlConnectionEvents)> {
        loop {
            debug!("Attempting to establish a new control connection");
            let attempt_time = Instant::now();

            match self
                .metadata_reader
                .establish_cc_and_fetch_metadata(false)
                .await
            {
                Ok((cc, metadata)) => {
                    self.publish_metadata(metadata)?;

                    if let Some(cc) = cc {
                        return ControlFlow::Continue(cc);
                    }
                    // Metadata was fetched, but no control connection could be kept
                    // (e.g. every reachable node is rejected by the host filter).
                    // Keep trying at the repair cadence.
                }
                Err(err) => {
                    debug!(
                        error = %err,
                        "Failed to establish a control connection and fetch metadata"
                    );
                    // Nobody else can act on a failed fetch, so the error only goes to the
                    // requester, if there is one.
                    if let Some(request) = self.pending_request.take() {
                        // We can ignore sending error - if no one waits for the response we can drop it
                        let _ = request.response_chan.send(Err(err));
                    }
                }
            }

            // Wait until it's time for the next attempt, unless a refresh request
            // makes us attempt earlier.
            let sleep_until = deadline_after(attempt_time, CONTROL_CONNECTION_REPAIR_INTERVAL);

            tokio::select! {
                _sleep_finished = tokio::time::sleep_until(sleep_until) => (),

                maybe_refresh_request = self.refresh_channel.recv() => {
                    match maybe_refresh_request {
                        Some(request) => self.set_pending_request(request),
                        None => return ControlFlow::Break(()), // If refresh_channel was closed then cluster was dropped, we can stop working
                    }
                }
            }
        }
    }

    /// Serves the given control connection: refreshes metadata on it (periodically,
    /// on request and in reaction to server events) and drains its events.
    ///
    /// # This loop never blocks
    ///
    /// The connection's reader task delivers server events through a bounded
    /// channel and blocks when the channel is full; blocked, it does not process
    /// query responses either. If this loop awaited a fetch directly, a burst of
    /// events could thus deadlock it: the fetch would wait for the reader task,
    /// which waits for this loop to drain events, which waits for the fetch.
    ///
    /// Hence the rule: this loop awaits only in `select!`, with event draining as
    /// one of the branches, and everything outside the `select!` is synchronous.
    /// Fetches run as futures owned by a [`PendingFetches`] value, polled by the
    /// `select!` alongside the other branches. Fetch work that cannot start yet
    /// is recorded in a [`FetchPlan`]. On every iteration, before awaiting,
    /// [`PendingFetches::start_due_fetches`] converts due plan entries into
    /// pending fetches, under these rules:
    ///
    /// - At most one fetch per type runs at a time; work for a busy type waits
    ///   in the plan.
    /// - A due full fetch preempts everything: it abandons the running partial
    ///   fetch and drops the plan's partial work. Both are subsumed - the full
    ///   fetch reads all of that data, and reads it after the events that made
    ///   the partial work due. Conversely, partial work that becomes due *while*
    ///   a full fetch runs stays in the plan: the fetch may have read the
    ///   corresponding tables before the triggering event arrived.
    ///
    /// Returns [`ControlFlow::Continue`] once the control connection is deemed
    /// defunct and should be replaced, or [`ControlFlow::Break`] if the worker
    /// should stop.
    async fn work_on_cc(
        &mut self,
        cc: ControlConnection,
        mut cc_events: ControlConnectionEvents,
    ) -> ControlFlow<()> {
        let mut plan = FetchPlan::default();
        let mut next_refresh_deadline =
            deadline_after(Instant::now(), self.cluster_metadata_refresh_interval);

        // The in-flight fetches. Their futures borrow `cc` and nothing else,
        // so the loop stays free to use `self` (in particular
        // `self.metadata_reader`) while fetches are in flight.
        let mut pending_fetches = PendingFetches::Idle;

        loop {
            pending_fetches.start_due_fetches(
                &mut plan,
                &mut next_refresh_deadline,
                self.cluster_metadata_refresh_interval,
                &cc,
            );

            // Computed before `select!` for the branch guards, which cannot
            // borrow `pending_fetches`: the fetch branch polls it mutably.
            let full_fetch_in_flight = matches!(pending_fetches, PendingFetches::Full { .. });

            tokio::select! {
                fetch_outcome = &mut pending_fetches => {
                    match fetch_outcome {
                        FetchOutcome::Full(Ok(topology_update)) => {
                            debug!("Fetched new metadata");
                            let metadata = topology_update.apply(&mut self.metadata_reader);
                            self.publish_metadata(metadata)?;
                        }
                        FetchOutcome::Full(Err(err)) => {
                            debug!(
                                error = %err,
                                "Failed to fetch metadata on the current control connection. \
                                Will try to establish a new one."
                            );
                            // The control connection is considered defunct - drop it. The pending
                            // request, if any, is retried (and answered) while establishing a new one.
                            return ControlFlow::Continue(());
                        }
                        FetchOutcome::ClientRoutes(Ok(None)) => (), // Nothing to apply.
                        FetchOutcome::ClientRoutes(Ok(Some(routes))) => {
                            if self.send_update(|slot| MetadataUpdate::merge_client_routes_update(slot, routes)).is_err() {
                                return ControlFlow::Break(());
                            }
                        }
                        FetchOutcome::ClientRoutes(Err(err)) => {
                            error!(
                                "Error when fetching client route updates: {err}. \
                                Scheduling a metadata refresh, because the control connection is likely defunct."
                            );
                            plan.note_full_needed();
                        }
                    }
                }

                // While a full fetch is in flight, refresh requests wait in the
                // channel (`Cluster::refresh_metadata` awaits the bounded send).
                // Accepting one now would break the promise behind
                // `pending_request`: the running fetch was started before the
                // request, so its data may predate it. With this guard, a
                // request is only received when the starter step can begin a
                // full fetch for it right away.
                maybe_refresh_request = self.refresh_channel.recv(), if !full_fetch_in_flight => {
                    match maybe_refresh_request {
                        Some(request) => {
                            self.set_pending_request(request);
                            plan.note_full_needed();
                        }
                        None => return ControlFlow::Break(()), // If refresh_channel was closed then cluster was dropped, we can stop working
                    }
                }

                control_connection_event = cc_events.wait_for_event() => {
                    match control_connection_event {
                        ControlConnectionEvent::Shutdown => {
                            // The runtime is shutting down. We can stop working.
                            //
                            // Known issue (predating this worker): if a refresh
                            // request is pending, dropping it here makes the
                            // requester's `Cluster::refresh_metadata` panic on the
                            // response channel. During runtime shutdown the
                            // requester task is being torn down anyway.
                            debug!("Got shutdown control connection event. Shutting down MetadataWorker.");
                            return ControlFlow::Break(());
                        },
                        ControlConnectionEvent::Broken(_err) => {
                            // The control connection was broken. Have a new one established;
                            // the first attempt will be immediate, and if it does not succeed,
                            // subsequent attempts will be issued every second.
                            //
                            // The in-flight fetches, if any, are abandoned: establishing
                            // a new control connection fetches full metadata, which
                            // subsumes them.
                            return ControlFlow::Continue(());
                        },
                        ControlConnectionEvent::ServerEvent(event) => {
                            Self::absorb_server_event(&mut self.updates, event, &mut plan)?;
                        }
                    }
                }

                // While a full fetch is in flight the deadline is irrelevant:
                // it is reset when the next fetch starts. Once no full fetch
                // runs, a passed deadline completes this branch immediately and
                // the starter step begins the periodic fetch.
                _deadline_passed = tokio::time::sleep_until(next_refresh_deadline), if !full_fetch_in_flight => {
                    // Nothing to do here: the starter step notices the passed
                    // deadline.
                }
            }
        }
    }

    /// Handles a single server event synchronously (status hints are published
    /// right away) and returns the fetch work the event implies.
    ///
    /// An associated function taking just the updates sender, so that event
    /// handling does not require exclusive access to the whole worker.
    ///
    /// Returns [`ControlFlow::Break`] if the worker should stop (the cluster
    /// worker is gone).
    fn handle_server_event(
        updates: &mut merge_channel::Sender<MetadataUpdate>,
        event: Event,
    ) -> ControlFlow<(), EventAction> {
        debug!("Received server event: {:?}", event);
        match event {
            Event::TopologyChange(_) => ControlFlow::Continue(EventAction::FetchFull),
            Event::ClientRoutesChange(evt) => {
                // An UPDATE_NODES event pairs `connection_ids[i]` with
                // `host_ids[i]`.
                #[deny(clippy::wildcard_enum_match_arm)]
                let pairs = match evt {
                    ClientRoutesChangeEvent::UpdateNodes {
                        connection_ids,
                        host_ids,
                    } => connection_ids.into_iter().zip(host_ids).collect(),
                    _ => unreachable!("clippy testifies that the match is exhaustive"),
                };
                ControlFlow::Continue(EventAction::FetchClientRoutes { pairs })
            }
            Event::StatusChange(status) => {
                // Tracking node status using events is unreliable because of the possibility of losing events
                // when control connection is broken. A better thing to do here is to treat those events as hints
                // for:
                // - PoolRefiller - UP triggers immediate pool refill attempt, and
                // - Keepaliver - DOWN triggers immediate keepalive query attempt.

                match status {
                    StatusChangeEvent::Up(addr) => {
                        // When receiving an UP event, it is likely that the node just came back up and is now reachable.
                        // We optimistically trigger pool refill for this node.
                        // This is not guaranteed to be correct. It is for example possible that a network partition happened,
                        // the node lost connectivity to the cluster and driver; then it regained connectivity to the cluster,
                        // but not to the driver, and thus is still unreachable from the driver's perspective.
                        // However, in this case triggering pool refill is not harmful - if the node is actually reachable,
                        // then new connections will be opened to it, and if it is not reachable,
                        // then connection attempts will fail and the node will be marked as unreachable by `PoolRefiller`,
                        // so it won't be targeted by the load balancing policy.
                        if Self::send_update_on(updates, |slot| {
                            MetadataUpdate::merge_up_hint(slot, addr)
                        })
                        .is_err()
                        {
                            return ControlFlow::Break(());
                        }
                    }
                    StatusChangeEvent::Down(addr) => {
                        // When receiving a DOWN event, and the driver still sees the node as connected,
                        // we send a keepalive query on its connections to verify their liveness.
                        // The node is supposedly DOWN, so connections to this node are likely defunct.
                        // We expect that the keepalive query fails, in which case connections will be closed.
                        // As a result, the connection pool will report 0 connections to this node,
                        // and thus the node will not be targeted by the LoadBalancingPolicy,
                        // which is the desired behaviour. However, if the keepalive query succeeds,
                        // then the node is likely still alive (got stale event?), and we keep targeting it.
                        if Self::send_update_on(updates, |slot| {
                            MetadataUpdate::merge_down_hint(slot, addr)
                        })
                        .is_err()
                        {
                            return ControlFlow::Break(());
                        }
                    }
                }
                ControlFlow::Continue(EventAction::None)
            }
            _ => ControlFlow::Continue(EventAction::None),
        }
    }

    /// Handles a single server event and folds the fetch work it implies into
    /// `plan`.
    ///
    /// Returns [`ControlFlow::Break`] if the worker should stop (the cluster
    /// worker is gone).
    fn absorb_server_event(
        updates: &mut merge_channel::Sender<MetadataUpdate>,
        event: Event,
        plan: &mut FetchPlan,
    ) -> ControlFlow<()> {
        match Self::handle_server_event(updates, event)? {
            EventAction::None => (),
            EventAction::FetchFull => plan.note_full_needed(),
            EventAction::FetchClientRoutes { pairs } => {
                plan.note_client_routes(ClientRoutesFetchRequest { pairs })
            }
        }
        ControlFlow::Continue(())
    }

    /// Publishes freshly fetched metadata to the cluster worker.
    ///
    /// The pending refresh request, if any, is attached: it is answered by the
    /// cluster worker once the state resulting from this metadata is published -
    /// this is what makes `Cluster::refresh_metadata` return only after the new
    /// state is visible.
    ///
    /// Returns [`ControlFlow::Break`] if the worker should stop (the cluster
    /// worker is gone).
    fn publish_metadata(&mut self, metadata: Metadata) -> ControlFlow<()> {
        let response_chan = self
            .pending_request
            .take()
            .map(|request| request.response_chan);
        match self.send_update(|slot| MetadataUpdate::merge_metadata(slot, metadata, response_chan))
        {
            Ok(()) => ControlFlow::Continue(()),
            Err(_) => ControlFlow::Break(()),
        }
    }

    /// Stores a freshly received refresh request as the pending one.
    ///
    /// Upholds the `pending_request` invariant: both loops take the pending request
    /// before awaiting the next one, so there can never be one already stored.
    fn set_pending_request(&mut self, request: RefreshRequest) {
        debug_assert!(self.pending_request.is_none());
        self.pending_request = Some(request);
    }

    /// Merges an update into the pending one, to be picked up by the cluster worker.
    ///
    /// An error means that the cluster worker is gone, in which case this worker
    /// has nothing left to work for and should end.
    fn send_update(
        &mut self,
        f: impl FnOnce(&mut Option<MetadataUpdate>),
    ) -> Result<(), merge_channel::SendError> {
        Self::send_update_on(&mut self.updates, f)
    }

    /// [`send_update`](Self::send_update) for callers that hold the updates
    /// sender rather than the whole worker.
    fn send_update_on(
        updates: &mut merge_channel::Sender<MetadataUpdate>,
        f: impl FnOnce(&mut Option<MetadataUpdate>),
    ) -> Result<(), merge_channel::SendError> {
        updates.modify(f).inspect_err(|_| {
            debug!("The cluster worker is gone. Shutting down MetadataWorker.");
        })
    }
}

/// `start + interval`, saturating to "now" on overflow (as good as any instant
/// that far in the future).
fn deadline_after(start: Instant, interval: Duration) -> Instant {
    start.checked_add(interval).unwrap_or_else(Instant::now)
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};

    use crate::cluster::metadata::Metadata;

    use super::{FetchOutcome, PendingFetches, TopologyUpdateGuard};

    fn poll_once(pending_fetches: &mut PendingFetches<'_>) -> Poll<FetchOutcome> {
        Pin::new(pending_fetches).poll(&mut Context::from_waker(Waker::noop()))
    }

    fn dummy_topology_update() -> TopologyUpdateGuard {
        TopologyUpdateGuard::new(Metadata::new_dummy(&[]))
    }

    /// While idle, `PendingFetches` must pend (and not panic or spin):
    /// `work_on_cc` polls it in its `select!` unconditionally.
    #[test]
    fn idle_pends() {
        let mut pending_fetches = PendingFetches::Idle;

        assert!(poll_once(&mut pending_fetches).is_pending());
    }

    /// A completed fetch yields its output, tagged with the fetch type, and
    /// empties the value back to `Idle`, so that the next starter step can
    /// start another fetch (and the completed future is never polled again).
    #[test]
    fn completed_fetch_resolves_and_empties() {
        let mut pending_fetches = PendingFetches::Full {
            fetch: Box::pin(async { Ok(dummy_topology_update()) }),
        };
        assert!(matches!(
            poll_once(&mut pending_fetches),
            Poll::Ready(FetchOutcome::Full(Ok(_)))
        ));
        assert!(matches!(pending_fetches, PendingFetches::Idle));
        assert!(poll_once(&mut pending_fetches).is_pending());

        let mut pending_fetches = PendingFetches::Partial {
            client_routes_fetch: Box::pin(async { Ok(None) }),
        };
        assert!(matches!(
            poll_once(&mut pending_fetches),
            Poll::Ready(FetchOutcome::ClientRoutes(Ok(None)))
        ));
        assert!(matches!(pending_fetches, PendingFetches::Idle));
    }

    /// Losing a `select!` race (the polling borrow being dropped) must leave
    /// the in-flight fetch intact.
    #[test]
    fn unfinished_fetch_stays_in_flight() {
        let mut pending_fetches = PendingFetches::Full {
            fetch: Box::pin(pending()),
        };

        assert!(poll_once(&mut pending_fetches).is_pending());
        assert!(matches!(pending_fetches, PendingFetches::Full { .. }));
    }
}
