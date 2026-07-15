Tablet routing information and the tablet-version tracking used to keep it fresh.

For a tablet-enabled table the driver caches, per tablet, the token range and the replica
set so it can route each request straight to a replica. The server keeps that cache
correct by piggy-backing routing information on responses, negotiated through one of two
protocol extensions:

- `TABLETS_ROUTING_V1`: whenever the driver contacts a node that does not own the target
  tablet, the response carries a `tablets-routing-v1` custom payload describing the tablet
  so the driver can correct its cache.
- `TABLETS_ROUTING_V2` (experimental; negotiated on the wire as
  `TABLETS_ROUTING_V2_EXPERIMENTAL`): V2 subsumes V1 and adds *tablet-version tracking*.
  The driver attaches a one-byte "tablet-version block" to every `EXECUTE` (see
  [`choose_tablet_version_block`]); the server compares it against its own tablet version
  and returns a fresh `tablets-routing-v2` payload only when the driver's cached version is
  stale. The block encodes one randomly chosen nibble of the version, so successive
  requests probe the whole version and detect any change without the driver having to
  contact a wrong replica first.

The tablet version is an opaque 64-bit hash of the tablet's ordered replica list (not a
monotonic counter); only its bit pattern is meaningful. Because V2 is experimental its wire
name and payload are subject to change.

# Leader-aware routing for strongly-consistent tables

For a strongly-consistent (Raft-based) keyspace — one created with `consistency = 'global'`,
reflected in [`Keyspace::consistency_mode`] as [`ConsistencyMode::Global`] — the server orders
each tablet's V2 replica list with the Raft leader first. The driver stores the replicas in
payload order, so `replicas[0]` is the leader. The built-in load balancing policy uses this to
route leader-requiring requests straight to the leader, saving the extra coordinator-to-leader
hop.

A request is routed to the leader only once the tablet's V2 replica mapping has been cached
(so `replicas[0]` really is the leader), and then whenever its consistency level is anything
other than `ONE` or `LOCAL_ONE`:

- writes to such a keyspace must use `QUORUM`/`LOCAL_QUORUM` (the server rejects `ONE`/`LOCAL_ONE`
  writes), so they always reach the leader — and a write sent to a follower would only be bounced
  to it anyway;
- reads at `ONE`/`LOCAL_ONE` may be served by any replica, so they keep the normal
  load-spreading routing;
- all other reads go to the leader.

This mirrors the Python driver's behavior. The cached-version requirement matters because
`replicas[0]` is leader-ordered only for a mapping learned from a `TABLETS_ROUTING_V2` payload;
a tablet with no cached version — one sourced from the older `TABLETS_ROUTING_V1` path, or left
over from before a consistency-mode change — is not leader-ordered, so such a request is routed
normally instead of being treated as a leader hint. The same holds until a versioned mapping
arrives, and for eventually-consistent keyspaces. The decision itself lives in the load balancing
policy.

[`Keyspace::consistency_mode`]: crate::cluster::metadata::Keyspace::consistency_mode
[`ConsistencyMode::Global`]: crate::cluster::metadata::ConsistencyMode::Global
