# Tablet awareness

ScyllaDB can partition a table into *tablets* instead of distributing it over
the token ring. A tablet owns a contiguous token range and is replicated on a
small set of nodes, and — unlike token-ring replication — the driver cannot
compute a tablet's replicas from the ring: tablets move between nodes as the
cluster balances itself, so the mapping has to be learned from the cluster and
kept up to date.

The driver learns it from the server, which piggy-backs routing information on
query responses. Two protocol extensions do this, negotiated per connection
during the handshake. No configuration is required for either: a token-aware
`DefaultPolicy` (the default) is all that is needed.

Because the routing information arrives on responses to prepared statements,
tablet awareness applies to prepared statements only. Unprepared statements
carry no token and are routed without it, exactly as they are for token-ring
tables.

## `TABLETS_ROUTING_V1`

Whenever the driver sends a request to a shard that does not own the target
tablet, the response carries a `tablets-routing-v1` payload describing that
tablet: its token range and its replicas. The driver caches it and routes
subsequent requests for that token range accordingly.

This is corrective: the driver only learns about a tablet by first guessing
wrong about it. That is cheap when a tablet is new to the driver, but it also
means a stale mapping is only noticed once it causes a misrouted request — and
if the driver's replica list is a subset of the real one, requests may keep
landing on a correct-but-outdated replica and the stale entry survives
indefinitely.

## `TABLETS_ROUTING_V2`

V2 subsumes V1 — a connection negotiates one or the other, never both — and
replaces the corrective scheme with an explicit staleness check.

Every tablet carries a **tablet version**: an opaque 64-bit value that changes
whenever anything the driver routes by changes, that is, the tablet's replica
set or (for the strongly-consistent tables described below) its leader. The
driver caches the version alongside the replicas.

On a V2 connection the driver appends a single byte, the *tablet-version block*,
to every `EXECUTE` request. The byte encodes one 4-bit slice of the cached
version together with that slice's position, and the server compares it against
its own version for the tablet:

- if they agree, the response carries no routing information at all;
- if they differ, the response carries a `tablets-routing-v2` payload with the
  tablet's token range, its replicas and its current version, and the driver
  replaces its cached entry.

The position is chosen at random for each request, so consecutive requests
sample different parts of the version and the driver converges on any change
within a few requests. When the driver has no version cached for a tablet, it
sends a random byte, which almost certainly disagrees and so prompts the server
to send the routing information straight away.

Compared to V1, this costs one byte on every request and, in exchange:

- staleness is detected without having to misroute a request first, so a changed
  replica set is picked up even while every request still happens to reach a
  valid replica;
- conversely, a request that *does* reach a non-owning shard costs nothing when
  the driver's cached mapping is already up to date. V1 keys off locality and
  resends the payload every time; V2 keys off the version and stays silent.

```{note}
`TABLETS_ROUTING_V2` is experimental. A node advertises it (on the wire as
`TABLETS_ROUTING_V2_EXPERIMENTAL`) only when started with the
`strongly-consistent-tables` experimental feature enabled; otherwise it offers
only `TABLETS_ROUTING_V1`. Negotiation is per connection, so during a rolling
upgrade a session can hold V2 connections to some nodes and V1 connections to
others at the same time. Both are handled correctly and no configuration
changes when this happens.
```
