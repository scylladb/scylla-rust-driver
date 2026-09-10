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

## Leader-aware routing for strongly-consistent tables

A keyspace created with `consistency = 'global'` is *strongly consistent*: its
tablets are replicated through Raft, and one replica of each tablet is the Raft
leader that coordinates the tablet's writes and its linearizable reads. A
request that has to go through the leader but arrives elsewhere is forwarded
there by the receiving node, which costs an extra network hop.

On a `TABLETS_ROUTING_V2` connection, the driver knows which replica leads each
tablet it has cached and sends such requests to the leader directly. Nothing
needs to be configured; as with tablet awareness in general, a token-aware
`DefaultPolicy` suffices.

Which requests are routed to the leader follows from how ScyllaDB serves each
operation on a strongly-consistent table:

| Operation | Consistency level        | Served by                       | Routed to the leader |
| --------- | ------------------------ | ------------------------------- | -------------------- |
| Read      | `ONE`, `LOCAL_ONE`       | any replica, no read barrier    | no                   |
| Read      | anything else            | the Raft leader (linearizable)  | yes                  |
| Write     | `QUORUM`, `LOCAL_QUORUM` | the Raft leader, through Raft   | yes                  |
| Write     | anything else            | rejected by the server          | –                    |

A read at `ONE` or `LOCAL_ONE` is not linearizable: any replica may serve it
without taking a Raft read barrier, so there is nothing to gain from preferring
the leader and these keep normal token-aware routing, spread across the
replicas. Everything else has to be coordinated by the leader, so the driver
targets it directly. Writes are restricted to `QUORUM` and `LOCAL_QUORUM`;
the server rejects the rest regardless of routing.

Eventually-consistent tables are unaffected and keep their usual token-aware
(optionally shuffled) replica ordering.

### Interaction with datacenter and rack preferences

For a leader-requiring request, the leader outranks *distance*: it is tried
ahead of nearer replicas, including a leader in a remote datacenter ahead of a
replica in the preferred rack. A nearer replica would only forward the request
to the leader anyway, and since the table is globally consistent — there is one
leader for the whole cluster, not one per datacenter — keeping the request
inside a single datacenter buys no consistency either.

Leader awareness does not, however, override the policy's own restrictions. The
leader is only targeted if the policy would contact it at all:

- with a preferred datacenter and datacenter failover disabled
  (`permit_dc_failover: false`, the default), a leader in another datacenter is
  not contacted. The request goes to a local replica and the server forwards it
  to the leader, exactly as it would without V2. **Leader awareness never
  introduces cross-datacenter traffic that the policy would otherwise forbid.**
- a leader that is down, or excluded by the policy's own predicate, is skipped
  and the request is routed normally.

Rack preference does not restrict the leader: a leader elsewhere in the
preferred datacenter is still targeted directly.

Only the leader is promoted. The remaining replicas keep their normal ordering,
so if the leader cannot be reached, retries still spread across the others
rather than piling onto one node.

```{note}
Because the leader is only reported over `TABLETS_ROUTING_V2`, and because that
extension is experimental, leader-aware routing is active only against a
cluster that negotiates it. Elsewhere the requests are routed by plain tablet
awareness and the server does the forwarding — correct either way, just with
the extra hop.
```
