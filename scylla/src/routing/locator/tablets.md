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
