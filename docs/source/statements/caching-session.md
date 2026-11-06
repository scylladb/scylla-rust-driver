# Caching session

[`CachingSession`](https://docs.rs/scylla/latest/scylla/client/caching_session/struct.CachingSession.html)
wraps a `Session` and prepares statements for you.
The first time a statement string is executed through it, the string is prepared and the
resulting prepared statement is stored in a cache. Every later execution of the same string
reuses the cached one.

Use it when you do not want to prepare statements up front and keep them around yourself,
but you still want the [benefits of prepared statements](prepared.md).

**Note:** The cache key is the statement string. Two strings that differ in any way are two cache entries,
so put values in bind markers (`?`) instead of formatting them into the string.

:::{warning}
The keyspace is not part of the cache key. A statement written without a keyspace name is prepared
against the session's keyspace at that moment, and the cached statement keeps running against it:
after `use_keyspace` switches the session to another keyspace, the same string still reads and writes
the first one, and there is no error. Set the keyspace before you use the caching session and do not
change it, or write the keyspace name in each statement (`ks.tab`, as the examples here do).
:::

## Creating a caching session

```rust
# extern crate scylla;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: Session) -> Result<(), Box<dyn Error>> {
use scylla::client::caching_session::CachingSessionBuilder;

let caching_session = CachingSessionBuilder::new(session)
    .max_capacity(256)
    .build();
# Ok(())
# }
```

`max_capacity` is the number of prepared statements the cache holds (128 by default,
and it must be greater than 0). When the cache is full and a new statement is added,
an entry is evicted to make room. If your application runs more distinct statements
than the default, raise it.

`CachingSessionBuilder::new_shared` takes an `Arc<Session>` instead, if you share the session
with other code. `CachingSession::from(session, cache_size)` is a shorter way to build one
with the default hasher.

## Executing statements

`CachingSession` has the same execution methods as `Session`'s prepared API, and they take
a statement string (or a `Statement`) instead of a `PreparedStatement`:

```rust
# extern crate scylla;
# use scylla::client::caching_session::CachingSession;
# use std::error::Error;
# async fn check_only_compiles(caching_session: &CachingSession) -> Result<(), Box<dyn Error>> {
// Prepared on the first call, reused on the next ones.
caching_session
    .execute_unpaged("INSERT INTO ks.tab (a) VALUES (?)", (12345_i32,))
    .await?;
caching_session
    .execute_unpaged("INSERT INTO ks.tab (a) VALUES (?)", (67890_i32,))
    .await?;
# Ok(())
# }
```

- `execute_unpaged`, `execute_iter` and `execute_single_page` work as their `Session`
  counterparts do; see [paged queries](paged.md) for when to use which.
- `batch` prepares the statements of a [batch](batch.md) through the cache.
  `prepare_batch` does so without executing it.
- `add_prepared_statement` puts a statement in the cache ahead of time, for example at startup,
  so that the first real request does not pay for the preparation.
- `get_session` gives you the wrapped `Session`.

A `Statement` keeps its own options (consistency, timeout, page size, ...),
and they apply to the executions that go through the cache:

```rust
# extern crate scylla;
# use scylla::client::caching_session::CachingSession;
# use std::error::Error;
# async fn check_only_compiles(caching_session: &CachingSession) -> Result<(), Box<dyn Error>> {
use scylla::statement::Consistency;
use scylla::statement::unprepared::Statement;

let mut statement = Statement::new("SELECT a FROM ks.tab WHERE a = ?");
statement.set_consistency(Consistency::One);

caching_session.execute_unpaged(statement, (12345_i32,)).await?;
# Ok(())
# }
```

## Caching session or prepared statements?

| | `CachingSession` | `Session::prepare` + `execute_*` |
|---|---|---|
| Prepare step | implicit, on first use | explicit, you keep the `PreparedStatement` |
| Lookup cost | a hash map lookup by statement string per execution | none |
| Memory | bounded by `max_capacity` | whatever you keep |
| Good for | code where statements are built or chosen in many places | hot paths with a fixed set of statements |

Both are fine, and both avoid sending the statement string to be parsed on every request.

## Example

See [`examples/caching_session.rs`](https://github.com/scylladb/scylla-rust-driver/blob/main/examples/caching_session.rs).
