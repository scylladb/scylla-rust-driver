# Bound statement

A `BoundStatement` is a [prepared statement](prepared.md) with its values already
bound to it - i.e., serialized and stored inside the statement. It is created by
binding values to a `PreparedStatement`:

```rust
# extern crate scylla;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::statement::bound::BoundStatement;
use scylla::statement::prepared::PreparedStatement;

// Prepare the statement ONCE, as always.
let prepared: PreparedStatement = session
    .prepare("INSERT INTO ks.tab (a) VALUES(?)")
    .await?;

// Bind values to it. This serializes the values - and type erases them.
let bound: BoundStatement = prepared.clone().bind(&(12345,))?;

// Execute the bound statement. No values are passed anymore - it carries its own.
session.execute_bound_unpaged(&bound).await?;
# Ok(())
# }
```

Note that `PreparedStatement::bind` consumes the statement. Because binding is
cheap while preparing is not, keep the prepared statement around and `clone()`
it for each binding, as above - `PreparedStatement` is cheap to clone.

### What is it for?

With `execute_*` you pass the values at the moment of execution, so the values
must be alive - and of a known Rust type - at that moment. `BoundStatement` is
for the cases when that does not hold: it lets you serialize the values up
front and carry the result around as a single, type-erased,
`SerializeRow`-independent value.

This enables, among others:

- **Storing statements ready for execution.** A `Vec<BoundStatement>` can hold
  statements with completely different value types - the values are already
  serialized, so nothing about their Rust types leaks into the type of the
  collection. With `PreparedStatement` you would need to keep the values
  alongside it, and they would all have to be of the same type (or boxed
  behind a trait object).
- **Separating value preparation from execution.** The code that knows the
  values does not have to be the code that executes the statement; the bound
  statement can be handed over to another layer, task, or a queue, which needs
  to know nothing about the values' types.
- **Serializing the values only once.** `PreparedStatement::calculate_token`
  and `Session::execute_*` each serialize the values they are given, so
  computing a statement's token and then executing it serializes the same
  values twice. A `BoundStatement` holds the serialized values, and both
  [token calculation](#token-calculation) and execution reuse them - one
  serialization for both.

### Execution

`Session::execute_bound_[unpaged/single_page/iter]` mirror the
`Session::execute_[unpaged/single_page/iter]` family, minus the `values`
argument. Everything else - paging, results, errors - works exactly as
described for [prepared statements](prepared.md).

### Token calculation

`BoundStatement::calculate_token` returns the token that the statement will be
routed by - no values need to be passed, as the statement already has them:

```rust
# extern crate scylla;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::routing::Token;

let prepared = session
    .prepare("INSERT INTO ks.tab (a) VALUES(?)")
    .await?;
let bound = prepared.bind(&(12345,))?;

let token: Option<Token> = bound.calculate_token()?;

// The very same serialized values are then sent with the execution:
// no second serialization takes place.
session.execute_bound_unpaged(&bound).await?;
# Ok(())
# }
```

Compare with the prepared statement equivalent, which serializes `(12345,)`
twice - once inside `calculate_token`, and again inside `execute_unpaged`:

```rust
# extern crate scylla;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
let prepared = session
    .prepare("INSERT INTO ks.tab (a) VALUES(?)")
    .await?;

let token = prepared.calculate_token(&(12345,))?; // serializes the values
session.execute_unpaged(&prepared, (12345,)).await?; // serializes them again
# Ok(())
# }
```

### Configuration

`BoundStatement` does not expose configuration modifiers. Configure the `PreparedStatement`
(consistency, page size, execution profile, ...) *before* binding - the bound statement
inherits all of its settings, and you can inspect them through `BoundStatement::prepared`.

```rust
# extern crate scylla;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::statement::Consistency;

let mut prepared = session
    .prepare("INSERT INTO ks.tab (a) VALUES(?)")
    .await?;

// Set the options first...
prepared.set_consistency(Consistency::One);

// ...then bind. The bound statement will be executed with Consistency::One.
let bound = prepared.bind(&(12345,))?;
assert_eq!(bound.prepared().get_consistency(), Some(Consistency::One));

session.execute_bound_unpaged(&bound).await?;
# Ok(())
# }
```

See [BoundStatement API documentation](https://docs.rs/scylla/latest/scylla/statement/bound/struct.BoundStatement.html)
for more.
