# Retry policy configuration

After a query fails the driver might decide to retry it based on its `Retry Policy` and the query itself.
Retry policy can be configured for `Session` or just for a single query.

### Retry policies
By default there are three retry policies:
* [Fallthrough Retry Policy](fallthrough.md) - never retries, returns all errors straight to the user
* [Default Retry Policy](default.md) - used by default, might retry if there is a high chance of success
* [Downgrading Consistency Retry Policy](downgrading-consistency.md) - behaves as [Default Retry Policy](default.md), but also,
    in some more cases, it retries **with lower `Consistency`**.

It's possible to implement a custom `Retry Policy` by implementing the traits `RetryPolicy` and `RetrySession`.

### Query idempotence
A query is idempotent if it can be applied multiple times without changing the result of the initial application

Specifying that a query is idempotent increases the chances that it will be retried in case of failure.
Idempotent queries can be retried in situations where retrying non idempotent queries would be dangerous.

Idempotence has to be specified manually, the driver is not able to figure it out by itself.
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::query::Query;
use scylla::prepared_statement::PreparedStatement;

// Specify that a Query is idempotent
let mut my_query: Query = Query::new("SELECT a FROM ks.tab");
my_query.set_is_idempotent(true);


// Specify that a PreparedStatement is idempotent
let mut prepared: PreparedStatement = session
    .prepare("SELECT a FROM ks.tab")
    .await?;

prepared.set_is_idempotent(true);
# Ok(())
# }
```

```eval_rst
.. toctree::
   :hidden:
   :glob:

   fallthrough
   default
   downgrading-consistency

```
