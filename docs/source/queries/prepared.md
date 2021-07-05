# Prepared query

Prepared queries provide much better performance than simple queries, 
but they need to be prepared before use.

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::prepared_statement::PreparedStatement;

// Prepare the query for later execution
let prepared: PreparedStatement = session
    .prepare("INSERT INTO ks.tab (a) VALUES(?)")
    .await?;

// Run the prepared query with some values, just like a simple query
let to_insert: i32 = 12345;
session.execute(&prepared, (to_insert,)).await?;
# Ok(())
# }
```

> ***Warning***  
> For token/shard aware load balancing to work properly, all partition key values
> must be sent as bound values (see [performance section](#performance))

> ***Warning***  
> Prepared query returns only a single page of results.
> If number of rows might exceed single page size use a [paged query](paged.md) instead.

### `Session::prepare`
`Session::prepare` takes query text and prepares the query on all nodes and shards.
If at least one succeds returns success.

### `Session::execute`
`Session::execute` takes a prepared query and bound values and runs the query.
Passing values and the result is the same as in [simple query](simple.md).

### Query options
To specify custom options, set them on the `PreparedStatement` before execution.  
For example to change the consistency:

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::prepared_statement::PreparedStatement;
use scylla::statement::Consistency;

// Prepare the query for later execution
let mut prepared: PreparedStatement = session
    .prepare("INSERT INTO ks.tab (a) VALUES(?)")
    .await?;

// Set prepared query consistency to One
// This is the consistency with which this query will be executed
prepared.set_consistency(Consistency::One);

// Run the prepared query with some values, just like a simple query
let to_insert: i32 = 12345;
session.execute(&prepared, (to_insert,)).await?;
# Ok(())
# }
```

See [PreparedStatement API documentation](https://docs.rs/scylla/0.2.0/scylla/statement/prepared_statement/struct.PreparedStatement.html) 
for more options

### Performance
Prepared queries have good performance, much better than simple queries.  
By default they use shard/token aware load balancing.

> **Always** pass partition key values as bound values. 
> Otherwise the driver can't hash them to compute partition key 
> and they will be sent to the wrong node, which worsens performance.

Let's say we have a table like this:
```sql
TABLE ks.prepare_table (
    a int,
    b int,
    c int,
    PRIMARY KEY (a, b)
)
```

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::prepared_statement::PreparedStatement;

// WRONG - partition key value is passed in query string
// Load balancing will compute the wrong partition key
let wrong_prepared: PreparedStatement = session
    .prepare("INSERT INTO ks.prepare_table (a, b, c) VALUES(12345, ?, 16)")
    .await?;

session.execute(&wrong_prepared, (54321,)).await?;

// GOOD - partition key values are sent as bound values
// Other values can be sent any way you like, it doesn't matter
let good_prepared: PreparedStatement = session
    .prepare("INSERT INTO ks.prepare_table (a, b, c) VALUES(?, ?, 16)")
    .await?;

session.execute(&good_prepared, (12345, 54321)).await?;

# Ok(())
# }
```
