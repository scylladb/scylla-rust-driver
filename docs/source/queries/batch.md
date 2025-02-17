# Batch statement

A batch statement allows to execute many data-modifying statements at once.\
These statements can be [simple](simple.md) or [prepared](prepared.md).\
Only `INSERT`, `UPDATE` and `DELETE` statements are allowed.

```rust
# extern crate scylla;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::statement::batch::Batch;
use scylla::statement::query::Statement;
use scylla::statement::prepared::PreparedStatement;

// Create a batch statement
let mut batch: Batch = Default::default();

// Add a simple statement to the batch using its text
batch.append_statement("INSERT INTO ks.tab(a, b) VALUES(1, 2)");

// Add a simple statement created manually to the batch
let simple: Statement = Statement::new("INSERT INTO ks.tab (a, b) VALUES(3, 4)");
batch.append_statement(simple);

// Add a prepared statement to the batch
let prepared: PreparedStatement = session
    .prepare("INSERT INTO ks.tab (a, b) VALUES(?, 6)")
    .await?;
batch.append_statement(prepared);

// Specify bound values to use with each statement
let batch_values = ((),
                    (),
                    (5_i32,));

// Run the batch
session.batch(&batch, batch_values).await?;
# Ok(())
# }
```

> ***Warning***\
> Using simple statements with bind markers in batches is strongly discouraged.
> For each simple statement with a non-empty list of values in the batch,
> the driver will send a prepare request, and it will be done **sequentially**.
> Results of preparation are not cached between `Session::batch` calls.
> Consider preparing the statements before putting them into the batch.

### Preparing a batch
Instead of preparing each statement individually, it's possible to prepare a whole batch at once:

```rust
# extern crate scylla;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::statement::batch::Batch;

// Create a batch statement with unprepared statements
let mut batch: Batch = Default::default();
batch.append_statement("INSERT INTO ks.simple_unprepared1 VALUES(?, ?)");
batch.append_statement("INSERT INTO ks.simple_unprepared2 VALUES(?, ?)");

// Prepare all statements in the batch at once
let prepared_batch: Batch = session.prepare_batch(&batch).await?;

// Specify bound values to use with each statement
let batch_values = ((1_i32, 2_i32),
                    (3_i32, 4_i32));

// Run the prepared batch
session.batch(&prepared_batch, batch_values).await?;
# Ok(())
# }
```

### Batch options
You can set various options by operating on the `Batch` object.\
For example to change consistency:
```rust
# extern crate scylla;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::statement::batch::Batch;
use scylla::statement::Consistency;

// Create a batch
let mut batch: Batch = Default::default();
batch.append_statement("INSERT INTO ks.tab(a) VALUES(16)");

// Set batch consistency to One
batch.set_consistency(Consistency::One);

// Run the batch
session.batch(&batch, ((), )).await?;
# Ok(())
# }
```

See [Batch API documentation](https://docs.rs/scylla/latest/scylla/statement/batch/struct.Batch.html)
for more options

### Batch values
Batch takes a tuple of values specified just like in [simple](simple.md) or [prepared](prepared.md) queries.

Length of batch values must be equal to the number of statements in a batch.\
Each statement must have its values specified, even if they are empty.

Values passed to `Session::batch` must implement the trait `BatchValues`.\
By default this includes tuples `()` and slices `&[]` of tuples and slices which implement `SerializeRow`.

Example:
```rust
# extern crate scylla;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::statement::batch::Batch;

let mut batch: Batch = Default::default();

// A statement with two bound values
batch.append_statement("INSERT INTO ks.tab(a, b) VALUES(?, ?)");

// A statement with one bound value
batch.append_statement("INSERT INTO ks.tab(a, b) VALUES(3, ?)");

// A statement with no bound values
batch.append_statement("INSERT INTO ks.tab(a, b) VALUES(5, 6)");

// Batch values is a tuple of 3 tuples containing values for each statement
let batch_values = ((1_i32, 2_i32), // Tuple with two values for the first statement
                    (4_i32,),       // Tuple with one value for the second statement
                    ());            // Empty tuple/unit for the third statement

// Run the batch
// Note that the driver will prepare the first two statements, due to them
// not being prepared and having a non-empty list of values.
session.batch(&batch, batch_values).await?;
# Ok(())
# }
```
For more information about sending values in a statement see [Query values](values.md)


### Performance
Batch statements do not use token/shard aware load balancing, batches are sent to a random node.

Use [prepared queries](prepared.md) for best performance
