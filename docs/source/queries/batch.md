# Batch statement

A batch statement allows to run many queries at once.  
These queries can be [simple queries](simple.md) or [prepared queries](prepared.md).  
Only queries like `INSERT` or `UPDATE` can be in a batch, batch doesn't return any rows.

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::batch::Batch;
use scylla::query::Query;
use scylla::prepared_statement::PreparedStatement;

// Create a batch statement
let mut batch: Batch = Default::default();

// Add a simple query to the batch using its text
batch.append_statement("INSERT INTO ks.tab(a, b) VALUES(?, ?)");

// Add a simple query created manually to the batch
let simple: Query = Query::new("INSERT INTO ks.tab (a, b) VALUES(3, 4)".to_string());
batch.append_statement(simple);

// Add a prepared query to the batch
let prepared: PreparedStatement = session
    .prepare("INSERT INTO ks.tab (a, b) VALUES(?, 6)")
    .await?;
batch.append_statement(prepared);

// Specify bound values to use with each query
let batch_values = ((1_i32, 2_i32),
                    (),
                    (5_i32,));

// Run the batch, doesn't return any rows
session.batch(&batch, batch_values).await?;
# Ok(())
# }
```

### Batch options
You can set various options by operating on the `Batch` object.  
For example to change consistency:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::batch::Batch;
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

See [Batch API documentation](https://docs.rs/scylla/0.2.0/scylla/statement/batch/struct.Batch.html)
for more options

### Batch values
Batch takes a tuple of values specified just like in [simple](simple.md) or [prepared](prepared.md) queries.

Length of batch values must be equal to the number of statements in a batch.  
Each query must have its values specified, even if they are empty.

Values passed to `Session::batch` must implement the trait `BatchValues`.  
By default this includes tuples `()` and slices `&[]` of tuples and slices which implement `ValueList`.  

Example:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::batch::Batch;

let mut batch: Batch = Default::default();

// A query with two bound values
batch.append_statement("INSERT INTO ks.tab(a, b) VALUES(?, ?)");

// A query with one bound value
batch.append_statement("INSERT INTO ks.tab(a, b) VALUES(3, ?)");

// A query with no bound values
batch.append_statement("INSERT INTO ks.tab(a, b) VALUES(5, 6)");

// Batch values is a tuple of 3 tuples containing values for each query
let batch_values = ((1_i32, 2_i32), // Tuple with two values for the first query
                    (4_i32,),       // Tuple with one value for the second query
                    ());            // Empty tuple/unit for the third query

// Run the batch
session.batch(&batch, batch_values).await?;
# Ok(())
# }
```
For more information about sending values in a query see [Query values](values.md)


### Performance
Batch statements do not use token/shard aware load balancing, batches are sent to a random node.

Use [prepared queries](prepared.md) for best performance
