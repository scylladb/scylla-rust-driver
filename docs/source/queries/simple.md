# Unprepared statement

Unprepared statement takes statement text and values and simply executes them on a `Session`:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn unprepared_statement_example(session: &Session) -> Result<(), Box<dyn Error>> {
// Insert a value into the table
let to_insert: i32 = 12345;
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;
# Ok(())
# }
```

> ***Warning***\
> Don't use unprepared statements to receive large amounts of data.\
> By default the statement execution is unpaged and might cause heavy load on the cluster.\
> In such cases set a page size and use [paged statement](paged.md) instead.\
> 
> When page size is set, `Session::query` will return only the first page of results.

> ***Warning***\
> If the values are not empty, driver first needs to send a `PREPARE` request
> in order to fetch information required to serialize values. This will affect
> performance because 2 round trips will be required instead of 1.

### First argument - the query
As the first argument `Session::query` takes anything implementing `Into<Query>`.\
You can create a query manually to set custom options. For example to change query consistency:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::unprepared_statement::UnpreparedStatement;
use scylla::statement::Consistency;

// Create an UnpreparedStatement manually to change the Consistency to ONE
let mut my_query: UnpreparedStatement =
    UnpreparedStatement::new("INSERT INTO ks.tab (a) VALUES(?)");
my_query.set_consistency(Consistency::One);

// Insert a value into the table
let to_insert: i32 = 12345;
session.query(my_query, (to_insert,)).await?;
# Ok(())
# }
```
See [Query API documentation](https://docs.rs/scylla/latest/scylla/statement/unprepared_statement/struct.UnpreparedStatement.html) for more options

### Second argument - the values
Query text is constant, but the values might change.
You can pass changing values to a query by specifying a list of variables as bound values.\
Each `?` in query text will be filled with the matching value. 

The easiest way is to pass values using a tuple:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
// Sending an integer and a string using a tuple
session
    .query("INSERT INTO ks.tab (a, b, c) VALUES(?, ?, 'text2')", (2_i32, "Some text"))
    .await?;
# Ok(())
# }
```
Here the first `?` will be filled with `2` and the second with `"Some text"`.
> **Never** pass values by adding strings, this could lead to [SQL Injection](https://en.wikipedia.org/wiki/SQL_injection)

See [Query values](values.md) for more information about sending values in queries

### Query result
`Session::query` returns `QueryResult` with rows represented as `Option<Vec<Row>>`.\
Each row can be parsed as a tuple of rust types using `rows_typed`:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;

// Query rows from the table and print them
let result = session.query("SELECT a FROM ks.tab", &[]).await?;
let mut iter = result.rows_typed::<(i32,)>()?;
while let Some(read_row) = iter.next().transpose()? {
    println!("Read a value from row: {}", read_row.0);
}
# Ok(())
# }
```
> In cases where page size is set, unprepared statement returns only a single page of results.\
> To receive all pages use a [paged statement](paged.md) instead.\

See [Query result](result.md) for more information about handling query results

### Performance
Unprepared statements should not be used in places where performance matters.\
If performance matters use a [Prepared statement](prepared.md) instead.

With unprepared statement the database has to parse statement text each time it's executed, which worsens performance.\

Additionally token and shard aware load balancing does not work with unprepared statements. They are sent to random nodes.
