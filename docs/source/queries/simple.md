# Simple query

Simple query takes query text and values and simply executes them on a `Session`:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn simple_query_example(session: &Session) -> Result<(), Box<dyn Error>> {
// Insert a value into the table
let to_insert: i32 = 12345;
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;
# Ok(())
# }
```

> ***Warning***  
> Simple query returns only a single page of results.
> If number of rows might exceed single page size use a [paged query](paged.md) instead.  

### First argument - the query
As the first argument `Session::query` takes anything implementing `Into<Query>`.  
You can create a query manually to set custom options. For example to change query consistency:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::query::Query;
use scylla::statement::Consistency;

// Create a Query manually to change the Consistency to ONE
let mut my_query: Query = Query::new("INSERT INTO ks.tab (a) VALUES(?)".to_string());
my_query.set_consistency(Consistency::One);

// Insert a value into the table
let to_insert: i32 = 12345;
session.query(my_query, (to_insert,)).await?;
# Ok(())
# }
```
See [Query API documentation](https://docs.rs/scylla/0.2.0/scylla/statement/query/struct.Query.html) for more options

### Second argument - the values
Query text is constant, but the values might change.
You can pass changing values to a query by specifying a list of variables as bound values.  
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
`Session::query` returns `QueryResult` with rows represented as `Option<Vec<Row>>`.  
Each row can be parsed as a tuple of rust types using `into_typed`:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;

// Query rows from the table and print them
if let Some(rows) = session.query("SELECT a FROM ks.tab", &[]).await?.rows {
    // Parse each row as a tuple containing single i32
    for row in rows.into_typed::<(i32,)>() {
        let read_row: (i32,) = row?;
        println!("Read a value from row: {}", read_row.0);
    }
}
# Ok(())
# }
```
> Simple query returns only a single page of results.
> If number of rows might exceed single page size use a [paged query](paged.md) instead.  

See [Query result](result.md) for more information about handling query results

### Performance
Simple queries should not be used in places where performance matters.  
If perfomance matters use a [Prepared query](prepared.md) instead.

With simple query the database has to parse query text each time it's executed, which worsens performance.  

Additionaly token and shard aware load balancing does not work with simple queries. They are sent to random nodes.
