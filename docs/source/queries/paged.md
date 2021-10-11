# Paged query
Sometimes query results might not fit in a single page. Paged queries
allow to receive the whole result page by page.

`Session::query_iter` and `Session::execute_iter` take a [simple query](simple.md) or a [prepared query](prepared.md)
and return an `async` iterator over result `Rows`.

### Examples
Use `query_iter` to perform a [simple query](simple.md) with paging:
```rust
# extern crate scylla;
# extern crate futures;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use futures::stream::StreamExt;

let mut rows_stream = session
    .query_iter("SELECT a, b FROM ks.t", &[])
    .await?
    .into_typed::<(i32, i32)>();

while let Some(next_row_res) = rows_stream.next().await {
    let (a, b): (i32, i32) = next_row_res?;
    println!("a, b: {}, {}", a, b);
}
# Ok(())
# }
```

Use `execute_iter` to perform a [prepared query](prepared.md) with paging:
```rust
# extern crate scylla;
# extern crate futures;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use scylla::prepared_statement::PreparedStatement;
use futures::stream::StreamExt;

let prepared: PreparedStatement = session
    .prepare("SELECT a, b FROM ks.t")
    .await?;

let mut rows_stream = session
    .execute_iter(prepared, &[])
    .await?
    .into_typed::<(i32, i32)>();

while let Some(next_row_res) = rows_stream.next().await {
    let (a, b): (i32, i32) = next_row_res?;
    println!("a, b: {}, {}", a, b);
}
# Ok(())
# }
```

Query values can be passed to `query_iter` and `execute_iter` just like in a [simple query](simple.md)

### Configuring page size
It's possible to configure the size of a single page.

On a `Query`:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::query::Query;

let mut query: Query = Query::new("SELECT a, b FROM ks.t".to_string());
query.set_page_size(16);

let _ = session.query_iter(query, &[]).await?; // ...
# Ok(())
# }
```

On a `PreparedStatement`:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::prepared_statement::PreparedStatement;

let mut prepared: PreparedStatement = session
    .prepare("SELECT a, b FROM ks.t")
    .await?;

prepared.set_page_size(16);

let _ = session.execute_iter(prepared, &[]).await?; // ...
# Ok(())
# }
```

### Passing the paging state manually
It's possible to fetch a single page from the table, extract the paging state
from the result and manually pass it to the next query. That way, the next
query will start fetching the results from where the previous one left off.

On a `Query`:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::query::Query;

let paged_query = Query::new("SELECT a, b, c FROM ks.t".to_owned()).with_page_size(6);
let res1 = session.query(paged_query.clone(), &[]).await?;
let res2 = session
    .query_paged(paged_query.clone(), &[], res1.paging_state)
    .await?;
# Ok(())
# }
```

On a `PreparedStatement`:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::query::Query;

let paged_prepared = session
    .prepare(Query::new("SELECT a, b, c FROM ks.t".to_owned()).with_page_size(7))
    .await?;
let res1 = session.execute(&paged_prepared, &[]).await?;
let res2 = session
    .execute_paged(&paged_prepared, &[], res1.paging_state)
    .await?;
# Ok(())
# }
```

### Performance
Performance is the same as in non-paged variants.  
For the best performance use [prepared queries](prepared.md).