# Paged query
Sometimes query results might not fit in a single page. Paged queries
allow to receive the whole result page by page.

`Session::query_iter` and `Session::execute_iter` take an [unprepared statement](simple.md) or a [prepared statement](prepared.md)
and return an `async` iterator over result `Rows`.

> ***Warning***\
> In case of unprepared variant (`Session::query_iter`) if the values are not empty
> driver will first fully prepare a statement (which means issuing additional request to each
> node in a cluster). This will have a performance penalty - how big it is depends on
> the size of your cluster (more nodes - more requests) and the size of returned
> result (more returned pages - more amortized penalty). In any case, it is preferable to
> use `Session::execute_iter`.

### Examples
Use `query_iter` to execute an [unprepared statement](simple.md) with paging:
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

Use `execute_iter` to execute a [prepared statement](prepared.md) with paging:
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

Bound values can be passed to `query_iter` and `execute_iter` just like in an [unprepared statement](simple.md)

### Configuring page size
It's possible to configure the size of a single page.

On an `UnpreparedStatement`:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::unprepared_statement::UnpreparedStatement;

let mut query: UnpreparedStatement =
    UnpreparedStatement::new("SELECT a, b FROM ks.t");
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

On an `UnpreparedStatement`:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::unprepared_statement::UnpreparedStatement;

let paged_query = UnpreparedStatement::new("SELECT a, b, c FROM ks.t")
    .with_page_size(6);
let res1 = session.query(paged_query.clone(), &[]).await?;
let res2 = session
    .query_paged(paged_query.clone(), &[], res1.paging_state)
    .await?;
# Ok(())
# }
```

> ***Warning***\
> If the values are not empty, driver first needs to send a `PREPARE` request
> in order to fetch information required to serialize values. This will affect
> performance because 2 round trips will be required instead of 1.

On a `PreparedStatement`:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::unprepared_statement::UnpreparedStatement;

let paged_prepared = session
    .prepare(UnpreparedStatement::new("SELECT a, b, c FROM ks.t").with_page_size(7))
    .await?;
let res1 = session.execute(&paged_prepared, &[]).await?;
let res2 = session
    .execute_paged(&paged_prepared, &[], res1.paging_state)
    .await?;
# Ok(())
# }
```

### Performance
Performance is the same as in non-paged variants.\
For the best performance use [prepared statements](prepared.md).