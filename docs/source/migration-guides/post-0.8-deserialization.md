# Post-0.8 deserialization API migration guide

After 0.8, a new deserialization API has been introduced. The new API improves type safety and performance of the old one, so it is highly recommended to switch to it. However, deserialization is an area of the API that users frequently interact with: deserialization traits appear in generic code and custom implementations have been written. In order to make migration easier, the driver still offers the old API, which - while opt-in - can be very easily switched to after version upgrade. Furthermore, a number of facilities have been introduced which help migrate the user code to the new API piece-by-piece.

The old API and migration facilities will be removed in the next major release (2.0).

## Introduction 

### Old traits

The legacy API works by deserializing rows in the query response to a sequence of `Row`s. The `Row` is just a `Vec<Option<CqlValue>>`, where `CqlValue` is an enum that is able to represent any CQL value.

The user can request this type-erased representation to be converted into something useful. There are two traits that power this:

__`FromRow`__

```rust
# extern crate scylla;
# use scylla::frame::response::cql_to_rust::FromRowError;
# use scylla::frame::response::result::Row;
pub trait FromRow: Sized {
    fn from_row(row: Row) -> Result<Self, FromRowError>;
}
```

__`FromCqlVal`__

```rust
# extern crate scylla;
# use scylla::frame::response::cql_to_rust::FromCqlValError;
// The `T` parameter is supposed to be either `CqlValue` or `Option<CqlVal>`
pub trait FromCqlVal<T>: Sized {
    fn from_cql(cql_val: T) -> Result<Self, FromCqlValError>;
}
```

These traits are implemented for some common types:

- `FromRow` is implemented for tuples up to 16 elements,
- `FromCqlVal` is implemented for a bunch of types, and each CQL type can be converted to one of them.

While it's possible to implement those manually, the driver provides procedural macros for automatic derivation in some cases:

- `FromRow` - implements `FromRow` for a struct.
- `FromUserType` - generated an implementation of `FromCqlVal` for the struct, trying to parse the CQL value as a UDT.

Note: the macros above have a default behavior that is different than what `FromRow` and `FromUserTypes` do.

### New traits

The new API introduce two analogous traits that, instead of consuming pre-parsed `Vec<Option<CqlValue>>`, are given raw, serialized data with full information about its type. This leads to better performance and allows for better type safety.

The new traits are:

__`DeserializeRow<'frame>`__

```rust
# extern crate scylla;
# use scylla::types::deserialize::row::ColumnIterator;
# use scylla::frame::frame_errors::ParseError;
# use scylla::frame::response::result::ColumnSpec;
pub trait DeserializeRow<'frame>
where
    Self: Sized,
{
    fn type_check(specs: &[ColumnSpec]) -> Result<(), ParseError>;
    fn deserialize(row: ColumnIterator<'frame>) -> Result<Self, ParseError>;
}
```

__`DeserializeCql<'frame>`__

```rust
# extern crate scylla;
# use scylla::types::deserialize::row::ColumnIterator;
# use scylla::types::deserialize::FrameSlice;
# use scylla::frame::frame_errors::ParseError;
# use scylla::frame::response::result::ColumnType;
pub trait DeserializeCql<'frame>
where
    Self: Sized,
{
    fn type_check(typ: &ColumnType) -> Result<(), ParseError>;
    fn deserialize(
        typ: &'frame ColumnType,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, ParseError>;
}
```

The above traits have been implemented for the same set of types as `FromRow` and `FromCqlVal`, respectively. Notably, `DeserializeRow` is implemented for `Row`, and `DeserializeCql` is implemented for `CqlValue`.

There are also `DeserializeRow` and `DeserializeCql` derive macros, analogous to `FromRow` and `FromUserType`, respectively - but with slightly different defaults (explained later in this doc page).

## Updating the code to use the new API

Some of the core types have been updated to use the new traits. Updating the code to use the new API should be straightforward.

### Basic queries

Sending queries with the single page API should work similarly as before. The `Session::query`, `Session::execute` and `Session::batch` functions have the same interface as before, the only exception being that they return a new, updated `QueryResult`.

Consuming rows from a result will require only minimal changes if you are using helper methods of the `QueryResult`. Now, there is no distinction between "typed" and "non-typed" methods; all methods that return rows need to have the type specified. For example, previously there used to be both `rows(self)` and `rows_typed<RowT: FromRow>(self)`, now there is only a single `rows<R: DeserializeRow<'frame>>(&self)`. Another thing worth mentioning is that the returned iterator now _borrows_ from the `QueryResult` instead of consuming it.

Note that the `QueryResult::rows` field is not available anymore. If you used to access it directly, you need to change your code to use the helper methods instead.

Before:

```rust
# extern crate scylla;
# use scylla::Legacy08Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Legacy08Session) -> Result<(), Box<dyn Error>> {
let iter = session
    .query("SELECT name, age FROM my_keyspace.people", &[])
    .await?
    .rows_typed::<(String, i32)>()?;
for row in iter {
    let (name, age) = row?;
    println!("{} has age {}", name, age);
}
# Ok(())
# }
```

After:

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
// 1. Note that the result must be assigned to a variable here, and only then
// an iterator created.
let result = session
    .query("SELECT name, age FROM my_keyspace.people", &[])
    .await?;

// 2. Note that `rows` is used here, not `rows_typed`.
for row in result.rows::<(String, i32)>()? {
    let (name, age) = row?;
    println!("{} has age {}", name, age);
}
# Ok(())
# }
```

### Iterator queries

The `Session::query_iter` and `Session::execute_iter` have been adjusted, too. They now return a `RawIterator` (notice it's "Raw" instead of "Row") - an intermediate object which needs to be converted into `TypedRowIterator` first before being actually iterated over.

This particular example should work without any changes:

```rust
# extern crate scylla;
# extern crate futures;
# use scylla::Session;
# use std::error::Error;
# use scylla::IntoTypedRows;
# use futures::stream::StreamExt;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
let mut rows_stream = session
    .query_iter("SELECT name, age FROM my_keyspace.people", &[])
    .await?
    .into_typed::<(String, i32)>();

while let Some(next_row_res) = rows_stream.next().await {
    let (a, b): (String, i32) = next_row_res?;
    println!("a, b: {}, {}", a, b);
}
# Ok(())
# }
```

### Procedural macros

As mentioned in the Introduction section, the driver provides new procedural macros for the `DeserializeRow` and `DeserializeCql` traits that are meant to replace `FromRow` and `FromUserType`, respectively. The new macros are designed to be slightly more type-safe by matching column/UDT field names to rust field names dynamically. This is a different behavior to what the old macros used to do, but the new macros can be configured with `#[attributes]` to simulate the old behavior.

__`FromRow` vs. `DeserializeRow`__

The impl generate by `FromRow` expects columns to be in the same order as the struct fields. The `FromRow` trait does not have information about column names, so it cannot match them with the struct field names. You can use `enforce_order` and `no_field_name_verification` attributes to achieve such behavior via `DeserializeRow` trait.

__`FromUserType` vs. `DeserializeCql`__

The impl generated by `FromUserType` expects UDT fields to be in the same order as the struct fields. Field names should be the same both in the UDT and in the struct. You can use the `enforce_order` attribute to achieve such behavior via the `DeserializeCql` trait.

### Adjusting custom impls of deserialization traits

If you have a custom type with a hand-written `impl FromRow` or `impl FromCqlVal`, the best thing to do is to just write a new impl for `DeserializeRow` or `DeserializeCql` manually. Although it's technically possible to implement the new traits by using the existing implementation of the old ones, rolling out a new implementation will avoid performance problems related to the inefficient `CqlValue` representation.

## Accessing the old API

Most important types related to deserialization of the old API have been renamed and contain a `Legacy08` prefix in their names:

- `Session` -> `Legacy08Session`
- `CachingSession` -> `Legacy08CachingSession`
- `RowIterator` -> `Legacy08RowIterator`
- `TypedRowIterator` -> `Legacy08TypedRowIterator`
- `QueryResult` -> `Legacy08QueryResult`

If you intend to quickly migrate your application by using the old API, you can just import the legacy stuff and alias it as the new one, e.g.:

```rust
# extern crate scylla;
use scylla::Legacy08Session as Session;
```

In order to create the `Legacy08Session` instead of the new `Session`, you need to use `SessionBuilder`'s `build_legacy()` method instead of `build()`:

```rust
# extern crate scylla;
# use scylla::{Legacy08Session, SessionBuilder};
# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
let session: Legacy08Session = SessionBuilder::new()
    .known_node("127.0.0.1")
    .build_legacy()
    .await?;
# Ok(())
# }
```

## Mixing the old and the new API

It is possible to use different APIs in different parts of the program. The `Session` allows to create a `Legacy08Session` object that has the old API but shares all resources with the session that has the new API (and vice versa - you can create a new API session from the old API session).

```rust
# extern crate scylla;
# use scylla::{Legacy08Session, Session};
# use std::error::Error;
# async fn check_only_compiles(new_api_session: &Session) -> Result<(), Box<dyn Error>> {
// All of the session objects below will use the same resources: connections,
// metadata, current keyspace, etc.
let old_api_session: Legacy08Session = new_api_session.make_shared_session_with_legacy_api();
let another_new_api_session: Session = old_api_session.make_shared_session_with_new_api();
# Ok(())
# }
```

In addition to that, it is possible to convert a `QueryResult` to `Legacy08QueryResult`:

```rust
# extern crate scylla;
# use scylla::{QueryResult, Legacy08QueryResult};
# use std::error::Error;
# async fn check_only_compiles(result: QueryResult) -> Result<(), Box<dyn Error>> {
let result: QueryResult = result;
let legacy_result: Legacy08QueryResult = result.into_legacy_result()?;
# Ok(())
# }
```

... and `RawIterator` into `Legacy08RowIterator`:

```rust
# extern crate scylla;
# use scylla::transport::iterator::{RawIterator, Legacy08RowIterator};
# use std::error::Error;
# async fn check_only_compiles(iter: RawIterator) -> Result<(), Box<dyn Error>> {
let iter: RawIterator = iter;
let legacy_result: Legacy08RowIterator = iter.into_legacy();
# Ok(())
# }
```
