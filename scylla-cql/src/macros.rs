/// #[derive(FromRow)] derives FromRow for struct
/// Works only on simple structs without generics etc
pub use scylla_macros::FromRow;

/// #[derive(FromUserType)] allows to parse struct as a User Defined Type
/// Works only on simple structs without generics etc
pub use scylla_macros::FromUserType;

/// #[derive(IntoUserType)] allows to pass struct a User Defined Type Value in queries
/// Works only on simple structs without generics etc
pub use scylla_macros::IntoUserType;

/// #[derive(ValueList)] allows to pass struct as a list of values for a query
pub use scylla_macros::ValueList;

/// Derive macro for the `DeserializeCql` trait that generates an implementation
/// which deserializes a User Defined Type with the same layout as the Rust
/// struct.
///
/// At the moment, only structs with named fields are supported.
///
/// This macro properly supports structs with lifetimes, meaning that you can
/// deserialize UDTs with fields that borrow memory from the serialized response.
///
/// # Example
///
/// A UDT defined like this:
///
/// ```notrust
/// CREATE TYPE ks.my_udt (a i32, b text, c blob);
/// ```
///
/// ...can be deserialized using the following struct:
///
/// ```rust
/// # use scylla_cql::macros::DeserializeCql;
/// #[derive(DeserializeCql)]
/// # #[scylla(crate = "scylla_cql")]
/// struct MyUdt<'a> {
///     a: i32,
///     b: Option<String>,
///     c: &'a [u8],
/// }
/// ```
///
/// # Attributes
///
/// The macro supports a number of attributes that customize the generated
/// implementation. Many of the attributes were inspired by procedural macros
/// from `serde` and try to follow the same naming conventions.
///
/// ## Struct attributes
///
/// `#[scylla(crate = "crate_name")]`
///
/// Specify a path to the `scylla` or `scylla-cql` crate to use from the
/// generated code. This attribute should be used if the crate or its API
/// is imported/re-exported under a different name.
///
/// `#[scylla(enforce_order)]`
///
/// By default, the generated deserialization code will be insensitive
/// to the UDT field order - when processing a field, it will look it up
/// in the Rust struct with the corresponding field and set it. However,
/// if the UDT field order is known to be the same both in the UDT
/// and the Rust struct, then the `enforce_order` annotation can be used
/// so that a more efficient implementation that does not perform lookups
/// is be generated. The UDT field names will still be checked during the
/// type check phase.
///
/// #[(scylla(no_field_name_verification))]
///
/// This attribute only works when used with `enforce_order`.
///
/// If set, the generated implementation will not verify the UDF field names at
/// all. Because it only works with `enforce_order`, it will deserialize first
/// UDF field into the first struct field, second UDF field into the second
/// struct field and so on. It will still still verify that the UDF field types
/// and struct field types match.
///
/// ## Field attributes
///
/// `#[scylla(skip)]`
///
/// The field will be completely ignored during deserialization and will
/// be initialized with `Default::default()`.
///
/// `#[scylla(default_when_missing)]`
///
/// If the UDT definition does not contain this field, it will be initialized
/// with `Default::default()`. __This attribute has no effect in `enforce_order`
/// mode.__
///
/// `#[scylla(rename = "field_name")`
///
/// By default, the generated implementation will try to match the Rust field
/// to a UDT field with the same name. This attribute allows to match to a
/// UDT field with provided name.
pub use scylla_macros::DeserializeCql;

/// Derive macro for the `DeserializeRow` trait that generates an implementation
/// which deserializes a row with a similar layout to the Rust struct.
///
/// At the moment, only structs with named fields are supported.
///
/// This macro properly supports structs with lifetimes, meaning that you can
/// deserialize columns that borrow memory from the serialized response.
///
/// # Example
///
/// A table defined like this:
///
/// ```notrust
/// CREATE TABLE ks.my_table (a PRIMARY KEY, b text, c blob);
/// ```
///
/// ...can be deserialized using the following struct:
///
/// ```rust
/// # use scylla_cql::macros::DeserializeRow;
/// #[derive(DeserializeRow)]
/// # #[scylla(crate = "scylla_cql")]
/// struct MyUdt<'a> {
///     a: i32,
///     b: Option<String>,
///     c: &'a [u8],
/// }
/// ```
///
/// # Attributes
///
/// The macro supports a number of attributes that customize the generated
/// implementation. Many of the attributes were inspired by procedural macros
/// from `serde` and try to follow the same naming conventions.
///
/// ## Struct attributes
///
/// `#[scylla(crate = "crate_name")]`
///
/// Specify a path to the `scylla` or `scylla-cql` crate to use from the
/// generated code. This attribute should be used if the crate or its API
/// is imported/re-exported under a different name.
///
/// `#[scylla(enforce_order)]`
///
/// By default, the generated deserialization code will be insensitive
/// to the column order - when processing a column, the corresponding Rust field
/// will be looked up and the column will be deserialized based on its type.
/// However, if the column order and the Rust field order is known to be the
/// same,  then the `enforce_order` annotation can be used so that a more
/// efficient implementation that does not perform lookups is be generated.
/// The generated code will still check that the column and field names match.
///
/// #[(scylla(no_field_name_verification))]
///
/// This attribute only works when used with `enforce_order`.
///
/// If set, the generated implementation will not verify the column names at
/// all. Because it only works with `enforce_order`, it will deserialize first
/// column into the first field, second column into the second field and so on.
/// It will still still verify that the column types and field types match.
///
/// ## Field attributes
///
/// `#[scylla(skip)]`
///
/// The field will be completely ignored during deserialization and will
/// be initialized with `Default::default()`.
///
/// `#[scylla(rename = "field_name")`
///
/// By default, the generated implementation will try to match the Rust field
/// to a column with the same name. This attribute allows to match to a column
/// with provided name.
pub use scylla_macros::DeserializeRow;

// Reexports for derive(IntoUserType)
pub use bytes::{BufMut, Bytes, BytesMut};

pub use crate::impl_from_cql_value_from_method;
