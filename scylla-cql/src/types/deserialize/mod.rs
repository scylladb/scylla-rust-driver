//! Framework for deserialization of data returned by database queries.
//!
//! Deserialization is based on two traits:
//!
//! - A type that implements `DeserializeValue<'frame, 'metadata>` can be deserialized
//!   from a single _CQL value_ - i.e. an element of a row in the query result,
//! - A type that implements `DeserializeRow<'frame>` can be deserialized
//!   from a single _row_ of a query result.
//!
//! Those traits are quite similar to each other, both in the idea behind them
//! and the interface that they expose.
//!
//! # `type_check` and `deserialize`
//!
//! The deserialization process is divided into two parts: type checking and
//! actual deserialization, represented by `DeserializeValue`/`DeserializeRow`'s
//! methods called `type_check` and `deserialize`.
//!
//! The `deserialize` method can assume that `type_check` was called before, so
//! it doesn't have to verify the type again. This can be a performance gain
//! when deserializing query results with multiple rows: as each row in a result
//! has the same type, it is only necessary to call `type_check` once for the
//! whole result and then `deserialize` for each row.
//!
//! Note that `deserialize` is not an `unsafe` method - although you can be
//! sure that the driver will call `type_check` before `deserialize`, you
//! shouldn't do unsafe things based on this assumption.
//!
//! # Data ownership
//!
//! Some CQL types can be easily consumed while still partially serialized.
//! For example, types like `blob` or `text` can be just represented with
//! `&[u8]` and `&str` that just point to a part of the serialized response.
//! This is more efficient than using `Vec<u8>` or `String` because it avoids
//! an allocation and a copy, however it is less convenient because those types
//! are bound with a lifetime.
//!
//! The framework supports types that refer to the serialized response's memory
//! in three different ways:
//!
//! ## Owned types
//!
//! Some types don't borrow anything and fully own their data, e.g. `i32` or
//! `String`. They aren't constrained by any lifetime and should implement
//! the respective trait for _all_ lifetimes, i.e.:
//!
//! ```rust
//! # use scylla_cql::frame::response::result::ColumnType;
//! # use scylla_cql::types::deserialize::{DeserializationError, FrameSlice, TypeCheckError};
//! # use scylla_cql::types::deserialize::value::DeserializeValue;
//! use thiserror::Error;
//! struct MyVec(Vec<u8>);
//! #[derive(Debug, Error)]
//! enum MyDeserError {
//!     #[error("Expected bytes")]
//!     ExpectedBytes,
//!     #[error("Expected non-null")]
//!     ExpectedNonNull,
//! }
//! impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for MyVec {
//!     fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
//!          if let ColumnType::Blob = typ {
//!              return Ok(());
//!          }
//!          Err(TypeCheckError::new(MyDeserError::ExpectedBytes))
//!      }
//!
//!      fn deserialize(
//!          _typ: &'metadata ColumnType<'metadata>,
//!          v: Option<FrameSlice<'frame>>,
//!      ) -> Result<Self, DeserializationError> {
//!          v.ok_or_else(|| DeserializationError::new(MyDeserError::ExpectedNonNull))
//!             .map(|v| Self(v.as_slice().to_vec()))
//!      }
//! }
//! ```
//!
//! ## Borrowing types
//!
//! Some types do not fully contain their data but rather will point to some
//! bytes in the serialized response, e.g. `&str` or `&[u8]`. Those types will
//! usually contain a lifetime in their definition. In order to properly
//! implement `DeserializeValue` or `DeserializeRow` for such a type, the `impl`
//! should still have a generic lifetime parameter, but the lifetimes from the
//! type definition should be constrained with the generic lifetime parameter.
//! For example:
//!
//! ```rust
//! # use scylla_cql::frame::response::result::ColumnType;
//! # use scylla_cql::types::deserialize::{DeserializationError, FrameSlice, TypeCheckError};
//! # use scylla_cql::types::deserialize::value::DeserializeValue;
//! use thiserror::Error;
//! struct MySlice<'a>(&'a [u8]);
//! #[derive(Debug, Error)]
//! enum MyDeserError {
//!     #[error("Expected bytes")]
//!     ExpectedBytes,
//!     #[error("Expected non-null")]
//!     ExpectedNonNull,
//! }
//! impl<'a, 'frame, 'metadata> DeserializeValue<'frame, 'metadata> for MySlice<'a>
//! where
//!     'frame: 'a,
//! {
//!     fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
//!          if let ColumnType::Blob = typ {
//!              return Ok(());
//!          }
//!          Err(TypeCheckError::new(MyDeserError::ExpectedBytes))
//!      }
//!
//!      fn deserialize(
//!          _typ: &'metadata ColumnType<'metadata>,
//!          v: Option<FrameSlice<'frame>>,
//!      ) -> Result<Self, DeserializationError> {
//!          v.ok_or_else(|| DeserializationError::new(MyDeserError::ExpectedNonNull))
//!             .map(|v| Self(v.as_slice()))
//!      }
//! }
//! ```
//!
//! ## Reference-counted types
//!
//! Internally, the driver uses the `bytes::Bytes` type to keep the contents
//! of the serialized response. It supports creating derived `Bytes` objects
//! which point to a subslice but keep the whole, original `Bytes` object alive.
//!
//! During deserialization, a type can obtain a `Bytes` subslice that points
//! to the serialized value. This approach combines advantages of the previous
//! two approaches - creating a derived `Bytes` object can be cheaper than
//! allocation and a copy (it supports `Arc`-like semantics) and the `Bytes`
//! type is not constrained by a lifetime. However, you should be aware that
//! the subslice will keep the whole `Bytes` object that holds the frame alive.
//! It is not recommended to use this approach for long-living objects because
//! it can introduce space leaks.
//!
//! Example:
//!
//! ```rust
//! # use scylla_cql::frame::response::result::ColumnType;
//! # use scylla_cql::types::deserialize::{DeserializationError, FrameSlice, TypeCheckError};
//! # use scylla_cql::types::deserialize::value::DeserializeValue;
//! # use bytes::Bytes;
//! use thiserror::Error;
//! struct MyBytes(Bytes);
//! #[derive(Debug, Error)]
//! enum MyDeserError {
//!     #[error("Expected bytes")]
//!     ExpectedBytes,
//!     #[error("Expected non-null")]
//!     ExpectedNonNull,
//! }
//! impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for MyBytes {
//!     fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
//!          if let ColumnType::Blob = typ {
//!              return Ok(());
//!          }
//!          Err(TypeCheckError::new(MyDeserError::ExpectedBytes))
//!      }
//!
//!      fn deserialize(
//!          _typ: &'metadata ColumnType<'metadata>,
//!          v: Option<FrameSlice<'frame>>,
//!      ) -> Result<Self, DeserializationError> {
//!          v.ok_or_else(|| DeserializationError::new(MyDeserError::ExpectedNonNull))
//!             .map(|v| Self(v.to_bytes()))
//!      }
//! }
//! ```

pub mod frame_slice;
pub mod result;
pub mod row;
pub mod value;

pub use frame_slice::FrameSlice;

pub use row::DeserializeRow;
pub use value::DeserializeValue;

use std::error::Error;
use std::sync::Arc;

use thiserror::Error;

// Errors

/// An error indicating that a failure happened during type check.
///
/// The error is type-erased so that the crate users can define their own
/// type check impls and their errors.
/// As for the impls defined or generated
/// by the driver itself, the following errors can be returned:
///
/// - [`row::BuiltinTypeCheckError`] is returned when type check of
///   one of types with an impl built into the driver fails. It is also returned
///   from impls generated by the `DeserializeRow` macro.
/// - [`value::BuiltinTypeCheckError`] is analogous to the above but is
///   returned from [`DeserializeValue::type_check`] instead both in the case of
///   builtin impls and impls generated by the `DeserializeValue` macro.
///   It won't be returned by the `Session` directly, but it might be nested
///   in the [`row::BuiltinTypeCheckError`].
#[derive(Debug, Clone, Error)]
#[error("TypeCheckError: {0}")]
pub struct TypeCheckError(pub(crate) Arc<dyn std::error::Error + Send + Sync>);

impl TypeCheckError {
    /// Constructs a new `TypeCheckError`.
    #[inline]
    pub fn new(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self(Arc::new(err))
    }

    /// Retrieve an error reason by downcasting to specific type.
    pub fn downcast_ref<T: std::error::Error + 'static>(&self) -> Option<&T> {
        self.0.downcast_ref()
    }
}

/// An error indicating that a failure happened during deserialization.
///
/// The error is type-erased so that the crate users can define their own
/// deserialization impls and their errors. As for the impls defined or generated
/// by the driver itself, the following errors can be returned:
///
/// - [`row::BuiltinDeserializationError`] is returned when deserialization of
///   one of types with an impl built into the driver fails. It is also returned
///   from impls generated by the `DeserializeRow` macro.
/// - [`value::BuiltinDeserializationError`] is analogous to the above but is
///   returned from [`DeserializeValue::deserialize`] instead both in the case of
///   builtin impls and impls generated by the `DeserializeValue` macro.
///   It won't be returned by the `Session` directly, but it might be nested
///   in the [`row::BuiltinDeserializationError`].
#[derive(Debug, Clone, Error)]
#[error("DeserializationError: {0}")]
pub struct DeserializationError(Arc<dyn Error + Send + Sync>);

impl DeserializationError {
    /// Constructs a new `DeserializationError`.
    #[inline]
    pub fn new(err: impl Error + Send + Sync + 'static) -> Self {
        Self(Arc::new(err))
    }

    /// Retrieve an error reason by downcasting to specific type.
    pub fn downcast_ref<T: Error + 'static>(&self) -> Option<&T> {
        self.0.downcast_ref()
    }
}

// This is a hack to enable setting the proper Rust type name in error messages,
// even though the error originates from some helper type used underneath.
// ASSUMPTION: This should be used:
// - ONLY in proper type_check()/deserialize() implementation,
// - BEFORE an error is cloned (because otherwise the Arc::get_mut fails).
macro_rules! make_error_replace_rust_name {
    ($fn_name: ident, $outer_err: ty, $inner_err: ty) => {
        // Not part of the public API; used in derive macros.
        #[doc(hidden)]
        pub fn $fn_name<RustT>(mut err: $outer_err) -> $outer_err {
            // Safety: the assumed usage of this function guarantees that the Arc has not yet been cloned.
            let arc_mut = std::sync::Arc::get_mut(&mut err.0).unwrap();

            let rust_name: &mut &str = {
                if let Some(err) = arc_mut.downcast_mut::<$inner_err>() {
                    &mut err.rust_name
                } else {
                    unreachable!(concat!(
                        "This function is assumed to be called only on built-in ",
                        stringify!($inner_err),
                        " kinds."
                    ))
                }
            };

            *rust_name = std::any::type_name::<RustT>();
            err
        }
    };
}
use make_error_replace_rust_name;

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};

    use crate::frame::response::result::{ColumnSpec, ColumnType, TableSpec};
    use crate::frame::types;

    pub(super) static CELL1: &[u8] = &[1, 2, 3];
    pub(super) static CELL2: &[u8] = &[4, 5, 6, 7];

    pub(super) fn serialize_cells(
        cells: impl IntoIterator<Item = Option<impl AsRef<[u8]>>>,
    ) -> Bytes {
        let mut bytes = BytesMut::new();
        for cell in cells {
            types::write_bytes_opt(cell, &mut bytes).unwrap();
        }
        bytes.freeze()
    }

    pub(super) fn spec<'a>(name: &'a str, typ: ColumnType<'a>) -> ColumnSpec<'a> {
        ColumnSpec::borrowed(name, typ, TableSpec::borrowed("ks", "tbl"))
    }
}
