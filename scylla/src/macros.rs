/// Prepares query values to be used with queries and prepared statements
#[macro_export]
macro_rules! values {
    ($($value:expr),*) => {
        {
            use $crate::frame::value::Value;
            [$(<_ as ::std::convert::Into<Value>>::into($value)),*]
        }
    };
}

#[macro_export]
macro_rules! try_values {
    ($($value:expr),*) => {
        {
            use $crate::frame::value::{Value, ValueTooBig, TryIntoValue};
            let lambda = || -> Result<_, ValueTooBig> {
                Ok([$(<_ as TryIntoValue>::try_into_value($value) ?),*])
            };

            lambda()
        }
    };
}

/// #[derive(FromRow)] derives FromRow for struct
/// Works only on simple structs without generics etc
pub use scylla_macros::FromRow;

/// #[derive(FromUserType)] allows to parse struct as a User Defined Type
/// Works only on simple structs without generics etc
pub use scylla_macros::FromUserType;

/// #[derive(IntoUserType)] allows to pass struct a User Defined Type Value in queries
/// Works only on simple structs without generics etc
pub use scylla_macros::IntoUserType;

// Reexports for derive(IntoUserType)
pub use bytes::{BufMut, Bytes, BytesMut};
