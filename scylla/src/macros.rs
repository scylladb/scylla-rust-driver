/// Prepares query values to be used with queries and prepared statements
#[macro_export]
macro_rules! values {
    () => {
        Ok($crate::frame::value::SerializedValues::new())
    };
    ($($value:expr),*) => {
        {
            use $crate::frame::value::{SerializedValues, AddValueError};

            let result: std::result::Result<SerializedValues, AddValueError> = {
                let mut serialized_values = SerializedValues::new();

                $(
                if let Err(e) = serialized_values.add_value(&$value) {
                    Err(e)
                } else
                )*
                {
                    Ok(serialized_values)
                }
            };

            result
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
