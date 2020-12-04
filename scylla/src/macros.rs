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
