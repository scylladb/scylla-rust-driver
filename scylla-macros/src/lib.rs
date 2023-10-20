use proc_macro::TokenStream;
use quote::ToTokens;

mod from_row;
mod from_user_type;
mod into_user_type;
mod parser;
mod value_list;

mod serialize;

/// See the documentation for this item in the `scylla` crate.
#[proc_macro_derive(SerializeCql, attributes(scylla))]
pub fn serialize_cql_derive(tokens_input: TokenStream) -> TokenStream {
    match serialize::cql::derive_serialize_cql(tokens_input) {
        Ok(t) => t.into_token_stream().into(),
        Err(e) => e.into_compile_error().into(),
    }
}

/// #[derive(FromRow)] derives FromRow for struct
/// Works only on simple structs without generics etc
#[proc_macro_derive(FromRow, attributes(scylla_crate))]
pub fn from_row_derive(tokens_input: TokenStream) -> TokenStream {
    let res = from_row::from_row_derive(tokens_input);
    res.unwrap_or_else(|e| e.into_compile_error().into())
}

/// #[derive(FromUserType)] allows to parse a struct as User Defined Type
/// Works only on simple structs without generics etc
#[proc_macro_derive(FromUserType, attributes(scylla_crate))]
pub fn from_user_type_derive(tokens_input: TokenStream) -> TokenStream {
    let res = from_user_type::from_user_type_derive(tokens_input);
    res.unwrap_or_else(|e| e.into_compile_error().into())
}

/// #[derive(IntoUserType)] allows to parse a struct as User Defined Type
/// Works only on simple structs without generics etc
#[proc_macro_derive(IntoUserType, attributes(scylla_crate))]
pub fn into_user_type_derive(tokens_input: TokenStream) -> TokenStream {
    let res = into_user_type::into_user_type_derive(tokens_input);
    res.unwrap_or_else(|e| e.into_compile_error().into())
}

/// #[derive(ValueList)] derives ValueList for struct
/// Works only on simple structs without generics etc
#[proc_macro_derive(ValueList, attributes(scylla_crate))]
pub fn value_list_derive(tokens_input: TokenStream) -> TokenStream {
    let res = value_list::value_list_derive(tokens_input);
    res.unwrap_or_else(|e| e.into_compile_error().into())
}
