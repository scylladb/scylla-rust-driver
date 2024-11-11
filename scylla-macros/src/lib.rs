use darling::{FromMeta, ToTokens};
use proc_macro::TokenStream;

mod from_row;
mod from_user_type;
mod into_user_type;
mod parser;
mod value_list;

// Flavor of serialization/deserialization macros ({De,S}erialize{Value,Row}).
#[derive(Copy, Clone, PartialEq, Eq, Default)]
enum Flavor {
    #[default]
    MatchByName,
    EnforceOrder,
}

impl FromMeta for Flavor {
    fn from_string(value: &str) -> darling::Result<Self> {
        match value {
            "match_by_name" => Ok(Self::MatchByName),
            "enforce_order" => Ok(Self::EnforceOrder),
            _ => Err(darling::Error::unknown_value(value)),
        }
    }
}

mod serialize;

/// Documentation for this macro can only be found
/// in `scylla` crate - not in scylla-macros nor in scylla-cql.
/// This is because of rustdocs limitations that are hard to explain here.
#[proc_macro_derive(SerializeValue, attributes(scylla))]
pub fn serialize_value_derive(tokens_input: TokenStream) -> TokenStream {
    match serialize::value::derive_serialize_value(tokens_input) {
        Ok(t) => t.into_token_stream().into(),
        Err(e) => e.into_compile_error().into(),
    }
}

/// Documentation for this macro can only be found
/// in `scylla` crate - not in scylla-macros nor in scylla-cql.
/// This is because of rustdocs limitations that are hard to explain here.
#[proc_macro_derive(SerializeRow, attributes(scylla))]
pub fn serialize_row_derive(tokens_input: TokenStream) -> TokenStream {
    match serialize::row::derive_serialize_row(tokens_input) {
        Ok(t) => t.into_token_stream().into(),
        Err(e) => e.into_compile_error().into(),
    }
}

/// Documentation for this macro can only be found
/// in `scylla` crate - not in scylla-macros nor in scylla-cql.
/// This is because of rustdocs limitations that are hard to explain here.
#[proc_macro_derive(FromRow, attributes(scylla_crate))]
pub fn from_row_derive(tokens_input: TokenStream) -> TokenStream {
    let res = from_row::from_row_derive(tokens_input);
    res.unwrap_or_else(|e| e.into_compile_error().into())
}

/// Documentation for this macro can only be found
/// in `scylla` crate - not in scylla-macros nor in scylla-cql.
/// This is because of rustdocs limitations that are hard to explain here.
#[proc_macro_derive(FromUserType, attributes(scylla_crate))]
pub fn from_user_type_derive(tokens_input: TokenStream) -> TokenStream {
    let res = from_user_type::from_user_type_derive(tokens_input);
    res.unwrap_or_else(|e| e.into_compile_error().into())
}

/// Documentation for this macro can only be found
/// in `scylla` crate - not in scylla-macros nor in scylla-cql.
/// This is because of rustdocs limitations that are hard to explain here.
#[proc_macro_derive(IntoUserType, attributes(scylla_crate))]
pub fn into_user_type_derive(tokens_input: TokenStream) -> TokenStream {
    let res = into_user_type::into_user_type_derive(tokens_input);
    res.unwrap_or_else(|e| e.into_compile_error().into())
}

/// Documentation for this macro can only be found
/// in `scylla` crate - not in scylla-macros nor in scylla-cql.
/// This is because of rustdocs limitations that are hard to explain here.
#[proc_macro_derive(ValueList, attributes(scylla_crate))]
pub fn value_list_derive(tokens_input: TokenStream) -> TokenStream {
    let res = value_list::value_list_derive(tokens_input);
    res.unwrap_or_else(|e| e.into_compile_error().into())
}

mod deserialize;

#[proc_macro_derive(DeserializeRow, attributes(scylla))]
pub fn deserialize_row_derive(tokens_input: TokenStream) -> TokenStream {
    match deserialize::row::deserialize_row_derive(tokens_input) {
        Ok(tokens) => tokens.into_token_stream().into(),
        Err(err) => err.into_compile_error().into(),
    }
}

#[proc_macro_derive(DeserializeValue, attributes(scylla))]
pub fn deserialize_value_derive(tokens_input: TokenStream) -> TokenStream {
    match deserialize::value::deserialize_value_derive(tokens_input) {
        Ok(tokens) => tokens.into_token_stream().into(),
        Err(err) => err.into_compile_error().into(),
    }
}
