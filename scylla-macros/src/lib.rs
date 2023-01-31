use darling::ToTokens;
use proc_macro::TokenStream;

mod deserialize;

mod from_row;
mod from_user_type;
mod into_user_type;
mod parser;
mod value_list;

#[proc_macro_derive(DeserializeRow, attributes(scylla, scylla_crate))]
pub fn deserialize_row_derive(tokens_input: TokenStream) -> TokenStream {
    match deserialize::row::deserialize_row_derive(tokens_input) {
        Ok(tokens) => tokens.into_token_stream().into(),
        Err(err) => err.into_compile_error().into(),
    }
}

#[proc_macro_derive(DeserializeCql, attributes(scylla))]
pub fn deserialize_cql_derive(tokens_input: TokenStream) -> TokenStream {
    match deserialize::cql::deserialize_user_type_derive(tokens_input) {
        Ok(tokens) => tokens.into_token_stream().into(),
        Err(err) => err.into_compile_error().into(),
    }
}

/// #[derive(FromRow)] derives FromRow for struct
/// Works only on simple structs without generics etc
#[proc_macro_derive(FromRow, attributes(scylla_crate))]
pub fn from_row_derive(tokens_input: TokenStream) -> TokenStream {
    from_row::from_row_derive(tokens_input)
}

/// #[derive(FromUserType)] allows to parse a struct as User Defined Type
/// Works only on simple structs without generics etc
#[proc_macro_derive(FromUserType, attributes(scylla_crate))]
pub fn from_user_type_derive(tokens_input: TokenStream) -> TokenStream {
    from_user_type::from_user_type_derive(tokens_input)
}

/// #[derive(IntoUserType)] allows to parse a struct as User Defined Type
/// Works only on simple structs without generics etc
#[proc_macro_derive(IntoUserType, attributes(scylla_crate))]
pub fn into_user_type_derive(tokens_input: TokenStream) -> TokenStream {
    into_user_type::into_user_type_derive(tokens_input)
}

/// #[derive(ValueList)] derives ValueList for struct
/// Works only on simple structs without generics etc
#[proc_macro_derive(ValueList, attributes(scylla_crate))]
pub fn value_list_derive(tokens_input: TokenStream) -> TokenStream {
    value_list::value_list_derive(tokens_input)
}
