use proc_macro::TokenStream;
use proc_macro2::Ident;
use syn::{parse2, Data, DeriveInput, Fields, FieldsNamed};

/// Parsers the tokens_input to a DeriveInput and returns the struct name from which it derives and
/// the named fields
pub(crate) fn parse(tokens_input: TokenStream, current_derive: &str) -> (Ident, FieldsNamed) {
    let input = parse2::<DeriveInput>(tokens_input.into()).expect("No DeriveInput");
    let struct_name = input.ident;
    let struct_fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(named_fields) => named_fields,
            _ => panic!(
                "derive({}) works only for structs with named fields. Tuples don't need derive.",
                current_derive
            ),
        },
        _ => panic!("derive({}) works only on structs!", current_derive),
    };

    (struct_name, struct_fields)
}
