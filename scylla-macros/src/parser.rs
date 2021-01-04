use proc_macro::TokenStream;
use syn::{parse, Data, DeriveInput, Fields, FieldsNamed, Ident};

/// Parsers the tokens_input to a DeriveInput and returns the struct name from which it derives and
/// the named fields
pub(crate) fn parse_struct_with_named_fields(tokens_input: TokenStream, current_derive: &str) -> (Ident, FieldsNamed) {
    let input = parse::<DeriveInput>(tokens_input).expect("No DeriveInput");
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
