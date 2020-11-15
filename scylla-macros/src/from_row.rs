use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use syn::{parse_macro_input, spanned::Spanned, Data, DeriveInput, Fields};

/// #[derive(FromRow)] derives FromRow for struct
/// Works only on simple structs without generics etc
pub fn from_row_derive(tokens_input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(tokens_input as DeriveInput);

    let struct_name = &input.ident;

    let struct_fields = match &input.data {
        Data::Struct(data) => {
            match &data.fields {
                Fields::Named(named_fields) => named_fields,
                _ => panic!("derive(FromRow) works only for structs with named fields. Tuples don't need derive."),
            }
        },
        _ => panic!("derive(FromRow) works only on structs!")
    };

    // Generates tokens for field_name: field_type::from_cql(vals_iter.next().ok_or(...)?), ...
    let set_fields_code = struct_fields.named.iter().map(|field| {
        let field_name = &field.ident;
        let field_type = &field.ty;

        quote_spanned! {field.span() =>
            #field_name: <#field_type as FromCQLVal<Option<CQLValue>>>::from_cql(
                vals_iter
                .next()
                .ok_or(FromRowError::RowTooShort) ?
            ) ?,
        }
    });

    let generated = quote! {
        impl FromRow for #struct_name {
            fn from_row(row: scylla::frame::response::result::Row)
            -> Result<Self, scylla::cql_to_rust::FromRowError> {
                use scylla::frame::response::result::CQLValue;
                use scylla::cql_to_rust::{FromCQLVal, FromRow, FromRowError};

                let mut vals_iter = row.columns.into_iter();

                Ok(#struct_name {
                    #(#set_fields_code)*
                })
            }
        }
    };

    TokenStream::from(generated)
}
