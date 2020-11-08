use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use syn::{parse_macro_input, spanned::Spanned, Data, DeriveInput, Fields};

/// #[derive(FromRow)] derives From<Row> for struct
/// Works only on simple structs without generics etc
#[proc_macro_derive(FromRow)]
pub fn from_row_derive(tokens_input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(tokens_input as DeriveInput);

    let struct_name = &input.ident;

    // Generates tokens for field_name: field_type::from(vals_iter.next().unwrap_or(...)), ...
    let set_fields = match &input.data {
        Data::Struct(ref data) => {
            match &data.fields {
                Fields::Named(fields) => {
                    fields.named.iter().map(|field| {
                        let field_name = &field.ident;
                        let field_type = &field.ty;

                        quote_spanned! {field.span() =>
                            #field_name: <#field_type as FromCQLVal<Option<CQLValue>>>::from_cql(
                                vals_iter
                                .next().unwrap_or_else(
                                || panic!("Row too short to convert to {}!", stringify!(#struct_name)))
                            ),
                        }
                    })
                },
                _ => panic!("derive(FromRow) works only for structs with named fields. Tuples don't need derive.")
            }
        },
        _ => panic!("derive(FromRow) works only on structs!")
    };

    let generated = quote! {
        impl From<scylla::frame::response::result::Row> for #struct_name {
            fn from(row: scylla::frame::response::result::Row) -> Self {
                let mut vals_iter = row.columns.into_iter();

                use scylla::frame::response::result::CQLValue;
                use scylla::frame::response::cql_to_rust::FromCQLVal;

                #struct_name {
                    #(#set_fields)*
                }
            }
        }
    };

    TokenStream::from(generated)
}
