use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;

/// #[derive(FromRow)] derives FromRow for struct
/// Works only on simple structs without generics etc
pub fn from_row_derive(tokens_input: TokenStream) -> TokenStream {
    let (struct_name, struct_fields) =
        crate::parser::parse_struct_with_named_fields(tokens_input, "FromRow");

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
