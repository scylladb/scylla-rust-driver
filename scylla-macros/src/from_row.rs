use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use syn::{spanned::Spanned, DeriveInput};

/// #[derive(FromRow)] derives FromRow for struct
pub fn from_row_derive(tokens_input: TokenStream) -> TokenStream {
    let item = syn::parse::<DeriveInput>(tokens_input).expect("No DeriveInput");
    let path = crate::parser::get_path(&item).expect("No path");
    let struct_fields = crate::parser::parse_named_fields(&item, "FromRow");

    let struct_name = &item.ident;
    let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();

    // Generates tokens for field_name: field_type::from_cql(vals_iter.next().ok_or(...)?), ...
    let set_fields_code = struct_fields.named.iter().map(|field| {
        let field_name = &field.ident;
        let field_type = &field.ty;

        quote_spanned! {field.span() =>
            #field_name: {
                let (col_ix, col_value) = vals_iter
                    .next()
                    .unwrap(); // vals_iter size is checked before this code is reached, so
                               // it is safe to unwrap

                <#field_type as FromCqlVal<::std::option::Option<CqlValue>>>::from_cql(col_value)
                    .map_err(|e| FromRowError::BadCqlVal {
                        err: e,
                        column: col_ix,
                    })?
            },
        }
    });

    let fields_count = struct_fields.named.len();
    let generated = quote! {
        impl #impl_generics #path::FromRow for #struct_name #ty_generics #where_clause {
            fn from_row(row: #path::Row)
            -> ::std::result::Result<Self, #path::FromRowError> {
                use #path::{CqlValue, FromCqlVal, FromRow, FromRowError};
                use ::std::result::Result::{Ok, Err};

                if #fields_count != row.columns.len() {
                    return Err(FromRowError::WrongRowSize {
                        expected: #fields_count,
                        actual: row.columns.len(),
                    });
                }
                let mut vals_iter = row.columns.into_iter().enumerate();

                Ok(#struct_name {
                    #(#set_fields_code)*
                })
            }
        }
    };

    TokenStream::from(generated)
}
