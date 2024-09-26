use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use syn::{spanned::Spanned, DeriveInput};

/// #[derive(FromRow)] derives FromRow for struct
pub(crate) fn from_row_derive(tokens_input: TokenStream) -> Result<TokenStream, syn::Error> {
    let item = syn::parse::<DeriveInput>(tokens_input)?;
    let path = crate::parser::get_path(&item)?;
    let struct_fields = crate::parser::parse_struct_fields(&item, "FromRow")?;

    let struct_name = &item.ident;
    let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();

    // Generates a token that sets the values of struct fields: field_type::from_cql(...)
    let (fill_struct_code, fields_count) = match struct_fields {
        crate::parser::StructFields::Named(fields) => {
            let set_fields_code = fields.named.iter().enumerate().map(|(col_ix, field)| {
                let field_name = &field.ident;
                let field_type = &field.ty;

                quote_spanned! {field.span() =>
                    #field_name: {
                        // To avoid unnecessary copy `std::mem::take` is used.
                        // Using explicit indexing operation is safe because `row_columns` is an array and `col_ix` is a litteral.
                        // <https://doc.rust-lang.org/nightly/nightly-rustc/rustc_lint/builtin/static.UNCONDITIONAL_PANIC.html>
                        let col_value = ::std::mem::take(&mut row_columns[#col_ix]);
                        <#field_type as FromCqlVal<::std::option::Option<CqlValue>>>::from_cql(col_value)
                            .map_err(|e| FromRowError::BadCqlVal {
                                err: e,
                                column: #col_ix,
                            })?
                    },
                }
            });

            // This generates: { field1: {field1_code}, field2: {field2_code}, ...}
            let fill_struct_code = quote! {
                {#(#set_fields_code)*}
            };
            (fill_struct_code, fields.named.len())
        }
        crate::parser::StructFields::Unnamed(fields) => {
            let set_fields_code = fields.unnamed.iter().enumerate().map(|(col_ix, field)| {
                let field_type = &field.ty;

                quote_spanned! {field.span() =>
                    {
                        // To avoid unnecessary copy `std::mem::take` is used.
                        // Using explicit indexing operation is safe because `row_columns` is an array and `col_ix` is a litteral.
                        // <https://doc.rust-lang.org/nightly/nightly-rustc/rustc_lint/builtin/static.UNCONDITIONAL_PANIC.html>
                        let col_value = ::std::mem::take(&mut row_columns[#col_ix]);
                        <#field_type as FromCqlVal<::std::option::Option<CqlValue>>>::from_cql(col_value)
                            .map_err(|e| FromRowError::BadCqlVal {
                                err: e,
                                column: #col_ix,
                            })?
                    },
                }
            });

            // This generates: ( {<field1_code>}, {<field2_code>}, ... )
            let fill_struct_code = quote! {
                (#(#set_fields_code)*)
            };
            (fill_struct_code, fields.unnamed.len())
        }
    };

    let generated = quote! {
        impl #impl_generics #path::FromRow for #struct_name #ty_generics #where_clause {
            fn from_row(row: #path::Row)
            -> ::std::result::Result<Self, #path::FromRowError> {
                use #path::{CqlValue, FromCqlVal, FromRow, FromRowError};
                use ::std::result::Result::{Ok, Err};
                use ::std::convert::TryInto;
                use ::std::iter::{Iterator, IntoIterator};


                let row_columns_len = row.columns.len();
                // An array used, to enable [uncoditional paniking](https://doc.rust-lang.org/nightly/nightly-rustc/rustc_lint/builtin/static.UNCONDITIONAL_PANIC.html)
                // for "index out of range" issues and be able to catch them during the compile time.
                let mut row_columns: [_; #fields_count] = row.columns.try_into().map_err(|_| FromRowError::WrongRowSize {
                    expected: #fields_count,
                        actual: row_columns_len,
                })?;

                Ok(#struct_name #fill_struct_code)
            }
        }
    };

    Ok(TokenStream::from(generated))
}
