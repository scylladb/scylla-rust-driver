use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use syn::{spanned::Spanned, DeriveInput};

/// #[derive(FromUserType)] allows to parse a struct as User Defined Type
pub(crate) fn from_user_type_derive(tokens_input: TokenStream) -> Result<TokenStream, syn::Error> {
    let item = syn::parse::<DeriveInput>(tokens_input)?;
    let path = crate::parser::get_path(&item)?;
    let struct_fields = crate::parser::parse_named_fields(&item, "FromUserType")?;

    let struct_name = &item.ident;
    let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();

    // Generates tokens for field_name: field_type::from_cql(fields.remove(stringify!(#field_name)).unwrap_or(None)) ?, ...
    let set_fields_code = struct_fields.named.iter().map(|field| {
        let field_name = &field.ident;
        let field_type = &field.ty;

        quote_spanned! {field.span() =>
            #field_name: <#field_type as FromCqlVal<::std::option::Option<CqlValue>>>::from_cql(
                {
                    let received_field_name: Option<&::std::string::String> = fields_iter
                        .peek()
                        .map(|(ref name, _)| name);

                    // Order of received fields is the same as the order of processed struct's
                    // fields. There cannot be an extra received field present, so it is safe to
                    // assign subsequent received fields, to processed struct's fields (inserting
                    // None if there is no received field corresponding to processed struct's
                    // field)
                    if let Some(received_field_name) = received_field_name {
                        if received_field_name == stringify!(#field_name) {
                            let (_, value) = fields_iter.next().expect("BUG: Validated iterator did not contain expected number of values");
                            value
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
            ) ?,
        }
    });

    let generated = quote! {
        impl #impl_generics #path::FromCqlVal<#path::CqlValue> for #struct_name #ty_generics #where_clause {
            fn from_cql(cql_val: #path::CqlValue)
            -> ::std::result::Result<Self, #path::FromCqlValError> {
                use ::std::collections::BTreeMap;
                use ::std::option::Option::{self, Some, None};
                use ::std::result::Result::{Ok, Err};
                use #path::{FromCqlVal, FromCqlValError, CqlValue};
                use ::std::iter::{Iterator, IntoIterator};

                // Interpret CqlValue as CQlValue::UserDefinedType
                let mut fields_iter = match cql_val {
                    CqlValue::UserDefinedType{fields, ..} => fields.into_iter().peekable(),
                    _ => return Err(FromCqlValError::BadCqlType),
                };

                // Parse struct using values from fields
                let result = #struct_name {
                    #(#set_fields_code)*
                };

                // There should be no unused fields when reading user defined type
                if fields_iter.next().is_some() {
                    return Err(FromCqlValError::BadCqlType);
                }

                return Ok(result);
            }
        }
    };

    Ok(TokenStream::from(generated))
}
