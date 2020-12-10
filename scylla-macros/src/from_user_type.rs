use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;

/// #[derive(FromUserType)] allows to parse a struct as User Defined Type
/// Works only on simple structs without generics etc
pub fn from_user_type_derive(tokens_input: TokenStream) -> TokenStream {
    let (struct_name, struct_fields) = crate::parser::parse(tokens_input, "FromUserType");

    // Generates tokens for field_name: field_type::from_cql(fields.remove(stringify!(#field_name)).unwrap_or(None)) ?, ...
    let set_fields_code = struct_fields.named.iter().map(|field| {
        let field_name = &field.ident;
        let field_type = &field.ty;

        quote_spanned! {field.span() =>
            #field_name: <#field_type as FromCQLVal<Option<CQLValue>>>::from_cql(
                // Take value with key #field_name out of fields map, if none found then return NULL
                fields.remove(stringify!(#field_name)).unwrap_or(None)
            ) ?,
        }
    });

    let generated = quote! {
        impl FromCQLVal<scylla::frame::response::result::CQLValue> for #struct_name {
            fn from_cql(cql_val: scylla::frame::response::result::CQLValue)
            -> Result<Self, scylla::cql_to_rust::FromCQLValError> {
                use std::collections::HashMap;
                use scylla::cql_to_rust::{FromCQLVal, FromCQLValError};
                use scylla::frame::response::result::CQLValue;

                // Interpret CQLValue as CQlValue::UserDefinedType
                let mut fields: HashMap<String, Option<CQLValue>> = match cql_val {
                    CQLValue::UserDefinedType{fields, ..} => fields,
                    _ => return Err(FromCQLValError::BadCQLType),
                };

                // Parse struct using values from fields
                let result = #struct_name {
                    #(#set_fields_code)*
                };

                // There should be no unused fields when reading user defined type
                if !fields.is_empty() {
                    return Err(FromCQLValError::BadCQLType);
                }

                return Ok(result);
            }
        }
    };

    TokenStream::from(generated)
}
