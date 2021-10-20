use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;

/// #[derive(FromUserType)] allows to parse a struct as User Defined Type
/// Works only on simple structs without generics etc
pub fn from_user_type_derive(tokens_input: TokenStream) -> TokenStream {
    let (struct_name, struct_fields) =
        crate::parser::parse_struct_with_named_fields(tokens_input, "FromUserType");

    // Generates tokens for field_name: field_type::from_cql(fields.remove(stringify!(#field_name)).unwrap_or(None)) ?, ...
    let set_fields_code = struct_fields.named.iter().map(|field| {
        let field_name = &field.ident;
        let field_type = &field.ty;

        quote_spanned! {field.span() =>
            #field_name: <#field_type as FromCqlVal<Option<CqlValue>>>::from_cql(
                {
                    let received_field_name: Option<&String> = fields_iter
                        .peek()
                        .map(|(ref name, _)| name);

                    // Order of received fields is the same as the order of processed struct's
                    // fields. There cannot be an extra received field present, so it is safe to
                    // assign subsequent received fields, to processed struct's fields (inserting
                    // None if there is no received field corresponding to processed struct's
                    // field)
                    if let Some(received_field_name) = received_field_name {
                        if received_field_name == stringify!(#field_name) {
                            let (_, value) = fields_iter.next().unwrap();
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
        impl FromCqlVal<scylla::frame::response::result::CqlValue> for #struct_name {
            fn from_cql(cql_val: scylla::frame::response::result::CqlValue)
            -> Result<Self, scylla::cql_to_rust::FromCqlValError> {
                use std::collections::BTreeMap;
                use scylla::cql_to_rust::{FromCqlVal, FromCqlValError};
                use scylla::frame::response::result::CqlValue;

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

    TokenStream::from(generated)
}
