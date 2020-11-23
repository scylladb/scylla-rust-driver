use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use syn::{parse_macro_input, spanned::Spanned, Data, DeriveInput, Fields};

/// #[derive(IntoUserType)] allows to parse a struct as User Defined Type
/// Works only on simple structs without generics etc
pub fn into_user_type_derive(tokens_input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(tokens_input as DeriveInput);

    let struct_name = &input.ident;

    let struct_fields = match &input.data {
        Data::Struct(data) => {
            match &data.fields {
                Fields::Named(named_fields) => named_fields,
                _ => panic!("derive(IntoUserType) works only for structs with named fields. Tuples don't need derive."),
            }
        },
        _ => panic!("derive(IntoUserType) works only on structs!")
    };

    let convert_code = struct_fields.named.iter().map(|field| {
        let field_name = &field.ident;
        let field_type = &field.ty;

        quote_spanned! {field.span() =>
            // Convert field to scylla Value
            let #field_name: Value = <#field_type as TryIntoValue>::try_into_value(self.#field_name) ?;
            // Convert scylla Value to Bytes
            let #field_name: Bytes = <Value as ::std::convert::TryInto<Bytes>>::try_into(#field_name) ?;
            result_size += #field_name.len();
        }
    });

    let set_result_code = struct_fields.named.iter().map(|field| {
        let field_name = &field.ident;

        quote_spanned! {field.span() =>
            result_bytes.put(#field_name);
        }
    });

    let generated = quote! {
        impl scylla::frame::value::TryIntoValue for #struct_name {
            fn try_into_value(self) -> Result<scylla::frame::value::Value, scylla::frame::value::ValueTooBig> {
                use scylla::frame::value::{Value, ValueTooBig, TryIntoValue};
                use scylla::macros::{Bytes, BytesMut, BufMut};

                let mut result_size: usize = 0;

                #(#convert_code)*

                let mut result_bytes = BytesMut::with_capacity(result_size);

                #(#set_result_code)*

                Ok(Value::Val(result_bytes.into()))
            }
        }
    };

    TokenStream::from(generated)
}
