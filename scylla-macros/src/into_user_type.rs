use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use syn::{spanned::Spanned, DeriveInput};

/// #[derive(IntoUserType)] allows to parse a struct as User Defined Type
pub fn into_user_type_derive(tokens_input: TokenStream) -> TokenStream {
    let item = syn::parse::<DeriveInput>(tokens_input).expect("No DeriveInput");
    let path = crate::parser::get_path(&item).expect("No path");
    let struct_fields = crate::parser::parse_named_fields(&item, "IntoUserType");

    let struct_name = &item.ident;
    let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();

    let serialize_code = struct_fields.named.iter().map(|field| {
        let field_name = &field.ident;

        quote_spanned! {field.span() =>
            <_ as Value>::serialize(&self.#field_name, buf) ?;
        }
    });

    let generated = quote! {
        impl #impl_generics #path::Value for #struct_name #ty_generics #where_clause {
            fn serialize(&self, buf: &mut ::std::vec::Vec<::core::primitive::u8>) -> ::std::result::Result<(), #path::ValueTooBig> {
                use #path::{BufMut, ValueTooBig, Value};
                use ::std::convert::TryInto;
                use ::core::primitive::{usize, i32};

                // Reserve space to put serialized size in
                let total_size_index: usize = buf.len();
                buf.put_i32(0);

                let len_before_serialize = buf.len();

                // Serialize fields
                #(#serialize_code)*

                // Put serialized size in its place
                let total_size : usize = buf.len() - len_before_serialize;
                let total_size_i32: i32 = total_size.try_into().map_err(|_| ValueTooBig) ?;
                buf[total_size_index..(total_size_index+4)].copy_from_slice(&total_size_i32.to_be_bytes()[..]);

                ::std::result::Result::Ok(())
            }
        }
    };

    TokenStream::from(generated)
}
