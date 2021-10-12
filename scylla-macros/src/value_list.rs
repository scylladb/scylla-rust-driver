use proc_macro::TokenStream;
use quote::quote;

/// #[derive(ValueList)] allows to parse a struct as a list of values,
/// which can be fed to the query directly.
/// Works only on simple structs without generics etc
pub fn value_list_derive(tokens_input: TokenStream) -> TokenStream {
    let (struct_name, struct_fields) =
        crate::parser::parse_struct_with_named_fields(tokens_input, "ValueList");

    let values_len = struct_fields.named.len();
    let field_name = struct_fields.named.iter().map(|field| &field.ident);
    let generated = quote! {
        impl scylla::frame::value::ValueList for #struct_name {
            fn serialized(&self) -> scylla::frame::value::SerializedResult {
                let mut result = scylla::frame::value::SerializedValues::with_capacity(#values_len);
                #(
                    result.add_value(&self.#field_name)?;
                )*

                Ok(std::borrow::Cow::Owned(result))
            }
        }
    };

    TokenStream::from(generated)
}
