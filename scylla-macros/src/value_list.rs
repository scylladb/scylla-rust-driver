use proc_macro::TokenStream;
use quote::quote;
use syn::DeriveInput;

/// #[derive(ValueList)] allows to parse a struct as a list of values,
/// which can be fed to the query directly.
pub fn value_list_derive(tokens_input: TokenStream) -> TokenStream {
    let item = syn::parse::<DeriveInput>(tokens_input).expect("No DeriveInput");
    let struct_fields = crate::parser::parse_named_fields(&item, "ValueList");

    let struct_name = &item.ident;
    let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();

    let values_len = struct_fields.named.len();
    let field_name = struct_fields.named.iter().map(|field| &field.ident);
    let generated = quote! {
        impl #impl_generics scylla::frame::value::ValueList for #struct_name #ty_generics #where_clause {
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
