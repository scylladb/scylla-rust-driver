use syn::{Data, DeriveInput, Fields, FieldsNamed};

/// Parses a struct DeriveInput and returns named fields of this struct.
pub(crate) fn parse_named_fields<'a>(
    input: &'a DeriveInput,
    current_derive: &str,
) -> Result<&'a FieldsNamed, syn::Error> {
    let create_err_msg =
        || format!("derive({current_derive}) works only for structs with named fields");

    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named_fields) => Ok(named_fields),
            _ => Err(syn::Error::new_spanned(data.struct_token, create_err_msg())),
        },
        Data::Enum(e) => Err(syn::Error::new_spanned(e.enum_token, create_err_msg())),
        Data::Union(u) => Err(syn::Error::new_spanned(u.union_token, create_err_msg())),
    }
}
