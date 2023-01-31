use syn::{Data, DeriveInput, Fields, FieldsNamed, Lit, Meta};

/// Parses the tokens_input to a DeriveInput and returns the struct name from which it derives and
/// the named fields
pub(crate) fn parse_named_fields<'a>(
    input: &'a DeriveInput,
    current_derive: &str,
) -> &'a FieldsNamed {
    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named_fields) => named_fields,
            _ => panic!(
                "derive({}) works only for structs with named fields. Tuples don't need derive.",
                current_derive
            ),
        },
        _ => panic!("derive({}) works only on structs!", current_derive),
    }
}

pub(crate) fn get_path(input: &DeriveInput) -> Result<syn::Path, syn::Error> {
    let mut this_path: Option<syn::Path> = None;
    for attr in input.attrs.iter() {
        if !attr.path.is_ident("scylla_crate") {
            continue;
        }
        match attr.parse_meta() {
            Ok(Meta::NameValue(meta_name_value)) => {
                if let Lit::Str(lit_str) = &meta_name_value.lit {
                    let path_val = &lit_str.value().parse::<proc_macro2::TokenStream>().unwrap();
                    if this_path.is_none() {
                        this_path = Some(syn::parse_quote!(#path_val::_macro_internal));
                    } else {
                        return Err(syn::Error::new_spanned(
                            &meta_name_value.lit,
                            "the `scylla_crate` attribute was set multiple times",
                        ));
                    }
                } else {
                    return Err(syn::Error::new_spanned(
                        &meta_name_value.lit,
                        "the `scylla_crate` attribute should be a string literal",
                    ));
                }
            }
            Ok(other) => {
                return Err(syn::Error::new_spanned(
                    other,
                    "the `scylla_crate` attribute have a single value",
                ));
            }
            Err(err) => {
                return Err(err);
            }
        }
    }
    Ok(this_path.unwrap_or_else(|| syn::parse_quote!(scylla::_macro_internal)))
}
