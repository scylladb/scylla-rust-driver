use syn::{Data, DeriveInput, ExprLit, Fields, FieldsNamed, Lit};
use syn::{Expr, Meta};

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

pub(crate) fn get_path(input: &DeriveInput) -> Result<proc_macro2::TokenStream, syn::Error> {
    let mut this_path: Option<proc_macro2::TokenStream> = None;
    for attr in input.attrs.iter() {
        if !attr.path().is_ident("scylla_crate") {
            continue;
        }
        match &attr.meta {
            Meta::NameValue(name_value) => {
                if let Expr::Lit(ExprLit {
                    lit: Lit::Str(lit), ..
                }) = &name_value.value
                {
                    let path = syn::Ident::new(&lit.value(), lit.span());
                    if this_path.is_none() {
                        this_path = Some(quote::quote!(#path::_macro_internal));
                    } else {
                        return Err(syn::Error::new_spanned(
                            &name_value.path,
                            "the `scylla_crate` attribute was set multiple times",
                        ));
                    }
                } else {
                    return Err(syn::Error::new_spanned(
                        &name_value.value,
                        "the `scylla_crate` attribute should be a string literal",
                    ));
                }
            }
            other => {
                return Err(syn::Error::new_spanned(
                    other,
                    "the `scylla_crate` attribute have a single value",
                ));
            }
        }
    }
    Ok(this_path.unwrap_or_else(|| quote::quote!(scylla::_macro_internal)))
}
