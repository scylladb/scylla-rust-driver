use syn::{Data, DeriveInput, Expr, ExprLit, Fields, FieldsNamed, Lit, Meta};

/// Parses a struct DeriveInput and returns named fields of this struct.
pub(crate) fn parse_named_fields<'a>(
    input: &'a DeriveInput,
    current_derive: &str,
) -> Result<&'a FieldsNamed, syn::Error> {
    let create_err_msg = || {
        format!(
            "derive({}) works only for structs with named fields",
            current_derive
        )
    };

    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named_fields) => Ok(named_fields),
            _ => Err(syn::Error::new_spanned(data.struct_token, create_err_msg())),
        },
        Data::Enum(e) => Err(syn::Error::new_spanned(e.enum_token, create_err_msg())),
        Data::Union(u) => Err(syn::Error::new_spanned(u.union_token, create_err_msg())),
    }
}

pub(crate) fn get_path(input: &DeriveInput) -> Result<syn::Path, syn::Error> {
    let mut this_path: Option<syn::Path> = None;
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
                        this_path = Some(syn::parse_quote!(#path::_macro_internal));
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
    Ok(this_path.unwrap_or_else(|| syn::parse_quote!(scylla::_macro_internal)))
}
