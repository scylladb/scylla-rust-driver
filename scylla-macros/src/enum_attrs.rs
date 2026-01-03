use darling::FromAttributes;
use syn::parse_quote;

#[derive(FromAttributes)]
#[darling(attributes(scylla))]
pub(crate) struct EnumAttrs {
    #[darling(rename = "crate")]
    pub(crate) crate_path: Option<syn::Path>,
}

impl EnumAttrs {
    pub(crate) fn crate_path(&self) -> syn::Path {
        self.crate_path
            .as_ref()
            .map(|p| parse_quote!(#p::_macro_internal))
            .unwrap_or_else(|| parse_quote!(::scylla::_macro_internal))
    }
}

pub(crate) fn get_enum_repr_type(input: &syn::DeriveInput) -> Result<String, syn::Error> {
    let mut repr_type = None;

    for attr in &input.attrs {
        if attr.path().is_ident("repr") {
            attr.parse_nested_meta(|meta| {
                let path = &meta.path;
                if let Some(ident) = path.get_ident() {
                    let s = ident.to_string();
                    match s.as_str() {
                        "i8" | "i16" | "i32" | "i64" => {
                            repr_type = Some(s);
                            Ok(())
                        }
                        _ => Ok(()),
                    }
                } else {
                    Ok(())
                }
            })?;
        }
    }

    match repr_type {
        Some(t) => Ok(t),
        None => Err(syn::Error::new_spanned(
            input,
            "SerializeValue/DeserializeValue for enums requires a standard #[repr(...)] attribute with a signed integer type (i8, i16, i32, i64). Example: #[repr(i32)]",
        )),
    }
}
