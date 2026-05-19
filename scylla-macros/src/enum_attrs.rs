use darling::FromAttributes;
use syn::parse_quote;

#[derive(FromAttributes)]
#[darling(attributes(scylla))]
pub(crate) struct EnumAttrs {
    #[darling(rename = "crate")]
    pub(crate) crate_path: Option<syn::Path>,
}

impl EnumAttrs {
    pub(crate) fn macro_internal_path(&self) -> syn::Path {
        self.crate_path
            .as_ref()
            .map(|p| parse_quote!(#p::_macro_internal))
            .unwrap_or_else(|| parse_quote!(::scylla::_macro_internal))
    }
}

pub(crate) fn get_enum_repr_type(input: &syn::DeriveInput) -> Result<String, syn::Error> {
    let mut repr_type = None;

    for attr in &input.attrs {
        if !attr.path().is_ident("repr") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            let path = &meta.path;
            let Some(ident) = path.get_ident() else {
                return Ok(());
            };
            let s = ident.to_string();
            match s.as_str() {
                "i8" | "i16" | "i32" | "i64" => {
                    repr_type = Some(s);
                }
                _ => {}
            }
            Ok(())
        })?;
    }

    match repr_type {
        Some(t) => Ok(t),
        None => Err(syn::Error::new_spanned(
            input,
            "SerializeValue/DeserializeValue for enums requires a standard #[repr(...)] attribute with a signed integer type (i8, i16, i32, i64). Example: #[repr(i32)]",
        )),
    }
}

pub(crate) fn validate_c_style_enum(data_enum: &syn::DataEnum) -> Result<(), syn::Error> {
    if data_enum.variants.is_empty() {
        return Err(syn::Error::new_spanned(
            &data_enum.variants,
            "Cannot derive trait for enums with no variants",
        ));
    }

    for variant in &data_enum.variants {
        if !variant.fields.is_empty() {
            return Err(syn::Error::new_spanned(
                variant,
                "Trait can only be derived for enums with unit variants (no data fields).",
            ));
        }
    }

    Ok(())
}
