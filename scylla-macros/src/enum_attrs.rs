use darling::FromAttributes;
use syn::parse_quote;

#[derive(FromAttributes)]
#[darling(attributes(scylla))]
pub(crate) struct EnumAttrs {
    #[darling(rename = "crate")]
    pub(crate) crate_path: Option<syn::Path>,

    // Allows the user to specify the target type, e.g., "i32", "i8"
    #[darling(default)]
    pub(crate) repr: Option<String>,
}

impl EnumAttrs {
    pub(crate) fn crate_path(&self) -> syn::Path {
        self.crate_path
            .as_ref()
            .map(|p| parse_quote!(#p::_macro_internal))
            .unwrap_or_else(|| parse_quote!(::scylla::_macro_internal))
    }
}
