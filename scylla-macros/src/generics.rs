use proc_macro2::{TokenStream, TokenTree};
use quote::ToTokens;
use syn::parse_quote;

/// Generates `FieldType: Trait` bounds for the impl derived for a generic struct.
///
/// Field types are bounded instead of type parameters, because a field type
/// may implement the trait under other bounds. For example, a `Wrapper<T>`
/// field may implement it for any `T`.
///
/// Only field types that mention a type or const parameter are bounded. The
/// compiler checks the other ones anyway, and bounds on types that differ only
/// in lifetimes, like `&'a str` and `&'b str`, are ambiguous for it.
///
/// Because of the trait solver, this doesn't work for:
/// - fields like `&'a T` and `&'b T`, which still get ambiguous bounds (E0283),
/// - recursive structs, e.g. `MyStruct<T>` with an `Option<Box<MyStruct<T>>>`
///   field, whose bounds can't be proven (E0275).
pub(crate) fn field_bounds<'a>(
    generics: &'a syn::Generics,
    field_types: impl IntoIterator<Item = &'a syn::Type>,
    trait_: syn::Path,
) -> impl Iterator<Item = syn::WherePredicate> {
    field_types
        .into_iter()
        .filter(|typ| mentions_type_or_const_param(typ, generics))
        .map(move |typ| parse_quote!(#typ: #trait_))
}

fn mentions_type_or_const_param(typ: &syn::Type, generics: &syn::Generics) -> bool {
    fn is_param(ident: &proc_macro2::Ident, generics: &syn::Generics) -> bool {
        generics.params.iter().any(|param| match param {
            syn::GenericParam::Type(param) => param.ident == *ident,
            syn::GenericParam::Const(param) => param.ident == *ident,
            syn::GenericParam::Lifetime(_) => false,
        })
    }

    fn mentions(tokens: TokenStream, generics: &syn::Generics) -> bool {
        let tokens: Vec<TokenTree> = tokens.into_iter().collect();
        tokens.iter().enumerate().any(|(i, token)| match token {
            // A generic parameter can only start a path, so `a::T` doesn't
            // mention `T`. Neither does the lifetime `'T`.
            TokenTree::Ident(ident) => {
                let after_path_separator = matches!(
                    &tokens[..i],
                    [.., TokenTree::Punct(a), TokenTree::Punct(b)]
                        if a.as_char() == ':' && b.as_char() == ':'
                );
                let in_lifetime =
                    matches!(&tokens[..i], [.., TokenTree::Punct(p)] if p.as_char() == '\'');
                !after_path_separator && !in_lifetime && is_param(ident, generics)
            }
            TokenTree::Group(group) => mentions(group.stream(), generics),
            TokenTree::Punct(_) | TokenTree::Literal(_) => false,
        })
    }
    mentions(typ.to_token_stream(), generics)
}
