use darling::{FromAttributes, FromField};
use proc_macro2::Span;
use quote::quote;
use syn::parse_quote;

pub(crate) mod cql;
pub(crate) mod row;

/// Common attributes that all deserialize impls should understand.
trait DeserializeCommonStructAttrs {
    /// The path to either `scylla` or `scylla_cql` crate
    fn crate_path(&self) -> syn::Path;
}

/// Provides access to attributes that are common to DeserializeCql
/// and DeserializeRow traits.
trait DeserializeCommonFieldAttrs {
    /// Does the type of this field need Default to be implemented?
    fn needs_default(&self) -> bool;

    /// The type of the field, i.e. what this field deserializes to.
    fn deserialize_target(&self) -> syn::Type;
}

/// A structure helpful in implementing DeserializeCql and DeserializeRow.
///
/// It implements some common logic for both traits:
/// - Generates a unique lifetime that binds all other lifetimes in both structs,
/// - Adds appropriate trait bounds (DeserializeCql + Default)
struct StructDescForDeserialize<Attrs, Field> {
    name: syn::Ident,
    attrs: Attrs,
    fields: Vec<Field>,
    constraint_trait: syn::Path,
    constraint_lifetime: syn::Lifetime,

    generics: syn::Generics,
}

impl<Attrs, Field> StructDescForDeserialize<Attrs, Field>
where
    Attrs: FromAttributes + DeserializeCommonStructAttrs,
    Field: FromField + DeserializeCommonFieldAttrs,
{
    fn new(
        input: &syn::DeriveInput,
        trait_name: &str,
        constraint_trait: syn::Path,
    ) -> Result<Self, syn::Error> {
        let attrs = Attrs::from_attributes(&input.attrs)?;

        // TODO: Handle errors from parse_name_fields
        let fields = crate::parser::parse_named_fields(input, trait_name)
            .named
            .iter()
            .map(Field::from_field)
            .collect::<Result<_, _>>()?;

        let constraint_lifetime = generate_unique_lifetime_for_impl(&input.generics);

        Ok(Self {
            name: input.ident.clone(),
            attrs,
            fields,
            constraint_trait,
            constraint_lifetime,
            generics: input.generics.clone(),
        })
    }

    fn struct_attrs(&self) -> &Attrs {
        &self.attrs
    }

    fn constraint_lifetime(&self) -> &syn::Lifetime {
        &self.constraint_lifetime
    }

    fn fields(&self) -> &[Field] {
        &self.fields
    }

    fn generate_impl(&self, trait_: syn::Path, items: Vec<syn::ImplItem>) -> syn::ItemImpl {
        let constraint_lifetime = &self.constraint_lifetime;
        let (_, ty_generics, _) = self.generics.split_for_impl();
        let impl_generics = &self.generics.params;

        let scylla_crate = self.attrs.crate_path();
        let struct_name = &self.name;
        let mut predicates = Vec::new();
        predicates.extend(generate_lifetime_constraints_for_impl(
            &self.generics,
            self.constraint_trait.clone(),
            self.constraint_lifetime.clone(),
        ));
        predicates.extend(generate_default_constraints(&self.fields));
        let trait_ = quote!(#scylla_crate::#trait_);

        parse_quote! {
            impl<#constraint_lifetime, #impl_generics> #trait_<#constraint_lifetime> for #struct_name #ty_generics
            where #(#predicates),*
            {
                #(#items)*
            }
        }
    }
}

/// Generates T: Default constraints for those fields that need it.
fn generate_default_constraints<Field>(fields: &[Field]) -> Vec<syn::WherePredicate>
where
    Field: DeserializeCommonFieldAttrs,
{
    fields
        .iter()
        .filter(|f| f.needs_default())
        .map(|f| {
            let t = f.deserialize_target();
            parse_quote!(#t: std::default::Default)
        })
        .collect()
}

/// Helps introduce a lifetime to an `impl` definition that constrains
/// other lifetimes and types.
///
/// The original use case is DeriveCql and DeriveRow. Both of those traits
/// are parametrized with a lifetime. If T: DeriveCql<'a> then this means
/// that you can deserialize T as some CQL value from bytes that have
/// lifetime 'a, similarly for DeriveRow. In impls for those traits,
/// an additional lifetime must be introduced and properly constrained.
fn generate_lifetime_constraints_for_impl(
    generics: &syn::Generics,
    trait_full_name: syn::Path,
    constraint_lifetime: syn::Lifetime,
) -> Vec<syn::WherePredicate> {
    let mut predicates = Vec::new();

    // Constrain the new lifetime with the existing lifetime parameters
    //     'lifetime: 'a + 'b + 'c ...
    let lifetimes: Vec<_> = generics.lifetimes().map(|l| l.lifetime.clone()).collect();
    if !lifetimes.is_empty() {
        predicates.push(parse_quote!(#constraint_lifetime: #(#lifetimes)+*));
    }

    // For each type parameter T, constrain it like this:
    //     T: DeriveCql<'lifetime>,
    for t in generics.type_params() {
        let t_ident = &t.ident;
        predicates.push(parse_quote!(#t_ident: #trait_full_name<#constraint_lifetime>));
    }

    predicates
}

/// Generates a new lifetime parameter, with a different name to any of the
/// existing generic lifetimes.
fn generate_unique_lifetime_for_impl(generics: &syn::Generics) -> syn::Lifetime {
    let mut constraint_lifetime_name = "'lifetime".to_string();
    while generics
        .lifetimes()
        .any(|l| l.lifetime.to_string() == constraint_lifetime_name)
    {
        constraint_lifetime_name += "e";
    }
    syn::Lifetime::new(&constraint_lifetime_name, Span::call_site())
}
