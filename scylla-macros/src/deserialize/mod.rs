use darling::{FromAttributes, FromField};
use proc_macro2::Span;
use syn::parse_quote;

pub(crate) mod row;
pub(crate) mod value;

/// Common attributes that all deserialize impls should understand.
trait DeserializeCommonStructAttrs {
    /// The path to either `scylla` or `scylla_cql` crate.
    fn crate_path(&self) -> Option<&syn::Path>;

    /// The path to `macro_internal` module,
    /// which contains exports used by macros.
    fn macro_internal_path(&self) -> syn::Path {
        match self.crate_path() {
            Some(path) => parse_quote!(#path::_macro_internal),
            None => parse_quote!(scylla::_macro_internal),
        }
    }
}

/// Provides access to attributes that are common to DeserializeValue
/// and DeserializeRow traits.
trait DeserializeCommonFieldAttrs {
    /// Does the type of this field need Default to be implemented?
    fn needs_default(&self) -> bool;

    /// The type of the field, i.e. what this field deserializes to.
    fn deserialize_target(&self) -> &syn::Type;
}

/// A structure helpful in implementing DeserializeValue and DeserializeRow.
///
/// It implements some common logic for both traits:
/// - Generates a unique lifetime that binds all other lifetimes in both structs,
/// - Adds appropriate trait bounds (DeserializeValue + Default)
struct StructDescForDeserialize<Attrs, Field> {
    name: syn::Ident,
    attrs: Attrs,
    fields: Vec<Field>,
    constraint_trait: syn::Path,
    constraint_lifetimes: (syn::Lifetime, syn::Lifetime),

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

        // TODO: support structs with unnamed fields.
        // A few things to consider:
        // - such support would necessarily require `enforce_order` and `skip_name_checks` attributes to be passed,
        // - either:
        //      - the inner code would have to represent unnamed fields differently and handle the errors differently,
        //      - or we could use `.0, .1` or `0`, `1` as names for consecutive fields, making representation and error handling uniform.
        let fields = crate::parser::parse_named_fields(input, trait_name)
            .unwrap_or_else(|err| panic!("{}", err))
            .named
            .iter()
            .map(Field::from_field)
            .collect::<Result<_, _>>()?;

        let constraint_lifetimes = generate_pair_of_unique_lifetimes_for_impl(&input.generics);

        Ok(Self {
            name: input.ident.clone(),
            attrs,
            fields,
            constraint_trait,
            constraint_lifetimes,
            generics: input.generics.clone(),
        })
    }

    fn struct_attrs(&self) -> &Attrs {
        &self.attrs
    }

    fn constraint_lifetimes(&self) -> &(syn::Lifetime, syn::Lifetime) {
        &self.constraint_lifetimes
    }

    fn fields(&self) -> &[Field] {
        &self.fields
    }

    fn generate_impl(
        &self,
        trait_: syn::Path,
        items: impl IntoIterator<Item = syn::ImplItem>,
    ) -> syn::ItemImpl {
        let (frame_lifetime, metadata_lifetime) = self.constraint_lifetimes();
        let (_, ty_generics, _) = self.generics.split_for_impl();
        let impl_generics = &self.generics.params;

        let macro_internal = self.attrs.macro_internal_path();
        let struct_name = &self.name;
        let predicates = generate_lifetime_constraints_for_impl(
            &self.generics,
            self.constraint_trait.clone(),
            frame_lifetime,
        )
        .chain(generate_default_constraints(&self.fields));
        let trait_: syn::Path = parse_quote!(#macro_internal::#trait_);
        let items = items.into_iter();

        // This `if` is purely temporary. When in a next commit DeserializeRow receives a 'metadata lifetime,
        // the handling of both traits is unified again.
        if trait_
            .segments
            .last()
            .is_some_and(|name| name.ident == "DeserializeValue")
        {
            parse_quote! {
                impl<#frame_lifetime, #metadata_lifetime, #impl_generics>
                    #trait_<#frame_lifetime, #metadata_lifetime> for #struct_name #ty_generics
                where #(#predicates),*
                {
                    #(#items)*
                }
            }
        } else {
            parse_quote! {
                impl<#frame_lifetime, #impl_generics>
                    #trait_<#frame_lifetime> for #struct_name #ty_generics
                where #(#predicates),*
                {
                    #(#items)*
                }
            }
        }
    }
}

/// Generates T: Default constraints for those fields that need it.
fn generate_default_constraints<Field: DeserializeCommonFieldAttrs>(
    fields: &[Field],
) -> impl Iterator<Item = syn::WherePredicate> + '_ {
    fields.iter().filter(|f| f.needs_default()).map(|f| {
        let t = f.deserialize_target();
        parse_quote!(#t: std::default::Default)
    })
}

/// Helps introduce a lifetime to an `impl` definition that constrains
/// other lifetimes and types.
///
/// The original use case is DeserializeValue and DeserializeRow. Both of those traits
/// are parametrized with a lifetime. If T: DeserializeValue<'a> then this means
/// that you can deserialize T as some CQL value from bytes that have
/// lifetime 'a, similarly for DeserializeRow. In impls for those traits,
/// an additional lifetime must be introduced and properly constrained.
fn generate_lifetime_constraints_for_impl<'a>(
    generics: &'a syn::Generics,
    trait_full_name: syn::Path,
    constraint_lifetime: &'a syn::Lifetime,
) -> impl Iterator<Item = syn::WherePredicate> + 'a {
    // Constrain the new lifetime with the existing lifetime parameters
    //     'lifetime: 'a + 'b + 'c ...
    let mut lifetimes = generics.lifetimes().map(|l| &l.lifetime).peekable();
    let lifetime_constraints = std::iter::from_fn(move || {
        let lifetimes = lifetimes.by_ref();
        lifetimes
            .peek()
            .is_some()
            .then::<syn::WherePredicate, _>(|| parse_quote!(#constraint_lifetime: #(#lifetimes)+*))
    });

    // For each type parameter T, constrain it like this:
    //     T: DeserializeValue<'lifetime>,
    let type_constraints = generics.type_params().map(move |t| {
        let t_ident = &t.ident;
        parse_quote!(#t_ident: #trait_full_name<#constraint_lifetime>)
    });

    lifetime_constraints.chain(type_constraints)
}

/// Generates a pair of new lifetime parameters, with a different name to any of the
/// existing generic lifetimes.
fn generate_pair_of_unique_lifetimes_for_impl(
    generics: &syn::Generics,
) -> (syn::Lifetime, syn::Lifetime) {
    let mut constraint_lifetime_name = "'lifetime".to_string();
    fn lifetime_occupied(generics: &syn::Generics, lifetime_name: &str) -> bool {
        generics
            .lifetimes()
            .any(|l| l.lifetime.to_string() == lifetime_name)
    }

    while lifetime_occupied(generics, &constraint_lifetime_name) {
        // Extend the lifetime name with another underscore.
        constraint_lifetime_name += "_";
    }

    let lifetime1 = syn::Lifetime::new(&constraint_lifetime_name, Span::call_site());
    constraint_lifetime_name += "_";

    while lifetime_occupied(generics, &constraint_lifetime_name) {
        // Extend the lifetime name with another underscore.
        constraint_lifetime_name += "_";
    }
    let lifetime2 = syn::Lifetime::new(&constraint_lifetime_name, Span::call_site());

    (lifetime1, lifetime2)
}
