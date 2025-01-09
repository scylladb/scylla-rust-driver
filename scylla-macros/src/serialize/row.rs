use std::collections::HashMap;

use darling::FromAttributes;
use proc_macro::TokenStream;
use syn::parse_quote;

use crate::Flavor;

#[derive(FromAttributes)]
#[darling(attributes(scylla))]
struct Attributes {
    #[darling(rename = "crate")]
    crate_path: Option<syn::Path>,

    #[darling(default)]
    flavor: Flavor,

    // If true, then the type checking code won't verify the column names.
    // Columns will be matched to struct fields based solely on the order.
    //
    // This annotation only works if `enforce_order` flavor is specified.
    #[darling(default)]
    skip_name_checks: bool,
}

impl Attributes {
    fn crate_path(&self) -> syn::Path {
        self.crate_path
            .as_ref()
            .map(|p| parse_quote!(#p::_macro_internal))
            .unwrap_or_else(|| parse_quote!(::scylla::_macro_internal))
    }
}

struct Field {
    ident: syn::Ident,
    ty: syn::Type,
    attrs: FieldAttributes,
}

impl Field {
    fn column_name(&self) -> String {
        match &self.attrs.rename {
            Some(name) => name.clone(),
            None => self.ident.to_string(),
        }
    }
}

#[derive(FromAttributes)]
#[darling(attributes(scylla))]
struct FieldAttributes {
    // If set, then serializes from the column with this particular name
    // instead of the Rust field name.
    rename: Option<String>,

    // If set, then this field's columns are serialized using its own implementation
    // of `SerializeRow` and flattened as if they were fields in this struct.
    #[darling(default)]
    flatten: bool,

    // If true, then the field is not serialized at all, but simply ignored.
    // All other attributes are ignored.
    #[darling(default)]
    skip: bool,
}

struct Context {
    attributes: Attributes,
    fields: Vec<Field>,
    struct_name: syn::Ident,
    generics: syn::Generics,
}

pub(crate) fn derive_serialize_row(tokens_input: TokenStream) -> Result<syn::ItemImpl, syn::Error> {
    let input: syn::DeriveInput = syn::parse(tokens_input)?;
    let struct_name = input.ident.clone();
    let named_fields = crate::parser::parse_named_fields(&input, "SerializeRow")?;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let attributes = Attributes::from_attributes(&input.attrs)?;

    let crate_path = attributes.crate_path();
    let implemented_trait: syn::Path = parse_quote!(#crate_path::SerializeRow);

    let fields = named_fields
        .named
        .iter()
        .map(|f| {
            FieldAttributes::from_attributes(&f.attrs).map(|attrs| Field {
                ident: f.ident.clone().unwrap(),
                ty: f.ty.clone(),
                attrs,
            })
        })
        // Filter the fields now instead of at the places that use them later
        // as it's less error prone - we just filter in one place instead of N places.
        .filter(|f| f.as_ref().map(|f| !f.attrs.skip).unwrap_or(true))
        .collect::<Result<_, _>>()?;
    let ctx = Context {
        attributes,
        fields,
        struct_name: struct_name.clone(),
        generics: input.generics.clone(),
    };
    ctx.validate(&input.ident)?;

    let gen: Box<dyn Generator> = match ctx.attributes.flavor {
        Flavor::MatchByName => Box::new(ColumnSortingGenerator { ctx: &ctx }),
        Flavor::EnforceOrder => Box::new(ColumnOrderedGenerator { ctx: &ctx }),
    };

    let serialize_item = gen.generate_serialize();
    let is_empty_item = gen.generate_is_empty();

    let res = parse_quote! {
        #[automatically_derived]
        impl #impl_generics #implemented_trait for #struct_name #ty_generics #where_clause {
            #serialize_item
            #is_empty_item
        }
    };
    Ok(res)
}

impl Context {
    fn validate(&self, struct_ident: &syn::Ident) -> Result<(), syn::Error> {
        let mut errors = darling::Error::accumulator();

        if self.attributes.skip_name_checks {
            // Skipping name checks is only available in enforce_order mode
            if self.attributes.flavor != Flavor::EnforceOrder {
                let err = darling::Error::custom(
                    "the `skip_name_checks` attribute is only allowed with the `enforce_order` flavor",
                )
                .with_span(struct_ident);
                errors.push(err);
            }

            // `rename` annotations don't make sense with skipped name checks
            for field in self.fields.iter() {
                if field.attrs.rename.is_some() {
                    let err = darling::Error::custom(
                        "the `rename` annotations don't make sense with `skip_name_checks` attribute",
                    )
                    .with_span(&field.ident);
                    errors.push(err);
                }
            }
        }

        // Check that no renames are attempted on flattened fields
        let rename_flatten_errors = self
            .fields
            .iter()
            .filter(|f| f.attrs.flatten && f.attrs.rename.is_some())
            .map(|f| {
                darling::Error::custom(
                    "`rename` and `flatten` annotations do not make sense together",
                )
                .with_span(&f.ident)
            });
        errors.extend(rename_flatten_errors);

        // Check for name collisions
        let mut used_names = HashMap::<String, &Field>::new();
        for field in self.fields.iter() {
            let column_name = field.column_name();
            if let Some(other_field) = used_names.get(&column_name) {
                let other_field_ident = &other_field.ident;
                let msg = format!("the column / bind marker name `{column_name}` used by this struct field is already used by field `{other_field_ident}`");
                let err = darling::Error::custom(msg).with_span(&field.ident);
                errors.push(err);
            } else {
                used_names.insert(column_name, field);
            }
        }

        errors.finish()?;
        Ok(())
    }
}

trait Generator {
    fn generate_serialize(&self) -> syn::TraitItemFn;
    fn generate_is_empty(&self) -> syn::TraitItemFn;
}

// Generates an implementation of the trait which sorts the columns according
// to how they are defined in prepared statement metadata.
struct ColumnSortingGenerator<'a> {
    ctx: &'a Context,
}

impl Generator for ColumnSortingGenerator<'_> {
    fn generate_serialize(&self) -> syn::TraitItemFn {
        // Need to:
        // - Check that all required columns are there and no more
        // - Check that the column types match

        let crate_path = self.ctx.attributes.crate_path();
        let struct_name = &self.ctx.struct_name;
        let (impl_generics, ty_generics, where_clause) = self.ctx.generics.split_for_impl();
        let partial_struct_name = syn::Ident::new(
            &format!("_{}ScyllaSerPartial", struct_name),
            struct_name.span(),
        );
        let mut partial_generics = self.ctx.generics.clone();
        let partial_lt: syn::Lifetime = syn::parse_quote!('scylla_ser_partial);

        if !self.ctx.fields.is_empty() {
            // all fields have to outlive the partial struct lifetime
            partial_generics
                .params
                .iter_mut()
                .filter_map(|p| match p {
                    syn::GenericParam::Lifetime(lt) => Some(lt),
                    _ => None,
                })
                .for_each(|lt| {
                    lt.bounds.push(partial_lt.clone());
                });

            // now add the partial struct lifetime
            partial_generics
                .params
                .push(syn::GenericParam::Lifetime(syn::LifetimeParam {
                    attrs: vec![],
                    lifetime: partial_lt.clone(),
                    colon_token: None,
                    bounds: syn::punctuated::Punctuated::new(),
                }));
        }

        let (partial_impl_generics, partial_ty_generics, partial_where_clause) =
            partial_generics.split_for_impl();

        let flattened: Vec<_> = self.ctx.fields.iter().filter(|f| f.attrs.flatten).collect();
        let flattened_fields: Vec<_> = flattened.iter().map(|f| &f.ident).collect();
        let flattened_tys: Vec<_> = flattened.iter().map(|f| &f.ty).collect();

        let unflattened: Vec<_> = self
            .ctx
            .fields
            .iter()
            .filter(|f| !f.attrs.flatten)
            .collect();
        let unflattened_columns: Vec<_> = unflattened.iter().map(|f| f.column_name()).collect();
        let unflattened_fields: Vec<_> = unflattened.iter().map(|f| &f.ident).collect();
        let unflattened_tys: Vec<_> = unflattened.iter().map(|f| &f.ty).collect();

        let all_names = self.ctx.fields.iter().map(|f| f.column_name());

        let partial_struct: syn::ItemStruct = parse_quote! {
            pub struct #partial_struct_name #partial_generics {
                #(#unflattened_fields: &#partial_lt #unflattened_tys,)*
                #(#flattened_fields: <#flattened_tys as #crate_path::SerializeRowByName>::Partial<#partial_lt>,)*
                missing: ::std::collections::HashSet<&'static str>,
            }
        };

        let serialize_field_block: syn::Block = if self.ctx.fields.is_empty() {
            parse_quote! {{
                ::std::result::Result::Ok(#crate_path::ser::row::FieldStatus::NotUsed)
            }}
        } else {
            parse_quote! {{
                match spec.name() {
                    #(#unflattened_columns => {
                        #crate_path::ser::row::serialize_column::<#struct_name #ty_generics>(
                            &self.#unflattened_fields, spec, writer,
                        )?;
                        self.missing.remove(#unflattened_columns);
                    })*
                    _ => 'flatten_try: {
                        #({
                            match self.#flattened_fields.serialize_field(spec, writer)? {
                                #crate_path::ser::row::FieldStatus::Done => {
                                    self.missing.remove(stringify!(#flattened_fields));
                                    break 'flatten_try;
                                }
                                #crate_path::ser::row::FieldStatus::NotDone => {
                                    break 'flatten_try;
                                }
                                #crate_path::ser::row::FieldStatus::NotUsed => {}
                            };
                        })*

                        return ::std::result::Result::Ok(#crate_path::ser::row::FieldStatus::NotUsed);
                    }
                }

                ::std::result::Result::Ok(if self.missing.is_empty() {
                    #crate_path::ser::row::FieldStatus::Done
                } else {
                    #crate_path::ser::row::FieldStatus::NotDone
                })
            }}
        };

        let partial_serialize: syn::ItemImpl = parse_quote! {
            impl #partial_impl_generics #crate_path::PartialSerializeRowByName for #partial_struct_name #partial_ty_generics #partial_where_clause {
                fn serialize_field(
                    &mut self,
                    spec: &#crate_path::ColumnSpec,
                    writer: &mut #crate_path::RowWriter<'_>,
                ) -> ::std::result::Result<#crate_path::ser::row::FieldStatus, #crate_path::SerializationError> {
                    #serialize_field_block
                }

                fn check_missing(self) -> ::std::result::Result<(), #crate_path::SerializationError> {
                    use ::std::iter::{Iterator as _, IntoIterator as _};

                    let ::std::option::Option::Some(missing) = self.missing.into_iter().nth(0) else {
                        return ::std::result::Result::Ok(());
                    };

                    match missing {
                        #(stringify!(#flattened_fields) => self.#flattened_fields.check_missing(),)*
                        _ => ::std::result::Result::Err(#crate_path::ser::row::mk_typck_err::<#struct_name #ty_generics>(#crate_path::BuiltinRowTypeCheckErrorKind::ValueMissingForColumn {
                            name: <_ as ::std::borrow::ToOwned>::to_owned(missing),
                        }))
                    }
                }
            }
        };

        let serialize_by_name: syn::ItemImpl = parse_quote! {
            impl #impl_generics #crate_path::SerializeRowByName for #struct_name #ty_generics #where_clause {
                type Partial<#partial_lt> = #partial_struct_name #partial_ty_generics where Self: #partial_lt;

                fn partial(&self) -> Self::Partial<'_> {
                    use ::std::iter::FromIterator as _;

                    #partial_struct_name {
                        #(#unflattened_fields: &self.#unflattened_fields,)*
                        #(#flattened_fields: self.#flattened_fields.partial(),)*
                        missing: ::std::collections::HashSet::from_iter([#(#all_names,)*]),
                    }
                }
            }
        };

        parse_quote! {
            fn serialize<'_scylla_ser_row_writer_buffer>(
                &self,
                ctx: &#crate_path::RowSerializationContext,
                writer: &mut #crate_path::RowWriter<'_scylla_ser_row_writer_buffer>,
            ) -> ::std::result::Result<(), #crate_path::SerializationError> {
                #partial_struct
                #partial_serialize

                #[allow(non_local_definitions)]
                #serialize_by_name

                #crate_path::ser::row::ByName(self).serialize(ctx, writer)
            }
        }
    }

    fn generate_is_empty(&self) -> syn::TraitItemFn {
        let is_empty = self.ctx.fields.is_empty();
        parse_quote! {
            #[inline]
            fn is_empty(&self) -> bool {
                #is_empty
            }
        }
    }
}

// Generates an implementation of the trait which requires the columns
// to be placed in the same order as they are defined in the struct.
struct ColumnOrderedGenerator<'a> {
    ctx: &'a Context,
}

impl Generator for ColumnOrderedGenerator<'_> {
    fn generate_serialize(&self) -> syn::TraitItemFn {
        let crate_path = self.ctx.attributes.crate_path();
        let struct_name = &self.ctx.struct_name;
        let (impl_generics, ty_generics, where_clause) = self.ctx.generics.split_for_impl();
        let columns = self.ctx.fields.iter().map(|f| -> syn::Stmt {
            let field = &f.ident;
            if f.attrs.flatten {
                syn::parse_quote! {
                    <_ as #crate_path::SerializeRowInOrder>::serialize_in_order(&self.#field, columns, writer)?;
                }
            } else {
                let column = f.column_name();
                if self.ctx.attributes.skip_name_checks {
                    syn::parse_quote! {
                        columns.next_skip_name::<Self>(#column, &self.#field, writer)?;
                    }
                } else {
                    syn::parse_quote! {
                        columns.next::<Self>(#column, &self.#field, writer)?;
                    }
                }
            }
        });

        parse_quote! {
            fn serialize<'_scylla_ser_row_writer_buffer>(
                &self,
                ctx: &#crate_path::RowSerializationContext,
                writer: &mut #crate_path::RowWriter<'_scylla_ser_row_writer_buffer>,
            ) -> ::std::result::Result<(), #crate_path::SerializationError> {
                #[allow(non_local_definitions)]
                impl #impl_generics #crate_path::SerializeRowInOrder for #struct_name #ty_generics #where_clause {
                    fn serialize_in_order(
                        &self,
                        columns: &mut #crate_path::ser::row::ByColumn<'_, '_>,
                        writer: &mut #crate_path::RowWriter<'_>,
                    ) -> Result<(), #crate_path::SerializationError> {
                        #(#columns)*
                        Ok(())
                    }
                }

                #crate_path::ser::row::InOrder(self).serialize(ctx, writer)
            }
        }
    }

    fn generate_is_empty(&self) -> syn::TraitItemFn {
        let is_empty = self.ctx.fields.is_empty();
        parse_quote! {
            #[inline]
            fn is_empty(&self) -> bool {
                #is_empty
            }
        }
    }
}
