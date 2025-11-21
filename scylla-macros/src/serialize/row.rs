use std::collections::HashMap;

use darling::FromAttributes;
use proc_macro::TokenStream;
use quote::format_ident;
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

    // Used for deserialization only. Ignored in serialization.
    #[darling(default)]
    #[darling(rename = "allow_missing")]
    _default_when_missing: bool,
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
    typ: syn::Type,
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

    // If set, then the field is inlined into the struct, that is, it is
    // serialized using `SerializeRow`, with the serialized fields flattened
    // into (possibly multiple) columns as if they were in the parent struct.
    #[darling(default)]
    flatten: bool,

    // If true, then the field is not serialized at all, but simply ignored.
    // All other attributes are ignored.
    #[darling(default)]
    skip: bool,

    // Used for deserialization only. Ignored in serialization.
    #[darling(default)]
    #[darling(rename = "default_when_null")]
    _default_when_null: bool,

    // Used for deserialization only. Ignored in serialization.
    #[darling(default)]
    #[darling(rename = "allow_missing")]
    _default_when_missing: bool,
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
                typ: f.ty.clone(),
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

    let generator: Box<dyn Generator> = match ctx.attributes.flavor {
        Flavor::MatchByName => Box::new(ColumnSortingGenerator { ctx: &ctx }),
        Flavor::EnforceOrder => Box::new(ColumnOrderedGenerator { ctx: &ctx }),
    };

    let serialize_item = generator.generate_serialize();
    let is_empty_item = generator.generate_is_empty();

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
                let msg = format!(
                    "the column / bind marker name `{column_name}` used by this struct field is already used by field `{other_field_ident}`"
                );
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
        // Serializing by name requires:
        //
        // 1. Defining a partial struct: a struct that keeps references to each serializable field and
        // tracks completion of these fields
        //
        // 2. Implement serialization for the partial struct: When serializing a column check if this
        // column is one of our nonflattened fields, a nested field inside a flattened struct, or
        // not relevant to this struct. If relevant to this struct, typecheck it.
        //
        // 3. Implement SerializeRowByName: Creates an instance of the partial struct.
        //
        // 4. Implement SerializeRow: simply forwards to ser::row::ByName which will be in charge of
        // asking for a partial view of our struct and one by one sending columns to serialize to it
        // until it is done or an error occurs

        let crate_path = self.ctx.attributes.crate_path();
        let struct_name = &self.ctx.struct_name;
        // 1. Defining a partial struct
        let partial_struct_name = syn::Ident::new(
            &format!("_{struct_name}ScyllaSerPartial"),
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

        let flattened: Vec<_> = self.ctx.fields.iter().filter(|f| f.attrs.flatten).collect();
        let flattened_fields: Vec<_> = flattened.iter().map(|f| &f.ident).collect();
        let flattened_types: Vec<_> = flattened.iter().map(|f| &f.typ).collect();
        let flattened_visited_flag_names: Vec<_> = flattened_fields
            .iter()
            .map(|ident| format_ident!("__visited_flag_{}", ident))
            .collect();

        let nonflattened: Vec<_> = self
            .ctx
            .fields
            .iter()
            .filter(|f| !f.attrs.flatten)
            .collect();
        let nonflattened_fields: Vec<_> = nonflattened.iter().map(|f| &f.ident).collect();
        let nonflattened_visited_flag_names: Vec<_> = nonflattened_fields
            .iter()
            .map(|ident| format_ident!("__visited_flag_{}", ident))
            .collect();
        let nonflattened_types: Vec<_> = nonflattened.iter().map(|f| &f.typ).collect();

        let partial_struct: syn::ItemStruct = parse_quote! {
            pub struct #partial_struct_name #partial_generics {
                #(#nonflattened_fields: &#partial_lt #nonflattened_types,)*
                #(#flattened_fields: <#flattened_types as #crate_path::SerializeRowByName>::Partial<#partial_lt>,)*
                #(#flattened_visited_flag_names: bool,)*
                #(#nonflattened_visited_flag_names: bool,)*
                remaining_count: usize,
            }
        };

        // 2. Implement serialization for the partial struct
        let nonflattened_columns: Vec<_> = nonflattened.iter().map(|f| f.column_name()).collect();
        let (impl_generics, ty_generics, where_clause) = self.ctx.generics.split_for_impl();
        let serialize_field_block: syn::Block = if self.ctx.fields.is_empty() {
            parse_quote! {{
                ::std::result::Result::Ok(#crate_path::ser::row::FieldStatus::NotUsed)
            }}
        } else {
            parse_quote! {{
                match spec.name() {
                    // first check if the spec name matches a non-flattened column
                    #(#nonflattened_columns => {
                        #crate_path::ser::row::serialize_column::<#struct_name #ty_generics>(
                            &self.#nonflattened_fields, spec, writer,
                        )?;
                        if !self.#nonflattened_visited_flag_names {
                            self.#nonflattened_visited_flag_names = true;
                            self.remaining_count -= 1;
                        }
                    })*
                    // if not, then check if any flattened field has a column for it
                    _ => 'flatten_try: {
                        #({
                            match <<#flattened_types as #crate_path::SerializeRowByName>::Partial<#partial_lt> as #crate_path::PartialSerializeRowByName>::serialize_field(&mut self.#flattened_fields, spec, writer)? {
                                // there is a column and the field is done
                                #crate_path::ser::row::FieldStatus::Done => {
                                    if !self.#flattened_visited_flag_names {
                                        self.#flattened_visited_flag_names = true;
                                        self.remaining_count -= 1;
                                    }
                                    break 'flatten_try;
                                }
                                // there is a column in this flattened field but we need more
                                // columns for this field
                                #crate_path::ser::row::FieldStatus::NotDone => {
                                    return ::std::result::Result::Ok(#crate_path::ser::row::FieldStatus::NotDone);
                                }
                                // there wasn't any column on this field -- try the next one
                                #crate_path::ser::row::FieldStatus::NotUsed => {}
                            };
                        })*

                        // we didn't break out of 'flatten_try so the column didn't match any
                        // flattened field
                        return ::std::result::Result::Ok(#crate_path::ser::row::FieldStatus::NotUsed);
                    }
                }

                // report if we are done; so that any parent struct knows if this field is done in
                // case this field is flattened
                ::std::result::Result::Ok(if self.remaining_count == 0 {
                    #crate_path::ser::row::FieldStatus::Done
                } else {
                    #crate_path::ser::row::FieldStatus::NotDone
                })
            }}
        };

        let (partial_impl_generics, partial_ty_generics, partial_where_clause) =
            partial_generics.split_for_impl();
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
                    if self.remaining_count == 0 {
                        return ::std::result::Result::Ok(());
                    }

                    #(if !self.#nonflattened_visited_flag_names {
                        return ::std::result::Result::Err(#crate_path::ser::row::mk_typck_err::<#struct_name #ty_generics>(#crate_path::BuiltinRowTypeCheckErrorKind::NoColumnWithName {
                            name: <_ as ::std::borrow::ToOwned>::to_owned(#nonflattened_columns),
                        }))
                    })*

                    // if what is missing is a flattened field then report that error
                    #(if !self.#flattened_visited_flag_names {
                        return <<#flattened_types as #crate_path::SerializeRowByName>::Partial<#partial_lt> as #crate_path::PartialSerializeRowByName>::check_missing(self.#flattened_fields)
                    })*

                    ::std::unreachable!()
                }
            }
        };

        // 3. Implement SerializeRowByName
        let num_fields = flattened_visited_flag_names.len() + nonflattened_visited_flag_names.len();
        let serialize_by_name: syn::ItemImpl = parse_quote! {
            impl #impl_generics #crate_path::SerializeRowByName for #struct_name #ty_generics #where_clause {
                type Partial<#partial_lt> = #partial_struct_name #partial_ty_generics where Self: #partial_lt;

                fn partial(&self) -> Self::Partial<'_> {
                    use ::std::iter::FromIterator as _;

                    #partial_struct_name {
                        #(#nonflattened_fields: &self.#nonflattened_fields,)*
                        #(#flattened_fields: <_ as #crate_path::SerializeRowByName>::partial(&self.#flattened_fields),)*
                        #(#nonflattened_visited_flag_names: false,)*
                        #(#flattened_visited_flag_names: false,)*
                        remaining_count: #num_fields,
                    }
                }
            }
        };

        // 4. Implement SerializeRow
        parse_quote! {
            fn serialize<'_scylla_ser_row_writer_buffer>(
                &self,
                ctx: &#crate_path::RowSerializationContext,
                writer: &mut #crate_path::RowWriter<'_scylla_ser_row_writer_buffer>,
            ) -> ::std::result::Result<(), #crate_path::SerializationError> {
                #partial_struct
                #partial_serialize

                #[expect(non_local_definitions)]
                #serialize_by_name

                #crate_path::ser::row::ByName::<Self>::serialize(#crate_path::ser::row::ByName(self), ctx, writer)
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
        let column_serializers = self.ctx.fields.iter().map(|f| -> syn::Stmt {
            let field = &f.ident;
            if f.attrs.flatten {
                syn::parse_quote! {
                    <_ as #crate_path::SerializeRowInOrder>::serialize_in_order(&self.#field, columns, writer)?;
                }
            } else {
                let column = f.column_name();
                let enforce_name = !self.ctx.attributes.skip_name_checks;
                syn::parse_quote! {
                    #crate_path::ser::row::NextColumnSerializer::serialize::<Self, #enforce_name>(columns, #column, &self.#field, writer)?;
                }
            }
        });

        parse_quote! {
            fn serialize<'_scylla_ser_row_writer_buffer>(
                &self,
                ctx: &#crate_path::RowSerializationContext,
                writer: &mut #crate_path::RowWriter<'_scylla_ser_row_writer_buffer>,
            ) -> ::std::result::Result<(), #crate_path::SerializationError> {
                #[expect(non_local_definitions)]
                impl #impl_generics #crate_path::SerializeRowInOrder for #struct_name #ty_generics #where_clause {
                    fn serialize_in_order(
                        &self,
                        columns: &mut #crate_path::ser::row::NextColumnSerializer<'_, '_>,
                        writer: &mut #crate_path::RowWriter<'_>,
                    ) -> ::std::result::Result<(), #crate_path::SerializationError> {
                        #(#column_serializers)*
                        ::std::result::Result::Ok(())
                    }
                }

                #crate_path::ser::row::InOrder::<Self>::serialize(#crate_path::ser::row::InOrder(self), ctx, writer)
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
