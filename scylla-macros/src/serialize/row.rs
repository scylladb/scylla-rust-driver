use std::collections::HashMap;

use darling::FromAttributes;
use proc_macro::TokenStream;
use proc_macro2::Span;
use syn::parse_quote;

use super::Flavor;

#[derive(FromAttributes)]
#[darling(attributes(scylla))]
struct Attributes {
    #[darling(rename = "crate")]
    crate_path: Option<syn::Path>,

    #[darling(default)]
    flavor: Flavor,

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
    rename: Option<String>,

    #[darling(default)]
    skip: bool,
}

struct Context {
    attributes: Attributes,
    fields: Vec<Field>,
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
    let ctx = Context { attributes, fields };
    ctx.validate(&input.ident)?;

    let gen: Box<dyn Generator> = match ctx.attributes.flavor {
        Flavor::MatchByName => Box::new(ColumnSortingGenerator { ctx: &ctx }),
        Flavor::EnforceOrder => Box::new(ColumnOrderedGenerator { ctx: &ctx }),
    };

    let serialize_item = gen.generate_serialize();
    let is_empty_item = gen.generate_is_empty();

    let res = parse_quote! {
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

    fn generate_mk_typck_err(&self) -> syn::Stmt {
        let crate_path = self.attributes.crate_path();
        parse_quote! {
            let mk_typck_err = |kind: #crate_path::BuiltinRowTypeCheckErrorKind| -> #crate_path::SerializationError {
                #crate_path::SerializationError::new(
                    #crate_path::BuiltinRowTypeCheckError {
                        rust_name: ::std::any::type_name::<Self>(),
                        kind,
                    }
                )
            };
        }
    }

    fn generate_mk_ser_err(&self) -> syn::Stmt {
        let crate_path = self.attributes.crate_path();
        parse_quote! {
            let mk_ser_err = |kind: #crate_path::BuiltinRowSerializationErrorKind| -> #crate_path::SerializationError {
                #crate_path::SerializationError::new(
                    #crate_path::BuiltinRowSerializationError {
                        rust_name: ::std::any::type_name::<Self>(),
                        kind,
                    }
                )
            };
        }
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

impl<'a> Generator for ColumnSortingGenerator<'a> {
    fn generate_serialize(&self) -> syn::TraitItemFn {
        // Need to:
        // - Check that all required columns are there and no more
        // - Check that the column types match
        let mut statements: Vec<syn::Stmt> = Vec::new();

        let crate_path = self.ctx.attributes.crate_path();

        let rust_field_idents = self
            .ctx
            .fields
            .iter()
            .map(|f| f.ident.clone())
            .collect::<Vec<_>>();
        let rust_field_names = self
            .ctx
            .fields
            .iter()
            .map(|f| f.column_name())
            .collect::<Vec<_>>();
        let udt_field_names = rust_field_names.clone(); // For now, it's the same
        let field_types = self.ctx.fields.iter().map(|f| &f.ty).collect::<Vec<_>>();

        // Declare a helper lambda for creating errors
        statements.push(self.ctx.generate_mk_typck_err());
        statements.push(self.ctx.generate_mk_ser_err());

        // Generate a "visited" flag for each field
        let visited_flag_names = rust_field_names
            .iter()
            .map(|s| syn::Ident::new(&format!("visited_flag_{}", s), Span::call_site()))
            .collect::<Vec<_>>();
        statements.extend::<Vec<_>>(parse_quote! {
            #(let mut #visited_flag_names = false;)*
        });

        // Generate a variable that counts down visited fields.
        let field_count = self.ctx.fields.len();
        statements.push(parse_quote! {
            let mut remaining_count = #field_count;
        });

        // Generate a loop over the fields and a `match` block to match on
        // the field name.
        statements.push(parse_quote! {
            for spec in ctx.columns() {
                match spec.name() {
                    #(
                        #udt_field_names => {
                            let sub_writer = #crate_path::RowWriter::make_cell_writer(writer);
                            match <#field_types as #crate_path::SerializeValue>::serialize(&self.#rust_field_idents, spec.typ(), sub_writer) {
                                ::std::result::Result::Ok(_proof) => {}
                                ::std::result::Result::Err(err) => {
                                    return ::std::result::Result::Err(mk_ser_err(
                                        #crate_path::BuiltinRowSerializationErrorKind::ColumnSerializationFailed {
                                            name: <_ as ::std::borrow::ToOwned>::to_owned(spec.name()),
                                            err,
                                        }
                                    ));
                                }
                            }
                            if !#visited_flag_names {
                                #visited_flag_names = true;
                                remaining_count -= 1;
                            }
                        }
                    )*
                    _ => return ::std::result::Result::Err(mk_typck_err(
                        #crate_path::BuiltinRowTypeCheckErrorKind::NoColumnWithName {
                            name: <_ as ::std::borrow::ToOwned>::to_owned(spec.name()),
                        }
                    )),
                }
            }
        });

        // Finally, check that all fields were consumed.
        // If there are some missing fields, return an error
        statements.push(parse_quote! {
            if remaining_count > 0 {
                #(
                    if !#visited_flag_names {
                        return ::std::result::Result::Err(mk_typck_err(
                            #crate_path::BuiltinRowTypeCheckErrorKind::ValueMissingForColumn {
                                name: <_ as ::std::string::ToString>::to_string(#rust_field_names),
                            }
                        ));
                    }
                )*
                ::std::unreachable!()
            }
        });

        parse_quote! {
            fn serialize<'b>(
                &self,
                ctx: &#crate_path::RowSerializationContext,
                writer: &mut #crate_path::RowWriter<'b>,
            ) -> ::std::result::Result<(), #crate_path::SerializationError> {
                #(#statements)*
                ::std::result::Result::Ok(())
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

impl<'a> Generator for ColumnOrderedGenerator<'a> {
    fn generate_serialize(&self) -> syn::TraitItemFn {
        let mut statements: Vec<syn::Stmt> = Vec::new();

        let crate_path = self.ctx.attributes.crate_path();

        // Declare a helper lambda for creating errors
        statements.push(self.ctx.generate_mk_typck_err());
        statements.push(self.ctx.generate_mk_ser_err());

        // Create an iterator over fields
        statements.push(parse_quote! {
            let mut column_iter = ctx.columns().iter();
        });

        // Serialize each field
        for field in self.ctx.fields.iter() {
            let rust_field_ident = &field.ident;
            let rust_field_name = field.column_name();
            let typ = &field.ty;
            let name_check_expression: syn::Expr = if !self.ctx.attributes.skip_name_checks {
                parse_quote! { spec.name() == #rust_field_name }
            } else {
                parse_quote! { true }
            };
            statements.push(parse_quote! {
                match column_iter.next() {
                    Some(spec) => {
                        if #name_check_expression {
                            let cell_writer = #crate_path::RowWriter::make_cell_writer(writer);
                            match <#typ as #crate_path::SerializeValue>::serialize(&self.#rust_field_ident, spec.typ(), cell_writer) {
                                Ok(_proof) => {},
                                Err(err) => {
                                    return ::std::result::Result::Err(mk_ser_err(
                                        #crate_path::BuiltinRowSerializationErrorKind::ColumnSerializationFailed {
                                            name: <_ as ::std::borrow::ToOwned>::to_owned(spec.name()),
                                            err,
                                        }
                                    ));
                                }
                            }
                        } else {
                            return ::std::result::Result::Err(mk_typck_err(
                                #crate_path::BuiltinRowTypeCheckErrorKind::ColumnNameMismatch {
                                    rust_column_name: <_ as ::std::string::ToString>::to_string(#rust_field_name),
                                    db_column_name: <_ as ::std::borrow::ToOwned>::to_owned(spec.name()),
                                }
                            ));
                        }
                    }
                    None => {
                        return ::std::result::Result::Err(mk_typck_err(
                            #crate_path::BuiltinRowTypeCheckErrorKind::ValueMissingForColumn {
                                name: <_ as ::std::string::ToString>::to_string(#rust_field_name),
                            }
                        ));
                    }
                }
            });
        }

        // Check whether there are some columns remaining
        statements.push(parse_quote! {
            if let Some(spec) = column_iter.next() {
                return ::std::result::Result::Err(mk_typck_err(
                    #crate_path::BuiltinRowTypeCheckErrorKind::NoColumnWithName {
                        name: <_ as ::std::borrow::ToOwned>::to_owned(spec.name()),
                    }
                ));
            }
        });

        parse_quote! {
            fn serialize<'b>(
                &self,
                ctx: &#crate_path::RowSerializationContext,
                writer: &mut #crate_path::RowWriter<'b>,
            ) -> ::std::result::Result<(), #crate_path::SerializationError> {
                #(#statements)*
                ::std::result::Result::Ok(())
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
