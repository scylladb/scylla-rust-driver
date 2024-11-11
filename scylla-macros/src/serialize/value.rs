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

    // If true, then the type checking code will require that the UDT does not
    // contain excess fields at its suffix. Otherwise, if UDT has some fields
    // at its suffix that do not correspond to Rust struct's fields,
    // they will be sent as NULLs (if they are in the middle of the UDT) or not
    // sent at all (if they are in the prefix of the UDT), which means that
    // the DB will interpret them as NULLs anyway.
    #[darling(default)]
    forbid_excess_udt_fields: bool,
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
    fn field_name(&self) -> String {
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

pub(crate) fn derive_serialize_value(
    tokens_input: TokenStream,
) -> Result<syn::ItemImpl, syn::Error> {
    let input: syn::DeriveInput = syn::parse(tokens_input)?;
    let struct_name = input.ident.clone();
    let named_fields = crate::parser::parse_named_fields(&input, "SerializeValue")?;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let attributes = Attributes::from_attributes(&input.attrs)?;

    let crate_path = attributes.crate_path();
    let implemented_trait: syn::Path = parse_quote!(#crate_path::SerializeValue);

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
        Flavor::MatchByName => Box::new(FieldSortingGenerator { ctx: &ctx }),
        Flavor::EnforceOrder => Box::new(FieldOrderedGenerator { ctx: &ctx }),
    };

    let serialize_item = gen.generate_serialize();

    let res = parse_quote! {
        impl #impl_generics #implemented_trait for #struct_name #ty_generics #where_clause {
            #serialize_item
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
            let field_name = field.field_name();
            if let Some(other_field) = used_names.get(&field_name) {
                let other_field_ident = &other_field.ident;
                let msg = format!("the UDT field name `{field_name}` used by this struct field is already used by field `{other_field_ident}`");
                let err = darling::Error::custom(msg).with_span(&field.ident);
                errors.push(err);
            } else {
                used_names.insert(field_name, field);
            }
        }

        errors.finish()?;
        Ok(())
    }

    fn generate_udt_type_match(&self, err: syn::Expr) -> syn::Stmt {
        let crate_path = self.attributes.crate_path();

        parse_quote! {
            let (type_name, keyspace, field_types) = match typ {
                #crate_path::ColumnType::UserDefinedType { type_name, keyspace, field_types, .. } => {
                    (type_name, keyspace, field_types)
                }
                _ => return ::std::result::Result::Err(mk_typck_err(#err)),
            };
        }
    }

    fn generate_mk_typck_err(&self) -> syn::Stmt {
        let crate_path = self.attributes.crate_path();
        parse_quote! {
            let mk_typck_err = |kind: #crate_path::UdtTypeCheckErrorKind| -> #crate_path::SerializationError {
                #crate_path::SerializationError::new(
                    #crate_path::BuiltinTypeTypeCheckError {
                        rust_name: ::std::any::type_name::<Self>(),
                        got: <_ as ::std::clone::Clone>::clone(typ).into_owned(),
                        kind: #crate_path::BuiltinTypeTypeCheckErrorKind::UdtError(kind),
                    }
                )
            };
        }
    }

    fn generate_mk_ser_err(&self) -> syn::Stmt {
        let crate_path = self.attributes.crate_path();
        parse_quote! {
            let mk_ser_err = |kind: #crate_path::UdtSerializationErrorKind| -> #crate_path::SerializationError {
                #crate_path::SerializationError::new(
                    #crate_path::BuiltinTypeSerializationError {
                        rust_name: ::std::any::type_name::<Self>(),
                        got: <_ as ::std::clone::Clone>::clone(typ).into_owned(),
                        kind: #crate_path::BuiltinTypeSerializationErrorKind::UdtError(kind),
                    }
                )
            };
        }
    }
}

trait Generator {
    fn generate_serialize(&self) -> syn::TraitItemFn;
}

// Generates an implementation of the trait which sorts the fields according
// to how it is defined in the database.
struct FieldSortingGenerator<'a> {
    ctx: &'a Context,
}

impl<'a> Generator for FieldSortingGenerator<'a> {
    fn generate_serialize(&self) -> syn::TraitItemFn {
        // Need to:
        // - Check that all required fields are there and no more
        // - Check that the field types match
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
            .map(|f| f.field_name())
            .collect::<Vec<_>>();
        let udt_field_names = rust_field_names.clone(); // For now, it's the same
        let field_types = self.ctx.fields.iter().map(|f| &f.ty).collect::<Vec<_>>();

        let missing_rust_field_expression: syn::Expr =
            if self.ctx.attributes.forbid_excess_udt_fields {
                parse_quote! {
                    return ::std::result::Result::Err(mk_typck_err(
                        #crate_path::UdtTypeCheckErrorKind::NoSuchFieldInUdt {
                            field_name: <_ as ::std::clone::Clone>::clone(field_name).into_owned(),
                        }
                    ))
                }
            } else {
                parse_quote! {
                    skipped_fields += 1
                }
            };

        let serialize_missing_nulls_statement: syn::Stmt = if self
            .ctx
            .attributes
            .forbid_excess_udt_fields
        {
            // Not sure if there is better way to create no-op statement
            // parse_quote!{} / parse_quote!{ ; } doesn't work
            parse_quote! {
                ();
            }
        } else {
            parse_quote! {
                while skipped_fields > 0 {
                    let sub_builder = #crate_path::CellValueBuilder::make_sub_writer(&mut builder);
                    sub_builder.set_null();
                    skipped_fields -= 1;
                }
            }
        };

        // Declare helper lambdas for creating errors
        statements.push(self.ctx.generate_mk_typck_err());
        statements.push(self.ctx.generate_mk_ser_err());

        // Check that the type we want to serialize to is a UDT
        statements.push(
            self.ctx
                .generate_udt_type_match(parse_quote!(#crate_path::UdtTypeCheckErrorKind::NotUdt)),
        );

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

        // We want to send nulls for missing rust fields in the middle, but send
        // nothing for those fields at the end of UDT. While executing the loop
        // we don't know if there will be any more present fields. The solution is
        // to count how many fields we missed and send them when we find any present field.
        if !self.ctx.attributes.forbid_excess_udt_fields {
            statements.push(parse_quote! {
                let mut skipped_fields = 0;
            });
        }

        // Turn the cell writer into a value builder
        statements.push(parse_quote! {
            let mut builder = #crate_path::CellWriter::into_value_builder(writer);
        });

        // Generate a loop over the fields and a `match` block to match on
        // the field name.
        statements.push(parse_quote! {
            for (field_name, field_type) in field_types {
                match ::std::ops::Deref::deref(field_name) {
                    #(
                        #udt_field_names => {
                            #serialize_missing_nulls_statement
                            let sub_builder = #crate_path::CellValueBuilder::make_sub_writer(&mut builder);
                            match <#field_types as #crate_path::SerializeValue>::serialize(&self.#rust_field_idents, field_type, sub_builder) {
                                ::std::result::Result::Ok(_proof) => {}
                                ::std::result::Result::Err(err) => {
                                    return ::std::result::Result::Err(mk_ser_err(
                                        #crate_path::UdtSerializationErrorKind::FieldSerializationFailed {
                                            field_name: <_ as ::std::clone::Clone>::clone(field_name).into_owned(),
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
                    _ => #missing_rust_field_expression,
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
                            #crate_path::UdtTypeCheckErrorKind::ValueMissingForUdtField {
                                field_name: <_ as ::std::string::ToString>::to_string(#rust_field_names),
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
                typ: &#crate_path::ColumnType,
                writer: #crate_path::CellWriter<'b>,
            ) -> ::std::result::Result<#crate_path::WrittenCellProof<'b>, #crate_path::SerializationError> {
                #(#statements)*
                let proof = #crate_path::CellValueBuilder::finish(builder)
                    .map_err(|_| #crate_path::SerializationError::new(
                        #crate_path::BuiltinTypeSerializationError {
                            rust_name: ::std::any::type_name::<Self>(),
                            got: <_ as ::std::clone::Clone>::clone(typ).into_owned(),
                            kind: #crate_path::BuiltinTypeSerializationErrorKind::SizeOverflow,
                        }
                    ) as #crate_path::SerializationError)?;
                ::std::result::Result::Ok(proof)
            }
        }
    }
}

// Generates an implementation of the trait which requires the fields
// to be placed in the same order as they are defined in the struct.
struct FieldOrderedGenerator<'a> {
    ctx: &'a Context,
}

impl<'a> Generator for FieldOrderedGenerator<'a> {
    fn generate_serialize(&self) -> syn::TraitItemFn {
        let mut statements: Vec<syn::Stmt> = Vec::new();

        let crate_path = self.ctx.attributes.crate_path();

        // Declare a helper lambda for creating errors
        statements.push(self.ctx.generate_mk_typck_err());
        statements.push(self.ctx.generate_mk_ser_err());

        // Check that the type we want to serialize to is a UDT
        statements.push(
            self.ctx
                .generate_udt_type_match(parse_quote!(#crate_path::UdtTypeCheckErrorKind::NotUdt)),
        );

        // Turn the cell writer into a value builder
        statements.push(parse_quote! {
            let mut builder = #crate_path::CellWriter::into_value_builder(writer);
        });

        // Create an iterator over fields
        statements.push(parse_quote! {
            let mut field_iter = field_types.iter();
        });

        // Serialize each field
        for field in self.ctx.fields.iter() {
            let rust_field_ident = &field.ident;
            let rust_field_name = field.field_name();
            let typ = &field.ty;
            let name_check_expression: syn::Expr = if !self.ctx.attributes.skip_name_checks {
                parse_quote! { field_name == #rust_field_name }
            } else {
                parse_quote! { true }
            };
            statements.push(parse_quote! {
                match field_iter.next() {
                    Some((field_name, typ)) => {
                        if #name_check_expression {
                            let sub_builder = #crate_path::CellValueBuilder::make_sub_writer(&mut builder);
                            match <#typ as #crate_path::SerializeValue>::serialize(&self.#rust_field_ident, typ, sub_builder) {
                                Ok(_proof) => {},
                                Err(err) => {
                                    return ::std::result::Result::Err(mk_ser_err(
                                        #crate_path::UdtSerializationErrorKind::FieldSerializationFailed {
                                            field_name: <_ as ::std::clone::Clone>::clone(field_name).into_owned(),
                                            err,
                                        }
                                    ));
                                }
                            }
                        } else {
                            return ::std::result::Result::Err(mk_typck_err(
                                #crate_path::UdtTypeCheckErrorKind::FieldNameMismatch {
                                    rust_field_name: <_ as ::std::string::ToString>::to_string(#rust_field_name),
                                    db_field_name: <_ as ::std::clone::Clone>::clone(field_name).into_owned(),
                                }
                            ));
                        }
                    }
                    None => {
                        return ::std::result::Result::Err(mk_typck_err(
                            #crate_path::UdtTypeCheckErrorKind::ValueMissingForUdtField {
                                field_name: <_ as ::std::string::ToString>::to_string(#rust_field_name),
                            }
                        ));
                    }
                }
            });
        }

        if self.ctx.attributes.forbid_excess_udt_fields {
            // Check whether there are some fields remaining
            statements.push(parse_quote! {
                if let Some((field_name, typ)) = field_iter.next() {
                    return ::std::result::Result::Err(mk_typck_err(
                        #crate_path::UdtTypeCheckErrorKind::NoSuchFieldInUdt {
                            field_name: <_ as ::std::clone::Clone>::clone(field_name).into_owned(),
                        }
                    ));
                }
            });
        }

        parse_quote! {
            fn serialize<'b>(
                &self,
                typ: &#crate_path::ColumnType,
                writer: #crate_path::CellWriter<'b>,
            ) -> ::std::result::Result<#crate_path::WrittenCellProof<'b>, #crate_path::SerializationError> {
                #(#statements)*
                let proof = #crate_path::CellValueBuilder::finish(builder)
                    .map_err(|_| #crate_path::SerializationError::new(
                        #crate_path::BuiltinTypeSerializationError {
                            rust_name: ::std::any::type_name::<Self>(),
                            got: <_ as ::std::clone::Clone>::clone(typ).into_owned(),
                            kind: #crate_path::BuiltinTypeSerializationErrorKind::SizeOverflow,
                        }
                    ) as #crate_path::SerializationError)?;
                ::std::result::Result::Ok(proof)
            }
        }
    }
}
