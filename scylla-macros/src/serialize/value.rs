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

    // If true, then the type checking code won't verify the UDT field names.
    // UDT fields will be matched to struct fields based solely on the order.
    //
    // This annotation only works if `enforce_order` flavor is specified.
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
    fn field_name(&self) -> String {
        match &self.attrs.rename {
            Some(name) => name.clone(),
            None => self.ident.to_string(),
        }
    }

    // Returns whether this field must be serialized (can't be ignored in case
    // that there is no corresponding field in UDT).
    fn is_required(&self) -> bool {
        !self.attrs.skip && !self.attrs.ignore_missing
    }
}

#[derive(FromAttributes)]
#[darling(attributes(scylla))]
struct FieldAttributes {
    // If set, then serializes from the UDT field with this particular name
    // instead of the Rust field name.
    rename: Option<String>,

    // If true, then the field is not serialized at all, but simply ignored.
    // All other attributes are ignored.
    #[darling(default)]
    skip: bool,

    // If true, then - if this field is missing from the UDT fields metadata
    // - it will be ignored during serialization.
    #[darling(default)]
    #[darling(rename = "allow_missing")]
    ignore_missing: bool,

    // Used for deserialization only. Ignored in serialization.
    #[darling(default)]
    #[darling(rename = "default_when_null")]
    _default_when_null: bool,
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
                typ: f.ty.clone(),
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
        #[automatically_derived]
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

            // When name checks are skipped, fields with `allow_missing` are only
            // permitted at the end of the struct, i.e. no field without
            // `allow_missing` and `skip` is allowed to be after any field
            // with `allow_missing`.
            let invalid_default_when_missing_field = self
                .fields
                .iter()
                .rev()
                // Skip the whole suffix of <allow_missing> and <skip>.
                .skip_while(|field| !field.is_required())
                // skip_while finished either because the iterator is empty or it found a field without both <allow_missing> and <skip>.
                // In either case, there aren't allowed to be any more fields with `allow_missing`.
                .find(|field| field.attrs.ignore_missing);
            if let Some(invalid) = invalid_default_when_missing_field {
                let error =
                darling::Error::custom(
                    "when `skip_name_checks` is on, fields with `allow_missing` are only permitted at the end of the struct, \
                          i.e. no field without `allow_missing` and `skip` is allowed to be after any field with `allow_missing`."
            ).with_span(&invalid.ident);
                errors.push(error);
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
                #crate_path::ColumnType::UserDefinedType { definition: udt, .. } => {
                    (&udt.name, &udt.keyspace, &udt.field_types)
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

impl Generator for FieldSortingGenerator<'_> {
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
        let rust_field_ignore_missing_flags =
            self.ctx.fields.iter().map(|f| f.attrs.ignore_missing);
        let udt_field_names = rust_field_names.clone(); // For now, it's the same
        let field_types = self.ctx.fields.iter().map(|f| &f.typ).collect::<Vec<_>>();

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

        fn make_visited_flag_ident(field_name: &syn::Ident) -> syn::Ident {
            syn::Ident::new(&format!("visited_flag_{field_name}"), field_name.span())
        }

        // Generate a "visited" flag for each field
        let visited_flag_names = rust_field_idents
            .iter()
            .map(make_visited_flag_ident)
            .collect::<Vec<_>>();
        statements.extend::<Vec<_>>(parse_quote! {
            #(let mut #visited_flag_names = false;)*
        });

        // An iterator over names of Rust fields that can't be ignored
        // (i.e., if UDT misses a corresponding field, an error should be raised).
        let nonignorable_rust_field_names = self
            .ctx
            .fields
            .iter()
            .filter(|f| !f.attrs.ignore_missing)
            .map(|f| &f.ident);
        // An iterator over visited flags of Rust fields that can't be ignored
        // (i.e., if UDT misses a corresponding field, an error should be raised).
        let nonignorable_visited_flag_names =
            nonignorable_rust_field_names.map(make_visited_flag_ident);

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
        // If there are some missing fields that don't have the `#[allow_missing]`
        // attribute on them, return an error.
        statements.push(parse_quote! {
            if remaining_count > 0 {
                #(
                    if !#nonignorable_visited_flag_names && !#rust_field_ignore_missing_flags {
                        return ::std::result::Result::Err(mk_typck_err(
                            #crate_path::UdtTypeCheckErrorKind::ValueMissingForUdtField {
                                field_name: <_ as ::std::string::ToString>::to_string(#rust_field_names),
                            }
                        ));
                    }
                )*
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

impl Generator for FieldOrderedGenerator<'_> {
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

        // Create a peekable iterator over fields.
        statements.push(parse_quote! {
            let mut field_iter = ::std::iter::Iterator::peekable(field_types.iter());
        });

        // Serialize each field
        for field in self.ctx.fields.iter() {
            let rust_field_ident = &field.ident;
            let rust_field_name = field.field_name();
            let field_can_be_ignored = field.attrs.ignore_missing;
            let typ = &field.typ;
            let name_check_expression: syn::Expr = if !self.ctx.attributes.skip_name_checks {
                parse_quote! { field_name == #rust_field_name }
            } else {
                parse_quote! { true }
            };
            statements.push(parse_quote! {
                match field_iter.peek() {
                    ::std::option::Option::Some((field_name, typ)) => {
                        if #name_check_expression {
                            // Advance the iterator.
                            ::std::iter::Iterator::next(&mut field_iter);

                            let sub_builder = #crate_path::CellValueBuilder::make_sub_writer(&mut builder);
                            match <#typ as #crate_path::SerializeValue>::serialize(&self.#rust_field_ident, typ, sub_builder) {
                                ::std::result::Result::Ok(_proof) => {},
                                ::std::result::Result::Err(err) => {
                                    return ::std::result::Result::Err(mk_ser_err(
                                        #crate_path::UdtSerializationErrorKind::FieldSerializationFailed {
                                            field_name: <_ as ::std::clone::Clone>::clone(field_name).into_owned(),
                                            err,
                                        }
                                    ));
                                }
                            }
                        } else if !#field_can_be_ignored {
                            return ::std::result::Result::Err(mk_typck_err(
                                #crate_path::UdtTypeCheckErrorKind::FieldNameMismatch {
                                    rust_field_name: <_ as ::std::string::ToString>::to_string(#rust_field_name),
                                    db_field_name: <_ as ::std::clone::Clone>::clone(field_name).into_owned(),
                                }
                            ));
                        }
                        // Else simply ignore the field.
                    }
                    ::std::option::Option::None => {
                        if !#field_can_be_ignored {
                            return ::std::result::Result::Err(mk_typck_err(
                                #crate_path::UdtTypeCheckErrorKind::ValueMissingForUdtField {
                                    field_name: <_ as ::std::string::ToString>::to_string(#rust_field_name),
                                }
                            ));
                        }
                        // Else the field is ignored and we continue with other fields.
                    }
                }
            });
        }

        if self.ctx.attributes.forbid_excess_udt_fields {
            // Check whether there are some fields remaining
            statements.push(parse_quote! {
                if let ::std::option::Option::Some((field_name, typ)) = ::std::iter::Iterator::next(&mut field_iter) {
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
