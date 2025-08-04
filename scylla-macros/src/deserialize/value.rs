use std::collections::HashMap;

use darling::{FromAttributes, FromField};
use proc_macro::TokenStream;
use proc_macro2::Span;
use syn::{ext::IdentExt, parse_quote};

use crate::Flavor;

use super::{DeserializeCommonFieldAttrs, DeserializeCommonStructAttrs};

#[derive(FromAttributes)]
#[darling(attributes(scylla))]
struct StructAttrs {
    #[darling(rename = "crate")]
    crate_path: Option<syn::Path>,

    #[darling(default)]
    flavor: Flavor,

    // If true, then the type checking code won't verify the UDT field names.
    // UDT fields will be matched to struct fields based solely on the order.
    //
    // This annotation only works if `enforce_order` is specified.
    #[darling(default)]
    skip_name_checks: bool,

    // If true, then the type checking code will require that the UDT does not
    // contain excess fields at its suffix. Otherwise, if UDT has some fields
    // at its suffix that do not correspond to Rust struct's fields,
    // they will be ignored. With true, an error will be raised.
    #[darling(default)]
    forbid_excess_udt_fields: bool,

    // If true, then - if this field is missing from the UDT fields metadata
    // - it will be initialized to Default::default().
    #[darling(default)]
    #[darling(rename = "allow_missing")]
    default_when_missing: bool,
}

impl DeserializeCommonStructAttrs for StructAttrs {
    fn crate_path(&self) -> Option<&syn::Path> {
        self.crate_path.as_ref()
    }
}

#[derive(FromField)]
#[darling(attributes(scylla))]
struct Field {
    // If true, then the field is not parsed at all, but it is initialized
    // with Default::default() instead. All other attributes are ignored.
    #[darling(default)]
    skip: bool,

    // If true, then - if this field is missing from the UDT fields metadata
    // - it will be initialized to Default::default().
    #[darling(default)]
    #[darling(rename = "allow_missing")]
    default_when_missing: bool,

    // If true, then - if this field is present among UDT fields metadata
    // but at the same time missing from serialized data or set to null
    // - it will be initialized to Default::default().
    #[darling(default)]
    default_when_null: bool,

    // If set, then deserializes from the UDT field with this particular name
    // instead of the Rust field name.
    #[darling(default)]
    rename: Option<String>,

    ident: Option<syn::Ident>,
    ty: syn::Type,
}

impl DeserializeCommonFieldAttrs for Field {
    fn needs_default(&self) -> bool {
        self.skip || self.default_when_missing
    }

    fn deserialize_target(&self) -> &syn::Type {
        &self.ty
    }
}

// derive(DeserializeValue) for the DeserializeValue trait
pub(crate) fn deserialize_value_derive(
    tokens_input: TokenStream,
) -> Result<syn::ItemImpl, syn::Error> {
    let input = syn::parse(tokens_input)?;

    let implemented_trait: syn::Path = parse_quote!(DeserializeValue);
    let implemented_trait_name = implemented_trait
        .segments
        .last()
        .unwrap()
        .ident
        .unraw()
        .to_string();
    let constraining_trait = implemented_trait.clone();
    let s = StructDesc::new(&input, &implemented_trait_name, constraining_trait)?;

    validate_attrs(&s.attrs, s.fields())?;

    let items = [
        s.generate_type_check_method().into(),
        s.generate_deserialize_method().into(),
    ];

    Ok(s.generate_impl(implemented_trait, items))
}

fn validate_attrs(attrs: &StructAttrs, fields: &[Field]) -> Result<(), darling::Error> {
    let mut errors = darling::Error::accumulator();

    if attrs.skip_name_checks {
        // Skipping name checks is only available in enforce_order mode
        if attrs.flavor != Flavor::EnforceOrder {
            let error = darling::Error::custom(
                "attribute <skip_name_checks> requires <flavor = enforce_order>.",
            );
            errors.push(error);
        }

        // Fields with `allow_missing` are only permitted at the end of the
        // struct, i.e. no field without `allow_missing` and `skip` is allowed
        // to be after any field with `allow_missing`.
        let invalid_default_when_missing_field = fields
            .iter()
            .rev()
            // Skip the whole suffix of <allow_missing> and <skip>.
            .skip_while(|field| !field.is_required())
            // skip_while finished either because the iterator is empty or it found a field without both <allow_missing> and <skip>.
            // In either case, there aren't allowed to be any more fields with `allow_missing`.
            .find(|field| field.default_when_missing);
        if let Some(invalid) = invalid_default_when_missing_field {
            let error =
                darling::Error::custom(
                    "when <skip_name_checks> is on, fields with <allow_missing> are only permitted at the end of the struct, \
                          i.e. no field without <allow_missing> and <skip> is allowed to be after any field with <allow_missing>."
            ).with_span(&invalid.ident);
            errors.push(error);
        }

        // <rename> annotations don't make sense with skipped name checks
        for field in fields {
            if field.rename.is_some() {
                let err = darling::Error::custom(
                    "<rename> annotations don't make sense with <skip_name_checks> attribute",
                )
                .with_span(&field.ident);
                errors.push(err);
            }
        }
    } else {
        // Detect name collisions caused by <rename>.
        let mut used_names = HashMap::<String, &Field>::new();
        for field in fields {
            let udt_field_name = field.udt_field_name();
            if let Some(other_field) = used_names.get(&udt_field_name) {
                let other_field_ident = other_field.ident.as_ref().unwrap();
                let msg = format!("the UDT field name `{udt_field_name}` used by this struct field is already used by field `{other_field_ident}`");
                let err = darling::Error::custom(msg).with_span(&field.ident);
                errors.push(err);
            } else {
                used_names.insert(udt_field_name, field);
            }
        }
    }

    errors.finish()
}

impl Field {
    // Returns whether this field is mandatory for deserialization.
    fn is_required(&self) -> bool {
        !self.skip && !self.default_when_missing
    }

    // The name of UDT field corresponding to this Rust struct field
    fn udt_field_name(&self) -> String {
        match self.rename.as_ref() {
            Some(rename) => rename.to_owned(),
            None => self.ident.as_ref().unwrap().unraw().to_string(),
        }
    }

    // A Rust literal representing the name of this field
    fn cql_name_literal(&self) -> syn::LitStr {
        syn::LitStr::new(&self.udt_field_name(), Span::call_site())
    }
}

type StructDesc = super::StructDescForDeserialize<StructAttrs, Field>;

impl StructDesc {
    /// Generates an expression which extracts the UDT fields or returns an error.
    fn generate_extract_fields_from_type(&self, typ_expr: syn::Expr) -> syn::Expr {
        let macro_internal = &self.struct_attrs().macro_internal_path();
        parse_quote!(
            match #typ_expr {
                #macro_internal::ColumnType::UserDefinedType { definition, .. } => &definition.field_types,
                other => return ::std::result::Result::Err(
                    #macro_internal::mk_value_typck_err::<Self>(
                        &other,
                        #macro_internal::DeserUdtTypeCheckErrorKind::NotUdt,
                    )
                ),
            }
        )
    }

    fn generate_type_check_method(&self) -> syn::ImplItemFn {
        match self.attrs.flavor {
            Flavor::MatchByName => TypeCheckUnorderedGenerator(self).generate(),
            Flavor::EnforceOrder => TypeCheckAssumeOrderGenerator(self).generate(),
        }
    }

    fn generate_deserialize_method(&self) -> syn::ImplItemFn {
        match self.attrs.flavor {
            Flavor::MatchByName => DeserializeUnorderedGenerator(self).generate(),
            Flavor::EnforceOrder => DeserializeAssumeOrderGenerator(self).generate(),
        }
    }
}

struct TypeCheckAssumeOrderGenerator<'sd>(&'sd StructDesc);

impl TypeCheckAssumeOrderGenerator<'_> {
    // Generates name and type validation for given Rust struct's field.
    fn generate_field_validation(&self, rust_field_idx: usize, field: &Field) -> syn::Expr {
        let macro_internal = self.0.struct_attrs().macro_internal_path();
        let (frame_lifetime, metadata_lifetime) = self.0.constraint_lifetimes();
        let rust_field_name = field.cql_name_literal();
        let rust_field_typ = field.deserialize_target();
        let default_when_missing = self.0.attrs.default_when_missing || field.default_when_missing;
        let skip_name_checks = self.0.attrs.skip_name_checks;

        // Action performed in case of field name mismatch.
        let name_mismatch: syn::Expr = if default_when_missing {
            parse_quote! {
                {
                    // If the Rust struct's field is marked as `default_when_missing`, then let's assume
                    // optimistically that the remaining UDT fields match required Rust struct fields.
                    // For that, store the read UDT field to be fit against the next Rust struct field.
                    saved_cql_field = ::std::option::Option::Some(next_cql_field);
                    break 'verifications; // Skip type verification, because the UDT field is absent.
                }
            }
        } else {
            parse_quote! {
                {
                    // Error - required value for field not present among the CQL fields.
                    return ::std::result::Result::Err(
                        #macro_internal::mk_value_typck_err::<Self>(
                            typ,
                            #macro_internal::DeserUdtTypeCheckErrorKind::FieldNameMismatch {
                                position: #rust_field_idx,
                                rust_field_name: <_ as ::std::borrow::ToOwned>::to_owned(#rust_field_name),
                                db_field_name: <_ as ::std::clone::Clone>::clone(cql_field_name).into_owned(),
                            }
                        )
                    );
                }
            }
        };

        // Optional name verification.
        let name_verification: Option<syn::Expr> = (!skip_name_checks).then(|| {
            parse_quote! {
                if #rust_field_name != cql_field_name {
                    // The read UDT field is not the one expected by the Rust struct.
                    #name_mismatch
                }
            }
        });

        parse_quote! {
            'field: {
                let next_cql_field = match saved_cql_field
                    // We may have a stored CQL UDT field that did not match the previous Rust struct's field.
                    .take()
                    // If not, simply fetch another CQL UDT field from the iterator.
                    .or_else(|| ::std::iter::Iterator::next(&mut cql_field_iter)) {
                        ::std::option::Option::Some(cql_field) => cql_field,
                        // In case the Rust field allows default-initialisation and there are no more CQL fields,
                        // simply assume it's going to be default-initialised.
                        ::std::option::Option::None if #default_when_missing => break 'field,
                        ::std::option::Option::None => return ::std::result::Result::Err(too_few_fields()),
                };
                let (cql_field_name, cql_field_typ) = next_cql_field;

                'verifications: {
                    // Verify the name (unless `skip_name_checks` is specified)
                    // In a specific case when this Rust field is going to be default-initialised
                    // due to no corresponding CQL UDT field, the below type verification will be skipped.
                    #name_verification

                    // Verify the type
                    <#rust_field_typ as #macro_internal::DeserializeValue<#frame_lifetime, #metadata_lifetime>>::type_check(cql_field_typ)
                        .map_err(|err| #macro_internal::mk_value_typck_err::<Self>(
                            typ,
                            #macro_internal::DeserUdtTypeCheckErrorKind::FieldTypeCheckFailed {
                                field_name: <_ as ::std::borrow::ToOwned>::to_owned(#rust_field_name),
                                err,
                            }
                        ))?;
                }
            }
        }
    }

    // Generates the type_check method for when ensure_order == true.
    fn generate(&self) -> syn::ImplItemFn {
        // The generated method will:
        // - Check that every required field appears on the list in the same order as struct fields
        // - Every type on the list is correct

        let macro_internal = self.0.struct_attrs().macro_internal_path();

        let extract_fields_expr = self.0.generate_extract_fields_from_type(parse_quote!(typ));

        let required_fields_iter = || self.0.fields().iter().filter(|f| f.is_required());

        let required_field_count = required_fields_iter().count();
        let required_field_count_lit =
            syn::LitInt::new(&required_field_count.to_string(), Span::call_site());

        let required_fields_names = required_fields_iter().map(|field| field.ident.as_ref());

        let nonskipped_fields_iter = || {
            self.0
                .fields()
                .iter()
                // It is important that we enumerate **before** filtering, because otherwise we would not
                // count the skipped fields, which might be confusing.
                .enumerate()
                .filter(|(_idx, f)| !f.skip)
        };

        let field_validations =
            nonskipped_fields_iter().map(|(idx, field)| self.generate_field_validation(idx, field));

        let check_excess_udt_fields: Option<syn::Expr> =
            self.0.attrs.forbid_excess_udt_fields.then(|| {
                parse_quote! {
                    if let ::std::option::Option::Some((cql_field_name, cql_field_typ)) = saved_cql_field
                        .take()
                        .or_else(|| ::std::iter::Iterator::next(&mut cql_field_iter)) {
                        return ::std::result::Result::Err(#macro_internal::mk_value_typck_err::<Self>(
                            typ,
                            #macro_internal::DeserUdtTypeCheckErrorKind::ExcessFieldInUdt {
                                db_field_name: <_ as ::std::clone::Clone>::clone(cql_field_name).into_owned(),
                            }
                        ));
                    }
                }
            });

        parse_quote! {
            fn type_check(
                typ: &#macro_internal::ColumnType,
            ) -> ::std::result::Result<(), #macro_internal::TypeCheckError> {
                // Extract information about the field types from the UDT
                // type definition.
                let fields = #extract_fields_expr;

                let too_few_fields = || #macro_internal::mk_value_typck_err::<Self>(
                    typ,
                    #macro_internal::DeserUdtTypeCheckErrorKind::TooFewFields {
                        required_fields: ::std::vec![
                            #(stringify!(#required_fields_names),)*
                        ],
                        present_fields: ::std::iter::Iterator::collect(::std::iter::Iterator::map(fields.iter(), |(name, _typ)| ::std::clone::Clone::clone(name).into_owned())),
                    }
                );

                // Verify that the field count is correct
                if fields.len() < #required_field_count_lit {
                    return ::std::result::Result::Err(too_few_fields());
                }

                let mut cql_field_iter = fields.iter();
                // A CQL UDT field that has already been fetched from the field iterator,
                // but not yet matched to a Rust struct field (because the previous
                // Rust struct field didn't match it and had #[allow_missing] specified).
                let mut saved_cql_field = ::std::option::Option::None::<
                    &(::std::borrow::Cow<'_, str>, #macro_internal::ColumnType),
                >;
                #(
                    #field_validations
                )*

                #check_excess_udt_fields

                // All is good!
                ::std::result::Result::Ok(())
            }
        }
    }
}

struct DeserializeAssumeOrderGenerator<'sd>(&'sd StructDesc);

impl DeserializeAssumeOrderGenerator<'_> {
    fn generate_finalize_field(&self, field: &Field) -> syn::Expr {
        if field.skip {
            // Skipped fields are initialized with Default::default()
            return parse_quote! {
                ::std::default::Default::default()
            };
        }

        let macro_internal = self.0.struct_attrs().macro_internal_path();
        let cql_name_literal = field.cql_name_literal();
        let deserializer = field.deserialize_target();
        let (frame_lifetime, metadata_lifetime) = self.0.constraint_lifetimes();
        let default_when_missing = field.default_when_missing;
        let default_when_null = field.default_when_null;
        let skip_name_checks = self.0.attrs.skip_name_checks;

        let deserialize: syn::Expr = parse_quote! {
            <#deserializer as #macro_internal::DeserializeValue<#frame_lifetime, #metadata_lifetime>>::deserialize(cql_field_typ, value)
                .map_err(|err| #macro_internal::mk_value_deser_err::<Self>(
                    typ,
                    #macro_internal::UdtDeserializationErrorKind::FieldDeserializationFailed {
                        field_name: <_ as ::std::borrow::ToOwned>::to_owned(#cql_name_literal),
                        err,
                    }
                ))?
        };

        let maybe_default_deserialize: syn::Expr = if default_when_null {
            parse_quote! {
                if value.is_none() {
                    ::std::default::Default::default()
                } else {
                    #deserialize
                }
            }
        } else {
            parse_quote! {
                #deserialize
            }
        };

        // Action performed in case of field name mismatch.
        let name_mismatch: syn::Expr = if default_when_missing {
            parse_quote! {
                {
                    // If the Rust struct's field is marked as `default_when_missing`, then let's assume
                    // optimistically that the remaining UDT fields match required Rust struct fields.
                    // For that, store the read UDT field to be fit against the next Rust struct field.
                    saved_cql_field = ::std::option::Option::Some(next_cql_field);

                    ::std::default::Default::default()
                }
            }
        } else {
            parse_quote! {
                {
                    ::std::panic!(
                        "type check should have prevented this scenario - field name mismatch! Rust field name {}, CQL field name {}",
                        #cql_name_literal,
                        cql_field_name
                    );
                }
            }
        };

        let maybe_name_check_and_deserialize_or_save: syn::Expr = if skip_name_checks {
            parse_quote! {
                #maybe_default_deserialize
            }
        } else {
            parse_quote! {
                if #cql_name_literal == cql_field_name {
                    #maybe_default_deserialize
                } else {
                    #name_mismatch
                }
            }
        };

        let no_more_fields: syn::Expr = if default_when_missing {
            parse_quote! {
                ::std::default::Default::default()
            }
        } else {
            parse_quote! {
                // Type check has ensured that there are enough CQL UDT fields.
                ::std::panic!("Too few CQL UDT fields - type check should have prevented this scenario!")
            }
        };

        parse_quote! {
            {
                let maybe_next_cql_field = saved_cql_field
                    .take()
                    .map(::std::result::Result::Ok)
                    .or_else(|| {
                        ::std::option::Option::map(
                            ::std::iter::Iterator::next(&mut cql_field_iter),
                            |(specs, value_res)| value_res.map(|value| (specs, value)))
                    })
                    .transpose()
                    // Propagate deserialization errors.
                    .map_err(|err| #macro_internal::mk_value_deser_err::<Self>(
                        typ,
                        #macro_internal::UdtDeserializationErrorKind::FieldDeserializationFailed {
                            field_name: <_ as ::std::borrow::ToOwned>::to_owned(#cql_name_literal),
                            err,
                        }
                    ))?;

                if let ::std::option::Option::Some(next_cql_field) = maybe_next_cql_field {
                    let ((cql_field_name, cql_field_typ), value) = next_cql_field;

                    // The value can be either
                    // - None - missing from the serialized representation
                    // - Some(None) - present in the serialized representation but null
                    // For now, we treat both cases as "null".
                    let value = value.flatten();

                    #maybe_name_check_and_deserialize_or_save
                } else {
                    #no_more_fields
                }
            }
        }
    }

    fn generate(&self) -> syn::ImplItemFn {
        // We can assume that type_check was called.

        let macro_internal = self.0.struct_attrs().macro_internal_path();
        let (frame_lifetime, metadata_lifetime) = self.0.constraint_lifetimes();
        let fields = self.0.fields();

        let field_idents = fields.iter().map(|f| f.ident.as_ref().unwrap());
        let field_finalizers = fields.iter().map(|f| self.generate_finalize_field(f));

        let iterator_type: syn::Type =
            parse_quote!(#macro_internal::UdtIterator<#frame_lifetime, #metadata_lifetime>);

        parse_quote! {
            fn deserialize(
                typ: &#metadata_lifetime #macro_internal::ColumnType<#metadata_lifetime>,
                v: ::std::option::Option<#macro_internal::FrameSlice<#frame_lifetime>>,
            ) -> ::std::result::Result<Self, #macro_internal::DeserializationError> {
                // Create an iterator over the fields of the UDT.
                let mut cql_field_iter = <#iterator_type as #macro_internal::DeserializeValue<#frame_lifetime, #metadata_lifetime>>::deserialize(typ, v)
                    .map_err(#macro_internal::value_deser_error_replace_rust_name::<Self>)?;

                // This is to hold another field that already popped up from the field iterator but appeared to not match
                // the expected nonrequired field. Therefore, that field is stored here, while the expected field
                // is default-initialized.
                let mut saved_cql_field = ::std::option::Option::None::<(
                    &(::std::borrow::Cow<'_, str>, #macro_internal::ColumnType),
                    ::std::option::Option<::std::option::Option<#macro_internal::FrameSlice>>
                )>;

                ::std::result::Result::Ok(Self {
                    #(#field_idents: #field_finalizers,)*
                })
            }
        }
    }
}

struct TypeCheckUnorderedGenerator<'sd>(&'sd StructDesc);

impl TypeCheckUnorderedGenerator<'_> {
    // An identifier for a bool variable that represents whether given
    // field was already visited during type check
    fn visited_flag_variable(field: &Field) -> syn::Ident {
        quote::format_ident!("visited_{}", field.ident.as_ref().unwrap().unraw())
    }

    // Generates a declaration of a "visited" flag for the purpose of type check.
    // We generate it even if the flag is not required in order to protect
    // from fields appearing more than once
    fn generate_visited_flag_decl(field: &Field) -> Option<syn::Stmt> {
        (!field.skip).then(|| {
            let visited_flag = Self::visited_flag_variable(field);
            parse_quote! {
                let mut #visited_flag = false;
            }
        })
    }

    // Generates code that, given variable `typ`, type-checks given field
    fn generate_type_check(&self, field: &Field) -> Option<syn::Block> {
        (!field.skip).then(|| {
            let macro_internal = self.0.struct_attrs().macro_internal_path();
            let (frame_lifetime, metadata_lifetime) = self.0.constraint_lifetimes();
            let visited_flag = Self::visited_flag_variable(field);
            let typ = field.deserialize_target();
            let cql_name_literal = field.cql_name_literal();
            let decrement_if_required: Option<syn::Stmt> = (!self.0.attrs.default_when_missing && field
                .is_required())
                .then(|| parse_quote! {remaining_required_cql_fields -= 1;});

            parse_quote! {
                {
                    if !#visited_flag {
                        <#typ as #macro_internal::DeserializeValue<#frame_lifetime, #metadata_lifetime>>::type_check(cql_field_typ)
                            .map_err(|err| #macro_internal::mk_value_typck_err::<Self>(
                                typ,
                                #macro_internal::DeserUdtTypeCheckErrorKind::FieldTypeCheckFailed {
                                    field_name: <_ as ::std::clone::Clone>::clone(cql_field_name).into_owned(),
                                    err,
                                }
                            ))?;
                        #visited_flag = true;
                        #decrement_if_required
                    } else {
                        return ::std::result::Result::Err(
                            #macro_internal::mk_value_typck_err::<Self>(
                                typ,
                                #macro_internal::DeserUdtTypeCheckErrorKind::DuplicatedField {
                                    field_name: <_ as ::std::borrow::ToOwned>::to_owned(#cql_name_literal),
                                }
                            )
                        )
                    }
                }
            }
        })
    }

    // Generates code that appends the field name if it is missing.
    // The generated code is used to construct a nice error message.
    fn generate_append_name(field: &Field) -> Option<syn::Block> {
        field.is_required().then(|| {
            let visited_flag = Self::visited_flag_variable(field);
            let cql_name_literal = field.cql_name_literal();
            parse_quote!(
                {
                    if !#visited_flag {
                        missing_fields.push(#cql_name_literal);
                    }
                }
            )
        })
    }

    // Generates the type_check method for when ensure_order == false.
    fn generate(&self) -> syn::ImplItemFn {
        // The generated method will:
        // - Check that every required field appears on the list exactly once, in any order
        // - Every type on the list is correct

        let macro_internal = &self.0.struct_attrs().macro_internal_path();
        let forbid_excess_udt_fields = self.0.attrs.forbid_excess_udt_fields;
        let rust_fields = self.0.fields();
        let visited_field_declarations = rust_fields
            .iter()
            .flat_map(Self::generate_visited_flag_decl);
        let type_check_blocks = rust_fields.iter().flat_map(|f| self.generate_type_check(f));
        let append_name_blocks = rust_fields.iter().flat_map(Self::generate_append_name);
        let rust_nonskipped_field_names = rust_fields
            .iter()
            .filter(|f| !f.skip)
            .map(|f| f.cql_name_literal());
        let required_cql_field_count = if self.0.attrs.default_when_missing {
            0
        } else {
            rust_fields.iter().filter(|f| f.is_required()).count()
        };

        let required_cql_field_count_lit =
            syn::LitInt::new(&required_cql_field_count.to_string(), Span::call_site());
        let extract_cql_fields_expr = self.0.generate_extract_fields_from_type(parse_quote!(typ));

        // If UDT contains a field with an unknown name, an error is raised iff
        // `forbid_excess_udt_fields` attribute is specified.
        let excess_udt_field_action: syn::Expr = if forbid_excess_udt_fields {
            parse_quote! {
                return ::std::result::Result::Err(
                    #macro_internal::mk_value_typck_err::<Self>(
                        typ,
                        #macro_internal::DeserUdtTypeCheckErrorKind::ExcessFieldInUdt {
                            db_field_name: <_ as ::std::borrow::ToOwned>::to_owned(unknown),
                        }
                    )
                )
            }
        } else {
            parse_quote! {
                // We ignore excess UDT fields, as this facilitates the process of adding new fields
                // to a UDT in running production cluster & clients.
                ()
            }
        };

        parse_quote! {
            fn type_check(
                typ: &#macro_internal::ColumnType,
            ) -> ::std::result::Result<(), #macro_internal::TypeCheckError> {
                // Extract information about the field types from the UDT
                // type definition.
                let cql_fields = #extract_cql_fields_expr;

                // Counts down how many required fields are remaining
                let mut remaining_required_cql_fields: ::std::primitive::usize = #required_cql_field_count_lit;

                // For each required field, generate a "visited" boolean flag
                #(#visited_field_declarations)*

                for (cql_field_name, cql_field_typ) in cql_fields {
                    // Pattern match on the name and verify that the type is correct.
                    match ::std::ops::Deref::deref(cql_field_name) {
                        #(#rust_nonskipped_field_names => #type_check_blocks,)*
                        unknown => #excess_udt_field_action,
                    }
                }

                if remaining_required_cql_fields > 0 {
                    // If there are some missing required fields, generate an error
                    // which contains missing field names
                    let mut missing_fields = ::std::vec::Vec::<&'static str>::with_capacity(remaining_required_cql_fields);
                    #(#append_name_blocks)*
                    return ::std::result::Result::Err(
                        #macro_internal::mk_value_typck_err::<Self>(
                            typ,
                            #macro_internal::DeserUdtTypeCheckErrorKind::ValuesMissingForUdtFields {
                                field_names: missing_fields,
                            }
                        )
                    )
                }

                ::std::result::Result::Ok(())
            }
        }
    }
}

struct DeserializeUnorderedGenerator<'sd>(&'sd StructDesc);

impl DeserializeUnorderedGenerator<'_> {
    /// An identifier for a variable that is meant to store the parsed variable
    /// before being ultimately moved to the struct on deserialize.
    fn deserialize_field_variable(field: &Field) -> syn::Ident {
        quote::format_ident!("f_{}", field.ident.as_ref().unwrap().unraw())
    }

    /// Generates an expression which produces a value ready to be put into a field
    /// of the target structure.
    fn generate_finalize_field(&self, field: &Field) -> syn::Expr {
        if field.skip {
            // Skipped fields are initialized with Default::default()
            return parse_quote!(::std::default::Default::default());
        }

        let deserialize_field = Self::deserialize_field_variable(field);
        if self.0.attrs.default_when_missing || field.default_when_missing {
            // Generate Default::default if the field was missing
            parse_quote! {
                #deserialize_field.unwrap_or_default()
            }
        } else {
            let cql_name_literal = field.cql_name_literal();
            parse_quote! {
                #deserialize_field.unwrap_or_else(|| ::std::panic!(
                    "field {} missing in UDT - type check should have prevented this!",
                    #cql_name_literal
                ))
            }
        }
    }

    /// Generates code that performs deserialization when the raw field
    /// is being processed.
    fn generate_deserialization(&self, field: &Field) -> Option<syn::Expr> {
        (!field.skip).then(|| {
            let macro_internal = self.0.struct_attrs().macro_internal_path();
            let (frame_lifetime, metadata_lifetime) = self.0.constraint_lifetimes();
            let deserialize_field = Self::deserialize_field_variable(field);
            let cql_name_literal = field.cql_name_literal();
            let deserializer = field.deserialize_target();

            let do_deserialize: syn::Expr = parse_quote! {
                <#deserializer as #macro_internal::DeserializeValue<#frame_lifetime, #metadata_lifetime>>::deserialize(cql_field_typ, value)
                .map_err(|err| #macro_internal::mk_value_deser_err::<Self>(
                    typ,
                    #macro_internal::UdtDeserializationErrorKind::FieldDeserializationFailed {
                        field_name: <_ as ::std::borrow::ToOwned>::to_owned(#cql_name_literal),
                        err,
                    }
                ))?
            };

            let deserialize_action: syn::Expr = if field.default_when_null {
                parse_quote! {
                    if value.is_some() {
                        #do_deserialize
                    } else {
                        ::std::default::Default::default()
                    }
                }
            } else {
                do_deserialize
            };

            parse_quote! {
                {
                    assert!(
                        #deserialize_field.is_none(),
                        "duplicated field {} - type check should have prevented this!",
                        stringify!(#deserialize_field)
                    );

                    // The value can be either
                    // - None - missing from the serialized representation
                    // - Some(None) - present in the serialized representation but null
                    // For now, we treat both cases as "null".
                    let value = value.flatten();

                    #deserialize_field = ::std::option::Option::Some(
                        #deserialize_action
                    );
                }
            }
        })
    }

    // Generate a declaration of a variable that temporarily keeps
    // the deserialized value
    fn generate_deserialize_field_decl(field: &Field) -> Option<syn::Stmt> {
        (!field.skip).then(|| {
            let deserialize_field = Self::deserialize_field_variable(field);
            parse_quote! {
                let mut #deserialize_field = ::std::option::Option::None;
            }
        })
    }

    fn generate(&self) -> syn::ImplItemFn {
        let macro_internal = self.0.struct_attrs().macro_internal_path();
        let (frame_lifetime, metadata_lifetime) = self.0.constraint_lifetimes();
        let fields = self.0.fields();

        let deserialize_field_decls = fields.iter().map(Self::generate_deserialize_field_decl);
        let deserialize_blocks = fields.iter().flat_map(|f| self.generate_deserialization(f));
        let rust_field_idents = fields.iter().map(|f| f.ident.as_ref().unwrap());
        let rust_nonskipped_field_names = fields
            .iter()
            .filter(|f| !f.skip)
            .map(|f| f.cql_name_literal());

        let field_finalizers = fields.iter().map(|f| self.generate_finalize_field(f));

        let iterator_type: syn::Type = parse_quote! {
            #macro_internal::UdtIterator<#frame_lifetime, #metadata_lifetime>
        };

        // TODO: Allow collecting unrecognized fields into some special field

        parse_quote! {
            fn deserialize(
                typ: &#metadata_lifetime #macro_internal::ColumnType<#metadata_lifetime>,
                v: ::std::option::Option<#macro_internal::FrameSlice<#frame_lifetime>>,
            ) -> ::std::result::Result<Self, #macro_internal::DeserializationError> {
                // Create an iterator over the fields of the UDT.
                let cql_field_iter = <#iterator_type as #macro_internal::DeserializeValue<#frame_lifetime, #metadata_lifetime>>::deserialize(typ, v)
                    .map_err(#macro_internal::value_deser_error_replace_rust_name::<Self>)?;

                // Generate fields that will serve as temporary storage
                // for the fields' values. Those are of type Option<FieldType>.
                #(#deserialize_field_decls)*

                for item in cql_field_iter {
                    let ((cql_field_name, cql_field_typ), value_res) = item;
                    let value = value_res.map_err(|err| #macro_internal::mk_value_deser_err::<Self>(
                        typ,
                        #macro_internal::UdtDeserializationErrorKind::FieldDeserializationFailed {
                            field_name: ::std::clone::Clone::clone(cql_field_name).into_owned(),
                            err,
                        }
                    ))?;
                    // Pattern match on the field name and deserialize.
                    match ::std::ops::Deref::deref(cql_field_name) {
                        #(#rust_nonskipped_field_names => #deserialize_blocks,)*
                        unknown => {
                            // Assuming we type checked sucessfully, this must be an excess field.
                            // Let's skip it.
                        },
                    }
                }

                // Create the final struct. The finalizer expressions convert
                // the temporary storage fields to the final field values.
                // For example, if a field is missing but marked as
                // `default_when_null` it will create a default value, otherwise
                // it will report an error.
                ::std::result::Result::Ok(Self {
                    #(#rust_field_idents: #field_finalizers,)*
                })
            }
        }
    }
}
