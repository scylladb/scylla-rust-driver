use darling::{FromAttributes, FromField};
use proc_macro::TokenStream;
use proc_macro2::Span;
use syn::{ext::IdentExt, parse_quote};

use super::{DeserializeCommonFieldAttrs, DeserializeCommonStructAttrs};

#[derive(FromAttributes)]
#[darling(attributes(scylla))]
struct StructAttrs {
    #[darling(rename = "crate")]
    crate_path: Option<syn::Path>,

    // If true, then the type checking code will require the order of the fields
    // to be the same in both the Rust struct and the UDT. This allows the
    // deserialization to be slightly faster because looking struct fields up
    // by name can be avoided, though it is less convenient.
    #[darling(default)]
    enforce_order: bool,
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

    // If set, then deserializes from the UDT field with this particular name
    // instead of the Rust field name.
    #[darling(default)]
    rename: Option<String>,

    ident: Option<syn::Ident>,
    ty: syn::Type,
}

impl DeserializeCommonFieldAttrs for Field {
    fn needs_default(&self) -> bool {
        self.skip
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

    let items = [
        s.generate_type_check_method().into(),
        s.generate_deserialize_method().into(),
    ];

    Ok(s.generate_impl(implemented_trait, items))
}
impl Field {
    // Returns whether this field is mandatory for deserialization.
    fn is_required(&self) -> bool {
        !self.skip
    }

    // A Rust literal representing the name of this field
    fn cql_name_literal(&self) -> syn::LitStr {
        let field_name = match self.rename.as_ref() {
            Some(rename) => rename.to_owned(),
            None => self.ident.as_ref().unwrap().unraw().to_string(),
        };
        syn::LitStr::new(&field_name, Span::call_site())
    }
}

type StructDesc = super::StructDescForDeserialize<StructAttrs, Field>;

impl StructDesc {
    /// Generates an expression which extracts the UDT fields or returns an error.
    fn generate_extract_fields_from_type(&self, typ_expr: syn::Expr) -> syn::Expr {
        let macro_internal = &self.struct_attrs().macro_internal_path();
        parse_quote!(
            match #typ_expr {
                #macro_internal::ColumnType::UserDefinedType { field_types, .. } => field_types,
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
        if self.attrs.enforce_order {
            TypeCheckAssumeOrderGenerator(self).generate()
        } else {
            TypeCheckUnorderedGenerator(self).generate()
        }
    }

    fn generate_deserialize_method(&self) -> syn::ImplItemFn {
        if self.attrs.enforce_order {
            DeserializeAssumeOrderGenerator(self).generate()
        } else {
            DeserializeUnorderedGenerator(self).generate()
        }
    }
}

struct TypeCheckAssumeOrderGenerator<'sd>(&'sd StructDesc);

impl<'sd> TypeCheckAssumeOrderGenerator<'sd> {
    // Generates name and type validation for given Rust struct's field.
    fn generate_field_validation(&self, rust_field_idx: usize, field: &Field) -> syn::Expr {
        let macro_internal = self.0.struct_attrs().macro_internal_path();
        let constraint_lifetime = self.0.constraint_lifetime();
        let rust_field_name = field.cql_name_literal();
        let rust_field_typ = field.deserialize_target();

        // Action performed in case of field name mismatch.
        let name_mismatch: syn::Expr = parse_quote! {
            {
                // Error - required value for field not present among the CQL fields.
                return ::std::result::Result::Err(
                    #macro_internal::mk_value_typck_err::<Self>(
                        typ,
                        #macro_internal::DeserUdtTypeCheckErrorKind::FieldNameMismatch {
                            position: #rust_field_idx,
                            rust_field_name: <_ as ::std::borrow::ToOwned>::to_owned(#rust_field_name),
                            db_field_name: <_ as ::std::borrow::ToOwned>::to_owned(cql_field_name),
                        }
                    )
                );
            }
        };

        let name_verification: syn::Expr = parse_quote! {
            if #rust_field_name != cql_field_name {
                // The read UDT field is not the one expected by the Rust struct.
                #name_mismatch
            }
        };

        parse_quote! {
            'field: {
                let next_cql_field = match cql_field_iter.next() {
                    ::std::option::Option::Some(cql_field) => cql_field,
                    ::std::option::Option::None => return Err(too_few_fields()),
                };
                let (cql_field_name, cql_field_typ) = next_cql_field;

                'verifications: {
                    #name_verification

                    // Verify the type
                    <#rust_field_typ as #macro_internal::DeserializeValue<#constraint_lifetime>>::type_check(cql_field_typ)
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
                        required_fields: vec![
                            #(stringify!(#required_fields_names),)*
                        ],
                        present_fields: fields.iter().map(|(name, _typ)| name.clone()).collect(),
                    }
                );

                // Verify that the field count is correct
                if fields.len() < #required_field_count_lit {
                    return ::std::result::Result::Err(too_few_fields());
                }

                let mut cql_field_iter = fields.iter();
                #(
                    #field_validations
                )*

                // All is good!
                ::std::result::Result::Ok(())
            }
        }
    }
}

struct DeserializeAssumeOrderGenerator<'sd>(&'sd StructDesc);

impl<'sd> DeserializeAssumeOrderGenerator<'sd> {
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
        let constraint_lifetime = self.0.constraint_lifetime();

        let deserialize: syn::Expr = parse_quote! {
            <#deserializer as #macro_internal::DeserializeValue<#constraint_lifetime>>::deserialize(cql_field_typ, value)
                .map_err(|err| #macro_internal::mk_value_deser_err::<Self>(
                    typ,
                    #macro_internal::UdtDeserializationErrorKind::FieldDeserializationFailed {
                        field_name: #cql_name_literal.to_owned(),
                        err,
                    }
                ))?
        };

        // Action performed in case of field name mismatch.
        let name_mismatch: syn::Expr = parse_quote! {
            panic!(
                "type check should have prevented this scenario - field name mismatch! Rust field name {}, CQL field name {}",
                #cql_name_literal,
                cql_field_name
            )
        };

        let name_check_and_deserialize: syn::Expr = parse_quote! {
            if #cql_name_literal == cql_field_name {
                #deserialize
            } else {
                #name_mismatch
            }
        };

        parse_quote! {
            {
                let next_cql_field = cql_field_iter.next()
                    .map(|(specs, value_res)| value_res.map(|value| (specs, value)))
                    // Type check has ensured that there are enough CQL UDT fields.
                    .expect("Too few CQL UDT fields - type check should have prevented this scenario!")
                    // Propagate deserialization errors.
                    .map_err(|err| #macro_internal::mk_value_deser_err::<Self>(
                        typ,
                        #macro_internal::UdtDeserializationErrorKind::FieldDeserializationFailed {
                            field_name: #cql_name_literal.to_owned(),
                            err,
                        }
                    ))?;


                let ((cql_field_name, cql_field_typ), value) = next_cql_field;

                // The value can be either
                // - None - missing from the serialized representation
                // - Some(None) - present in the serialized representation but null
                // For now, we treat both cases as "null".
                let value = value.flatten();

                #name_check_and_deserialize
            }
        }
    }

    fn generate(&self) -> syn::ImplItemFn {
        // We can assume that type_check was called.

        let macro_internal = self.0.struct_attrs().macro_internal_path();
        let constraint_lifetime = self.0.constraint_lifetime();
        let fields = self.0.fields();

        let field_idents = fields.iter().map(|f| f.ident.as_ref().unwrap());
        let field_finalizers = fields.iter().map(|f| self.generate_finalize_field(f));

        #[allow(unused_mut)]
        let mut iterator_type: syn::Type =
            parse_quote!(#macro_internal::UdtIterator<#constraint_lifetime>);

        parse_quote! {
            fn deserialize(
                typ: &#constraint_lifetime #macro_internal::ColumnType,
                v: ::std::option::Option<#macro_internal::FrameSlice<#constraint_lifetime>>,
            ) -> ::std::result::Result<Self, #macro_internal::DeserializationError> {
                // Create an iterator over the fields of the UDT.
                let mut cql_field_iter = <#iterator_type as #macro_internal::DeserializeValue<#constraint_lifetime>>::deserialize(typ, v)
                    .map_err(#macro_internal::value_deser_error_replace_rust_name::<Self>)?;

                ::std::result::Result::Ok(Self {
                    #(#field_idents: #field_finalizers,)*
                })
            }
        }
    }
}

struct TypeCheckUnorderedGenerator<'sd>(&'sd StructDesc);

impl<'sd> TypeCheckUnorderedGenerator<'sd> {
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
            let constraint_lifetime = self.0.constraint_lifetime();
            let visited_flag = Self::visited_flag_variable(field);
            let typ = field.deserialize_target();
            let cql_name_literal = field.cql_name_literal();
            let decrement_if_required: Option<syn::Stmt> = field
                .is_required()
                .then(|| parse_quote! {remaining_required_cql_fields -= 1;});

            parse_quote! {
                {
                    if !#visited_flag {
                        <#typ as #macro_internal::DeserializeValue<#constraint_lifetime>>::type_check(cql_field_typ)
                            .map_err(|err| #macro_internal::mk_value_typck_err::<Self>(
                                typ,
                                #macro_internal::DeserUdtTypeCheckErrorKind::FieldTypeCheckFailed {
                                    field_name: <_ as ::std::clone::Clone>::clone(cql_field_name),
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
        let required_cql_field_count = rust_fields.iter().filter(|f| f.is_required()).count();
        let required_cql_field_count_lit =
            syn::LitInt::new(&required_cql_field_count.to_string(), Span::call_site());
        let extract_cql_fields_expr = self.0.generate_extract_fields_from_type(parse_quote!(typ));

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
                    match cql_field_name.as_str() {
                        #(#rust_nonskipped_field_names => #type_check_blocks,)*
                        _unknown => {
                            // We ignore excess UDT fields, as this facilitates the process of adding new fields
                            // to a UDT in running production cluster & clients.
                        }
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

impl<'sd> DeserializeUnorderedGenerator<'sd> {
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
        let cql_name_literal = field.cql_name_literal();
        parse_quote!(#deserialize_field.unwrap_or_else(|| panic!(
            "field {} missing in UDT - type check should have prevented this!",
            #cql_name_literal
        )))
    }

    /// Generates code that performs deserialization when the raw field
    /// is being processed.
    fn generate_deserialization(&self, field: &Field) -> Option<syn::Expr> {
        (!field.skip).then(|| {
            let macro_internal = self.0.struct_attrs().macro_internal_path();
            let constraint_lifetime = self.0.constraint_lifetime();
            let deserialize_field = Self::deserialize_field_variable(field);
            let cql_name_literal = field.cql_name_literal();
            let deserializer = field.deserialize_target();

            let do_deserialize: syn::Expr = parse_quote! {
                <#deserializer as #macro_internal::DeserializeValue<#constraint_lifetime>>::deserialize(cql_field_typ, value)
                .map_err(|err| #macro_internal::mk_value_deser_err::<Self>(
                    typ,
                    #macro_internal::UdtDeserializationErrorKind::FieldDeserializationFailed {
                        field_name: #cql_name_literal.to_owned(),
                        err,
                    }
                ))?
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
                        #do_deserialize
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
        let constraint_lifetime = self.0.constraint_lifetime();
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
            #macro_internal::UdtIterator<#constraint_lifetime>
        };

        // TODO: Allow collecting unrecognized fields into some special field

        parse_quote! {
            fn deserialize(
                typ: &#constraint_lifetime #macro_internal::ColumnType,
                v: ::std::option::Option<#macro_internal::FrameSlice<#constraint_lifetime>>,
            ) -> ::std::result::Result<Self, #macro_internal::DeserializationError> {
                // Create an iterator over the fields of the UDT.
                let cql_field_iter = <#iterator_type as #macro_internal::DeserializeValue<#constraint_lifetime>>::deserialize(typ, v)
                    .map_err(#macro_internal::value_deser_error_replace_rust_name::<Self>)?;

                // Generate fields that will serve as temporary storage
                // for the fields' values. Those are of type Option<FieldType>.
                #(#deserialize_field_decls)*

                for item in cql_field_iter {
                    let ((cql_field_name, cql_field_typ), value_res) = item;
                    let value = value_res.map_err(|err| #macro_internal::mk_value_deser_err::<Self>(
                        typ,
                        #macro_internal::UdtDeserializationErrorKind::FieldDeserializationFailed {
                            field_name: ::std::clone::Clone::clone(cql_field_name),
                            err,
                        }
                    ))?;
                    // Pattern match on the field name and deserialize.
                    match cql_field_name.as_str() {
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
