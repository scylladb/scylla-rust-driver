use darling::{FromAttributes, FromField};
use proc_macro2::Span;
use quote::quote;
use syn::ext::IdentExt;
use syn::parse_quote;

use super::{DeserializeCommonFieldAttrs, DeserializeCommonStructAttrs};

#[derive(FromAttributes)]
#[darling(attributes(scylla))]
struct StructAttrs {
    #[darling(rename = "crate")]
    crate_path: Option<syn::Path>,

    // If true, then the type checking code will require the order of the fields
    // to be the same in both the Rust struct and the columns. This allows the
    // deserialization to be slightly faster because looking struct fields up
    // by name can be avoided, though it is less convenient.
    #[darling(default)]
    enforce_order: bool,

    // If true, then the type checking code won't verify the column names.
    // Columns will be matched to struct fields based solely on the order.
    //
    // This annotation only works if `enforce_order` is specified.
    #[darling(default)]
    no_field_name_verification: bool,
}

impl DeserializeCommonStructAttrs for StructAttrs {
    fn crate_path(&self) -> syn::Path {
        match &self.crate_path {
            Some(path) => parse_quote!(#path::_macro_internal),
            None => parse_quote!(scylla::_macro_internal),
        }
    }
}

#[derive(FromField)]
#[darling(attributes(scylla))]
struct Field {
    // If true, then the field is not parsed at all, but it is initialized
    // with Default::default() instead. All other attributes are ignored.
    #[darling(default)]
    skip: bool,

    // If set, then deserialization will look for the UDT field of given name
    // and deserialize to this Rust field, instead of just using the Rust
    // field name.
    #[darling(default)]
    rename: Option<String>,

    ident: Option<syn::Ident>,
    ty: syn::Type,
}

impl DeserializeCommonFieldAttrs for Field {
    fn needs_default(&self) -> bool {
        self.skip
    }

    fn deserialize_target(&self) -> syn::Type {
        self.ty.clone()
    }
}

// derive(DeserializeRow) for the new DeserializeRow trait
pub fn deserialize_row_derive(
    tokens_input: proc_macro::TokenStream,
) -> Result<syn::ItemImpl, syn::Error> {
    let input = syn::parse(tokens_input)?;

    let implemented_trait = parse_quote!(types::deserialize::row::DeserializeRow);
    let constraining_trait = parse_quote!(types::deserialize::value::DeserializeCql);
    let s = StructDesc::new(&input, "DeserializeRow", constraining_trait)?;

    let items = vec![
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
    fn generate_type_check_method(&self) -> syn::ImplItemMethod {
        if self.attrs.enforce_order {
            TypeCheckAssumeOrderGenerator(self).generate()
        } else {
            TypeCheckUnorderedGenerator(self).generate()
        }
    }

    fn generate_deserialize_method(&self) -> syn::ImplItemMethod {
        if self.attrs.enforce_order {
            DeserializeAssumeOrderGenerator(self).generate()
        } else {
            DeserializeUnorderedGenerator(self).generate()
        }
    }
}

struct TypeCheckAssumeOrderGenerator<'sd>(&'sd StructDesc);

impl<'sd> TypeCheckAssumeOrderGenerator<'sd> {
    fn generate_name_verification(
        &self,
        id: usize,
        field: &Field,
        column_spec: &syn::Ident,
    ) -> Option<syn::Expr> {
        if self.0.attrs.no_field_name_verification {
            return None;
        }

        let crate_path = &self.0.struct_attrs().crate_path;
        let field_name = field.cql_name_literal();

        Some(parse_quote!(
            if #column_spec.name != #field_name {
                return ::std::result::Result::Err(
                    #crate_path::frame::frame_errors::ParseError::BadIncomingData(
                        ::std::format!(
                            "Column #{} has wrong name, expected {} but got {}",
                            #id,
                            #column_spec.name,
                            #field_name
                        )
                    )
                );
            }
        ))
    }

    fn generate(&self) -> syn::ImplItemMethod {
        // TODO: Better error messages here, add more context to which field
        // failed to be parsed

        // The generated method will check that the order and the types
        // of the columns correspond fields' names/types.

        let crate_path = &self.0.struct_attrs().crate_path;
        let constraint_lifetime = self.0.constraint_lifetime();

        let required_fields: Vec<_> = self.0.fields().iter().filter(|f| f.is_required()).collect();
        let field_idents: Vec<_> = (0..required_fields.len())
            .map(|i| quote::format_ident!("f_{}", i))
            .collect();
        let name_verifications: Vec<_> = required_fields
            .iter()
            .zip(field_idents.iter())
            .enumerate()
            .map(|(id, (field, fidents))| self.generate_name_verification(id, field, fidents))
            .collect();

        let field_deserializers = required_fields.iter().map(|f| f.deserialize_target());

        let field_count = required_fields.len();

        parse_quote! {
            fn type_check(
                specs: &[#crate_path::frame::response::result::ColumnSpec],
            ) -> ::std::result::Result<(), #crate_path::frame::frame_errors::ParseError> {
                match specs {
                    [#(#field_idents),*] => {
                        #(
                            // Verify the name (unless `no_field_name_verification' is specified)
                            #name_verifications

                            // Verify the type
                            // TODO: Provide better context about which field this error is about
                            <#field_deserializers as #crate_path::types::deserialize::value::DeserializeCql<#constraint_lifetime>>::type_check(&#field_idents.typ)?;
                        )*
                        ::std::result::Result::Ok(())
                    },
                    _ => ::std::result::Result::Err(
                        #crate_path::frame::frame_errors::ParseError::BadIncomingData(
                            format!(
                                "Wrong number of columns, expected {} but got {}",
                                #field_count,
                                specs.len(),
                            )
                        ),
                    ),
                }
            }
        }
    }
}

struct DeserializeAssumeOrderGenerator<'sd>(&'sd StructDesc);

impl<'sd> DeserializeAssumeOrderGenerator<'sd> {
    fn generate_finalize_field(&self, field: &Field) -> syn::Expr {
        if field.skip {
            // Skipped fields are initialized with Default::default()
            return parse_quote!(::std::default::Default::default());
        }

        let crate_path = self.0.struct_attrs().crate_path();
        let deserializer = field.deserialize_target();
        let constraint_lifetime = self.0.constraint_lifetime();
        parse_quote!(
            {
                let col = row.next().ok_or_else(|| {
                    #crate_path::frame::frame_errors::ParseError::BadIncomingData(
                        "Not enough fields".to_owned(),
                    )
                })??;
                <#deserializer as #crate_path::types::deserialize::value::DeserializeCql<#constraint_lifetime>>::deserialize(&col.spec.typ, col.slice)?
            }
        )
    }

    fn generate(&self) -> syn::ImplItemMethod {
        let crate_path = &self.0.struct_attrs().crate_path;
        let constraint_lifetime = self.0.constraint_lifetime();

        let fields = self.0.fields();
        let field_idents = fields.iter().map(|f| f.ident.as_ref().unwrap());
        let field_finalizers = fields.iter().map(|f| self.generate_finalize_field(f));

        parse_quote! {
            fn deserialize(
                #[allow(unused_mut)]
                mut row: #crate_path::types::deserialize::row::ColumnIterator<#constraint_lifetime>,
            ) -> ::std::result::Result<Self, #crate_path::frame::frame_errors::ParseError> {
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
        if field.skip {
            return None;
        }

        let visited_flag = Self::visited_flag_variable(field);
        Some(parse_quote!(let mut #visited_flag = false;))
    }

    // Generates code that, given variable `typ`, type-checks given field
    fn generate_type_check(&self, field: &Field) -> Option<syn::Block> {
        if field.skip {
            return None;
        }

        let crate_path = self.0.struct_attrs().crate_path();
        let constraint_lifetime = self.0.constraint_lifetime();
        let visited_flag = Self::visited_flag_variable(field);
        let typ = field.deserialize_target();
        let cql_name_literal = field.cql_name_literal();
        let decrement_if_required = if field.is_required() {
            quote!(remaining_required_fields -= 1;)
        } else {
            quote!()
        };
        Some(parse_quote!(
            {
                if !#visited_flag {
                    <#typ as #crate_path::types::deserialize::value::DeserializeCql<#constraint_lifetime>>::type_check(&spec.typ)?;
                    #visited_flag = true;
                    #decrement_if_required
                } else {
                    return ::std::result::Result::Err(
                        #crate_path::frame::frame_errors::ParseError::BadIncomingData(
                            ::std::format!("Column {} occurs more than once in serialized data", #cql_name_literal),
                        ),
                    )
                }
            }
        ))
    }

    // Generates code that appends the flag name if it is missing.
    // The generated code is used to construct a nice error message.
    fn generate_append_name(field: &Field) -> Option<syn::Block> {
        if field.is_required() {
            let visited_flag = Self::visited_flag_variable(field);
            let cql_name_literal = field.cql_name_literal();
            Some(parse_quote!(
                {
                    if !#visited_flag {
                        missing_fields.push(#cql_name_literal);
                    }
                }
            ))
        } else {
            None
        }
    }

    fn generate(&self) -> syn::ImplItemMethod {
        let crate_path = self.0.struct_attrs().crate_path();

        let fields = self.0.fields();
        let visited_field_declarations = fields.iter().flat_map(Self::generate_visited_flag_decl);
        let type_check_blocks = fields.iter().flat_map(|f| self.generate_type_check(f));
        let append_name_blocks = fields.iter().flat_map(Self::generate_append_name);
        let field_names = fields
            .iter()
            .filter(|f| !f.skip)
            .map(|f| f.cql_name_literal());
        let field_count_lit = fields.iter().filter(|f| f.is_required()).count();

        parse_quote! {
            fn type_check(
                specs: &[#crate_path::frame::response::result::ColumnSpec],
            ) -> ::std::result::Result<(), #crate_path::frame::frame_errors::ParseError> {
                // Counts down how many required fields are remaining
                let mut remaining_required_fields: ::std::primitive::usize = #field_count_lit;

                // For each required field, generate a "visited" boolean flag
                #(#visited_field_declarations)*

                for spec in specs {
                    // Pattern match on the name and verify that the type is correct.
                    match spec.name.as_str() {
                        #(#field_names => #type_check_blocks,)*
                        unknown => {
                            return ::std::result::Result::Err(
                                #crate_path::frame::frame_errors::ParseError::BadIncomingData(
                                    ::std::format!("Unknown field: {}", unknown),
                                ),
                            )
                        }
                    }
                }

                if remaining_required_fields > 0 {
                    // If there are some missing required fields, generate an error
                    // which contains missing field names
                    let mut missing_fields = ::std::vec::Vec::<&'static str>::with_capacity(remaining_required_fields);
                    #(#append_name_blocks)*
                    return ::std::result::Result::Err(
                        #crate_path::frame::frame_errors::ParseError::BadIncomingData(
                            ::std::format!("Missing fields: {:?}", missing_fields),
                        ),
                    )
                }

                ::std::result::Result::Ok(())
            }
        }
    }
}

struct DeserializeUnorderedGenerator<'sd>(&'sd StructDesc);

impl<'sd> DeserializeUnorderedGenerator<'sd> {
    // An identifier for a variable that is meant to store the parsed variable
    // before being ultimately moved to the struct on deserialize
    fn deserialize_field_variable(field: &Field) -> syn::Ident {
        quote::format_ident!("f_{}", field.ident.as_ref().unwrap().unraw())
    }

    // Generates an expression which produces a value ready to be put into a field
    // of the target structure
    fn generate_finalize_field(&self, field: &Field) -> syn::Expr {
        if field.skip {
            // Skipped fields are initialized with Default::default()
            return parse_quote!(::std::default::Default::default());
        }

        let crate_path = self.0.struct_attrs().crate_path();
        let deserialize_field = Self::deserialize_field_variable(field);
        {
            let cql_name_literal = field.cql_name_literal();
            parse_quote!(#deserialize_field.ok_or_else(|| {
                #crate_path::frame::frame_errors::ParseError::BadIncomingData(
                    ::std::format!("Missing field: {}", #cql_name_literal)
                )
            })?)
        }
    }

    // Generated code that performs deserialization when the raw field
    // is being processed
    fn generate_deserialization(&self, field: &Field) -> Option<syn::Expr> {
        if field.skip {
            return None;
        }

        let crate_path = self.0.struct_attrs().crate_path();
        let constraint_lifetime = self.0.constraint_lifetime();
        let deserialize_field = Self::deserialize_field_variable(field);
        let cql_name_literal = field.cql_name_literal();
        let deserializer = field.deserialize_target();
        Some(parse_quote!(
            {
                if #deserialize_field.is_some() {
                    return ::std::result::Result::Err(
                        #crate_path::frame::frame_errors::ParseError::BadIncomingData(
                            ::std::format!("Field {} occurs more than once in serialized data", #cql_name_literal),
                        ),
                    );
                } else {
                    #deserialize_field = ::std::option::Option::Some(
                        <#deserializer as #crate_path::types::deserialize::value::DeserializeCql<#constraint_lifetime>>::deserialize(&col.spec.typ, col.slice)?
                    );
                }
            }
        ))
    }

    // Generate a declaration of a variable that temporarily keeps
    // the deserialized value
    fn generate_deserialize_field_decl(field: &Field) -> Option<syn::Stmt> {
        if field.skip {
            return None;
        }
        let deserialize_field = Self::deserialize_field_variable(field);
        Some(parse_quote!(let mut #deserialize_field = ::std::option::Option::None;))
    }

    fn generate(&self) -> syn::ImplItemMethod {
        let crate_path = self.0.struct_attrs().crate_path();
        let constraint_lifetime = self.0.constraint_lifetime();
        let fields = self.0.fields();

        let deserialize_field_decls = fields.iter().map(Self::generate_deserialize_field_decl);
        let deserialize_blocks = fields.iter().flat_map(|f| self.generate_deserialization(f));
        let field_idents = fields.iter().map(|f| f.ident.as_ref().unwrap());
        let field_names = fields
            .iter()
            .filter(|f| !f.skip)
            .map(|f| f.cql_name_literal());

        let field_finalizers = fields.iter().map(|f| self.generate_finalize_field(f));

        // TODO: Allow collecting unrecognized fields into some special field

        parse_quote! {
            fn deserialize(
                #[allow(unused_mut)]
                mut row: #crate_path::types::deserialize::row::ColumnIterator<#constraint_lifetime>,
            ) -> ::std::result::Result<Self, #crate_path::frame::frame_errors::ParseError> {

                // Generate fields that will serve as temporary storage
                // for the fields' values. Those are of type Option<FieldType>.
                #(#deserialize_field_decls)*

                for col in row {
                    let col = col?;
                    // Pattern match on the field name and deserialize.
                    match col.spec.name.as_str() {
                        #(#field_names => #deserialize_blocks,)*
                        unknown => return ::std::result::Result::Err(
                            #crate_path::frame::frame_errors::ParseError::BadIncomingData(
                                format!("Unknown column: {}", unknown),
                            )
                        )
                    }
                }

                // Create the final struct. The finalizer expressions convert
                // the temporary storage fields to the final field values.
                // For example, if a field is missing but marked as
                // `default_when_null` it will create a default value, otherwise
                // it will report an error.
                Ok(Self {
                    #(#field_idents: #field_finalizers,)*
                })
            }
        }
    }
}
