use darling::FromAttributes;
use proc_macro::TokenStream;
use proc_macro2::Span;
use syn::parse_quote;

#[derive(FromAttributes)]
#[darling(attributes(scylla))]
struct Attributes {
    #[darling(rename = "crate")]
    crate_path: Option<syn::Path>,
}

impl Attributes {
    fn crate_path(&self) -> syn::Path {
        self.crate_path
            .as_ref()
            .map(|p| parse_quote!(#p::_macro_internal))
            .unwrap_or_else(|| parse_quote!(::scylla::_macro_internal))
    }
}

struct Context {
    attributes: Attributes,
    fields: Vec<syn::Field>,
}

pub fn derive_serialize_row(tokens_input: TokenStream) -> Result<syn::ItemImpl, syn::Error> {
    let input: syn::DeriveInput = syn::parse(tokens_input)?;
    let struct_name = input.ident.clone();
    let named_fields = crate::parser::parse_named_fields(&input, "SerializeRow")?;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let attributes = Attributes::from_attributes(&input.attrs)?;

    let crate_path = attributes.crate_path();
    let implemented_trait: syn::Path = parse_quote!(#crate_path::SerializeRow);

    let fields = named_fields.named.iter().cloned().collect();
    let ctx = Context { attributes, fields };
    let gen = ColumnSortingGenerator { ctx: &ctx };

    let preliminary_type_check_item = gen.generate_preliminary_type_check();
    let serialize_item = gen.generate_serialize();
    let is_empty_item = gen.generate_is_empty();

    let res = parse_quote! {
        impl<#impl_generics> #implemented_trait for #struct_name #ty_generics #where_clause {
            #preliminary_type_check_item
            #serialize_item
            #is_empty_item
        }
    };
    Ok(res)
}

impl Context {
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

// Generates an implementation of the trait which sorts the columns according
// to how they are defined in prepared statement metadata.
struct ColumnSortingGenerator<'a> {
    ctx: &'a Context,
}

impl<'a> ColumnSortingGenerator<'a> {
    fn generate_preliminary_type_check(&self) -> syn::TraitItemFn {
        // Need to:
        // - Check that all required columns are there and no more
        // - Check that the column types match

        let mut statements: Vec<syn::Stmt> = Vec::new();

        let crate_path = self.ctx.attributes.crate_path();

        let rust_field_names = self
            .ctx
            .fields
            .iter()
            .map(|f| f.ident.as_ref().unwrap().to_string())
            .collect::<Vec<_>>();
        let column_names = rust_field_names.clone(); // For now, it's the same
        let field_types = self.ctx.fields.iter().map(|f| &f.ty).collect::<Vec<_>>();
        let field_count = self.ctx.fields.len();

        statements.push(self.ctx.generate_mk_typck_err());

        // Generate a "visited" flag for each field
        let visited_flag_names = rust_field_names
            .iter()
            .map(|s| syn::Ident::new(&format!("visited_flag_{}", s), Span::call_site()))
            .collect::<Vec<_>>();
        statements.extend::<Vec<_>>(parse_quote! {
            #(let mut #visited_flag_names = false;)*
        });

        // Generate a variable that counts down visited fields.
        statements.push(parse_quote! {
            let mut remaining_count = #field_count;
        });

        // Generate a loop over the fields and a `match` block to match on
        // the field name.
        statements.push(parse_quote! {
            for spec in ctx.columns() {
                match ::std::string::String::as_str(&spec.name) {
                    #(
                        #column_names => {
                            match <#field_types as #crate_path::SerializeCql>::preliminary_type_check(&spec.typ) {
                                ::std::result::Result::Ok(()) => {}
                                ::std::result::Result::Err(err) => {
                                    return ::std::result::Result::Err(mk_typck_err(
                                        #crate_path::BuiltinRowTypeCheckErrorKind::ColumnTypeCheckFailed {
                                            name: <_ as ::std::clone::Clone>::clone(&spec.name),
                                            err,
                                        }
                                    ));
                                }
                            };
                            if !#visited_flag_names {
                                #visited_flag_names = true;
                                remaining_count -= 1;
                            }
                        },
                    )*
                    _ => return ::std::result::Result::Err(mk_typck_err(
                        #crate_path::BuiltinRowTypeCheckErrorKind::MissingValueForColumn {
                            name: <_ as ::std::clone::Clone>::clone(&&spec.name),
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
                            #crate_path::BuiltinRowTypeCheckErrorKind::ColumnMissingForValue {
                                name: <_ as ::std::string::ToString>::to_string(#rust_field_names),
                            }
                        ));
                    }
                )*
                ::std::unreachable!()
            }
        });

        // Concatenate generated code and return
        parse_quote! {
            fn preliminary_type_check(
                ctx: &#crate_path::RowSerializationContext,
            ) -> ::std::result::Result<(), #crate_path::SerializationError> {
                #(#statements)*
                ::std::result::Result::Ok(())
            }
        }
    }

    fn generate_serialize(&self) -> syn::TraitItemFn {
        // Implementation can assume that preliminary_type_check was called
        // (although not in an unsafe way).
        // Need to write the fields as they appear in the type definition.
        let mut statements: Vec<syn::Stmt> = Vec::new();

        let crate_path = self.ctx.attributes.crate_path();

        let rust_field_idents = self
            .ctx
            .fields
            .iter()
            .map(|f| f.ident.clone())
            .collect::<Vec<_>>();
        let rust_field_names = rust_field_idents
            .iter()
            .map(|i| i.as_ref().unwrap().to_string())
            .collect::<Vec<_>>();
        let udt_field_names = rust_field_names.clone(); // For now, it's the same
        let field_types = self.ctx.fields.iter().map(|f| &f.ty).collect::<Vec<_>>();

        // Declare a helper lambda for creating errors
        statements.push(self.ctx.generate_mk_ser_err());

        // Generate a loop over the fields and a `match` block to match on
        // the field name.
        statements.push(parse_quote! {
            for spec in ctx.columns() {
                match ::std::string::String::as_str(&spec.name) {
                    #(
                        #udt_field_names => {
                            let sub_writer = <_ as #crate_path::RowWriter>::make_cell_writer(writer);
                            match <#field_types as #crate_path::SerializeCql>::serialize(&self.#rust_field_idents, &spec.typ, sub_writer) {
                                ::std::result::Result::Ok(_proof) => {}
                                ::std::result::Result::Err(err) => {
                                    return ::std::result::Result::Err(mk_ser_err(
                                        #crate_path::BuiltinRowSerializationErrorKind::ColumnSerializationFailed {
                                            name: <_ as ::std::clone::Clone>::clone(&spec.name),
                                            err,
                                        }
                                    ));
                                }
                            }
                        }
                    )*
                    _ => {}
                }
            }
        });

        parse_quote! {
            fn serialize<W: #crate_path::RowWriter>(
                &self,
                ctx: &#crate_path::RowSerializationContext,
                writer: &mut W,
            ) -> ::std::result::Result<(), #crate_path::SerializationError> {
                #(#statements)*
                ::std::result::Result::Ok(())
            }
        }
    }

    fn generate_is_empty(&self) -> syn::TraitItemFn {
        let is_empty_struct = self.ctx.fields.is_empty();

        parse_quote! {
            fn is_empty(&self) -> bool {
                #is_empty_struct
            }
        }
    }
}
