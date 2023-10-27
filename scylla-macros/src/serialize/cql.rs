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

    flavor: Option<Flavor>,
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

pub fn derive_serialize_cql(tokens_input: TokenStream) -> Result<syn::ItemImpl, syn::Error> {
    let input: syn::DeriveInput = syn::parse(tokens_input)?;
    let struct_name = input.ident.clone();
    let named_fields = crate::parser::parse_named_fields(&input, "SerializeCql")?;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let attributes = Attributes::from_attributes(&input.attrs)?;

    let crate_path = attributes.crate_path();
    let implemented_trait: syn::Path = parse_quote!(#crate_path::SerializeCql);

    let fields = named_fields.named.iter().cloned().collect();
    let ctx = Context { attributes, fields };

    let gen: Box<dyn Generator> = match ctx.attributes.flavor {
        Some(Flavor::MatchByName) | None => Box::new(FieldSortingGenerator { ctx: &ctx }),
        Some(Flavor::EnforceOrder) => Box::new(FieldOrderedGenerator { ctx: &ctx }),
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
                        got: <_ as ::std::clone::Clone>::clone(typ),
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
                        got: <_ as ::std::clone::Clone>::clone(typ),
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
        let rust_field_names = rust_field_idents
            .iter()
            .map(|i| i.as_ref().unwrap().to_string())
            .collect::<Vec<_>>();
        let udt_field_names = rust_field_names.clone(); // For now, it's the same
        let field_types = self.ctx.fields.iter().map(|f| &f.ty).collect::<Vec<_>>();

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

        // Turn the cell writer into a value builder
        statements.push(parse_quote! {
            let mut builder = #crate_path::CellWriter::into_value_builder(writer);
        });

        // Generate a loop over the fields and a `match` block to match on
        // the field name.
        statements.push(parse_quote! {
            for (field_name, field_type) in field_types {
                match ::std::string::String::as_str(field_name) {
                    #(
                        #udt_field_names => {
                            let sub_builder = #crate_path::CellValueBuilder::make_sub_writer(&mut builder);
                            match <#field_types as #crate_path::SerializeCql>::serialize(&self.#rust_field_idents, field_type, sub_builder) {
                                ::std::result::Result::Ok(_proof) => {}
                                ::std::result::Result::Err(err) => {
                                    return ::std::result::Result::Err(mk_ser_err(
                                        #crate_path::UdtSerializationErrorKind::FieldSerializationFailed {
                                            field_name: <_ as ::std::clone::Clone>::clone(field_name),
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
                        #crate_path::UdtTypeCheckErrorKind::UnexpectedFieldInDestination {
                            field_name: <_ as ::std::clone::Clone>::clone(field_name),
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
                            #crate_path::UdtTypeCheckErrorKind::MissingField {
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
                            got: <_ as ::std::clone::Clone>::clone(typ),
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
            let rust_field_ident = field.ident.as_ref().unwrap();
            let rust_field_name = rust_field_ident.to_string();
            let typ = &field.ty;
            statements.push(parse_quote! {
                match field_iter.next() {
                    Some((field_name, typ)) => {
                        if field_name == #rust_field_name {
                            let sub_builder = #crate_path::CellValueBuilder::make_sub_writer(&mut builder);
                            match <#typ as #crate_path::SerializeCql>::serialize(&self.#rust_field_ident, typ, sub_builder) {
                                Ok(_proof) => {},
                                Err(err) => {
                                    return ::std::result::Result::Err(mk_ser_err(
                                        #crate_path::UdtSerializationErrorKind::FieldSerializationFailed {
                                            field_name: <_ as ::std::clone::Clone>::clone(field_name),
                                            err,
                                        }
                                    ));
                                }
                            }
                        } else {
                            return ::std::result::Result::Err(mk_typck_err(
                                #crate_path::UdtTypeCheckErrorKind::FieldNameMismatch {
                                    rust_field_name: <_ as ::std::string::ToString>::to_string(#rust_field_name),
                                    db_field_name: <_ as ::std::clone::Clone>::clone(field_name),
                                }
                            ));
                        }
                    }
                    None => {
                        return ::std::result::Result::Err(mk_typck_err(
                            #crate_path::UdtTypeCheckErrorKind::MissingField {
                                field_name: <_ as ::std::string::ToString>::to_string(#rust_field_name),
                            }
                        ));
                    }
                }
            });
        }

        // Check whether there are some fields remaining
        statements.push(parse_quote! {
            if let Some((field_name, typ)) = field_iter.next() {
                return ::std::result::Result::Err(mk_typck_err(
                    #crate_path::UdtTypeCheckErrorKind::UnexpectedFieldInDestination {
                        field_name: <_ as ::std::clone::Clone>::clone(field_name),
                    }
                ));
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
                            got: <_ as ::std::clone::Clone>::clone(typ),
                            kind: #crate_path::BuiltinTypeSerializationErrorKind::SizeOverflow,
                        }
                    ) as #crate_path::SerializationError)?;
                ::std::result::Result::Ok(proof)
            }
        }
    }
}
