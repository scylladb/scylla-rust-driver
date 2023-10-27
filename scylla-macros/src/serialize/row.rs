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

    let gen: Box<dyn Generator> = match ctx.attributes.flavor {
        Some(Flavor::MatchByName) | None => Box::new(ColumnSortingGenerator { ctx: &ctx }),
        Some(Flavor::EnforceOrder) => Box::new(ColumnOrderedGenerator { ctx: &ctx }),
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
        let rust_field_names = rust_field_idents
            .iter()
            .map(|i| i.as_ref().unwrap().to_string())
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
                match ::std::string::String::as_str(&spec.name) {
                    #(
                        #udt_field_names => {
                            let sub_writer = #crate_path::RowWriter::make_cell_writer(writer);
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
                            if !#visited_flag_names {
                                #visited_flag_names = true;
                                remaining_count -= 1;
                            }
                        }
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
            let rust_field_ident = field.ident.as_ref().unwrap();
            let rust_field_name = rust_field_ident.to_string();
            let typ = &field.ty;
            statements.push(parse_quote! {
                match column_iter.next() {
                    Some(spec) => {
                        if spec.name == #rust_field_name {
                            let cell_writer = #crate_path::RowWriter::make_cell_writer(writer);
                            match <#typ as #crate_path::SerializeCql>::serialize(&self.#rust_field_ident, &spec.typ, cell_writer) {
                                Ok(_proof) => {},
                                Err(err) => {
                                    return ::std::result::Result::Err(mk_ser_err(
                                        #crate_path::BuiltinRowSerializationErrorKind::ColumnSerializationFailed {
                                            name: <_ as ::std::clone::Clone>::clone(&spec.name),
                                            err,
                                        }
                                    ));
                                }
                            }
                        } else {
                            return ::std::result::Result::Err(mk_typck_err(
                                #crate_path::BuiltinRowTypeCheckErrorKind::ColumnNameMismatch {
                                    rust_column_name: <_ as ::std::string::ToString>::to_string(#rust_field_name),
                                    db_column_name: <_ as ::std::clone::Clone>::clone(&spec.name),
                                }
                            ));
                        }
                    }
                    None => {
                        return ::std::result::Result::Err(mk_typck_err(
                            #crate_path::BuiltinRowTypeCheckErrorKind::ColumnMissingForValue {
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
                    #crate_path::BuiltinRowTypeCheckErrorKind::MissingValueForColumn {
                        name: <_ as ::std::clone::Clone>::clone(&spec.name),
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
