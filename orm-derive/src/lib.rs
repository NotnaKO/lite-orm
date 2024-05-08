#![forbid(unsafe_code)]

use proc_macro::TokenStream;

use quote::quote;
use syn::{parse_macro_input, parse_quote, Attribute, Data, DeriveInput, Generics};

#[proc_macro_derive(Object, attributes(table_name, column_name))]
pub fn derive_object(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let type_name = input.ident;
    let table_name = input
        .attrs
        .iter()
        .find_map(get_table_name)
        .unwrap_or(type_name.to_string());

    let generics = add_train_bounds(input.generics);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let fields = if let Data::Struct(data) = input.data {
        data.fields
    } else {
        panic!("Object derive only works on structs");
    };

    let mut column_names = Vec::with_capacity(fields.len());
    let mut types = Vec::with_capacity(fields.len());
    let mut attrs = Vec::with_capacity(fields.len());
    for field in fields {
        let field_name = field.ident.clone().expect("Unnamed field not supported");
        let column_name = field
            .attrs
            .iter()
            .find_map(get_column_name)
            .unwrap_or_else(|| field_name.to_string());
        column_names.push(column_name);
        types.push(field.ty);
        attrs.push(field_name);
    }

    let row_constructors = attrs.iter().enumerate().map(|(i, field_name)| {
        quote! {
            #field_name: row[#i].convert()
        }
    });

    let attr_names = attrs.iter().map(|attr| attr.to_string());

    let expanded = quote! {
        impl #impl_generics Object for #type_name #ty_generics #where_clause {
            fn schema() -> &'static orm::object::Schema {
                &orm::object::Schema {
                    table_name: #table_name,
                    type_name: stringify!(#type_name),
                    attrs: &[#(#attr_names),*],
                    columns: &[#((#column_names, <#types as orm::data::DetectDataType>::TYPE)),*],
                }
            }

            fn from_row(row: orm::storage::Row<'_>) -> Self {
                Self {
                    #(#row_constructors),*
                }
            }

            fn to_row(&self) -> orm::storage::Row<'_> {
                use orm::data::ValueConvert;
                vec![
                    #(self.#attrs.to_value()),*
                ]
            }
        }
    };

    expanded.into()
}

fn add_train_bounds(mut generics: Generics) -> Generics {
    for param in &mut generics.params {
        if let syn::GenericParam::Type(ref mut type_param) = *param {
            type_param.bounds.push(parse_quote!(ValueConvert));
            type_param.bounds.push(parse_quote!(DetectDataType));
        }
    }
    generics
}

fn get_table_name(attr: &Attribute) -> Option<String> {
    if attr.path().is_ident("table_name") {
        parse_name(attr).into()
    } else {
        None
    }
}

fn get_column_name(attr: &Attribute) -> Option<String> {
    if attr.path().is_ident("column_name") {
        parse_name(attr).into()
    } else {
        None
    }
}

fn parse_name(attr: &Attribute) -> String {
    let a: syn::Lit = attr.parse_args().unwrap();

    match a {
        syn::Lit::Str(s) => s.value(),
        _ => panic!("Expected string literal"),
    }
}
