use darling::{ast::Data, util::SpannedValue, FromDeriveInput, FromField};
use proc_macro2::{Literal, TokenStream, TokenTree};
use quote::{quote, quote_spanned};
use syn::{parse_macro_input, parse_quote, DeriveInput, GenericParam, Generics, TypeParamBound};

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(metric))]
struct MetricsEmitterAttrs {
    ident: syn::Ident,
    generics: syn::Generics,
    #[darling(default)]
    prefix: String,
    data: Data<(), SpannedValue<MetricsEmitterFieldAttrs>>,
}

#[derive(Debug, FromField)]
#[darling(attributes(metric))]
struct MetricsEmitterFieldAttrs {
    ident: Option<syn::Ident>,
    #[darling(default)]
    name: String,
}

#[proc_macro_derive(MetricsEmitter, attributes(metric))]
pub fn metrics_emitter_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = match SpannedValue::<MetricsEmitterAttrs>::from_derive_input(&parse_macro_input!(
        input as DeriveInput
    )) {
        Ok(input) => input,
        Err(err) => return err.write_errors().into(),
    };
    let name = &input.ident;

    // Add a bound `T: BaseMetric` to every type parameter T.
    let generic_bounds: [TypeParamBound; 1] = [parse_quote!(crate::metrics::v2::BaseMetric)];
    let generics = add_trait_bounds(input.generics.clone(), generic_bounds);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let emit_impl = implement_emit_metrics(&input);

    let expanded = quote! {
        #[automatically_derived]
        impl #impl_generics crate::metrics::v2::MetricsEmitter for #name #ty_generics #where_clause {
            fn emit_metrics(
                &self,
                client: &crate::metrics::v2::macro_support::StatsdClient,
                base_tags: &mut dyn ::std::iter::Iterator<Item = (&str, &str)>,
            ) -> ::std::result::Result<(), crate::metrics::v2::macro_support::MetricError> {
                #emit_impl
                ::std::result::Result::Ok(())
            }
        }
    };

    expanded.into()
}

fn implement_emit_metrics(attrs: &SpannedValue<MetricsEmitterAttrs>) -> TokenStream {
    match &attrs.data {
        Data::Struct(data) => {
            let recurse = data.fields.iter().enumerate().map(|(i, f)| {
                let field_name: TokenTree = f.ident.as_ref().map(|id| id.clone().into()).unwrap_or_else(|| Literal::usize_unsuffixed(i).into());
                let mut name_string = String::new();
                if !attrs.prefix.is_empty() {
                    name_string.push_str(&attrs.prefix);
                    name_string.push('.');
                }
                if !f.name.is_empty() {
                    name_string.push_str(&f.name);
                } else {
                    name_string.push_str(&field_name.to_string());
                }

                let name_string = Literal::string(&name_string);
                quote_spanned! { f.span() =>
                    crate::metrics::v2::BaseMetric::send_metric(&self.#field_name, client, #name_string, &mut base_tags.iter().copied())?;
                }
            });
            quote! {
                let base_tags: ::std::vec::Vec<(&str, &str)> = base_tags.collect();
                #(#recurse)*
            }
        }
        Data::Enum(_) => {
            darling::Error::custom("MetricsEmitter derive only implemented for structs")
                .with_span(&attrs.span())
                .write_errors()
        }
    }
}

#[derive(Debug, FromDeriveInput)]
#[darling(attributes(tag))]
struct TagKeyAttrs {
    ident: syn::Ident,
    generics: syn::Generics,
    data: Data<(), SpannedValue<TagKeyFieldAttrs>>,
}

#[derive(Debug, FromField)]
#[darling(attributes(tag))]
struct TagKeyFieldAttrs {
    ident: Option<syn::Ident>,
    #[darling(default)]
    name: String,
}

#[proc_macro_derive(TagKey, attributes(tag))]
pub fn tag_key_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = match SpannedValue::<TagKeyAttrs>::from_derive_input(&parse_macro_input!(
        input as DeriveInput
    )) {
        Ok(input) => input,
        Err(err) => return err.write_errors().into(),
    };
    let name = &input.ident;

    // Add a bound `T: DynamicTagValue + Hash + Eq` to every type parameter T.
    let generic_bounds: [TypeParamBound; 3] = [
        parse_quote!(crate::metrics::DynamicTagValue),
        parse_quote!(::std::hash::Hash),
        parse_quote!(::std::cmp::Eq),
    ];
    let generics = add_trait_bounds(input.generics.clone(), generic_bounds);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let (tag_key_impl, n) = implement_tag_key(&input);

    let expanded = quote! {
        #[automatically_derived]
        impl #impl_generics crate::metrics::v2::TagKey for #name #ty_generics #where_clause {
            type TagIterator<'a> = ::std::array::IntoIter<(&'a str, &'a str), #n>;
            fn tags<'a>(
                &'a self,
            ) -> Self::TagIterator<'a> {
                #tag_key_impl
            }
        }
    };

    expanded.into()
}

fn implement_tag_key(attrs: &SpannedValue<TagKeyAttrs>) -> (TokenStream, usize) {
    match &attrs.data {
        Data::Struct(data) => {
            let recurse = data.fields.iter().enumerate().map(|(i, f)| {
                let field_name: TokenTree = f
                    .ident
                    .as_ref()
                    .map(|id| id.clone().into())
                    .unwrap_or_else(|| Literal::usize_unsuffixed(i).into());
                let name_string = if f.name.is_empty() {
                    Literal::string(&field_name.to_string())
                } else {
                    Literal::string(&f.name)
                };
                quote_spanned! { f.span() =>
                    (#name_string, crate::metrics::DynamicTagValue::as_static_str(&self.#field_name))
                }
            });
            let expanded = quote! {
                [
                    #(#recurse,)*
                ].into_iter()
            };
            (expanded, data.fields.len())
        }
        Data::Enum(_) => (
            darling::Error::custom("TagKey derive only implemented for structs")
                .with_span(&attrs.span())
                .write_errors(),
            0,
        ),
    }
}

fn add_trait_bounds(
    mut generics: Generics,
    bounds: impl IntoIterator<Item = TypeParamBound> + Clone,
) -> Generics {
    for param in &mut generics.params {
        if let GenericParam::Type(ref mut type_param) = *param {
            type_param.bounds.extend(bounds.clone());
        }
    }
    generics
}
