extern crate proc_macro;
extern crate syn;
#[macro_use]
extern crate quote;

use darling::FromDeriveInput;
use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

use crate::disconfigs::Disconfig;
use crate::flags::FeatureFlags;

mod disconfigs;
mod flags;

#[proc_macro_derive(FeatureFlags, attributes(service, feature_flag))]
pub fn derive_feature_flags(input: TokenStream) -> TokenStream {
    fn feature_flags_inner(
        derive_input: syn::DeriveInput,
    ) -> darling::Result<proc_macro2::TokenStream> {
        let feature_flags = FeatureFlags::from_derive_input(&derive_input)?;
        let impls = feature_flags.impl_flags()?;

        Ok(quote! {
            #impls
        })
    }

    let derive_input = parse_macro_input!(input as DeriveInput);
    match feature_flags_inner(derive_input) {
        Ok(token_stream) => token_stream.into(),
        Err(e) => TokenStream::from(e.write_errors()),
    }
}

#[proc_macro_derive(Disconfig, attributes(config))]
pub fn derive_disconfig(input: TokenStream) -> TokenStream {
    fn disconfig_inner(
        derive_input: syn::DeriveInput,
    ) -> darling::Result<proc_macro2::TokenStream> {
        let disconfig = Disconfig::from_derive_input(&derive_input)?;
        let impls = disconfig.impl_config()?;

        Ok(quote! {
            #impls
        })
    }

    let derive_input = parse_macro_input!(input as DeriveInput);
    match disconfig_inner(derive_input) {
        Ok(token_stream) => token_stream.into(),
        Err(e) => TokenStream::from(e.write_errors()),
    }
}
