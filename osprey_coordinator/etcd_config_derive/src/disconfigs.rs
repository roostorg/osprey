use darling::{util::SpannedValue, FromDeriveInput, FromField, FromMeta};
use quote::quote;

#[derive(FromDeriveInput, Debug)]
#[darling(forward_attrs(config), supports(struct_any))]
pub(crate) struct Disconfig {
    ident: syn::Ident,
    attrs: Vec<syn::Attribute>,
    data: darling::ast::Data<(), SpannedValue<DisconfigField>>,
}

const CONFIG_KEY: &'static str = "config.b64";

impl Disconfig {
    pub(crate) fn impl_config(&self) -> darling::Result<proc_macro2::TokenStream> {
        // Get config name from #[config] attribute. This name is used to create the key root.
        let struct_name = &self.ident;
        if self.attrs.len() > 1 {
            return Err(
                darling::Error::custom("Only one #[config] attribute is supported.")
                    .with_span(&self.attrs[1]),
            );
        }

        let config_name_meta = &self
            .attrs
            .first()
            .ok_or(darling::Error::custom("#[config] name must be set."))?
            .meta;

        let config_name = ConfigName::from_meta(config_name_meta)?.name.value();
        let key_root = format!("/discord_configs/{config_name}/v1/");
        // Inside Disconfig struct, there should be one and only one ArcSwap field.
        let fields = self.fields();
        let mut arc_swap_field_count = 0;
        let mut config_field = None;

        for field in fields.iter() {
            let field_ident = field.ident.clone().expect("Field must have an identifier");
            let field_type = &field.ty;
            if let syn::Type::Path(type_path) = field_type {
                let segment = type_path.path.segments.first();
                if let Some(path_segment) = segment {
                    // Make sure the field type is an ArcSwap.
                    if path_segment.ident.to_string() != "ArcSwap"
                        && path_segment.ident.to_string() != "arc_swap::ArcSwap"
                    {
                        continue;
                    }
                    arc_swap_field_count += 1;
                    config_field = Some(field_ident);
                }
            }
        }
        if arc_swap_field_count != 1 {
            return Err(
                darling::Error::custom("Struct must have one and only one ArcSwap field.")
                    .with_span(&self.ident.span()),
            );
        }

        let field_name = config_field.unwrap();
        let config_field_name = field_name.to_string();
        let config_key_path = format!("{key_root}{CONFIG_KEY}");

        let gen = quote! {
            const DISCONFIG_KEY_ROOT: &'static str = #key_root;

            impl etcd_config::KeyHandler for #struct_name {
                fn handle_key_updated(&self, key: &str, value: Option<&str>) {
                    match key {
                        #config_key_path => {
                            let proto = match etcd_config::base64_to_proto(value) {
                                Ok(Some(proto)) => proto,
                                Ok(None) => {
                                    self.#field_name.store(std::sync::Arc::new(Default::default()));
                                    tracing::info!("Resetting disconfig {} to default", #config_field_name);

                                    self.after_update();
                                    return;
                                }
                                Err(e) => {
                                    tracing::error!("Failed to decode proto: {:?}", e);
                                    return;
                                }
                            };

                            let new_proto = match Self::validate(proto) {
                                Ok(new_proto) => new_proto,
                                Err(e) => {
                                    tracing::error!("Failed to validate proto: {:?}", e);
                                    return;
                                }
                            };

                            self.#field_name.store(std::sync::Arc::new(new_proto));

                            self.after_update();
                        }
                        _ => tracing::warn!("Unknown Disconfig entry: {} = {:?}", key, value),
                    }
                }
            }

            impl #struct_name {
                pub async fn start_watching(disconfig: std::sync::Arc<Self>) -> anyhow::Result<()> {
                    use anyhow::Context;

                    etcd_config::run_etcd_watcher(DISCONFIG_KEY_ROOT, disconfig.clone())
                        .await
                        .context("Starting etcd watcher")?;

                    Ok(())
                }

            }
        };

        Ok(gen.into())
    }

    fn fields(&self) -> &darling::ast::Fields<SpannedValue<DisconfigField>> {
        match &self.data {
            darling::ast::Data::Enum(_) => unreachable!(),
            darling::ast::Data::Struct(f) => f,
        }
    }
}

#[derive(FromField, Debug)]
#[darling(forward_attrs(disconfig))]
pub(crate) struct DisconfigField {
    pub(crate) ident: Option<syn::Ident>,
    pub(crate) ty: syn::Type,
}

#[derive(Debug, FromMeta)]
pub(crate) struct ConfigName {
    name: SpannedValue<syn::LitStr>,
}
