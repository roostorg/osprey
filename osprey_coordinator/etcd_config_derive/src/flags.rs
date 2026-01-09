use darling::{util::SpannedValue, FromDeriveInput, FromField, FromMeta};
use quote::quote;

#[derive(FromDeriveInput, Debug)]
#[darling(forward_attrs(service), supports(struct_any))]
pub(crate) struct FeatureFlags {
    ident: syn::Ident,
    attrs: Vec<syn::Attribute>,
    data: darling::ast::Data<(), SpannedValue<FeatureFlagField>>,
}

#[derive(Debug)]
enum FieldType {
    F32Type,
    BoolType,
    UsizeType,
    U64Type,
    U32Type,
}

impl FeatureFlags {
    pub(crate) fn impl_flags(&self) -> darling::Result<proc_macro2::TokenStream> {
        let struct_name = &self.ident;
        if self.attrs.len() > 1 {
            return Err(
                darling::Error::custom("Only one #[service] attribute is supported.")
                    .with_span(&self.attrs[1]),
            );
        }

        let service_name_meta = &self
            .attrs
            .first()
            .ok_or(darling::Error::custom("#[service] name must be set."))?
            .meta;
        let service_name = ServiceName::from_meta(service_name_meta)?.name.value();

        let key_root = format!("/config/{service_name}");

        let fields = self.fields();
        let mut handle_updates = Vec::with_capacity(fields.len());
        let mut check_methods = Vec::with_capacity(fields.len());

        for field in fields.iter() {
            let field_type = match &field.ty {
                syn::Type::Path(type_path) if type_path.path.is_ident("AtomicF32") => {
                    FieldType::F32Type
                }
                syn::Type::Path(type_path) if type_path.path.is_ident("AtomicBool") => {
                    FieldType::BoolType
                }
                syn::Type::Path(type_path) if type_path.path.is_ident("AtomicUsize") => {
                    FieldType::UsizeType
                }
                syn::Type::Path(type_path) if type_path.path.is_ident("AtomicU64") => {
                    FieldType::U64Type
                }
                syn::Type::Path(type_path) if type_path.path.is_ident("AtomicU32") => {
                    FieldType::U32Type
                }
                _ => return Err(darling::Error::custom(
                    "Only field type AtomicF32, AtomicBool, AtomicUsize, AtomicU32 or AtomicU64 is supported.",
                )),
            };

            let field_name = field.ident.clone().expect("field name must have ident.");
            let field_name_str = field_name.to_string();

            let config_key_path = format!("{key_root}/{field_name_str}");
            match field_type {
                FieldType::BoolType => {
                    handle_updates.push(quote! {
                        #config_key_path => {
                            let new_value = etcd_config::convert_to_bool(value);
                            self.#field_name.store(new_value, std::sync::atomic::Ordering::Relaxed);
                        }
                    });
                    check_methods.push(quote! {
                        pub fn #field_name(&self) -> bool {
                            self.#field_name.load(std::sync::atomic::Ordering::Relaxed)
                        }
                    });
                }
                FieldType::F32Type => {
                    let mut as_bool = false;
                    for attr in &field.attrs {
                        let feature_flag_attrs = FeatureFlagAttributes::from_meta(&attr.meta)?;
                        as_bool = *feature_flag_attrs.as_bool;
                    }

                    handle_updates.push(quote! {
                        #config_key_path => {
                            let new_value = etcd_config::convert_to_float(value);
                            self.#field_name.store(new_value);
                        }
                    });

                    // If has as_bool attribute, generate a method that returns bool.
                    if as_bool {
                        check_methods.push(quote! {
                            pub fn #field_name(&self) -> bool {
                                let value: f32 = ::rand::random();
                                value < self.#field_name.load()
                            }
                        });
                    } else {
                        check_methods.push(quote! {
                            pub fn #field_name(&self) -> f32 {
                                self.#field_name.load()
                            }
                        });
                    }
                }
                FieldType::UsizeType | FieldType::U64Type | FieldType::U32Type => {
                    let (convert_to_value, return_type) = match field_type {
                        FieldType::UsizeType => (
                            quote! { etcd_config::convert_to_usize(value) },
                            quote! {usize},
                        ),
                        FieldType::U64Type => {
                            (quote! { etcd_config::convert_to_u64(value) }, quote! {u64})
                        }
                        FieldType::U32Type => {
                            (quote! { etcd_config::convert_to_u32(value) }, quote! {u32})
                        }
                        _ => unreachable!(),
                    };

                    handle_updates.push(quote! {
                        #config_key_path => {
                            let new_value = #convert_to_value;
                            self.#field_name.store(new_value, std::sync::atomic::Ordering::Relaxed);
                        }
                    });
                    check_methods.push(quote! {
                        pub fn #field_name(&self) -> #return_type {
                            self.#field_name.load(std::sync::atomic::Ordering::Relaxed)
                        }
                    });
                }
            }
        }

        let fields_updates = handle_updates
            .into_iter()
            .fold(quote! {}, |acc, line| quote! {#acc #line});
        let fields_checks = check_methods
            .into_iter()
            .fold(quote! {}, |acc, line| quote! {#acc #line});

        let gen = quote! {
            const CONFIG_KEY_ROOT: &str = #key_root;

            impl etcd_config::KeyHandler for #struct_name {
                fn handle_key_updated(&self, key: &str, value: Option<&str>) {
                    match key {
                        #fields_updates
                        _ => tracing::warn!("Unknown EtcdConfig entry: {} = {:?}", key, value),
                    }
                }
            }

            impl #struct_name {
                pub async fn start_watching() -> anyhow::Result<std::sync::Arc<Self>> {
                    use anyhow::Context;

                    let config = std::sync::Arc::new(Self::default());
                    etcd_config::run_etcd_watcher(CONFIG_KEY_ROOT, config.clone())
                        .await
                        .context("Starting etcd watcher")?;

                    Ok(config)
                }

                #fields_checks
            }
        };

        Ok(gen.into())
    }

    fn fields(&self) -> &darling::ast::Fields<SpannedValue<FeatureFlagField>> {
        match &self.data {
            darling::ast::Data::Enum(_) => unreachable!(),
            darling::ast::Data::Struct(f) => f,
        }
    }
}

#[derive(FromField, Debug)]
#[darling(forward_attrs(feature_flag))]
pub(crate) struct FeatureFlagField {
    pub(crate) ident: Option<syn::Ident>,
    pub(crate) ty: syn::Type,
    pub(crate) attrs: Vec<syn::Attribute>,
}

#[derive(Debug, FromMeta)]
pub(crate) struct ServiceName {
    name: SpannedValue<syn::LitStr>,
}

#[derive(Debug, FromMeta)]
pub(crate) struct FeatureFlagAttributes {
    #[darling(default)]
    as_bool: SpannedValue<bool>,
}
