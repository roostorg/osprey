#[doc(hidden)]
#[macro_export]
macro_rules! __dynamic_metric_generate_struct_and_impls {
    ($metric_type: ident, $metric_struct_name: ident, $($metric_args: tt)*) => {
        $crate::__dynamic_metric_generate_struct_and_impls!(
            @parse,
            $metric_type,
            $metric_struct_name,
            [$($metric_args)*]
        );
    };

    // Phase 1: Parse AST
    //
    // This parse handles parsing of the DynamicCounter(...) variants.
    //
    // Variant 1: DynamicCounter([dynamic_tags, here => "..."]): Implicit name, dynamic tags provided.
    (@parse, $metric_type: ident, $metric_struct_name: ident, [[$($dynamic_args: tt)+]]) => {
        $crate::__dynamic_metric_generate_struct_and_impls!(
            @parse_dynamic_init,
            $metric_type,
            [$($dynamic_args)+],
            [
                metric_name: stringify!($metric_struct_name),
                metric_struct_name: $metric_struct_name,
                static_tags: [],
            ]
        );
    };
    // Variant 2: DynamicCounter([dynamic_tags, here => "..."], ["foo" => "bar"]): Implicit name, dynamic + static tags provided.
    (@parse, $metric_type: ident, $metric_struct_name: ident,
        [[$($dynamic_args: tt)+],
        [$($static_tag_key: expr => $static_tag_value: expr),+]]) => {
        $crate::__dynamic_metric_generate_struct_and_impls!(
            @parse_dynamic_init,
            $metric_type,
            [$($dynamic_args)+],
            [
                metric_name: stringify!($metric_struct_name),
                metric_struct_name: $metric_struct_name,
                static_tags: [$($static_tag_key => $static_tag_value,)+],
            ]
        );
    };
    // Variant 3: DynamicCounter("explicit.name", [dynamic_tags, here => "..."]): Explicit name + dynamic tags provided.
    (@parse, $metric_type: ident, $metric_struct_name: ident, [$metric_name: expr, [$($dynamic_args: tt)+]]) => {
        $crate::__dynamic_metric_generate_struct_and_impls!(
            @parse_dynamic_init,
            $metric_type,
            [$($dynamic_args)+],
            [
                metric_name: $metric_name,
                metric_struct_name: $metric_struct_name,
                static_tags: [],
            ]
        );
    };
    // Variant 4: DynamicCounter("explicit.name", [dynamic_tags, here => "..."], ["static_tags" => "are_here"]): Explicit name + dynamic +static tags provided.
    (@parse, $metric_type: ident, $metric_struct_name: ident,
        [$metric_name: expr, [$($dynamic_args: tt)+],
        [$($static_tag_key: expr => $static_tag_value: expr),+]]) => {
        $crate::__dynamic_metric_generate_struct_and_impls!(
            @parse_dynamic_init,
            $metric_type,
            [$($dynamic_args)+],
            [
                metric_name: $metric_name,
                metric_struct_name: $metric_struct_name,
                static_tags: [$($static_tag_key => $static_tag_value,)+],
            ]
        );
    };

    // Phase 2: Parse Dynamic Init
    //
    // These macro heads will try to parse the dynamic token tags, normalizing the following:
    //  a)  [foo, bar]                     ====>   [foo => "foo", bar => "bar"]
    //  b)  [foo, bar => "bar.aliased"]    ====>   [foo => "foo", bar => "bar.aliased"]
    // This allows us to accept dynamic tags that have an ident, and an alias to a string, if necessary.
    // This step just sets up the accumulators, and then advance to phase 3.
    (@parse_dynamic_init, $metric_type: ident, [$($dynamic_args: tt)*], [$($config: tt)*]) => {
        $crate::__dynamic_metric_generate_struct_and_impls!(
            @parse_dynamic,
            $metric_type,
            [$($dynamic_args)*],
            [],
            [$($config)*]
        );
    };

    // Phase 3: Parse Dynamic
    //
    // Munch an ident with a comma e.g. `foo,`
    (@parse_dynamic, $metric_type: ident, [$dynamic_arg_name: ident, $($dynamic_args_rest: tt)+], [$($acc: tt)*], [$($config: tt)*]) => {
        $crate::__dynamic_metric_generate_struct_and_impls!(
            @parse_dynamic,
            $metric_type,
            [$($dynamic_args_rest)+],
            [$($acc)* $dynamic_arg_name => stringify!($dynamic_arg_name),],
            [$($config)*]
        );
    };
    // Munch an ident without a trailing comma, at the end of the tag list, e.g. `foo`
    (@parse_dynamic, $metric_type: ident, [$dynamic_arg_name: ident], [$($acc: tt)*], [$($config: tt)*]) => {
        $crate::__dynamic_metric_generate_struct_and_impls!(
            @parse_dynamic,
            $metric_type,
            [],
            [$($acc)* $dynamic_arg_name => stringify!($dynamic_arg_name)],
            [$($config)*]
        );
    };

    // Munch an ident => expr with a comma e.g. `foo => "foo.aliased",`
    (@parse_dynamic, $metric_type: ident,
        [$dynamic_arg_name: ident => $dynamic_tag_key: expr, $($dynamic_args_rest: tt)+],
        [$($acc: tt)*], [$($config: tt)*]) => {
        $crate::__dynamic_metric_generate_struct_and_impls!(
            @parse_dynamic,
            $metric_type,
            [$($dynamic_args_rest)+],
            [$($acc)* $dynamic_arg_name => $dynamic_tag_key,],
            [$($config)*]
        );
    };

    // Munch an ident => expr without a trailing comma, at the end of the tag list, e.g. `foo => "foo.aliased"`
    (@parse_dynamic, $metric_type: ident, [$dynamic_arg_name: ident => $dynamic_tag_key: expr],
        [$($acc: tt)*], [$($config: tt)*]) => {
        $crate::__dynamic_metric_generate_struct_and_impls!(
            @parse_dynamic,
            $metric_type,
            [],
            [$($acc)* $dynamic_arg_name => $dynamic_tag_key],
            [$($config)*]
        );
    };
    // Nothing more to parse, move onto phase 4. Phase 4 has variants for DynamicCounter and DynamicHistogram
    (@parse_dynamic, $metric_type: ident, [],
        [$($dynamic_arg_name: ident => $dynamic_tag_key: expr),+], [$($config: tt)*]) => {
        $crate::__dynamic_metric_generate_struct_and_impls!(
            @gen, $metric_type, [
                $($config)*
                dynamic_args: [
                    $($dynamic_arg_name => $dynamic_tag_key,)+
                ],
            ]
        );
    };
    // Some shared code gen needed in phase 4.
    (@gen_shared, $metric_name: expr,
        $metric_struct_name: ident,
        [$($static_tag_key: expr => $static_tag_value: expr,)*],
        [$($dynamic_tag_field_name: ident => $dynamic_tag_key: expr,)+],
    ) => {
        $crate::metrics::macros::paste! {
            #[derive(Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
            pub struct [<$metric_struct_name:camel Key>] {
                $(pub $dynamic_tag_field_name: &'static str,)+
            }

            impl prelude::DynamicTagKey for [<$metric_struct_name:camel Key>] {
                fn fold_tag_pairs<T, F>(&self, mut t: T, f: F) -> T
                where
                    F: Fn(T, &'static str, &'static str) -> T,
                {
                    $(t = f(t, $dynamic_tag_key, self.$dynamic_tag_field_name);)+
                    t
                }

                fn get_tag_keys() -> &'static [&'static str] {
                    &[$($dynamic_tag_key,)+]
                }
            }

            impl prelude::BaseMetric for [<$metric_struct_name:camel>] {
                const NAME: &'static str = $metric_name;
                const STATIC_TAGS: &'static [prelude::StaticTag] = &[
                    $(
                        prelude::StaticTag::new($static_tag_key, $static_tag_value),
                    )*
                ];
            }
        }
    };
    // Phase 4: Code Gen for DynamicCounter
    //
    // Given the transformed AST that now has all the data we need to emit code, perform
    // code-gen here.
    (@gen, DynamicCounter, [
        metric_name: $metric_name: expr,
        metric_struct_name: $metric_struct_name: ident,
        static_tags: [
            $($static_tag_key: expr => $static_tag_value: expr,)*
        ],
        dynamic_args: [
            $($dynamic_tag_field_name: ident => $dynamic_tag_key: expr,)+
        ],
    ]) => {
        $crate::metrics::macros::paste! {
            pub struct [<$metric_struct_name:camel>] {
                storage: prelude::DynamicCounterStorage<[<$metric_struct_name:camel Key>]>,
            }

            $crate::__dynamic_metric_generate_struct_and_impls!(@gen_shared,
                $metric_name,
                $metric_struct_name,
                [$($static_tag_key => $static_tag_value,)*],
                [$($dynamic_tag_field_name => $dynamic_tag_key,)+],
            );

            impl prelude::DynamicCounter for [<$metric_struct_name:camel>] {
                type Key = [<$metric_struct_name:camel Key>];

                fn storage(&self) -> &prelude::DynamicCounterStorage<Self::Key> {
                    &self.storage
                }
            }

            #[allow(clippy::too_many_arguments)]
            impl [<$metric_struct_name:camel>] {
                #[allow(unused)]
                pub fn incr(&self, $($dynamic_tag_field_name: impl prelude::DynamicTagValue),+) {
                    prelude::DynamicCounter::incr_with_key(
                        self,
                        [<$metric_struct_name:camel Key>] { $($dynamic_tag_field_name: $dynamic_tag_field_name.as_static_str()),+ },
                        1
                    );
                }

                #[allow(unused)]
                pub fn incr_by(&self, by: i64, $($dynamic_tag_field_name: impl prelude::DynamicTagValue),+) {
                    prelude::DynamicCounter::incr_with_key(
                        self,
                        [<$metric_struct_name:camel Key>] { $($dynamic_tag_field_name: $dynamic_tag_field_name.as_static_str()),+ },
                        by
                    );
                }

                #[allow(unused)]
                pub fn decr(&self, $($dynamic_tag_field_name: impl prelude::DynamicTagValue),+) {
                    prelude::DynamicCounter::decr_with_key(
                        self,
                        [<$metric_struct_name:camel Key>] { $($dynamic_tag_field_name: $dynamic_tag_field_name.as_static_str()),+ },
                        1
                    );
                }

                #[allow(unused)]
                pub fn decr_by(&self, by: i64, $($dynamic_tag_field_name: impl prelude::DynamicTagValue),+) {
                    prelude::DynamicCounter::decr_with_key(
                        self,
                        [<$metric_struct_name:camel Key>] { $($dynamic_tag_field_name: $dynamic_tag_field_name.as_static_str()),+ },
                        by
                    );
                }
            }

            impl Default for [<$metric_struct_name:camel>] {
                fn default() -> Self {
                    Self {
                        storage: Default::default()
                    }
                }
            }
        }
    };
    // Phase 4: Code Gen for DynamicGauge
    //
    // Given the transformed AST that now has all the data we need to emit code, perform
    // code-gen here.
    (@gen, DynamicGauge, [
        metric_name: $metric_name: expr,
        metric_struct_name: $metric_struct_name: ident,
        static_tags: [
            $($static_tag_key: expr => $static_tag_value: expr,)*
        ],
        dynamic_args: [
            $($dynamic_tag_field_name: ident => $dynamic_tag_key: expr,)+
        ],
    ]) => {
        $crate::metrics::macros::paste! {
            pub struct [<$metric_struct_name:camel>] {
                storage: prelude::DynamicGaugeStorage<[<$metric_struct_name:camel Key>]>,
            }

            $crate::__dynamic_metric_generate_struct_and_impls!(@gen_shared,
                $metric_name,
                $metric_struct_name,
                [$($static_tag_key => $static_tag_value,)*],
                [$($dynamic_tag_field_name => $dynamic_tag_key,)+],
            );

            impl prelude::DynamicGauge for [<$metric_struct_name:camel>] {
                type Key = [<$metric_struct_name:camel Key>];

                fn storage(&self) -> &prelude::DynamicGaugeStorage<Self::Key> {
                    &self.storage
                }
            }

            #[allow(clippy::too_many_arguments)]
            impl [<$metric_struct_name:camel>] {
                /// Increments the gauge by the given value with metrics tags.
                ///
                /// # Safety
                /// This method must be called with an accompanied [`DyanmicGauge::decr_by`], otherwise the state of the gauge
                /// will be incorrect.
                ///
                /// More conveniently and safely though, the use of [`DyanmicGauge::acquire_owned`]
                /// or [`DynamicGauge::acquire_many_owned`] is recommended,
                /// which automatically decrements the gauge for you when the returned [`GaugeHandleOwned`] falls out of scope.
                #[allow(unused)]
                pub unsafe fn incr_by(&self, by: u64, $($dynamic_tag_field_name: impl prelude::DynamicTagValue),+) {
                     prelude::DynamicGauge::incr_with_key(
                        self,
                        [<$metric_struct_name:camel Key>] { $($dynamic_tag_field_name: $dynamic_tag_field_name.as_static_str()),+ },
                        by
                    );
                }

                /// Increments the gauge by 1.
                ///
                /// # Safety
                /// This method must be called with an accompanied [`DyanmicGauge::decr`], otherwise the state of the gauge
                /// will be incorrect.
                ///
                /// See [`DynamicGauge::incr_by`] for more information.
                #[allow(unused)]
                pub unsafe fn incr(&self, $($dynamic_tag_field_name: impl prelude::DynamicTagValue),+) {
                    prelude::DynamicGauge::incr_with_key(
                        self,
                        [<$metric_struct_name:camel Key>] { $($dynamic_tag_field_name: $dynamic_tag_field_name.as_static_str()),+ },
                        1
                    );
                }

                /// Decrements the gauge by 1.
                ///
                /// # Safety
                /// The gauge value after decrement could underflow.
                ///
                /// See [`StaticGauge::incr_by`] for more information.
                #[allow(unused)]
                pub unsafe fn decr(&self, $($dynamic_tag_field_name: impl prelude::DynamicTagValue),+) {
                    prelude::DynamicGauge::decr_with_key(
                        self,
                        [<$metric_struct_name:camel Key>] { $($dynamic_tag_field_name: $dynamic_tag_field_name.as_static_str()),+ },
                        1
                    );
                }

                /// Decrements the gauge by a given value.
                ///
                /// # Safety
                /// The gauge value after decrement could underflow.
                ///
                /// See [`StaticGauge::incr_by`] for more information.
                #[allow(unused)]
                pub unsafe fn decr_by(&self, by: u64, $($dynamic_tag_field_name: impl prelude::DynamicTagValue),+) {
                    prelude::DynamicGauge::decr_with_key(
                        self,
                        [<$metric_struct_name:camel Key>] { $($dynamic_tag_field_name: $dynamic_tag_field_name.as_static_str()),+ },
                        by
                    );
                }

                /// Sets the gauge to a given value with metrics tags.
                /// This method is useful when you have a background task that periodically reports a given value.
                #[allow(unused)]
                pub fn set(&self, val: u64, $($dynamic_tag_field_name: impl prelude::DynamicTagValue),+) {
                    prelude::DynamicGauge::set_with_key(
                        self,
                        [<$metric_struct_name:camel Key>] { $($dynamic_tag_field_name: $dynamic_tag_field_name.as_static_str()),+ },
                        val
                    );
                }

                /// Increments the gauge by a given value with metrics tags
                /// and returns a [`GaugeHandleOwned`] which will automatically decrement the gauge by that value when dropped.
                #[allow(unused)]
                pub fn acquire_many_owned(&self, val: u64, $($dynamic_tag_field_name: impl prelude::DynamicTagValue),+)
                -> prelude::GaugeHandleOwned<prelude::Many> {
                    prelude::DynamicGauge::acquire_many_with_key_owned(
                        self,
                        [<$metric_struct_name:camel Key>] { $($dynamic_tag_field_name: $dynamic_tag_field_name.as_static_str()),+ },
                        val
                    )
                }

                /// Increments the gauge by 1 with metrics tags
                /// and returns a [`GaugeHandleOwned`] which will automatically decrement the gauge when dropped.
                #[allow(unused)]
                pub fn acquire_owned(&self, $($dynamic_tag_field_name: impl prelude::DynamicTagValue),+)
                -> prelude::GaugeHandleOwned<prelude::One> {
                    prelude::DynamicGauge::acquire_with_key_owned(
                        self,
                        [<$metric_struct_name:camel Key>] { $($dynamic_tag_field_name: $dynamic_tag_field_name.as_static_str()),+ }
                    )
                }
            }

            impl Default for [<$metric_struct_name:camel>] {
                fn default() -> Self {
                    Self {
                        storage: Default::default()
                    }
                }
            }
        }
    };
    // Phase 4: Code Gen for DynamicHistogram
    //
    // Given the transformed AST that now has all the data we need to emit code, perform
    // code-gen here.
    (@gen, DynamicHistogram, [
        metric_name: $metric_name: expr,
        metric_struct_name: $metric_struct_name: ident,
        static_tags: [
            $($static_tag_key: expr => $static_tag_value: expr,)*
        ],
        dynamic_args: [
            $($dynamic_tag_field_name: ident => $dynamic_tag_key: expr,)+
        ],
    ]) => {
        $crate::metrics::macros::paste! {
            pub struct [<$metric_struct_name:camel>] {
                storage: prelude::DynamicHistogramStorage<[<$metric_struct_name:camel Key>]>,
            }

            $crate::__dynamic_metric_generate_struct_and_impls!(@gen_shared,
                $metric_name,
                $metric_struct_name,
                [$($static_tag_key => $static_tag_value,)*],
                [$($dynamic_tag_field_name => $dynamic_tag_key,)+],
            );

            impl prelude::DynamicHistogram for [<$metric_struct_name:camel>] {
                type Key = [<$metric_struct_name:camel Key>];

                fn storage(&self) -> &prelude::DynamicHistogramStorage<Self::Key> {
                    &self.storage
                }
            }

            #[allow(clippy::too_many_arguments)]
            impl [<$metric_struct_name:camel>] {
                #[allow(unused)]
                pub fn record_with_key(&self, $($dynamic_tag_field_name: impl prelude::DynamicTagValue),+, duration: std::time::Duration) {
                    prelude::DynamicHistogram::record_with_key(
                        self,
                        [<$metric_struct_name:camel Key>] { $($dynamic_tag_field_name: $dynamic_tag_field_name.as_static_str()),+ },
                        duration
                    );
                }

                #[allow(unused)]
                pub fn record_value_with_key(&self, $($dynamic_tag_field_name: impl prelude::DynamicTagValue),+, value: u64) {
                    prelude::DynamicHistogram::record_value_with_key(
                        self,
                        [<$metric_struct_name:camel Key>] { $($dynamic_tag_field_name: $dynamic_tag_field_name.as_static_str()),+ },
                        value
                    );
                }
            }

            impl Default for [<$metric_struct_name:camel>] {
                fn default() -> Self {
                    Self {
                        storage: Default::default()
                    }
                }
            }
        }
    };
}
