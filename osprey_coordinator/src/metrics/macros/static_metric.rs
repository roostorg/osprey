/// Handler for `StaticCounter(...), StaticGauge(...) or StaticHistogram(...)`.

#[doc(hidden)]
#[macro_export]
macro_rules! __static_metric_generate_struct_and_impls {
    // Generates the static metric struct + trait implementation based on parsing the metric args.
    ($metric_type: ident, $metric_struct_name: ident, $value_type: ty, $($metric_args: tt)*) => {
        $crate::__static_metric_generate_struct_and_impls!(
            @parse,
            $metric_type,
            $metric_struct_name,
            $value_type,
            [$($metric_args)*]
        );
    };

    // Parse Args:
    // This section handles parsing the arguments of `StaticCounter/StaticGauge/StaticHistogram`, namely the following variants:
    // The document below will use StaticCounter as an example.
    //
    // 1: StaticCounter(): Tag name inferred, no static tags defined.
    (@parse, $metric_type: ident, $metric_struct_name: ident, $value_type: ty, []) => {
        $crate::__static_metric_generate_struct_and_impls!(
            @gen,
            $metric_type,
            $metric_struct_name,
            $value_type,
            [
                metric_name: stringify!($metric_struct_name),
                static_tags: [],
            ]
        );
    };
    // 2: StaticCounter(["static_tags" => "are_here"]): Tag name inferred, static tags defined.
    (@parse, $metric_type: ident, $metric_struct_name: ident, $value_type: ty, [[$($tag_key:expr => $tag_value: expr),*]]) => {
        $crate::__static_metric_generate_struct_and_impls!(
            @gen,
            $metric_type,
            $metric_struct_name,
            $value_type,
            [
                metric_name: stringify!($metric_struct_name),
                static_tags: [$($tag_key => $tag_value),*],
            ]
        );
    };
    // 3: StaticCounter("name.here"): Tag name explicit, no static tags defined.
    (@parse, $metric_type: ident, $metric_struct_name: ident, $value_type: ty, [$metric_name: expr]) => {
        $crate::__static_metric_generate_struct_and_impls!(
            @gen,
            $metric_type,
            $metric_struct_name,
            $value_type,
            [
                metric_name: $metric_name,
                static_tags: [],
            ]
        );
    };
    // 4: StaticCounter("name.here", ["static_tags" => "are_here"]): Tag name expicit, static tags defined.
    (@parse, $metric_type: ident, $metric_struct_name: ident, $value_type: ty,
        [$metric_name: expr, [$($tag_key:expr => $tag_value: expr),*]]) => {
        $crate::__static_metric_generate_struct_and_impls!(
            @gen,
            $metric_type,
            $metric_struct_name,
            $value_type,
            [
                metric_name: $metric_name,
                static_tags: [$($tag_key => $tag_value),*],
            ]
        );
    };

    // Following the successful result of the parser above, actually write the impl code for StaticGauge variant:
    (@gen, StaticGauge, $metric_struct_name: ident, $value_type: ty, [
        metric_name: $metric_name: expr,
        static_tags: [$($tag_key:expr => $tag_value: expr),*],
    ]) => {
        $crate::metrics::macros::paste! {
            #[derive(Default)]
            pub struct [<$metric_struct_name:camel>] {
                value: std::sync::Arc<prelude::$value_type>,
            }

            impl prelude::BaseMetric for [<$metric_struct_name:camel>] {
                const NAME: &'static str = $metric_name;
                const STATIC_TAGS: &'static [prelude::StaticTag] = &[
                    $(
                        prelude::StaticTag::new($tag_key, $tag_value),
                    )*
                ];
            }

            impl prelude::StaticGauge for [<$metric_struct_name:camel>] {
                fn inner(&self) -> &prelude::$value_type {
                    &self.value.as_ref()
                }

                fn inner_arc(&self) -> std::sync::Arc<prelude::$value_type> {
                    std::sync::Arc::clone(&self.value)
                }
            }
        }
    };

    // Following the successful result of the parser above, actually write the impl code
    // for StaticCounter and StaticHistogram variants:
    (@gen, $metric_type: ident, $metric_struct_name: ident, $value_type: ty, [
        metric_name: $metric_name: expr,
        static_tags: [$($tag_key:expr => $tag_value: expr),*],
    ]) => {
        $crate::metrics::macros::paste! {
            #[derive(Default)]
            pub struct [<$metric_struct_name:camel>] {
                value: prelude::$value_type,
            }

            impl prelude::BaseMetric for [<$metric_struct_name:camel>] {
                const NAME: &'static str = $metric_name;
                const STATIC_TAGS: &'static [prelude::StaticTag] = &[
                    $(
                        prelude::StaticTag::new($tag_key, $tag_value),
                    )*
                ];
            }

            impl prelude::$metric_type for [<$metric_struct_name:camel>] {
                fn value(&self) -> &prelude::$value_type {
                    &self.value
                }
            }
        }
    }
}
