mod dynamic_metric;
mod static_metric;

pub use async_trait::async_trait;
pub use paste::paste;

/// Defines a set of metrics that can be collected for a given application.
///
/// The `define_metrics!` macro should be used at the module level, and not within
/// a function.
///
/// The `define_metrics!` macro accepts the following parts:
///   1) `MetricStructName`
///   2) A list of *metrics*, and their configuration.
///
/// ### The following *metrics* are currently supported.
///
///  - `StaticCounter`: A very fast static counter that can be used in super high-throughput codepaths.
///    The underlying counter is implemented using a single [`std::sync::atomic::AtomicI64`], and thus
///    is very fast.
///  - `DynamicCounter`: A counter that allows for dynamic "tags" to be applied. These tags translate to
///    to statsd tags, and can be used in places where the tags need to be programtically computed at
///    runtime. Compared to a static counter, dynamic counters are much slower, as they involve a RwLock
///    and a hashmap. The RwLock is only acquired in write mode for a new dynamic tag combination that has
///    not been seen before in the runtime of the program. Additionally, dynamic counters can only take
///    tags which are `&'static str`'s, guaranteeing that the set of tags will not grow in an unbounded
///    fashion during runtime (assuming you aren't Box::leak'ing Strings).
///  - `StaticGauge`: A very fast static gauge that can be used in super high-throughput codepaths.
///    The underlying gauge is implemented using three single [`std::sync::atomic::AtomicU64`] to track
///    the gauge current value and min, max values in the reporting window, and thus is very fast.
///  - `DynamicGauge`: A gauge that allows for dynamic "tags" to be applied. The underlying implementation
///    of DyanmicGauge also involves a RwLock and a hashmap. Therefore, it is much slower than static gauges.
///  - `StaticHistogram`: A very fast static histogram that can be used in super high-throughput codepaths.
///    The underlying implementation uses many [`hdrhistogram::Histogram`] whose maximum count is
///    the number of cpus on this node. These histograms are stored in a thread safe [`dynamic_pool::DynamicPool`].
///  - `DynamicHistogram`: A histogram that allows for dynamic "tags" to be applied. The underlying implementation
///    of DyanmicHistogram also involves a RwLock and a hashmap. Each histogram per dynamic "tag" is the same as
///    the thread safe pool of histograms used in `StaticHistogram`.
///
///
/// More details on the metric types can be found below.
/// The macro definition of Histogram and Gauge is the same as Counter.
///
/// ### Syntax for defining metrics.
///
/// The syntax for defining a metric is as follows:
///
/// ```no_run
/// use crate::metrics::define_metrics;
///
/// define_metrics!(FooMetrics, [
///     foo => StaticCounter(),
///     bar => DynamicCounter([tag_a, tag_b]),
/// ]);
/// ```
///
/// To break this down, `FooMetrics` is the name of the struct that is generated that holds all the metrics
/// defined. `foo`, and `bar` are fields within the struct that contain the metric to the right hand-side of
/// the arrow `=>`.
///
/// In order to use the metrics, you must first create an instance of `FooMetrics`, usually this is done by using
/// the `new` method, which will return an `Arc<FooMetrics>`.
/// ```
/// use crate::metrics::define_metrics;
/// use crate::metrics::counters::StaticCounter;
/// use crate::metrics::counters::DynamicCounter;
///
/// define_metrics!(FooMetrics, [
///     foo => StaticCounter(),
///     bar => DynamicCounter([tag_a, tag_b]),
/// ]);
/// fn main() {
///     let metrics = FooMetrics::new();
///     metrics.foo.incr();
///     metrics.bar.incr("tag_a_value", "tag_b_value");
/// }
/// ```
///
/// Besides counting your metrics, however, these metrics will not be emitted anywhere. This is because you need
/// to spawn a background worker to flush the metrics to a [`crate::SharedMetrics`]. The odd naming right now is
/// due to a transitional period where we are both using the legacy [`crate::SharedMetrics`] to emit metrics, and
/// also as the underlying statsd client which is used to emit metrics.
///
/// In order to spawn metrics, we simply need to employ the [`crate::SpawnEmitWorker`] trait. This must be ran within
/// a tokio runtime context. Putting this all together:
///
/// ```no_run
/// use crate::metrics::{new_client, define_metrics, SpawnEmitWorker};
/// use crate::metrics::counters::StaticCounter;
///
/// define_metrics!(FooMetrics, [
///    foo => StaticCounter(),
///    bar => DynamicCounter([tag_a, tag_b]),
/// ]);
///
/// #[tokio::main]
/// async fn main() {
///     let metrics = FooMetrics::new();
///     let _worker_guard = metrics.clone().spawn_emit_worker(new_client("prefix.here").unwrap());
///
///     metrics.foo.incr();
///     metrics.bar.incr("a", "b");
/// }
/// ```
///
/// ## Metric Types:
///
/// ### `StaticCounter`
///
/// A static counter can be defined in the following forms:
/// ```
/// # use crate::metrics::define_metrics;
/// define_metrics!(FooMetrics, [
///    foo => StaticCounter(),
///    bar => StaticCounter("explicit.name"),
///    baz => StaticCounter(["static" => "tags", "are" => "defined.here"]),
///    qux => StaticCounter("explicit.name", ["static" => "tags"]),
/// ]);
/// ```
///
/// You can think of this as the following: `StaticCounter("explicit.name", ["static" => "tags"])`, where
/// the explicit name and static tags arguments are optional.
///
/// If an explicit name is not provided, the field name within the `define_metrics!` macro
/// (e.g. `foo` in `foo => StaticCounter()`) will be used as the metric name.
///
/// Static tags are defined as a list of `key => value` strings. For example: `["static" => "tags", "are" => "defined.here"]`.
///
/// Providing tags, or an explicit name will not increase the runtime overhead.
///
/// ### `DynamicCounter`
///
/// A dynamic counter can be defined in the following forms:
/// ```
/// # use crate::metrics::define_metrics;
/// define_metrics!(FooMetrics, [
///    foo => DynamicCounter([tag_a, tag_b]),
///    bar => DynamicCounter("explicit.name", [tag_a, tag_b]),
///    baz => DynamicCounter("explicit.name", [tag_a, tag_b], ["static" => "tags"]),
/// ]);
/// ```
///
/// You can think of this this as the following: `DynamicCounter("explicit.name", [tag_a, tag_b], ["static" => "tags"])`
/// where the following parts are optional: `"explicit.name"` and `["static" => "tags"]`.
///
/// If an explicit name is not provided, the field name within the `define_metrics!` macro
/// (e.g. `foo` in `foo => DynamicCounter([tag_a])`) will be used as the metric name.
///
/// Static tags are defined as a list of `key => value` strings. For example: `["static" => "tags", "are" => "defined.here"]`.
///
/// Dynamic tags are defined as a list of ident's, e.g. `[foo, bar, baz]`.
///
/// Additionally, dynamic tags can be aliased, allowing you to emit metrics which are not valid rust ident's, for example:
/// ```
/// # use crate::metrics::define_metrics;
/// define_metrics!(FooMetrics, [
///    foo => DynamicCounter([tag_a => "tag.a", tag_b => "tag.b"]),
/// ]);
/// ```
///
/// So we can define the dynamic tags as: `[foo]`, `[foo => "alias"]`, `[foo, bar => "bar.alias"]`. Where the ident is required,
/// and an optional `=> "alias"` can be provided after each ident.
///
/// Providing static tags, or an explicit name will not increase the runtime overhead.
///
#[macro_export]
macro_rules! define_metrics {
    ($metrics_struct_name: ident, [
        $($metric_name: ident => $metric_type:ident($($metric_args: tt)*),)+
    ]) => {
        #[derive(Debug, Default)]
        #[allow(non_camel_case_types)]
        pub struct $metrics_struct_name {
           $(pub(crate) $metric_name: $crate::__generate_metric_struct_field_type!($metric_type, $metrics_struct_name, $metric_name)),+
        }

        impl $metrics_struct_name {
            #[allow(unused)]
            pub fn new() -> ::std::sync::Arc<Self> {
                Default::default()
            }
        }

        #[$crate::metrics::macros::async_trait]
        impl $crate::metrics::EmitMetrics for $metrics_struct_name {
            async fn emit_metrics(&self, metrics: &$crate::metrics::SharedMetrics) {
                $(
                    self.$metric_name.emit_metrics(metrics).await;
                )+
            }
        }

        $crate::metrics::macros::paste! {
            mod [<$metrics_struct_name:snake:lower>] {
                mod prelude {
                    #[allow(unused_imports)]
                    pub use crate::metrics::{BaseMetric, DynamicTagKey, DynamicTagValue, StaticTag};
                    #[allow(unused_imports)]
                    pub use std::sync::atomic::AtomicI64;
                    #[allow(unused_imports)]
                    pub use crate::metrics::counters::{DynamicCounter, StaticCounter, DynamicCounterStorage};
                    #[allow(unused_imports)]
                    pub use crate::metrics::gauges::{DynamicGauge, GaugeInner, StaticGauge, DynamicGaugeStorage};
                    #[allow(unused_imports)]
                    pub use crate::metrics::gauges::{GaugeHandleOwned, Many, One};
                    #[allow(unused_imports)]
                    pub use crate::metrics::histograms::{DynamicHistogram, StaticHistogram, DynamicHistogramStorage, PoolHistograms};
                }

                $(
                    $crate::__generate_metric_struct_and_impls!($metric_type, $metric_name, $($metric_args)*);
                )+
            }
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __generate_metric_struct_field_type {
    (DynamicCounter, $metrics_module_name: ident, $metric_struct_name: ident) => {
        $crate::metrics::macros::paste! {
            crate::metrics::counters::dynamic_counter::DynamicCounterWrapper<[<$metrics_module_name:snake:lower>]::[<$metric_struct_name:camel>]>
        }
    };
    (StaticCounter, $metrics_module_name: ident, $metric_struct_name: ident) => {
        $crate::metrics::macros::paste! {
            crate::metrics::counters::static_counter::StaticCounterWrapper<[<$metrics_module_name:snake:lower>]::[<$metric_struct_name:camel>]>
        }
    };
    (DynamicGauge, $metrics_module_name: ident, $metric_struct_name: ident) => {
        $crate::metrics::macros::paste! {
            crate::metrics::gauges::dynamic_gauge::DynamicGaugeWrapper<[<$metrics_module_name:snake:lower>]::[<$metric_struct_name:camel>]>
        }
    };
    (StaticGauge, $metrics_module_name: ident, $metric_struct_name: ident) => {
        $crate::metrics::macros::paste! {
            crate::metrics::gauges::static_gauge::StaticGaugeWrapper<[<$metrics_module_name:snake:lower>]::[<$metric_struct_name:camel>]>
        }
    };
    (DynamicHistogram, $metrics_module_name: ident, $metric_struct_name: ident) => {
        $crate::metrics::macros::paste! {
            crate::metrics::histograms::dynamic_histogram::DynamicHistogramWrapper<[<$metrics_module_name:snake:lower>]::[<$metric_struct_name:camel>]>
        }
    };
    (StaticHistogram, $metrics_module_name: ident, $metric_struct_name: ident) => {
        $crate::metrics::macros::paste! {
            crate::metrics::histograms::static_histogram::StaticHistogramWrapper<[<$metrics_module_name:snake:lower>]::[<$metric_struct_name:camel>]>
        }
    };
    ($other: ident, $metrics_module_name: ident, $metric_struct_name: ident) => {
        compile_error!(concat!(
            "Unknown metric type: ", stringify!($other),
            ". Must be one of DynamicCounter, StaticCounter, DynamicGauge, StaticGauge, DynamicHistogram or StaticHistogram"
        ))
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! __generate_metric_struct_and_impls {
    (StaticCounter, $metric_name: ident, $($metric_args: tt)*) => {
        $crate::__static_metric_generate_struct_and_impls!(StaticCounter, $metric_name, AtomicI64, $($metric_args)*);
    };
    (DynamicCounter, $metric_name: ident, $($metric_args: tt)*) => {
        $crate::__dynamic_metric_generate_struct_and_impls!(DynamicCounter, $metric_name, $($metric_args)*);
    };
    (StaticGauge, $metric_name: ident, $($metric_args: tt)*) => {
        $crate::__static_metric_generate_struct_and_impls!(StaticGauge, $metric_name, GaugeInner, $($metric_args)*);
    };
    (DynamicGauge, $metric_name: ident, $($metric_args: tt)*) => {
        $crate::__dynamic_metric_generate_struct_and_impls!(DynamicGauge, $metric_name, $($metric_args)*);
    };
    (StaticHistogram, $metric_name: ident, $($metric_args: tt)*) => {
        $crate::__static_metric_generate_struct_and_impls!(StaticHistogram, $metric_name, PoolHistograms, $($metric_args)*);
    };
    (DynamicHistogram, $metric_name: ident, $($metric_args: tt)*) => {
        $crate::__dynamic_metric_generate_struct_and_impls!(DynamicHistogram, $metric_name, $($metric_args)*);
    };
    ($other: ident, $metric_name: ident, $($metric_args: tt)*) => {
        // Do nothing, the error is handled in `__generate_metric_struct_field_type`, let's not contribute to more error spam.
    }
}
