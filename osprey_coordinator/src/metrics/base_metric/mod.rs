use std::fmt::Debug;
use std::hash::Hash;

use crate::metrics::SharedMetrics;

#[derive(Debug)]
pub struct StaticTag {
    key: &'static str,
    value: &'static str,
}

impl StaticTag {
    pub const fn new(key: &'static str, value: &'static str) -> Self {
        Self { key, value }
    }
}

pub trait BaseMetric: Send + Sync + Default + 'static {
    const NAME: &'static str;
    const STATIC_TAGS: &'static [StaticTag];

    /// Calls a reducer function `f` over the static tag pairs for this counter.
    fn fold_static_tag_pairs<T, F>(mut t: T, f: F) -> T
    where
        F: Fn(T, &'static str, &'static str) -> T,
    {
        for StaticTag { key, value } in Self::STATIC_TAGS {
            t = f(t, key, value);
        }
        t
    }
}

#[async_trait::async_trait]
pub trait EmitMetrics: Send + Sync + 'static {
    async fn emit_metrics(&self, metrics: &SharedMetrics);
}

pub trait DynamicTagKey: Hash + Eq + Debug + PartialEq + Eq + Ord + PartialOrd + 'static {
    fn fold_tag_pairs<T, F>(&self, t: T, f: F) -> T
    where
        F: Fn(T, &'static str, &'static str) -> T;

    fn get_tag_keys() -> &'static [&'static str];
}

pub trait DynamicTagValue: 'static {
    fn as_static_str(&self) -> &'static str;
}

impl DynamicTagValue for &'static str {
    #[inline(always)]
    fn as_static_str(&self) -> &'static str {
        self
    }
}

impl DynamicTagValue for bool {
    #[inline(always)]
    fn as_static_str(&self) -> &'static str {
        match self {
            true => "true",
            false => "false",
        }
    }
}

impl DynamicTagValue for tonic::Code {
    fn as_static_str(&self) -> &'static str {
        match self {
            tonic::Code::Ok => "OK",
            tonic::Code::Cancelled => "CANCELLED",
            tonic::Code::Unknown => "UNKNOWN",
            tonic::Code::InvalidArgument => "INVALID_ARGUMENT",
            tonic::Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
            tonic::Code::NotFound => "NOT_FOUND",
            tonic::Code::AlreadyExists => "ALREADY_EXISTS",
            tonic::Code::PermissionDenied => "PERMISSION_DENIED",
            tonic::Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
            tonic::Code::FailedPrecondition => "FAILED_PRECONDITION",
            tonic::Code::Aborted => "ABORTED",
            tonic::Code::OutOfRange => "OUT_OF_RANGE",
            tonic::Code::Unimplemented => "UNIMPLEMENTED",
            tonic::Code::Internal => "INTERNAL",
            tonic::Code::Unavailable => "UNAVAILABLE",
            tonic::Code::DataLoss => "DATA_LOSS",
            tonic::Code::Unauthenticated => "UNAUTHENTICATED",
        }
    }
}
