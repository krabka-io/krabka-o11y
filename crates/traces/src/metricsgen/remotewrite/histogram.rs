use super::*;

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct Histogram {
    #[prost(double, tag = "2")]
    pub(crate) count_float: f64,
    #[prost(double, tag = "3")]
    pub(crate) sum: f64,
    #[prost(sint32, tag = "4")]
    pub(crate) schema: i32,
    #[prost(double, tag = "5")]
    pub(crate) zero_threshold: f64,
    #[prost(double, tag = "7")]
    pub(crate) zero_count_float: f64,
    #[prost(message, repeated, tag = "11")]
    pub(crate) positive_spans: Vec<RemoteWriteBucketSpan>,
    #[prost(double, repeated, tag = "13")]
    pub(crate) positive_counts: Vec<f64>,
    #[prost(enumeration = "ResetHint", tag = "14")]
    pub(crate) reset_hint: i32,
    #[prost(int64, tag = "15")]
    pub(crate) timestamp: i64,
}
