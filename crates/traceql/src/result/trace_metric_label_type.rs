/// A scalar metric label's OTLP `AnyValue` type. String labels need no marker.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub enum TraceMetricLabelType {
    Int,
    Double,
    Bool,
}
