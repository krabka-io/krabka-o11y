/// A metric label's type and OTLP `AnyValue` shape. String labels need no marker.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub enum TraceMetricLabelType {
    Int,
    Double,
    Bool,
    /// Canonical OTLP array value, preserving element order and types.
    Array,
    /// Pinned frontend-decoded nil, distinct from the string label `"nil"`.
    Nil,
}
