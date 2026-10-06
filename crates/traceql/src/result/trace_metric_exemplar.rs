/// One Prometheus-style exemplar attached to a `TraceQL` metrics series.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TraceMetricExemplar {
    pub labels: Vec<(String, String)>,
    /// Scalar types that cannot be recovered from a display label. Omitted
    /// entries are strings; e.g. integer 1 and string "1" remain distinct.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub label_types: std::collections::BTreeMap<String, super::TraceMetricLabelType>,
    pub value: f64,
    pub timestamp_ns: i64,
}
