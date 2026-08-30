use super::*;

/// One metric sample: `{"timestampMs": "<ms>", "value": <f64>}`.
///
/// Tempo's protojson renders the int64 millisecond timestamp as a string, so it
/// stays a string here. The merge compares and orders it numerically.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricSample {
    #[serde(rename = "timestampMs")]
    pub timestamp_ms: String,
    pub value: f64,
}
