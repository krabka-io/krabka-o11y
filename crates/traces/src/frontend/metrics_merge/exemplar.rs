use super::*;

/// One exemplar in Tempo's metrics shape.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Exemplar {
    #[serde(default)]
    pub labels: Vec<KeyValue>,
    pub value: f64,
    #[serde(rename = "timestampMs", default)]
    pub timestamp_ms: String,
}
