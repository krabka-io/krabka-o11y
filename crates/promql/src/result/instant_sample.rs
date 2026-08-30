use super::*;

/// One labeled point in an instant vector.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct InstantSample {
    pub labels: Labels,
    pub ts_ms: i64,
    pub value: SampleValue,
}
