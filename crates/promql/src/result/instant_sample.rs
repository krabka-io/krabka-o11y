use super::{Labels, SampleValue};

/// One labeled point in an instant vector.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct InstantSample {
    pub labels: Labels,
    pub ts_ms: i64,
    pub value: SampleValue,
    /// Whether `__name__` must be removed at the outer query boundary.
    #[serde(skip)]
    pub(crate) drop_name: bool,
}
