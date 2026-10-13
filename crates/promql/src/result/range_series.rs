use super::{Labels, SampleValue};

/// A value and its timestamp in a range matrix.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RangeSeriesSample {
    pub timestamp_ms: i64,
    pub value: SampleValue,
}

impl RangeSeriesSample {
    #[must_use]
    pub fn new(timestamp_ms: i64, value: SampleValue) -> Self {
        Self {
            timestamp_ms,
            value,
        }
    }
}

/// One labeled series of points in a range matrix.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RangeSeries {
    pub labels: Labels,
    /// Whether `__name__` must be removed at the outer query boundary.
    /// Subquery materialization retains the name until its consumers finish.
    #[serde(skip)]
    pub drop_name: bool,
    /// Original counter start times, keyed by sample timestamp.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub start_timestamps_ms: std::collections::BTreeMap<i64, i64>,
    pub samples: Vec<RangeSeriesSample>,
}
