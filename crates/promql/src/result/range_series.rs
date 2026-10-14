use super::{Labels, SampleValue};

/// A value and its timestamp in a range matrix.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(from = "(i64, SampleValue)", into = "(i64, SampleValue)")]
pub struct RangeSeriesSample {
    pub timestamp_ms: i64,
    pub value: SampleValue,
}

impl From<(i64, SampleValue)> for RangeSeriesSample {
    fn from((timestamp_ms, value): (i64, SampleValue)) -> Self {
        Self {
            timestamp_ms,
            value,
        }
    }
}

impl From<RangeSeriesSample> for (i64, SampleValue) {
    fn from(sample: RangeSeriesSample) -> Self {
        (sample.timestamp_ms, sample.value)
    }
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

#[cfg(test)]
mod tests {
    use super::{RangeSeriesSample, SampleValue};

    #[test]
    fn keeps_range_samples_in_the_legacy_tuple_json_shape() {
        let sample = RangeSeriesSample::new(123, SampleValue::Float(1.5));
        let legacy = serde_json::json!([123, { "Float": 1.5 }]);

        assert_eq!(serde_json::to_value(&sample).unwrap(), legacy);
        assert_eq!(
            serde_json::from_value::<RangeSeriesSample>(legacy).unwrap(),
            sample
        );
    }
}
