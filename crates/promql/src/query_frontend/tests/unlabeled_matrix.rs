use super::*;

/// Builds a matrix of one series with the empty label set, one sample at a
/// time.
#[derive(Default)]
pub(crate) struct UnlabeledMatrix {
    samples: Vec<(i64, SampleValue)>,
}

impl UnlabeledMatrix {
    /// Adds `sample_value` at `ts_ms`.
    pub(crate) fn at(mut self, ts_ms: i64, sample_value: SampleValue) -> Self {
        self.samples.push((ts_ms, sample_value));
        self
    }

    pub(crate) fn matrix(self) -> QueryResult {
        QueryResult::RangeMatrix(vec![RangeSeries {
            drop_name: false,
            start_timestamps_ms: std::collections::BTreeMap::new(),
            labels: labels(&[]).into(),
            samples: self.samples,
        }])
    }
}
