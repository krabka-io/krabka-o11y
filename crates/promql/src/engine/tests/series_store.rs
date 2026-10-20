use super::*;

/// Builds a store holding one `tenant-a` float series, one point at a time.
pub(crate) struct SeriesFixture {
    labels: Labels,
    store: InMemoryMetricStore,
}

impl SeriesFixture {
    pub(crate) fn new(labels: Labels) -> Self {
        Self {
            labels,
            store: InMemoryMetricStore::new(),
        }
    }

    /// Adds the series' float `sample_value` at `ts_ms`.
    pub(crate) fn at(mut self, ts_ms: i64, sample_value: f64) -> Self {
        self.store
            .push_float("tenant-a", self.labels.clone(), ts_ms, sample_value);
        self
    }

    pub(crate) fn store(self) -> InMemoryMetricStore {
        self.store
    }
}

pub(crate) fn queue_depth_store(samples: &[(i64, f64)]) -> InMemoryMetricStore {
    samples
        .iter()
        .fold(
            SeriesFixture::new(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "queue_depth"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ])),
            |series, (timestamp_ms, value)| series.at(*timestamp_ms, *value),
        )
        .store()
}

pub(crate) fn up_series_store(samples: &[(i64, f64)]) -> InMemoryMetricStore {
    samples
        .iter()
        .fold(
            SeriesFixture::new(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ])),
            |series, (timestamp_ms, value)| series.at(*timestamp_ms, *value),
        )
        .store()
}
