use super::*;

/// Builds a store of float samples, each taken at 10s.
#[derive(Default)]
pub(crate) struct FloatStore {
    store: InMemoryMetricStore,
}

impl FloatStore {
    /// Adds a `tenant-a` sample of `series` at 10s.
    pub(crate) fn sample(self, series: Labels, sample_value: f64) -> Self {
        self.tenant_sample("tenant-a", series, sample_value)
    }

    /// Adds an `up` sample at 10s with the usual job and instance labels.
    pub(crate) fn up_sample(self, job: &str, instance: &str, sample_value: f64) -> Self {
        self.sample(
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", job),
                crate::test_support::TestLabelPair::new("instance", instance),
            ]),
            sample_value,
        )
    }

    /// Adds an `up` sample with a job label at 10s.
    pub(crate) fn up_job_sample(self, job: &str, sample_value: f64) -> Self {
        self.sample(
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", job),
            ]),
            sample_value,
        )
    }

    /// Adds a sample with a metric name and extra labels at 10s.
    pub(crate) fn named_sample(
        self,
        metric: &str,
        extra_labels: &[crate::test_support::TestLabelPair<'_>],
        sample_value: f64,
    ) -> Self {
        let metric_name = crate::test_support::TestLabelPair::new("__name__", metric);
        let label_pairs = std::iter::once(metric_name)
            .chain(extra_labels.iter().copied())
            .collect::<Vec<_>>();
        self.sample(labels(&label_pairs), sample_value)
    }

    /// Adds a `tenant` sample of `series` at 10s.
    pub(crate) fn tenant_sample(mut self, tenant: &str, series: Labels, sample_value: f64) -> Self {
        self.store.push_float(tenant, series, 10_000, sample_value);
        self
    }

    /// An engine with default options over the samples.
    pub(crate) fn engine(self) -> PromqlEngine<InMemoryMetricStore> {
        default_engine(self.store)
    }
}

/// An engine with default options over `store`.
pub(crate) fn default_engine(store: InMemoryMetricStore) -> PromqlEngine<InMemoryMetricStore> {
    PromqlEngine::new(Arc::new(store), EngineOpts::default())
}

/// An engine over `store` with the given `lookback_delta` and a 100-sample cap.
pub(crate) fn lookback_engine(
    store: InMemoryMetricStore,
    lookback_delta: Time,
) -> PromqlEngine<InMemoryMetricStore> {
    PromqlEngine::new(
        Arc::new(store),
        EngineOpts {
            lookback_delta,
            max_samples: 100,
            ..EngineOpts::default()
        },
    )
}
