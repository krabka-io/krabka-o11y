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
