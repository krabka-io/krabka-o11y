use super::*;

/// An engine with a 30s lookback over `up{job="api"}`: 1 at 60s and 2 at 120s.
pub(crate) fn short_lookback_up_engine() -> PromqlEngine<InMemoryMetricStore> {
    lookback_engine(
        SeriesFixture::new(labels(&[("__name__", "up"), ("job", "api")]))
            .at(60_000, 1.0)
            .at(120_000, 2.0)
            .store(),
        millis(30_000),
    )
}
