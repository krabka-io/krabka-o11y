use super::*;

/// An engine over `metric_name{job="api"}`: 0 at 0s, 1 at 60s and 3 at 90s, so
/// the last two samples rise by 2 over 30s.
pub(crate) fn late_jump_engine(metric_name: &str) -> PromqlEngine<InMemoryMetricStore> {
    let store = SeriesFixture::new(labels(&[("__name__", metric_name), ("job", "api")]))
        .at(0_i64, 0.0)
        .at(60_000, 1.0)
        .at(90_000, 3.0)
        .store();
    default_engine(store)
}

/// An engine over `http_requests_total{job="api"}` counting up by one each
/// minute: `n` at `n` minutes, from 0 at 0s through `last_minute`.
pub(crate) fn requests_counter_engine(last_minute: i32) -> PromqlEngine<InMemoryMetricStore> {
    let fixture = (0..=last_minute).fold(
        SeriesFixture::new(labels(&[
            ("__name__", "http_requests_total"),
            ("job", "api"),
        ])),
        |fixture, minute| fixture.at(i64::from(minute) * 60_000, f64::from(minute)),
    );
    default_engine(fixture.store())
}

/// An engine over `latency_seconds{job="api"}` at 10s, with one series per
/// entry of `latencies`, whose `instance` label is the entry's index.
pub(crate) fn api_latency_engine(latencies: &[f64]) -> PromqlEngine<InMemoryMetricStore> {
    latencies
        .iter()
        .enumerate()
        .fold(FloatStore::default(), |store, (instance, latency)| {
            store.sample(
                labels(&[
                    ("__name__", "latency_seconds"),
                    ("job", "api"),
                    ("instance", &instance.to_string()),
                ]),
                *latency,
            )
        })
        .engine()
}
