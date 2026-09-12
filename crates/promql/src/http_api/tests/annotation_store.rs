use super::*;

// One float series `up` and one native-histogram series `h`, both at timestamp
// zero. A float-only query warns, a histogram-against-float comparison raises an
// info, and a selector that matches nothing raises neither.
pub(crate) fn annotation_store() -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    let mut up = Labels::new();
    up.insert("__name__", "up");
    up.insert("job", "api");
    store.push_float("tenant-a", up, 0, 1.0);

    let mut histogram = Labels::new();
    histogram.insert("__name__", "h");
    histogram.insert("job", "api");
    store.push_histogram(
        "tenant-a",
        histogram,
        0,
        NativeHistogram {
            schema: 0,
            is_float: true,
            reset_hint: ResetHint::No,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 4.0,
            sum: 5.0,
            positive_spans: vec![],
            positive_counts: vec![],
            negative_spans: vec![],
            negative_counts: vec![],
            custom_values: None,
            start_timestamp_ms: None,
        },
    );
    store
}
