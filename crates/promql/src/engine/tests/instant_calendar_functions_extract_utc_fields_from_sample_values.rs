use super::*;

#[tokio::test]
pub(crate) async fn instant_calendar_functions_extract_utc_fields_from_sample_values() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "event_timestamp_seconds"), ("case", "leap")]),
        10_000,
        1_709_178_060.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        ("year(event_timestamp_seconds)", 2024.0),
        ("month(event_timestamp_seconds)", 2.0),
        ("day_of_month(event_timestamp_seconds)", 29.0),
        ("day_of_week(event_timestamp_seconds)", 4.0),
        ("day_of_year(event_timestamp_seconds)", 60.0),
        ("days_in_month(event_timestamp_seconds)", 29.0),
        ("hour(event_timestamp_seconds)", 3.0),
        ("minute(event_timestamp_seconds)", 41.0),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert2::assert!(samples.len() == 1);
        assert2::assert!(samples[0].labels.get("__name__") == None);
        assert2::assert!(samples[0].labels.get("case") == Some("leap"));
        assert2::assert!(approx_eq(float_value(&samples[0].value), expected));
    }
}
