use super::*;

#[tokio::test]
pub(crate) async fn vector_vector_comparison_matches_native_histogram_equality() {
    let mut left = native_histogram(4.0, 10.0);
    left.zero_count = 1.0;
    left.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 1,
    }];
    left.positive_counts = vec![3.0];
    let equal = left.clone();
    let mut different = left.clone();
    different.sum = 11.0;

    let mut store = InMemoryMetricStore::new();
    for (name, histogram) in [("a", left), ("b", equal), ("c", different)] {
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", name), ("job", "api"), ("x", "1")]),
            10_000,
            histogram,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let equal = engine
        .query_instant(
            &tenant_id("tenant-a"),
            "histogram_count(a == on (x) b)",
            10_000,
        )
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = &equal else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(samples[0].labels == labels(&[("x", "1")]));
    assert2::assert!(samples[0].value == SampleValue::Float(4.0));

    let not_equal = engine
        .query_instant(
            &tenant_id("tenant-a"),
            "histogram_count(a != on (x) c)",
            10_000,
        )
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = &not_equal else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(samples[0].labels == labels(&[("x", "1")]));
    assert2::assert!(samples[0].value == SampleValue::Float(4.0));

    let samples = instant_vector(&engine, "a == on (x) c", 10_000).await;
    assert2::assert!(samples.is_empty());

    let samples = instant_vector(&engine, "a == bool on (x) c", 10_000).await;
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job").is_none());
    check!(samples[0].labels.get("x") == Some("1"));
    check!(approx_eq(float_value(&samples[0].value), 0.0));

    let samples = instant_vector(&engine, "a > bool on (x) b", 10_000).await;
    assert2::assert!(samples.is_empty());
}
