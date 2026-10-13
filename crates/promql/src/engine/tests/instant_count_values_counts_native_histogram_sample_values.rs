use super::*;

#[derive(bon::Builder)]
struct HistogramInstance {
    instance: &'static str,
    histogram: NativeHistogram,
}

#[tokio::test]
pub(crate) async fn instant_count_values_counts_native_histogram_sample_values() {
    let mut repeated = native_histogram(4.0, 10.0);
    repeated.zero_count = 1.0;
    repeated.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 1,
    }];
    repeated.positive_counts = vec![3.0];
    let mut distinct = repeated.clone();
    distinct.sum = 12.0;

    let mut store = InMemoryMetricStore::new();
    for sample in [
        HistogramInstance::builder()
            .instance("a")
            .histogram(repeated.clone())
            .build(),
        HistogramInstance::builder()
            .instance("b")
            .histogram(repeated.clone())
            .build(),
        HistogramInstance::builder()
            .instance("c")
            .histogram(distinct)
            .build(),
    ] {
        store.push_histogram(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "request_duration_seconds"),
                crate::test_support::TestLabelPair::new("job", "api"),
                crate::test_support::TestLabelPair::new("instance", sample.instance),
            ]),
            10_000,
            sample.histogram,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            &tenant_id("tenant-a"),
            r#"count_values by (job) ("histogram", request_duration_seconds)"#,
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    let mut values = samples
        .iter()
        .map(|sample| float_value(&sample.value))
        .collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    check!(samples.len() == 2);
    check!(samples.iter().all(|sample| {
        sample.labels.get("__name__").is_none()
            && sample.labels.get("job") == Some("api")
            && sample.labels.get("histogram").is_some()
    }));
    check!(values == vec![1.0, 2.0]);
}
