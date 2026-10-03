use super::*;

fn custom_histogram(
    sum: f64,
    custom_values: &[f64],
    offset: i32,
    counts: &[f64],
) -> NativeHistogram {
    NativeHistogram {
        schema: -53,
        count: counts.iter().sum(),
        sum,
        positive_spans: vec![BucketSpan {
            offset,
            length: u32::try_from(counts.len()).expect("a short bucket list"),
        }],
        positive_counts: counts.to_vec(),
        custom_values: Some(custom_values.to_vec()),
        ..native_histogram(0.0, 0.0)
    }
}

#[tokio::test]
pub(crate) async fn histogram_stdvar_takes_custom_bucket_bounds_from_get_bound() {
    // Each want is the answer of the Prometheus v3.14.0 engine, through
    // `promqltest` of github.com/prometheus/prometheus v0.314.0.
    let cases = [
        (
            "first bucket is open below",
            "histogram_stdvar",
            custom_histogram(1.0, &[0.1, 0.5], 0, &[1.0, 1.0]),
            f64::INFINITY,
        ),
        (
            "last bucket is open above",
            "histogram_stdvar",
            custom_histogram(1.0, &[0.1, 0.5], 1, &[1.0, 1.0]),
            f64::INFINITY,
        ),
        (
            "closed buckets",
            "histogram_stdvar",
            custom_histogram(0.5, &[0.1, 0.5, 2.5], 1, &[1.0, 1.0]),
            0.7825,
        ),
        (
            "closed buckets",
            "histogram_stddev",
            custom_histogram(0.5, &[0.1, 0.5, 2.5], 1, &[1.0, 1.0]),
            0.884_590_300_647_706_6,
        ),
        (
            "empty open buckets",
            "histogram_stdvar",
            custom_histogram(0.5, &[0.1, 0.5, 2.5], 0, &[0.0, 1.0, 1.0, 0.0]),
            0.7825,
        ),
        (
            "negative bounds",
            "histogram_stdvar",
            custom_histogram(-3.0, &[-4.0, -2.0, 0.0], 1, &[1.0, 1.0]),
            1.25,
        ),
        (
            "bucket across zero",
            "histogram_stdvar",
            custom_histogram(-1.0, &[-1.0, 1.0], 1, &[2.0]),
            0.25,
        ),
        (
            "one bucket open at both ends",
            "histogram_stdvar",
            custom_histogram(1.0, &[], 0, &[1.0]),
            f64::NAN,
        ),
    ];

    for (name, function, histogram, want) in cases {
        let mut store = InMemoryMetricStore::new();
        store.push_histogram("tenant-a", labels(&[("__name__", "h")]), 10_000, histogram);
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let query = format!("{function}(h)");

        let result = engine
            .query_instant(&tenant_id("tenant-a"), &query, 10_000)
            .await
            .unwrap_or_else(|error| panic!("{name}: {error}"));

        let QueryResult::InstantVector(samples) = result else {
            panic!("{name}: expected a vector");
        };
        check!(samples.len() == 1, "{name}");
        let got = float_value(&samples[0].value);
        // Every NaN matches every other: its sign bit depends on the target
        // CPU, and Prometheus does not compare NaN payloads either.
        check!(
            (got.is_nan() && want.is_nan()) || got.total_cmp(&want).is_eq() || approx_eq(got, want),
            "{name} {function}: {got} != {want}"
        );
    }
}
