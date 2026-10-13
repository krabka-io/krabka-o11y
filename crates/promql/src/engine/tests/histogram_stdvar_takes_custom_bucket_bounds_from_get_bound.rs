use super::*;

#[derive(bon::Builder)]
struct HistogramVarianceCase {
    name: &'static str,
    function: &'static str,
    histogram: NativeHistogram,
    want: f64,
}

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
        HistogramVarianceCase::builder()
            .name("first bucket is open below")
            .function("histogram_stdvar")
            .histogram(custom_histogram(1.0, &[0.1, 0.5], 0, &[1.0, 1.0]))
            .want(f64::INFINITY)
            .build(),
        HistogramVarianceCase::builder()
            .name("last bucket is open above")
            .function("histogram_stdvar")
            .histogram(custom_histogram(1.0, &[0.1, 0.5], 1, &[1.0, 1.0]))
            .want(f64::INFINITY)
            .build(),
        HistogramVarianceCase::builder()
            .name("closed buckets")
            .function("histogram_stdvar")
            .histogram(custom_histogram(0.5, &[0.1, 0.5, 2.5], 1, &[1.0, 1.0]))
            .want(0.7825)
            .build(),
        HistogramVarianceCase::builder()
            .name("closed buckets")
            .function("histogram_stddev")
            .histogram(custom_histogram(0.5, &[0.1, 0.5, 2.5], 1, &[1.0, 1.0]))
            .want(0.884_590_300_647_706_6)
            .build(),
        HistogramVarianceCase::builder()
            .name("empty open buckets")
            .function("histogram_stdvar")
            .histogram(custom_histogram(
                0.5,
                &[0.1, 0.5, 2.5],
                0,
                &[0.0, 1.0, 1.0, 0.0],
            ))
            .want(0.7825)
            .build(),
        HistogramVarianceCase::builder()
            .name("negative bounds")
            .function("histogram_stdvar")
            .histogram(custom_histogram(-3.0, &[-4.0, -2.0, 0.0], 1, &[1.0, 1.0]))
            .want(1.25)
            .build(),
        HistogramVarianceCase::builder()
            .name("bucket across zero")
            .function("histogram_stdvar")
            .histogram(custom_histogram(-1.0, &[-1.0, 1.0], 1, &[2.0]))
            .want(0.25)
            .build(),
        HistogramVarianceCase::builder()
            .name("one bucket open at both ends")
            .function("histogram_stdvar")
            .histogram(custom_histogram(1.0, &[], 0, &[1.0]))
            .want(f64::NAN)
            .build(),
    ];

    for case_index in 0..cases.len() {
        let case = &cases[case_index];
        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "h")]),
            10_000,
            case.histogram.clone(),
        );
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let query = format!("{}(h)", case.function);

        let result = engine
            .query_instant(&tenant_id("tenant-a"), &query, 10_000)
            .await
            .unwrap_or_else(|error| panic!("{}: {error}", case.name));

        let QueryResult::InstantVector(samples) = result else {
            panic!("{}: expected a vector", case.name);
        };
        check!(samples.len() == 1, "{}", case.name);
        let got = float_value(&samples[0].value);
        // Every NaN matches every other: its sign bit depends on the target
        // CPU, and Prometheus does not compare NaN payloads either.
        check!(
            (got.is_nan() && case.want.is_nan())
                || got.total_cmp(&case.want).is_eq()
                || approx_eq(got, case.want),
            "{} {}: {got} != {}",
            case.name,
            case.function,
            case.want
        );
    }
}
