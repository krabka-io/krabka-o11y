use super::*;

#[derive(bon::Builder)]
struct HistogramPoint {
    timestamp_ms: i64,
    sum: f64,
}

struct QueryExpectation {
    query: &'static str,
    expected: f64,
}

#[tokio::test]
pub(crate) async fn instant_over_time_windows_are_open_at_the_start_and_closed_at_the_end() {
    let mut store = InMemoryMetricStore::new();
    let points = [
        HistogramPoint::builder().timestamp_ms(0).sum(1.0).build(),
        HistogramPoint::builder()
            .timestamp_ms(10_000)
            .sum(2.0)
            .build(),
        HistogramPoint::builder()
            .timestamp_ms(60_000)
            .sum(3.0)
            .build(),
    ];
    for point_index in 0..points.len() {
        let point = &points[point_index];
        store.push_histogram(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "queue_depth"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]),
            point.timestamp_ms,
            native_histogram(4.0, point.sum),
        );
    }

    // A `[50s]` range ending at 60s starts at exactly 10s. Prometheus' window
    // is half-open, so the sample sitting on that start is out and only the
    // one at 60s is in. This pins that boundary as engine behaviour rather
    // than as a property of whichever layer happens to apply the trim.
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let queries = [
        QueryExpectation {
            query: "histogram_sum(first_over_time(queue_depth[50s]))",
            expected: 3.0,
        },
        QueryExpectation {
            query: "histogram_sum(last_over_time(queue_depth[50s]))",
            expected: 3.0,
        },
        QueryExpectation {
            query: "count_over_time(queue_depth[50s])",
            expected: 1.0,
        },
    ];
    for query_index in 0..queries.len() {
        let query = &queries[query_index];
        let samples = instant_vector(&engine, query.query, 60_000).await;
        check!(samples.len() == 1, "{}", query.query);
        check!(
            approx_eq(float_value(&samples[0].value), query.expected),
            "{}: {}",
            query.query,
            float_value(&samples[0].value)
        );
    }
}
