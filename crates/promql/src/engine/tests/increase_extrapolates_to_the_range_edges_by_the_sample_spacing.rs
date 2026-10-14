use super::*;

#[derive(bon::Builder)]
struct SamplePoint {
    timestamp_ms: i64,
    value: f64,
}

#[derive(bon::Builder)]
struct SeriesCase {
    name: &'static str,
    points: Vec<SamplePoint>,
}

struct QueryExpectation {
    query: &'static str,
    want: f64,
}

/// Prometheus extrapolates a counter out to the edges of the range, but only
/// as far as 1.1x the average sample spacing; past that it settles for half a
/// spacing. Each geometry below trips a different part of that: the first
/// clamps at the end and is cut short at the start by the counter's own zero
/// crossing, the second clamps at the start with a counter that never crosses
/// zero, and the third clamps at neither edge. A series that simply spans its
/// range never enters any of them.
#[tokio::test]
pub(crate) async fn increase_extrapolates_to_the_range_edges_by_the_sample_spacing() {
    let mut store = InMemoryMetricStore::new();
    let series = [
        SeriesCase::builder()
            .name("ends_short")
            .points(vec![
                SamplePoint::builder()
                    .timestamp_ms(50_000)
                    .value(0.0)
                    .build(),
                SamplePoint::builder()
                    .timestamp_ms(60_000)
                    .value(10.0)
                    .build(),
                SamplePoint::builder()
                    .timestamp_ms(70_000)
                    .value(20.0)
                    .build(),
                SamplePoint::builder()
                    .timestamp_ms(80_000)
                    .value(30.0)
                    .build(),
            ])
            .build(),
        SeriesCase::builder()
            .name("starts_late")
            .points(vec![
                SamplePoint::builder()
                    .timestamp_ms(70_000)
                    .value(5.0)
                    .build(),
                SamplePoint::builder()
                    .timestamp_ms(80_000)
                    .value(15.0)
                    .build(),
                SamplePoint::builder()
                    .timestamp_ms(90_000)
                    .value(25.0)
                    .build(),
            ])
            .build(),
        SeriesCase::builder()
            .name("spans_range")
            .points(vec![
                SamplePoint::builder()
                    .timestamp_ms(45_000)
                    .value(2.0)
                    .build(),
                SamplePoint::builder()
                    .timestamp_ms(60_000)
                    .value(4.0)
                    .build(),
                SamplePoint::builder()
                    .timestamp_ms(75_000)
                    .value(6.0)
                    .build(),
                SamplePoint::builder()
                    .timestamp_ms(95_000)
                    .value(10.0)
                    .build(),
            ])
            .build(),
    ];
    for series_index in 0..series.len() {
        let series = &series[series_index];
        for point_index in 0..series.points.len() {
            let point = &series.points[point_index];
            store.push_float(
                "tenant-a",
                labels(&[crate::test_support::TestLabelPair::new(
                    "__name__",
                    series.name,
                )]),
                point.timestamp_ms,
                point.value,
            );
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries = [
        // 20s to the range end is over 1.1x the 10s spacing, so it clamps to
        // half a spacing; the counter starts at zero, so the extrapolation
        // back to the range start is cut to the zero crossing instead.
        QueryExpectation {
            query: "increase(ends_short[1m])",
            want: 35.0,
        },
        // The mirror: 30s to the range start clamps, 10s to the end does not,
        // and a counter starting above zero leaves the start alone.
        QueryExpectation {
            query: "increase(starts_late[1m])",
            want: 35.0,
        },
        // Neither gap reaches the threshold, so both extrapolate in full.
        QueryExpectation {
            query: "increase(spans_range[1m])",
            want: 9.6,
        },
        // `rate` is that same extrapolation over the range in seconds.
        QueryExpectation {
            query: "rate(spans_range[1m])",
            want: 0.16,
        },
    ];
    for query_index in 0..queries.len() {
        let query = &queries[query_index];
        assert_lone_value_at(
            &engine,
            LoneValueQuery {
                query: query.query,
                time_ms: 100_000,
                want: query.want,
            },
        )
        .await;
    }
}
