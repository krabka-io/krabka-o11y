use super::*;
use crate::test_support::nh;

#[derive(bon::Builder)]
struct HistogramPoint {
    timestamp_ms: i64,
    count: f64,
}

struct HistogramSeriesCase {
    name: &'static str,
    points: Vec<HistogramPoint>,
}

struct QueryExpectation {
    query: &'static str,
    want: f64,
}

/// The histogram extrapolation is a third copy of the same geometry, and the
/// span test above only reads the shape of the result, never its numbers --
/// which is why the sample spacing, the 1.1x threshold and both half-interval
/// clamps were all free here. The three series below clamp at the end, at the
/// start, and at a gap sitting between 1.1x the spacing and the spacing plus
/// 1.1.
#[tokio::test]
pub(crate) async fn increase_extrapolates_native_histogram_counts_by_the_sample_spacing() {
    let mut store = InMemoryMetricStore::new();
    let series = [
        HistogramSeriesCase {
            name: "gh_ends_short",
            points: vec![
                HistogramPoint::builder()
                    .timestamp_ms(50_000)
                    .count(0.0)
                    .build(),
                HistogramPoint::builder()
                    .timestamp_ms(60_000)
                    .count(10.0)
                    .build(),
                HistogramPoint::builder()
                    .timestamp_ms(70_000)
                    .count(20.0)
                    .build(),
                HistogramPoint::builder()
                    .timestamp_ms(80_000)
                    .count(30.0)
                    .build(),
            ],
        },
        HistogramSeriesCase {
            name: "gh_starts_late",
            points: vec![
                HistogramPoint::builder()
                    .timestamp_ms(70_000)
                    .count(5.0)
                    .build(),
                HistogramPoint::builder()
                    .timestamp_ms(80_000)
                    .count(15.0)
                    .build(),
                HistogramPoint::builder()
                    .timestamp_ms(90_000)
                    .count(25.0)
                    .build(),
            ],
        },
        HistogramSeriesCase {
            name: "gh_thr_edge",
            points: vec![
                HistogramPoint::builder()
                    .timestamp_ms(68_950)
                    .count(0.0)
                    .build(),
                HistogramPoint::builder()
                    .timestamp_ms(78_950)
                    .count(10.0)
                    .build(),
                HistogramPoint::builder()
                    .timestamp_ms(88_950)
                    .count(20.0)
                    .build(),
            ],
        },
    ];
    for series_index in 0..series.len() {
        let series = &series[series_index];
        for point_index in 0..series.points.len() {
            let point = &series.points[point_index];
            store.push_histogram(
                "tenant-a",
                labels(&[crate::test_support::TestLabelPair::new(
                    "__name__",
                    series.name,
                )]),
                point.timestamp_ms,
                nh(
                    point.count,
                    point.count,
                    0,
                    &[crate::test_support::BucketCount {
                        index: 0,
                        count: point.count,
                    }],
                ),
            );
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries = [
        // 20s to the range end clamps to half a 10s spacing; 10s to the start
        // does not.
        QueryExpectation {
            query: "increase(gh_ends_short[1m])",
            want: 35.0,
        },
        // The mirror.
        QueryExpectation {
            query: "increase(gh_starts_late[1m])",
            want: 35.0,
        },
        // 11.05s to the end: over 1.1x the spacing, under the spacing plus 1.1.
        QueryExpectation {
            query: "increase(gh_thr_edge[1m])",
            want: 25.0,
        },
    ];
    for query_index in 0..queries.len() {
        let query = &queries[query_index];
        let result = engine
            .query_instant(&tenant_id("tenant-a"), query.query, 100_000)
            .await
            .unwrap_or_else(|error| panic!("{}: {error}", query.query));
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected a vector for {}", query.query);
        };
        assert2::assert!(samples.len() == 1, "{}", query.query);
        let SampleValue::Histogram(histogram) = &samples[0].value else {
            panic!("expected a histogram sample for {}", query.query);
        };
        assert2::assert!(approx_eq(histogram.count, query.want), "{}", query.query);
    }
}
