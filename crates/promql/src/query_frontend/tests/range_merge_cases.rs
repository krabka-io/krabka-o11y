use super::*;

mod range_query_merge_combines_time_split_samples_for_same_series {
use super::*;

#[test]
pub(crate) fn range_query_merge_combines_time_split_samples_for_same_series() {
    let api_labels = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    let worker_labels = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "worker"),
    ]);
    let result = merge_range_query_results(vec![
        QueryResult::RangeMatrix(vec![
            RangeSeries {
                drop_name: false,
                start_timestamps_ms: std::collections::BTreeMap::new(),
                labels: api_labels.clone().into(),
                samples: vec![RangeSeriesSample::new(60_000, SampleValue::Float(2.0))],
            },
            RangeSeries {
                drop_name: false,
                start_timestamps_ms: std::collections::BTreeMap::new(),
                labels: worker_labels.clone().into(),
                samples: vec![RangeSeriesSample::new(0, SampleValue::Float(3.0))],
            },
        ]),
        QueryResult::RangeMatrix(vec![RangeSeries {
            drop_name: false,
            start_timestamps_ms: std::collections::BTreeMap::new(),
            labels: api_labels.clone().into(),
            samples: vec![
                RangeSeriesSample::new(0, SampleValue::Float(1.0)),
                RangeSeriesSample::new(120_000, SampleValue::Float(4.0)),
            ],
        }]),
    ])
    .unwrap();

    assert2::assert!(
        result
            == QueryResult::RangeMatrix(vec![
                RangeSeries {
                    drop_name: false,
                    start_timestamps_ms: std::collections::BTreeMap::new(),
                    labels: api_labels.into(),
                    samples: vec![
                        RangeSeriesSample::new(0, SampleValue::Float(1.0)),
                        RangeSeriesSample::new(60_000, SampleValue::Float(2.0)),
                        RangeSeriesSample::new(120_000, SampleValue::Float(4.0)),
                    ],
                },
                RangeSeries {
                    drop_name: false,
                    start_timestamps_ms: std::collections::BTreeMap::new(),
                    labels: worker_labels.into(),
                    samples: vec![RangeSeriesSample::new(0, SampleValue::Float(3.0))],
                },
            ])
    );
}

}

mod range_query_merge_rejects_non_matrix_subquery_results {
use super::*;

#[test]
pub(crate) fn range_query_merge_rejects_non_matrix_subquery_results() {
    let err = merge_range_query_results(vec![QueryResult::Scalar {
        ts_ms: 0,
        value: 1.0,
    }])
    .unwrap_err();

    assert2::assert!(format!("{err}").contains("range matrix"));
}

}

mod range_query_merge_sums_native_histograms_with_different_span_layouts {
use super::*;

#[test]
pub(crate) fn range_query_merge_sums_native_histograms_with_different_span_layouts() {
    let labels = labels(&[]);
    let result = merge_range_query_results(vec![
        UnlabeledMatrix::default()
            .at(
                0,
                SampleValue::Histogram(native_histogram_with_positive_buckets(
                    3.0,
                    9.0,
                    vec![BucketSpan {
                        offset: 0,
                        length: 2,
                    }],
                    vec![1.0, 2.0],
                )),
            )
            .matrix(),
        UnlabeledMatrix::default()
            .at(
                0,
                SampleValue::Histogram(native_histogram_with_positive_buckets(
                    7.0,
                    21.0,
                    vec![BucketSpan {
                        offset: 1,
                        length: 2,
                    }],
                    vec![3.0, 4.0],
                )),
            )
            .matrix(),
    ])
    .unwrap();

    let QueryResult::RangeMatrix(series) = result else {
        panic!("expected range matrix");
    };
    assert2::assert!(series.len() == 1);
    assert2::assert!(&series[0].labels == &labels);
    let SampleValue::Histogram(histogram) = &series[0].samples[0].value else {
        panic!("expected histogram sample");
    };
    assert2::assert!((histogram.count - 10.0).abs() < f64::EPSILON);
    assert2::assert!((histogram.sum - 30.0).abs() < f64::EPSILON);
    assert2::assert!(
        &histogram.positive_spans
            == &vec![BucketSpan {
                offset: 0,
                length: 3,
            }]
    );
    assert2::assert!(&histogram.positive_counts == &vec![1.0, 5.0, 4.0]);
}

}

mod range_query_merge_sums_sharded_partial_float_samples_for_same_series {
use super::*;

#[test]
pub(crate) fn range_query_merge_sums_sharded_partial_float_samples_for_same_series() {
    let result = merge_range_query_results(vec![
        UnlabeledMatrix::default()
            .at(0, SampleValue::Float(1.0))
            .at(60_000, SampleValue::Float(2.0))
            .matrix(),
        UnlabeledMatrix::default()
            .at(0, SampleValue::Float(10.0))
            .at(60_000, SampleValue::Float(20.0))
            .matrix(),
    ])
    .unwrap();

    assert2::assert!(
        result
            == UnlabeledMatrix::default()
                .at(0, SampleValue::Float(11.0))
                .at(60_000, SampleValue::Float(22.0))
                .matrix()
    );
}

}


mod shard_query_injection_adds_mimir_selector_to_vector_and_matrix_selectors {
use super::*;
use super::*;

#[test]
pub(crate) fn shard_query_injection_adds_mimir_selector_to_vector_and_matrix_selectors() {
    let rewritten = query_with_shard_selector(
        r#"sum(rate(http_requests_total{job="api"}[5m])) + up"#,
        QueryShard { index: 1, total: 2 },
    )
    .unwrap();

    for needle in [
        r#"__query_shard__="1_of_2""#,
        r#"job="api""#,
        "http_requests_total",
        r#"up{__query_shard__="1_of_2"}"#,
    ] {
        assert2::assert!(rewritten.contains(needle));
    }

    let rewritten = query_with_shard_selector(
        "sum(histogram_count((h </ 2) >/ 1))",
        QueryShard { index: 1, total: 2 },
    )
    .unwrap();
    let reparsed = crate::parse_promql(&rewritten).unwrap();
    let ast = crate::serialize_promql_expr(&reparsed).unwrap();
    assert2::assert!(ast["expr"]["args"][0]["op"] == ">/");
    assert2::assert!(ast["expr"]["args"][0]["lhs"]["expr"]["op"] == "</");
    assert2::assert!(
        ast["expr"]["args"][0]["lhs"]["expr"]["lhs"]["matchers"]
            == serde_json::json!([{"name":"__query_shard__","type":"=","value":"1_of_2"}])
    );
}
}
