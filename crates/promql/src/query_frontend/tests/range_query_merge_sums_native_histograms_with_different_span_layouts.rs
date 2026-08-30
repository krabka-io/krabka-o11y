use super::*;

#[test]
pub(crate) fn range_query_merge_sums_native_histograms_with_different_span_layouts() {
    let labels = labels(&[]);
    let result = merge_range_query_results(vec![
        QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels.clone(),
            samples: vec![(
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
            )],
        }]),
        QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels.clone(),
            samples: vec![(
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
            )],
        }]),
    ])
    .unwrap();

    let QueryResult::RangeMatrix(series) = result else {
        panic!("expected range matrix");
    };
    assert2::assert!(series.len() == 1);
    assert2::assert!(&series[0].labels == &labels);
    let SampleValue::Histogram(histogram) = &series[0].samples[0].1 else {
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
