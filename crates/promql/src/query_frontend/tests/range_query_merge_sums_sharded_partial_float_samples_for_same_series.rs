use super::*;

#[test]
pub(crate) fn range_query_merge_sums_sharded_partial_float_samples_for_same_series() {
    let labels = labels(&[]);
    let result = merge_range_query_results(vec![
        QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels.clone(),
            samples: vec![
                (0, SampleValue::Float(1.0)),
                (60_000, SampleValue::Float(2.0)),
            ],
        }]),
        QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels.clone(),
            samples: vec![
                (0, SampleValue::Float(10.0)),
                (60_000, SampleValue::Float(20.0)),
            ],
        }]),
    ])
    .unwrap();

    assert2::assert!(
        result
            == QueryResult::RangeMatrix(vec![RangeSeries {
                labels,
                samples: vec![
                    (0, SampleValue::Float(11.0)),
                    (60_000, SampleValue::Float(22.0)),
                ],
            }])
    );
}
