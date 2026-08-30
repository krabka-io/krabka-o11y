use super::*;

#[test]
pub(crate) fn range_query_merge_combines_time_split_samples_for_same_series() {
    let api_labels = labels(&[("__name__", "up"), ("job", "api")]);
    let worker_labels = labels(&[("__name__", "up"), ("job", "worker")]);
    let result = merge_range_query_results(vec![
        QueryResult::RangeMatrix(vec![
            RangeSeries {
                labels: api_labels.clone(),
                samples: vec![(60_000, SampleValue::Float(2.0))],
            },
            RangeSeries {
                labels: worker_labels.clone(),
                samples: vec![(0, SampleValue::Float(3.0))],
            },
        ]),
        QueryResult::RangeMatrix(vec![RangeSeries {
            labels: api_labels.clone(),
            samples: vec![
                (0, SampleValue::Float(1.0)),
                (120_000, SampleValue::Float(4.0)),
            ],
        }]),
    ])
    .unwrap();

    assert2::assert!(
        result
            == QueryResult::RangeMatrix(vec![
                RangeSeries {
                    labels: api_labels,
                    samples: vec![
                        (0, SampleValue::Float(1.0)),
                        (60_000, SampleValue::Float(2.0)),
                        (120_000, SampleValue::Float(4.0)),
                    ],
                },
                RangeSeries {
                    labels: worker_labels,
                    samples: vec![(0, SampleValue::Float(3.0))],
                },
            ])
    );
}
