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
