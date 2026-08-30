use super::*;

pub(crate) fn native_histogram_rows() -> Vec<(u64, i64, NativeHistogram)> {
    vec![
        (
            7_u64,
            99_i64,
            NativeHistogram {
                schema: 2,
                is_float: false,
                reset_hint: ResetHint::No,
                zero_threshold: 1e-128,
                zero_count: 3.0,
                count: 10.0,
                sum: 42.5,
                positive_spans: vec![BucketSpan {
                    offset: 0,
                    length: 2,
                }],
                positive_counts: vec![4.0, 6.0],
                negative_spans: Vec::new(),
                negative_counts: Vec::new(),
                custom_values: None,
                start_timestamp_ms: None,
            },
        ),
        (
            8_u64,
            109_i64,
            NativeHistogram {
                schema: -53,
                is_float: true,
                reset_hint: ResetHint::Gauge,
                zero_threshold: 0.25,
                zero_count: 0.5,
                count: 4.0,
                sum: 7.5,
                positive_spans: vec![BucketSpan {
                    offset: 2,
                    length: 2,
                }],
                positive_counts: vec![1.25, 2.0],
                negative_spans: vec![BucketSpan {
                    offset: -1,
                    length: 1,
                }],
                negative_counts: vec![0.75],
                custom_values: Some(vec![0.5, 1.0, 2.0]),
                start_timestamp_ms: Some(123),
            },
        ),
        (
            9_u64,
            119_i64,
            NativeHistogram {
                schema: 1,
                is_float: false,
                reset_hint: ResetHint::Unknown,
                zero_threshold: 0.0,
                zero_count: 0.0,
                count: 0.0,
                sum: 0.0,
                positive_spans: Vec::new(),
                positive_counts: Vec::new(),
                negative_spans: Vec::new(),
                negative_counts: Vec::new(),
                custom_values: Some(Vec::new()),
                start_timestamp_ms: Some(456),
            },
        ),
    ]
}
