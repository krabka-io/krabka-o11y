use super::*;
use crate::test_support::native_histogram_fixture;

pub(crate) fn native_histogram_rows() -> Vec<NativeHistogramRow> {
    vec![
        NativeHistogramRow {
            fingerprint: 7_u64,
            timestamp_ms: 99_i64,
            hist: NativeHistogram {
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
        },
        NativeHistogramRow {
            fingerprint: 8_u64,
            timestamp_ms: 109_i64,
            hist: NativeHistogram {
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
        },
        NativeHistogramRow {
            fingerprint: 9_u64,
            timestamp_ms: 119_i64,
            hist: native_histogram_fixture(
                1,
                false,
                ResetHint::Unknown,
                Some(Vec::new()),
                Some(456),
            ),
        },
    ]
}

/// The columns of an encoded native-histogram `batch`, as one struct array.
pub(crate) fn native_histogram_struct_array(batch: &RecordBatch) -> ArrayRef {
    Arc::new(StructArray::from(
        batch
            .schema()
            .fields()
            .iter()
            .cloned()
            .zip(batch.columns().iter().cloned())
            .collect::<Vec<_>>(),
    ))
}
