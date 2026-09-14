use std::fmt::Write as _;

use super::{
    BOUNDARY_CLOSED_BOTH, BOUNDARY_OPEN_LEFT, BOUNDARY_OPEN_RIGHT, HistogramBucketJson,
    NativeHistogram, append_custom_histogram_buckets, append_standard_histogram_buckets,
    sample_string,
};

/// Renders Prometheus's `FloatHistogram.String()` representation.
pub(crate) fn native_histogram_string(histogram: &NativeHistogram) -> String {
    let mut buckets = Vec::new();
    if histogram.is_nhcb() {
        append_custom_histogram_buckets(&mut buckets, histogram);
    } else {
        append_standard_histogram_buckets(&mut buckets, histogram);
    }
    buckets.sort_by(|left, right| left.lower.total_cmp(&right.lower));

    let mut output = format!(
        "{{count:{}, sum:{}",
        sample_string(histogram.count),
        sample_string(histogram.sum)
    );
    for bucket in buckets {
        append_bucket(&mut output, &bucket);
    }
    output.push('}');
    output
}

fn append_bucket(output: &mut String, bucket: &HistogramBucketJson) {
    let (left, right) = match bucket.boundary_rule {
        BOUNDARY_OPEN_LEFT => ('(', ']'),
        BOUNDARY_OPEN_RIGHT => ('[', ')'),
        BOUNDARY_CLOSED_BOTH => ('[', ']'),
        _ => ('(', ')'),
    };
    write!(
        output,
        ", {left}{},{}{right}:{}",
        sample_string(bucket.lower),
        sample_string(bucket.upper),
        sample_string(bucket.count)
    )
    .expect("writing to a String cannot fail");
}

#[cfg(test)]
mod tests {
    use krabka_metrics::{BucketSpan, ResetHint};

    use super::*;

    #[test]
    fn renders_float_histogram_bucket_notation() {
        let histogram = NativeHistogram {
            schema: 0,
            is_float: true,
            reset_hint: ResetHint::Unknown,
            zero_threshold: 0.001,
            zero_count: 2.0,
            count: 20.0,
            sum: 10.0,
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 2,
            }],
            positive_counts: vec![1.0, 2.0],
            negative_spans: vec![BucketSpan {
                offset: 0,
                length: 2,
            }],
            negative_counts: vec![1.0, 2.0],
            custom_values: None,
            start_timestamp_ms: None,
        };

        assert2::assert!(
            native_histogram_string(&histogram)
                == "{count:20, sum:10, [-2,-1):2, [-1,-0.5):1, [-0.001,0.001]:2, (0.5,1]:1, (1,2]:2}"
        );
    }
}
