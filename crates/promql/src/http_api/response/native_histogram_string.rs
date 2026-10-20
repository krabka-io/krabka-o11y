use std::fmt::Write as _;

use super::{
    BOUNDARY_CLOSED_BOTH, BOUNDARY_OPEN_LEFT, BOUNDARY_OPEN_RIGHT, HistogramBucketJson,
    NativeHistogram, native_histogram_buckets, sample_string,
};

struct BucketDelimiters {
    left: char,
    right: char,
}

/// Renders Prometheus's `FloatHistogram.String()` representation.
pub(crate) fn native_histogram_string(histogram: &NativeHistogram) -> String {
    let buckets = native_histogram_buckets(histogram);

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
    let delimiters = match bucket.boundary_rule {
        BOUNDARY_OPEN_LEFT => BucketDelimiters {
            left: '(',
            right: ']',
        },
        BOUNDARY_OPEN_RIGHT => BucketDelimiters {
            left: '[',
            right: ')',
        },
        BOUNDARY_CLOSED_BOTH => BucketDelimiters {
            left: '[',
            right: ']',
        },
        _ => BucketDelimiters {
            left: '(',
            right: ')',
        },
    };
    write!(
        output,
        ", {}{},{}{}:{}",
        delimiters.left,
        sample_string(bucket.lower),
        sample_string(bucket.upper),
        delimiters.right,
        sample_string(bucket.count)
    )
    .expect("writing to a String cannot fail");
}

#[cfg(test)]
mod tests {
    use krabka_metrics::ResetHint;

    use super::*;

    #[test]
    fn renders_float_histogram_bucket_notation() {
        let histogram =
            crate::test_support::two_sided_histogram(20.0, 10.0, ResetHint::Unknown, None);

        assert2::assert!(
            native_histogram_string(&histogram)
                == "{count:20, sum:10, [-2,-1):2, [-1,-0.5):1, [-0.001,0.001]:2, (0.5,1]:1, (1,2]:2}"
        );
    }
}
