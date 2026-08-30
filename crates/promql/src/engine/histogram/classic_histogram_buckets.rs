use super::{ClassicBucket, NativeQuantileBucket};

pub(crate) fn classic_histogram_buckets(buckets: &[ClassicBucket]) -> Vec<NativeQuantileBucket> {
    let mut out = Vec::with_capacity(buckets.len());
    let mut lower = if buckets
        .first()
        .is_some_and(|bucket| bucket.upper_bound <= 0.0)
    {
        f64::NEG_INFINITY
    } else {
        0.0
    };
    let mut previous_count = 0.0;
    for bucket in buckets {
        let count = bucket.count - previous_count;
        previous_count = bucket.count;
        out.push(NativeQuantileBucket {
            lower,
            upper: bucket.upper_bound,
            count,
        });
        lower = bucket.upper_bound;
    }
    out
}
