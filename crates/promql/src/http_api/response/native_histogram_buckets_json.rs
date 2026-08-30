use super::*;

pub(crate) fn native_histogram_buckets_json(histogram: &NativeHistogram) -> Vec<Value> {
    let mut buckets = Vec::new();
    if histogram.is_nhcb() {
        append_custom_histogram_buckets(&mut buckets, histogram);
    } else {
        append_standard_histogram_buckets(&mut buckets, histogram);
    }
    buckets.sort_by(|left, right| left.lower.total_cmp(&right.lower));
    buckets
        .into_iter()
        .map(|bucket| {
            json!([
                bucket.boundary_rule,
                sample_string(bucket.lower),
                sample_string(bucket.upper),
                sample_string(bucket.count),
            ])
        })
        .collect()
}
