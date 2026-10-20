use super::{BucketSpan, HistogramBucketJson};
use crate::engine::for_each_spanned_histogram_count;

pub(crate) fn append_spanned_buckets(
    buckets: &mut Vec<HistogramBucketJson>,
    spans: &[BucketSpan],
    counts: &[f64],
    mut bucket_for_index: impl FnMut(i32) -> HistogramBucketJson,
) {
    for_each_spanned_histogram_count(spans, counts, |index, count| {
        let mut bucket = bucket_for_index(index);
        bucket.count = count;
        if count != 0.0 || count.is_nan() {
            buckets.push(bucket);
        }
    });
}
