use super::*;

pub(crate) fn append_spanned_buckets(
    buckets: &mut Vec<HistogramBucketJson>,
    spans: &[BucketSpan],
    counts: &[f64],
    mut bucket_for_index: impl FnMut(i32) -> HistogramBucketJson,
) {
    let mut index = 0;
    let mut count_index = 0;
    for (span_index, span) in spans.iter().enumerate() {
        if span_index == 0 {
            index = span.offset;
        } else {
            index += span.offset;
        }
        for _ in 0..span.length {
            let Some(count) = counts.get(count_index).copied() else {
                return;
            };
            let mut bucket = bucket_for_index(index);
            bucket.count = count;
            buckets.push(bucket);
            index += 1;
            count_index += 1;
        }
    }
}
