use super::*;

pub(crate) fn append_native_spanned_buckets(
    buckets: &mut Vec<NativeQuantileBucket>,
    spans: &[BucketSpan],
    counts: &[f64],
    mut bucket_for_index: impl FnMut(i32) -> NativeQuantileBucket,
) {
    let mut index: i32 = 0;
    let mut count_index = 0;
    for (span_index, span) in spans.iter().enumerate() {
        // A malformed span whose offset overflows the running bucket index is
        // dropped (the rest of the spans with it) rather than overflow-panicking
        // on the `i32` accumulation.
        index = if span_index == 0 {
            span.offset
        } else {
            let Some(next) = index.checked_add(span.offset) else {
                return;
            };
            next
        };
        for _ in 0..span.length {
            let Some(count) = counts.get(count_index).copied() else {
                return;
            };
            let mut bucket = bucket_for_index(index);
            bucket.count = count;
            buckets.push(bucket);
            // A span that would walk the index past `i32::MAX` is similarly
            // dropped rather than wrapping.
            let Some(next) = index.checked_add(1) else {
                return;
            };
            index = next;
            count_index += 1;
        }
    }
}
