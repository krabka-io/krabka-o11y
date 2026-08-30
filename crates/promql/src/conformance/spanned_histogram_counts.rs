use super::*;

pub(crate) fn spanned_histogram_counts(spans: &[BucketSpan], counts: &[f64]) -> BTreeMap<i32, f64> {
    let mut buckets = BTreeMap::new();
    let mut index = 0_i32;
    let mut count_index = 0_usize;
    for (span_index, span) in spans.iter().enumerate() {
        if span_index == 0 {
            index = span.offset;
        } else {
            index += span.offset;
        }
        for _ in 0..span.length {
            let Some(count) = counts.get(count_index).copied() else {
                return buckets;
            };
            buckets.insert(index, count);
            index += 1;
            count_index += 1;
        }
    }
    buckets
}
