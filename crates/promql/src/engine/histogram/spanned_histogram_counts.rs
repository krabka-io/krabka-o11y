use super::{BTreeMap, BucketSpan};

pub(crate) fn spanned_histogram_counts(spans: &[BucketSpan], counts: &[f64]) -> BTreeMap<i32, f64> {
    let mut buckets = BTreeMap::new();
    for_each_spanned_histogram_count(spans, counts, |index, count| {
        buckets.insert(index, count);
    });
    buckets
}

pub(crate) fn for_each_spanned_histogram_count(
    spans: &[BucketSpan],
    counts: &[f64],
    mut visit: impl FnMut(i32, f64),
) {
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
                return;
            };
            visit(index, count);
            index += 1;
            count_index += 1;
        }
    }
}
