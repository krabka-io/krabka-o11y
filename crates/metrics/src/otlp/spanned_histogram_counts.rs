use super::{BucketSpan, BTreeMap};

pub(crate) fn spanned_histogram_counts(spans: &[BucketSpan], counts: &[f64]) -> BTreeMap<i32, f64> {
    let mut buckets = BTreeMap::new();
    let mut index = 0_i32;
    let mut count_index = 0_usize;
    // The first span's offset is absolute and every later one is a delta from
    // where the previous span ended. Starting the running index at zero makes
    // those the same operation.
    for span in spans {
        index += span.offset;
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
