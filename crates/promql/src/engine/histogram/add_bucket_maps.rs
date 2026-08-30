use super::{BTreeMap, BucketSpan, compact_spanned_histogram_counts};

pub(crate) fn add_bucket_maps(
    mut left: BTreeMap<i32, f64>,
    right: BTreeMap<i32, f64>,
) -> (Vec<BucketSpan>, Vec<f64>) {
    for (index, count) in right {
        *left.entry(index).or_insert(0.0) += count;
    }
    compact_spanned_histogram_counts(left)
}
