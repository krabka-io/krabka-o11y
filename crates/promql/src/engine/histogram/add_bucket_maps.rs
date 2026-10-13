use super::{BTreeMap, BucketSpan, compact_spanned_histogram_counts};

pub(crate) struct BucketMapSum {
    pub(crate) spans: Vec<BucketSpan>,
    pub(crate) counts: Vec<f64>,
}

pub(crate) fn add_bucket_maps(
    mut left: BTreeMap<i32, f64>,
    right: BTreeMap<i32, f64>,
) -> BucketMapSum {
    for entry in right {
        let index = entry.0;
        let count = entry.1;
        *left.entry(index).or_insert(0.0) += count;
    }
    let result = compact_spanned_histogram_counts(left);
    BucketMapSum {
        spans: result.spans,
        counts: result.counts,
    }
}
