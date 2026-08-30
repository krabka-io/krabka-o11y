use super::*;

pub(crate) fn remap_custom_counts(
    spans: &[BucketSpan],
    counts: &[f64],
    source_values: &[f64],
    target_values: &[f64],
) -> BTreeMap<i32, f64> {
    let mut out = BTreeMap::new();
    for (index, count) in spanned_histogram_counts(spans, counts) {
        let upper = custom_histogram_bound(index, source_values);
        let target_index = target_values
            .iter()
            .position(|bound| bound.total_cmp(&upper).is_ge())
            .unwrap_or(target_values.len());
        let target_index = i32::try_from(target_index).unwrap_or(i32::MAX);
        *out.entry(target_index).or_insert(0.0) += count;
    }
    out
}
