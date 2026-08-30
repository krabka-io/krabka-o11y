use super::*;

pub(crate) fn reduced_counts_outside_zero(
    histogram: &NativeHistogram,
    spans: &[BucketSpan],
    counts: &[f64],
    zero_threshold: f64,
    target_schema: i8,
) -> BTreeMap<i32, f64> {
    let schema_delta =
        u32::try_from(i16::from(histogram.schema) - i16::from(target_schema)).unwrap_or_default();
    let divisor = 1_i32.checked_shl(schema_delta).unwrap_or(i32::MAX);
    let mut out = BTreeMap::new();
    for (index, count) in spanned_histogram_counts(spans, counts) {
        if standard_histogram_bound(index - 1, histogram.schema) < zero_threshold {
            continue;
        }
        let quotient = index.div_euclid(divisor);
        let target_index = quotient + i32::from(index.rem_euclid(divisor) != 0);
        *out.entry(target_index).or_insert(0.0) += count;
    }
    out
}
