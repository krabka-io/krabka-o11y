use super::{BTreeMap, BucketSpan, spanned_histogram_counts, standard_histogram_bound};

/// The custom-bucket schema, which carries no exponential bucket bounds.
const CUSTOM_BUCKETS_SCHEMA: i8 = -53;

/// One side of a histogram's buckets, keyed by index, as reset detection reads
/// them.
///
/// This is `FloatHistogram.floatBucketIterator` in the shape `DetectReset` asks
/// it for: every bucket keyed by its index in `target_schema`, with the buckets
/// of a finer `schema` merged into the coarser parent they fall in, and with
/// every bucket whose upper bound sits at or below `absolute_start_value`
/// dropped, because that bucket lies inside the zero region the comparison is
/// being made outside of. Keying by index is the whole point: two histograms
/// line up bucket for bucket only once their span layouts are resolved to
/// indices, and a positional walk of the two count vectors compares whichever
/// buckets happen to share an offset in the vector.
pub(crate) fn detect_reset_bucket_counts(
    spans: &[BucketSpan],
    counts: &[f64],
    schema: i8,
    target_schema: i8,
    absolute_start_value: f64,
) -> BTreeMap<i32, f64> {
    let shift = u32::try_from(i16::from(schema) - i16::from(target_schema))
        .unwrap_or_default()
        .min(i32::BITS - 1);
    let mut out = BTreeMap::<i32, f64>::new();
    for (index, count) in spanned_histogram_counts(spans, counts) {
        // `targetIdx`: the bucket of the coarser schema that `index` falls in.
        let target_index = (index.saturating_sub(1) >> shift).saturating_add(1);
        *out.entry(target_index).or_insert(0.0) += count;
    }
    // A zero threshold of zero excludes nothing, and a custom-bucket schema has
    // no zero bucket to exclude against.
    if absolute_start_value > 0.0 && target_schema != CUSTOM_BUCKETS_SCHEMA {
        out.retain(|index, _| {
            standard_histogram_bound(*index, target_schema) > absolute_start_value
        });
    }
    out
}
