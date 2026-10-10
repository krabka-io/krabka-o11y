use super::{
    ClassicBucket, FractionBounds, HistogramReducers, InstantSample, NativeHistogram, Result,
    apply_histogram_reduction, classic_histogram_fraction, native_histogram_fraction,
};

/// Applies `histogram_fraction(lower, upper, v)` to an instant vector `v`,
/// with `bounds` holding `lower` and `upper`.
///
/// This function mirrors `PromqlEngine::eval_histogram_fraction_call` exactly.
/// Native-histogram rows fold through [`native_histogram_fraction`] and keep the
/// source timestamp. This function groups classic `<metric>_bucket{le}` float
/// rows by labelset and drops `__name__` and `le` from the group. Each group
/// then folds through [`classic_histogram_fraction`] and carries `time_ms`.
///
/// This function drops a labelset that carries both a classic and a native
/// histogram from the output. It raises the
/// `MixedClassicNativeHistogramsWarning` through the in-scope annotation sink,
/// exactly as the interpreter does. The interpreter and the operator path share
/// this function, so the two are parity-exact.
///
/// # Errors
///
/// Returns [`PromqlError`] for an unparseable `le` bound. Returns
/// [`PromqlError`] for a non-float classic bucket count. These are exactly the
/// errors the interpreter raised inline.
pub(crate) fn apply_histogram_fraction(
    bounds: FractionBounds,
    samples: Vec<InstantSample>,
    time_ms: i64,
) -> Result<Vec<InstantSample>> {
    let FractionBounds { lower, upper } = bounds;
    apply_histogram_reduction(
        samples,
        time_ms,
        HistogramReducers {
            native: |histogram: &NativeHistogram, metric: &str| {
                native_histogram_fraction(lower, upper, histogram, metric)
            },
            classic: |_: &str, buckets: &mut Vec<ClassicBucket>| {
                classic_histogram_fraction(lower, upper, buckets)
            },
        },
    )
}
