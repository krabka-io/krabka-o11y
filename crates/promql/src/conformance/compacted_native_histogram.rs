use super::{NativeHistogram, compact_spanned_histogram_counts, spanned_histogram_counts};

/// Returns `histogram` with every empty bucket removed.
///
/// This is `FloatHistogram.Compact(0)`, which Prometheus applies to BOTH sides
/// before it compares two histograms. An empty bucket carries no observation,
/// so whether a fold left one behind is not a difference in the answer -- only
/// in how the answer was encoded.
pub(crate) fn compacted_native_histogram(histogram: &NativeHistogram) -> NativeHistogram {
    let mut out = histogram.clone();
    (out.positive_spans, out.positive_counts) = compact_spanned_histogram_counts(
        spanned_histogram_counts(&histogram.positive_spans, &histogram.positive_counts),
    );
    (out.negative_spans, out.negative_counts) = compact_spanned_histogram_counts(
        spanned_histogram_counts(&histogram.negative_spans, &histogram.negative_counts),
    );
    out
}
