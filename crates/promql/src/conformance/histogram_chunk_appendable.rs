use super::{NativeHistogram, ResetHint, histogram_buckets_shrank};

/// Whether Prometheus's TSDB would append `next` to the open chunk that `last`
/// currently ends, instead of cutting a fresh chunk for it.
///
/// The caller has already established that `last` and `next` agree on whether
/// the chunk is a gauge chunk; this decides the remaining cases that
/// `FloatHistogramAppender::appendable` and its gauge twin reject.
pub(crate) fn histogram_chunk_appendable(last: &NativeHistogram, next: &NativeHistogram) -> bool {
    if next.schema != last.schema
        || next.zero_threshold.to_bits() != last.zero_threshold.to_bits()
        || (next.is_nhcb() && next.custom_values != last.custom_values)
    {
        return false;
    }
    if next.reset_hint == ResetHint::Gauge {
        return true;
    }
    next.reset_hint != ResetHint::Yes
        && next.count >= last.count
        && next.zero_count >= last.zero_count
        && !histogram_buckets_shrank(
            &last.positive_spans,
            &last.positive_counts,
            &next.positive_spans,
            &next.positive_counts,
        )
        && !histogram_buckets_shrank(
            &last.negative_spans,
            &last.negative_counts,
            &next.negative_spans,
            &next.negative_counts,
        )
}
