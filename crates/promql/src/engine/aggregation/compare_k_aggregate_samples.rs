use super::{TokenType, InstantSample, Ordering, float_sample_value, T_TOPK, labels_key};

/// Orders two samples for `topk`/`bottomk` selection.
///
/// The first key is the float value. `topk` uses `right.total_cmp(left)` so that
/// the highest value sorts first, and `bottomk` uses the reverse. The tie-break
/// is `labels_key`. A non-float sample, which the caller already filters out, or
/// a NaN sorts through `total_cmp`. This matches Prometheus.
pub(crate) fn compare_k_aggregate_samples(
    op: TokenType,
    left: &InstantSample,
    right: &InstantSample,
) -> Ordering {
    let left_value = float_sample_value(left).unwrap_or(f64::NAN);
    let right_value = float_sample_value(right).unwrap_or(f64::NAN);
    let by_value = if op.id() == T_TOPK {
        right_value.total_cmp(&left_value)
    } else {
        left_value.total_cmp(&right_value)
    };
    by_value.then_with(|| labels_key(&left.labels).cmp(&labels_key(&right.labels)))
}
