use super::{InstantSample, Ordering, T_TOPK, TokenType, float_sample_value, labels_key};

/// Orders two samples for `topk`/`bottomk` selection.
///
/// The first key is the float value. `topk` puts the highest value first, and
/// `bottomk` puts the lowest value first. The tie-break is `labels_key`.
///
/// A NaN sorts last under both operators, which is what Prometheus does: a NaN
/// is never a top value and never a bottom value, so `topk(3, ...)` and
/// `bottomk(3, ...)` over the same input both rank it below every number.
/// `total_cmp` alone would instead rank a positive NaN above every number and
/// make `topk` return it first. A non-float sample, which the caller already
/// filters out, reads as a NaN and so also sorts last.
pub(crate) fn compare_k_aggregate_samples(
    op: TokenType,
    left: &InstantSample,
    right: &InstantSample,
) -> Ordering {
    let left_value = float_sample_value(left).unwrap_or(f64::NAN);
    let right_value = float_sample_value(right).unwrap_or(f64::NAN);
    let by_value = match (left_value.is_nan(), right_value.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) if op.id() == T_TOPK => right_value.total_cmp(&left_value),
        (false, false) => left_value.total_cmp(&right_value),
    };
    by_value.then_with(|| labels_key(&left.labels).cmp(&labels_key(&right.labels)))
}
