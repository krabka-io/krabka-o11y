use std::cmp::Ordering;

use super::ToPrimitive;

/// Returns the `phi`-quantile of `values`, with linear interpolation between ranks.
///
/// This function is a direct port of the engine's `quantile_value`. It sorts a
/// local copy, so the UDF can take `&[f64]`. It returns `None` for an empty
/// slice.
pub(crate) fn quantile_value(phi: f64, values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    // Prometheus does NOT error on an out-of-range/NaN phi: NaN -> NaN, phi < 0
    // -> -Inf, phi > 1 -> +Inf (the engine raises an `InvalidQuantileWarning`
    // alongside). Mirror the engine's `quantile_value` leading guards so the UDF
    // and interpreter agree.
    if phi.is_nan() {
        return Some(f64::NAN);
    }
    if phi < 0.0 {
        return Some(f64::NEG_INFINITY);
    }
    if phi > 1.0 {
        return Some(f64::INFINITY);
    }
    let mut sorted = values.to_vec();
    // Prometheus sorts with `vectorByValueHeap.Less`, which calls a NaN LESS
    // than every other value, so a NaN sample is the smallest one in the window
    // and a quantile that lands between it and its neighbour interpolates to
    // NaN. `total_cmp` would instead sort a positive NaN last.
    sorted.sort_by(|left, right| {
        if left.is_nan() {
            return Ordering::Less;
        }
        if right.is_nan() {
            return Ordering::Greater;
        }
        left.total_cmp(right)
    });
    if sorted.len() == 1 {
        return Some(sorted[0]);
    }
    let rank = phi * (sorted.len() - 1).to_f64()?;
    let lower = rank.floor().to_usize()?;
    let upper = rank.ceil().to_usize()?;
    if lower == upper {
        return Some(sorted[lower]);
    }
    let weight = rank - lower.to_f64()?;
    Some(sorted[lower] * (1.0 - weight) + sorted[upper] * weight)
}
