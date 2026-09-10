use std::cmp::Ordering;

use super::ToPrimitive;

pub(crate) fn quantile_value(quantile: f64, values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    // Prometheus' `quantile()` does NOT error on an out-of-range/NaN phi: a NaN
    // phi yields NaN, phi < 0 yields -Inf, and phi > 1 yields +Inf (the caller
    // raises an `InvalidQuantileWarning` alongside). This mirrors the
    // `histogram_quantile` family's leading guards.
    if quantile.is_nan() {
        return Some(f64::NAN);
    }
    if quantile < 0.0 {
        return Some(f64::NEG_INFINITY);
    }
    if quantile > 1.0 {
        return Some(f64::INFINITY);
    }
    // Prometheus sorts with `vectorByValueHeap.Less`, which calls a NaN LESS
    // than every other value, so a NaN sample is the smallest one in the window
    // and a quantile that lands between it and its neighbour interpolates to
    // NaN. `total_cmp` would instead sort a positive NaN last.
    values.sort_by(|left, right| {
        if left.is_nan() {
            return Ordering::Less;
        }
        if right.is_nan() {
            return Ordering::Greater;
        }
        left.total_cmp(right)
    });
    if values.len() == 1 {
        return Some(values[0]);
    }

    let rank = quantile * (values.len() - 1).to_f64().unwrap_or(f64::MAX);
    let lower = rank.floor().to_usize()?;
    let upper = rank.ceil().to_usize()?;
    if lower == upper {
        return Some(values[lower]);
    }
    let weight = rank - lower.to_f64().unwrap_or(f64::MAX);
    Some(values[lower] * (1.0 - weight) + values[upper] * weight)
}
