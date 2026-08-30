use super::*;

/// Returns the arithmetic mean of a non-empty float window.
///
/// The fold uses Prometheus' incremental Kahan-compensated mean
/// (`avg_over_time` in `promql/engine.go`). The naive `sum / count` overflows
/// to ±Inf for very-large-magnitude windows. The incremental form keeps the
/// running mean finite. Once it does saturate to ±Inf, it keeps Prometheus'
/// same-sign-infinity handling.
pub(crate) fn over_time_mean(values: impl Iterator<Item = f64>) -> f64 {
    let mut count = 0.0_f64;
    let (mut mean, mut comp) = (0.0_f64, 0.0_f64);
    for value in values {
        count += 1.0;
        if mean.is_infinite() {
            // Both `> 0.0` here are permanent survivors against `>= 0.0`:
            // each operand is already known infinite, so neither is ever 0.0
            // and the two spellings pick the same sign.
            if value.is_infinite() && (value > 0.0) == (mean > 0.0) {
                // Same-sign infinity: the mean stays that infinity.
                continue;
            }
            if !value.is_infinite() && !value.is_nan() {
                // A finite sample cannot pull an already-infinite mean back.
                continue;
            }
        }
        let (new_mean, new_comp) = kahan_sum_inc(value / count - mean / count, mean, comp);
        mean = new_mean;
        comp = new_comp;
    }
    mean + comp
}
