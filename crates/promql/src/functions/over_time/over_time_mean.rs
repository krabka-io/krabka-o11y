use super::*;

/// Returns the arithmetic mean of a non-empty `values` window.
///
/// The fold uses Prometheus' incremental Kahan-compensated mean
/// (`avg_over_time`), a port of the engine's `over_time_mean`. The naive
/// `sum / count` overflows to ±Inf for very-large-magnitude windows. The
/// incremental form stays finite and keeps the same-sign-infinity handling once
/// it does saturate.
pub(crate) fn over_time_mean(values: &[f64]) -> f64 {
    let mut count = 0.0_f64;
    let (mut mean, mut comp) = (0.0_f64, 0.0_f64);
    for &value in values {
        count += 1.0;
        if keep_infinite_mean(mean, value) {
            continue;
        }
        let (new_mean, new_comp) = kahan_sum_inc(value / count - mean / count, mean, comp);
        mean = new_mean;
        comp = new_comp;
    }
    mean + comp
}
