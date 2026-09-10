use super::kahan_sum_inc;

/// Returns the Kahan-compensated sum of a float window.
///
/// Prometheus' `sum_over_time` compensates the running sum and adds the
/// compensation back at the end, so a window whose terms differ by many orders
/// of magnitude does not lose the small ones. A sum that saturates to an
/// infinity drops the compensation, which would otherwise be a NaN.
pub(crate) fn over_time_sum(values: impl Iterator<Item = f64>) -> f64 {
    let (mut sum, mut comp) = (0.0_f64, 0.0_f64);
    for value in values {
        (sum, comp) = kahan_sum_inc(value, sum, comp);
    }
    if sum.is_infinite() {
        return sum;
    }
    sum + comp
}
