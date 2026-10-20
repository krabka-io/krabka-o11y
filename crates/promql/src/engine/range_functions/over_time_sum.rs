use super::kahan_sum_inc;

/// Returns the Kahan-compensated sum of a float window.
///
/// Prometheus' `sum_over_time` compensates the running sum and adds the
/// compensation back at the end, so a window whose terms differ by many orders
/// of magnitude does not lose the small ones. A sum that saturates to an
/// infinity drops the compensation, which would otherwise be a NaN.
pub(crate) fn over_time_sum(values: impl Iterator<Item = f64>) -> f64 {
    let mut total = CompensatedSum {
        sum: 0.0,
        compensation: 0.0,
    };
    for value in values {
        kahan_sum_inc(value, &mut total.sum, &mut total.compensation);
    }
    if total.sum.is_infinite() {
        return total.sum;
    }
    total.sum + total.compensation
}

struct CompensatedSum {
    sum: f64,
    compensation: f64,
}
