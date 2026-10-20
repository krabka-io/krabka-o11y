use super::kahan_sum_inc;

#[derive(Default)]
struct CompensatedSum {
    sum: f64,
    compensation: f64,
}

/// Returns the population variance of `values`.
///
/// The fold uses Welford's online algorithm with Kahan-compensated
/// accumulation; the engine's range functions and the `*_over_time` UDFs
/// share it, and it matches Prometheus' `stdvar_over_time` and `stddev_over_time`. The naive
/// `E[x^2] - E[x]^2` form suffers catastrophic cancellation for large-magnitude
/// close-valued windows and gives a negative variance whose `sqrt` is NaN.
/// Welford stays stable.
pub(crate) fn over_time_variance(values: impl IntoIterator<Item = f64>) -> f64 {
    let mut count = 0.0_f64;
    let mut mean = CompensatedSum::default();
    let mut aux = CompensatedSum::default();
    for value in values {
        count += 1.0;
        let delta = value - (mean.sum + mean.compensation);
        kahan_sum_inc(delta / count, &mut mean.sum, &mut mean.compensation);
        kahan_sum_inc(
            delta * (value - (mean.sum + mean.compensation)),
            &mut aux.sum,
            &mut aux.compensation,
        );
    }
    (aux.sum + aux.compensation) / count
}
