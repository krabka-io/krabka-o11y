use super::kahan_sum_inc;

/// Returns the population variance of `values`.
///
/// The fold uses Welford's online algorithm with Kahan-compensated
/// accumulation, a port of the engine's `over_time_variance`, which matches
/// Prometheus' `stdvar_over_time` and `stddev_over_time`. The naive
/// `E[x^2] - E[x]^2` form suffers catastrophic cancellation for large-magnitude
/// close-valued windows and gives a negative variance whose `sqrt` is NaN.
/// Welford stays stable.
pub(crate) fn over_time_variance(values: &[f64]) -> f64 {
    let mut count = 0.0_f64;
    let (mut mean, mut mean_comp) = (0.0_f64, 0.0_f64);
    let (mut aux, mut aux_comp) = (0.0_f64, 0.0_f64);
    for value in values {
        count += 1.0;
        let delta = value - (mean + mean_comp);
        let (new_mean, new_mean_comp) = kahan_sum_inc(delta / count, mean, mean_comp);
        mean = new_mean;
        mean_comp = new_mean_comp;
        let (new_aux, new_aux_comp) =
            kahan_sum_inc(delta * (value - (mean + mean_comp)), aux, aux_comp);
        aux = new_aux;
        aux_comp = new_aux_comp;
    }
    (aux + aux_comp) / count
}
