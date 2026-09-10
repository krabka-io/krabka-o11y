use super::kahan_sum_inc;

/// Returns the arithmetic mean of a non-empty `values` window.
///
/// Prometheus computes `avg_over_time` as a DIRECT mean: it Kahan-sums the
/// window and divides once at the end, which is more accurate than an
/// incremental mean for the windows that matter in practice. The direct sum can
/// overflow `f64` on windows the incremental form handles, so the fold watches
/// the running sum and switches to the incremental mean the first time that sum
/// saturates -- carrying the running mean and its compensation across.
///
/// An empty window has no mean; callers filter those out before they get here,
/// and this returns NaN if one still arrives.
pub(crate) fn over_time_mean(values: &[f64]) -> f64 {
    let Some((&first, rest)) = values.split_first() else {
        return f64::NAN;
    };
    let mut sum = first;
    let mut count = 1.0_f64;
    let mut mean = 0.0_f64;
    let mut comp = 0.0_f64;
    let mut incremental = false;
    for &value in rest {
        count += 1.0;
        if !incremental {
            let (new_sum, new_comp) = kahan_sum_inc(value, sum, comp);
            if !new_sum.is_infinite() {
                sum = new_sum;
                comp = new_comp;
                continue;
            }
            incremental = true;
            mean = sum / (count - 1.0);
            comp /= count - 1.0;
        }
        let weight = (count - 1.0) / count;
        (mean, comp) = kahan_sum_inc(value / count, weight * mean, weight * comp);
    }
    if incremental {
        return mean + comp;
    }
    sum / count + comp / count
}
