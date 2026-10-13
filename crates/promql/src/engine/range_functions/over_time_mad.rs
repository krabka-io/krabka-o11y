use super::quantile_value;

pub(crate) fn over_time_mad(samples: &[super::FloatSample]) -> Option<f64> {
    if samples.iter().any(|sample| sample.value.is_nan()) {
        return Some(f64::NAN);
    }
    let mut values = samples
        .iter()
        .map(|sample| sample.value)
        .collect::<Vec<_>>();
    let median = quantile_value(0.5, &mut values)?;
    let mut deviations = samples
        .iter()
        .map(|sample| (sample.value - median).abs())
        .collect::<Vec<_>>();
    quantile_value(0.5, &mut deviations)
}
