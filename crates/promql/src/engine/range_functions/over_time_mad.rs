use super::quantile_value;

pub(crate) fn over_time_mad(samples: &[(i64, f64)]) -> Option<f64> {
    let mut values = samples.iter().map(|(_, value)| *value).collect::<Vec<_>>();
    let median = quantile_value(0.5, &mut values)?;
    let mut deviations = samples
        .iter()
        .map(|(_, value)| (value - median).abs())
        .collect::<Vec<_>>();
    quantile_value(0.5, &mut deviations)
}
