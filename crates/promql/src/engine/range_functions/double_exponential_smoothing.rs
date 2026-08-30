#[cfg(feature = "experimental-functions")]
pub(crate) fn double_exponential_smoothing(
    samples: &[(i64, f64)],
    smoothing_factor: f64,
    trend_factor: f64,
) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }

    let mut previous_smoothed = 0.0;
    let mut smoothed = samples[0].1;
    let mut trend = samples[1].1 - samples[0].1;

    for (index, (_, value)) in samples.iter().enumerate().skip(1) {
        if index != 1 {
            trend =
                trend_factor.mul_add(smoothed - previous_smoothed, (1.0 - trend_factor) * trend);
        }
        let scaled_value = smoothing_factor * value;
        let smoothed_with_trend = (1.0 - smoothing_factor) * (smoothed + trend);
        previous_smoothed = smoothed;
        smoothed = scaled_value + smoothed_with_trend;
    }

    Some(smoothed)
}
