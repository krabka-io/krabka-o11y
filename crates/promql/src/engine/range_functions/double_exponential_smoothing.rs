#[cfg(feature = "experimental-functions")]
use super::FloatSample;

#[cfg(feature = "experimental-functions")]
pub(crate) fn double_exponential_smoothing(
    samples: &[FloatSample],
    smoothing_factor: f64,
    trend_factor: f64,
) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }

    let mut previous_smoothed = 0.0;
    let mut smoothed = samples[0].value;
    let mut trend = samples[1].value - samples[0].value;

    for index in 1..samples.len() {
        let sample = &samples[index];
        if index != 1 {
            trend =
                trend_factor.mul_add(smoothed - previous_smoothed, (1.0 - trend_factor) * trend);
        }
        let scaled_value = smoothing_factor * sample.value;
        let smoothed_with_trend = (1.0 - smoothing_factor) * (smoothed + trend);
        previous_smoothed = smoothed;
        smoothed = scaled_value + smoothed_with_trend;
    }

    Some(smoothed)
}
