use super::*;

// Prometheus predicts gauges from a simple linear regression in f64 seconds.
pub(crate) fn predict_linear(samples: &[(i64, f64)], range_end_ms: i64, duration: Time) -> Option<f64> {
    let (slope, intercept) = regression_slope_and_intercept(samples, range_end_ms)?;
    Some(intercept + (slope * duration.secs_f64()))
}
