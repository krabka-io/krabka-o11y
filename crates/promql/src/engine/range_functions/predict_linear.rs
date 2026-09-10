use super::{Time, TimeExt, regression_slope_and_intercept};

/// Predicts a gauge's value `duration` after `intercept_ms`.
///
/// Prometheus fits a simple linear regression in `f64` seconds, taking the
/// intercept at the query's evaluation time rather than at the end of the
/// regression window.
pub(crate) fn predict_linear(
    samples: &[(i64, f64)],
    intercept_ms: i64,
    duration: Time,
) -> Option<f64> {
    let (slope, intercept) = regression_slope_and_intercept(samples, intercept_ms)?;
    Some(intercept + (slope * duration.secs_f64()))
}
