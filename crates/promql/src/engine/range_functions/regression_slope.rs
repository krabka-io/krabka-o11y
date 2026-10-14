use super::regression_slope_and_intercept;

pub(crate) fn regression_slope(samples: &[super::FloatSample], range_end_ms: i64) -> Option<f64> {
    regression_slope_and_intercept(samples, range_end_ms).map(|fit| fit.slope)
}
