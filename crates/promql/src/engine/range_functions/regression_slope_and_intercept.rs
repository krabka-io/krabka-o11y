use super::ToPrimitive;

pub(crate) struct RegressionFit {
    pub slope: f64,
    pub intercept: f64,
}

pub(crate) fn regression_slope_and_intercept(
    samples: &[super::FloatSample],
    range_end_ms: i64,
) -> Option<RegressionFit> {
    if samples.len() < 2 {
        return None;
    }

    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut count = 0.0;
    for sample in samples {
        sum_x += (sample.timestamp_ms - range_end_ms).to_f64()? / 1000.0;
        sum_y += sample.value;
        count += 1.0;
    }
    let mean_x = sum_x / count;
    let mean_y = sum_y / count;

    let mut covariance = 0.0;
    let mut variance = 0.0;
    for sample in samples {
        let x = (sample.timestamp_ms - range_end_ms).to_f64()? / 1000.0;
        let x_delta = x - mean_x;
        covariance += x_delta * (sample.value - mean_y);
        variance += x_delta * x_delta;
    }
    if variance == 0.0 {
        return None;
    }

    let slope = covariance / variance;
    let intercept = mean_y - (slope * mean_x);
    Some(RegressionFit { slope, intercept })
}
