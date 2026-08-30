use super::*;

pub(crate) fn regression_slope_and_intercept(samples: &[(i64, f64)], range_end_ms: i64) -> Option<(f64, f64)> {
    if samples.len() < 2 {
        return None;
    }

    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut count = 0.0;
    for (timestamp, value) in samples {
        sum_x += (*timestamp - range_end_ms).to_f64()? / 1000.0;
        sum_y += value;
        count += 1.0;
    }
    let mean_x = sum_x / count;
    let mean_y = sum_y / count;

    let mut covariance = 0.0;
    let mut variance = 0.0;
    for (timestamp, value) in samples {
        let x = (*timestamp - range_end_ms).to_f64()? / 1000.0;
        let x_delta = x - mean_x;
        covariance += x_delta * (value - mean_y);
        variance += x_delta * x_delta;
    }
    if variance == 0.0 {
        return None;
    }

    let slope = covariance / variance;
    let intercept = mean_y - (slope * mean_x);
    Some((slope, intercept))
}
