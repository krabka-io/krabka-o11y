use super::*;

/// Prometheus' instant estimator, shared by `irate`/`idelta`.
///
/// This function is a direct port of the engine's `instant_delta`. It uses only
/// the last two samples, and it clamps a negative `irate` delta to the last
/// value on a counter reset. It divides by the inter-sample interval for `irate`
/// only.
#[must_use]
pub fn instant_delta(timestamps: &[i64], values: &[f64], kind: InstantKind) -> Option<f64> {
    let n = timestamps.len();
    if n < 2 || values.len() != n {
        return None;
    }
    let previous = values[n - 2];
    let last = values[n - 1];
    let mut result = last - previous;
    if matches!(kind, InstantKind::Irate) && result < 0.0 {
        result = last;
    }

    if matches!(kind, InstantKind::Irate) {
        let interval = (timestamps[n - 1] - timestamps[n - 2]).to_f64()? / 1000.0;
        if interval <= 0.0 {
            return None;
        }
        result /= interval;
    }
    Some(result)
}
