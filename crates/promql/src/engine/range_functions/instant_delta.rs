use super::{IrateFn, ToPrimitive};

// Prometheus computes instant rate deltas in f64 seconds; timestamp deltas
// intentionally enter that float domain here.
pub(crate) fn instant_delta(timestamps: &[i64], values: &[f64], kind: IrateFn) -> Option<f64> {
    let n = timestamps.len();
    if n < 2 || values.len() != n {
        return None;
    }
    let previous = values[n - 2];
    let last = values[n - 1];
    let mut result = last - previous;
    if matches!(kind, IrateFn::Irate) && result < 0.0 {
        result = last;
    }

    if matches!(kind, IrateFn::Irate) {
        let interval = (timestamps[n - 1] - timestamps[n - 2]).to_f64()? / 1000.0;
        if interval <= 0.0 {
            return None;
        }
        result /= interval;
    }
    Some(result)
}
