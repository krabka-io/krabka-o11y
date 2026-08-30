use super::ToPrimitive;

pub(crate) fn remote_read_histogram_deltas(is_float: bool, counts: &[f64]) -> Vec<i64> {
    if is_float {
        return Vec::new();
    }
    let mut previous = 0.0;
    counts
        .iter()
        .map(|count| {
            let delta = *count - previous;
            previous = *count;
            delta.to_i64().unwrap_or_else(|| {
                if delta.is_sign_negative() {
                    i64::MIN
                } else {
                    i64::MAX
                }
            })
        })
        .collect()
}
