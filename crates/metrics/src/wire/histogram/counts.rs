use super::ToPrimitive;

pub(crate) fn counts(float_counts: &[f64], deltas: &[i64]) -> Vec<f64> {
    if !float_counts.is_empty() {
        return float_counts.to_vec();
    }

    let mut total = 0_i64;
    deltas
        .iter()
        .map(|delta| {
            total += delta;
            total.to_f64().unwrap_or(f64::MAX)
        })
        .collect()
}
