use super::*;

pub(crate) fn cumulative_to_bucket_counts(cumulative: &[f64]) -> Vec<f64> {
    let mut previous = 0.0;
    cumulative
        .iter()
        .map(|value| {
            let count = *value - previous;
            previous = *value;
            count
        })
        .collect()
}
