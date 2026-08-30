
pub(crate) fn count_changes(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    if values.len() < 2 {
        return Some(0.0);
    }

    let changes = values
        .windows(2)
        .filter(|window| window[0].to_bits() != window[1].to_bits())
        .fold(0.0, |count, _| count + 1.0);
    Some(changes)
}
