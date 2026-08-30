pub(crate) fn count_resets(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    if values.len() < 2 {
        return Some(0.0);
    }

    let resets = values
        .windows(2)
        .filter(|window| window[1] < window[0])
        .fold(0.0, |count, _| count + 1.0);
    Some(resets)
}
