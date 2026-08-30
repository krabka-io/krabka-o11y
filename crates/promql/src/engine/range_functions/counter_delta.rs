pub(crate) fn counter_delta(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return Some(0.0);
    }
    let mut result = values.last()? - values.first()?;
    for window in values.windows(2) {
        if window[1] < window[0] {
            result += window[0];
        }
    }
    Some(result)
}
