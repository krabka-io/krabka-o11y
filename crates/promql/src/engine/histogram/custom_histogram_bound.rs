pub(crate) fn custom_histogram_bound(index: i32, custom_values: &[f64]) -> f64 {
    match index {
        -1 if custom_values.first().is_some_and(|value| *value > 0.0) => 0.0,
        -1 => f64::NEG_INFINITY,
        _ => usize::try_from(index)
            .ok()
            .and_then(|index| custom_values.get(index).copied())
            .unwrap_or(f64::INFINITY),
    }
}
