pub(crate) fn rounded_positive_rate(rate: f64) -> u64 {
    if !rate.is_finite() || rate <= 0.0 {
        return 0;
    }
    rate.round().to_string().parse().unwrap_or(u64::MAX)
}
