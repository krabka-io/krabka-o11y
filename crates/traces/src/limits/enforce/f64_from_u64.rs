pub(crate) fn f64_from_u64(value: u64) -> f64 {
    value.to_string().parse().unwrap_or(f64::INFINITY)
}
