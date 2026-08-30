/// Converts an `i64` to the nearest representable `f64`.
pub(crate) fn f64_from_i64(value: i64) -> f64 {
    value
        .to_string()
        .parse()
        .expect("every i64 has a finite f64 representation")
}
