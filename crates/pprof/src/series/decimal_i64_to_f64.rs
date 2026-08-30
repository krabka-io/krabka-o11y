pub(crate) fn decimal_i64_to_f64(value: i64) -> f64 {
    value
        .to_string()
        .parse::<f64>()
        .expect("i64 decimal representation parses as f64")
}
