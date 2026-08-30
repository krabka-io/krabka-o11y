pub(crate) fn decimal_usize_to_f64(value: usize) -> f64 {
    value
        .to_string()
        .parse::<f64>()
        .expect("usize decimal representation parses as f64")
}
