/// Renders `f` in the Go `'e'` form, for example `1e+21`, `9.999e-07`, `-1.5e-07`.
///
/// The Rust `{:e}` format produces the same shortest mantissa but a bare
/// exponent, such as `1e21` and `9.999e-7`. Go always writes a sign and at least
/// two exponent digits. This function therefore re-assembles the exponent
/// suffix.
pub(crate) fn format_float_exponent(f: f64) -> String {
    let rust = format!("{f:e}");
    let (mantissa, exponent) = rust
        .split_once('e')
        .expect("Rust {:e} formatting always contains an exponent marker");
    let exponent: i32 = exponent
        .parse()
        .expect("Rust {:e} exponent is always a valid integer");
    let sign = if exponent < 0 { '-' } else { '+' };
    format!("{mantissa}e{sign}{:02}", exponent.abs())
}
