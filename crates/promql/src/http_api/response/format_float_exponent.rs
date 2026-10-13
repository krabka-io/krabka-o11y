/// Renders `f` in the Go `'e'` form, for example `1e+21`, `9.999e-07`, `-1.5e-07`.
///
/// The Rust `{:e}` format produces the same shortest mantissa but a bare
/// exponent, such as `1e21` and `9.999e-7`. Go always writes a sign and at least
/// two exponent digits. This function therefore re-assembles the exponent
/// suffix.
pub(crate) fn format_float_exponent(f: f64) -> String {
    let rust = format!("{f:e}");
    let exponent_marker = rust
        .find('e')
        .expect("Rust {:e} formatting always contains an exponent marker");
    let mantissa = &rust[..exponent_marker];
    let exponent: i32 = rust[exponent_marker + 1..]
        .parse()
        .expect("Rust {:e} exponent is always a valid integer");
    let sign = if exponent < 0 { '-' } else { '+' };
    format!("{mantissa}e{sign}{:02}", exponent.abs())
}
