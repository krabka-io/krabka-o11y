use super::*;

/// Formats a float exactly like Prometheus `jsonutil.MarshalFloat`.
///
/// `jsonutil.MarshalFloat` calls the Go function
/// `strconv.AppendFloat(f, fmt, -1, 64)`. Go picks the `'e'` scientific notation
/// when the magnitude is `< 1e-6` or `>= 1e21`, and the `'f'` plain decimal
/// notation otherwise. Precision `-1` means the shortest representation that
/// round-trips back to the same `f64`.
pub(crate) fn format_sample_value(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f == f64::INFINITY {
        return "+Inf".to_string();
    }
    if f == f64::NEG_INFINITY {
        return "-Inf".to_string();
    }

    let abs = f.abs();
    if abs != 0.0 && !(1e-6..1e21).contains(&abs) {
        format_float_exponent(f)
    } else {
        // Rust's `Display` for `f64` already emits the shortest round-tripping
        // plain-decimal form (no exponent), matching Go's `'f'` form: `3.0` ->
        // "3", `1e20` -> "100000000000000000000", `0.000001` -> "0.000001".
        format!("{f}")
    }
}
