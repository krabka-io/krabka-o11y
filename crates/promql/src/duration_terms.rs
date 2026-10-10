//! The compound-duration grammar shared by the ruler config and the conformance harness.

/// Sums the `<amount><unit>` terms of a Prometheus duration such as `1h30m`
/// into milliseconds.
///
/// The units are `ms`, `s`, `m`, `h`, `d`, `w` and `y`. The error is the
/// message the caller wraps into its own error type. The caller handles the
/// bare `0`, empty, and negative inputs before this.
pub(crate) fn duration_terms_ms(src: &str) -> Result<i64, String> {
    let mut total_ms = 0_i64;
    let mut index = 0;
    let bytes = src.as_bytes();

    while index < bytes.len() {
        let start = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
        if start == index {
            return Err(format!("invalid duration `{src}`"));
        }
        let amount = src[start..index]
            .parse::<i64>()
            .map_err(|err| format!("invalid duration amount `{src}`: {err}"))?;
        let unit_start = index;
        while index < bytes.len() && bytes[index].is_ascii_alphabetic() {
            index += 1;
        }
        let unit = &src[unit_start..index];
        let multiplier = match unit {
            "ms" => 1,
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            "w" => 604_800_000,
            "y" => 31_536_000_000,
            _ => return Err(format!("invalid duration unit `{unit}`")),
        };
        total_ms += amount
            .checked_mul(multiplier)
            .ok_or_else(|| format!("duration overflow `{src}`"))?;
    }

    Ok(total_ms)
}
