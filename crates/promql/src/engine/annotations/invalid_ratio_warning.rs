use super::*;

/// Exact Prometheus `InvalidRatioWarning` text.
///
/// Rust's `f64` `Display` matches Go's `%g` for the integral and one-decimal
/// ratios this annotation reports: `1` for `1.0`, `1.1` for `1.1`, and `-1` for
/// `-1.0`. The rendered text is then byte-for-byte the corpus-asserted text.
#[cfg(feature = "experimental-functions")]
pub(crate) fn invalid_ratio_warning(got: f64, capped_to: f64) -> String {
    format!(
        "PromQL warning: ratio value should be between -1 and 1, got {got}, capping to {capped_to}"
    )
}
