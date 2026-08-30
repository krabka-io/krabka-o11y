use super::{PromqlError, Result};

pub(crate) fn parse_classic_bucket_bound(value: &str) -> Result<f64> {
    // Both infinity arms are permanent mutation survivors, and both are
    // redundant: Rust's own float parser accepts "+Inf", "-Inf" and "Inf", so
    // deleting either falls through to the same value. They stay because the
    // `le` spellings are Prometheus wire format, and leaning on the standard
    // library happening to accept them is not something to discover later.
    match value {
        "+Inf" | "Inf" => Ok(f64::INFINITY),
        "-Inf" => Ok(f64::NEG_INFINITY),
        _ => value.parse::<f64>().map_err(|error| {
            PromqlError::Plan(format!(
                "invalid classic histogram bucket `{value}`: {error}"
            ))
        }),
    }
}
