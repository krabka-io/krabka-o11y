use super::*;

pub(crate) fn parse_bucket_bound(value: &str, line: Line<'_>) -> Result<f64> {
    match value {
        "+Inf" | "Inf" => Ok(f64::INFINITY),
        "-Inf" => Ok(f64::NEG_INFINITY),
        _ => parse_float(value, line),
    }
}
