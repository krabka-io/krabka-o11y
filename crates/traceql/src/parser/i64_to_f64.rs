use super::{Result, TraceqlError};

pub(crate) fn i64_to_f64(value: i64) -> Result<f64> {
    value
        .to_string()
        .parse()
        .map_err(|e: std::num::ParseFloatError| TraceqlError::Parse(e.to_string()))
}
