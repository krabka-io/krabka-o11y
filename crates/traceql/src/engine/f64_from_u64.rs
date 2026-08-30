use super::{Result, TraceqlError};

pub(crate) fn f64_from_u64(value: u64) -> Result<f64> {
    value
        .to_string()
        .parse()
        .map_err(|e: std::num::ParseFloatError| TraceqlError::Exec(e.to_string()))
}
