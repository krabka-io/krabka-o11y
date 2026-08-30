use super::{PromqlError, Result};

pub(crate) fn duration_unit_seconds(unit: &str) -> Result<f64> {
    match unit {
        "ms" => Ok(0.001),
        "s" => Ok(1.0),
        "m" => Ok(60.0),
        "h" => Ok(60.0 * 60.0),
        "d" => Ok(60.0 * 60.0 * 24.0),
        "w" => Ok(60.0 * 60.0 * 24.0 * 7.0),
        "y" => Ok(60.0 * 60.0 * 24.0 * 365.0),
        _ => Err(PromqlError::Parse(format!(
            "invalid duration expression unit `{unit}`"
        ))),
    }
}
