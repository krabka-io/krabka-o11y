#[cfg(feature = "experimental-functions")]
pub(crate) fn validate_smoothing_factor(name: &str, value: f64) -> Result<()> {
    if value <= 0.0 || value >= 1.0 {
        return Err(PromqlError::Plan(format!(
            "invalid {name}. Expected: 0 < factor < 1, got: {value}"
        )));
    }
    Ok(())
}
