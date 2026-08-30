use super::*;

pub(crate) fn parse_duration_component_nanos(number: &str, multiplier: i128, original: &str) -> Result<i128> {
    let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
    if whole.is_empty() && fraction.is_empty() {
        return Err(TraceqlError::Parse(format!(
            "invalid duration number {number:?}"
        )));
    }
    if fraction.contains('.') {
        return Err(TraceqlError::Parse(format!(
            "invalid duration number {number:?}"
        )));
    }

    let whole = if whole.is_empty() {
        0
    } else {
        whole
            .parse::<i128>()
            .map_err(|e| TraceqlError::Parse(e.to_string()))?
    };
    let whole_ns = whole
        .checked_mul(multiplier)
        .ok_or_else(|| TraceqlError::Parse(format!("duration out of range: {original:?}")))?;
    if fraction.is_empty() {
        return Ok(whole_ns);
    }

    let fraction_digits = fraction
        .parse::<i128>()
        .map_err(|e| TraceqlError::Parse(e.to_string()))?;
    let scale = 10_i128
        .checked_pow(u32::try_from(fraction.len()).map_err(|e| TraceqlError::Parse(e.to_string()))?)
        .ok_or_else(|| {
            TraceqlError::Parse(format!("duration precision too large: {original:?}"))
        })?;
    let fraction_ns = fraction_digits
        .checked_mul(multiplier)
        .ok_or_else(|| TraceqlError::Parse(format!("duration out of range: {original:?}")))?
        / scale;

    whole_ns
        .checked_add(fraction_ns)
        .ok_or_else(|| TraceqlError::Parse(format!("duration out of range: {original:?}")))
}
