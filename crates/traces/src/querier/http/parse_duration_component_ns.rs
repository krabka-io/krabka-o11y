
pub(crate) fn parse_duration_component_ns(number: &str, multiplier: u128) -> Result<u128, String> {
    let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
    if whole.is_empty() && fraction.is_empty() {
        return Err(format!("invalid number {number:?}"));
    }
    if fraction.contains('.') {
        return Err(format!("invalid number {number:?}"));
    }

    let whole = if whole.is_empty() {
        0
    } else {
        whole
            .parse::<u128>()
            .map_err(|_| format!("invalid number {number:?}"))?
    };
    let whole_ns = whole
        .checked_mul(multiplier)
        .ok_or_else(|| "duration out of range".to_string())?;
    if fraction.is_empty() {
        return Ok(whole_ns);
    }

    let fraction = fraction
        .parse::<u128>()
        .map_err(|_| format!("invalid number {number:?}"))?;
    let scale = (0..number.rsplit_once('.').map_or(0, |(_, frac)| frac.len()))
        .try_fold(1_u128, |acc, _| acc.checked_mul(10))
        .ok_or_else(|| "duration out of range".to_string())?;
    let fraction_ns = fraction
        .checked_mul(multiplier)
        .ok_or_else(|| "duration out of range".to_string())?
        / scale;
    whole_ns
        .checked_add(fraction_ns)
        .ok_or_else(|| "duration out of range".to_string())
}
