use super::detected_bytes_unit;

pub(crate) fn is_bytes_literal(value: &str) -> bool {
    let unit_start = value
        .find(|ch: char| ch.is_ascii_alphabetic())
        .unwrap_or(value.len());
    // No early return for a value with no letters: the unit is then the
    // empty string, which `detected_bytes_unit` already refuses.
    let Ok(amount) = value[..unit_start].parse::<f64>() else {
        return false;
    };
    amount.is_finite() && amount >= 0.0 && detected_bytes_unit(&value[unit_start..]).is_some()
}
